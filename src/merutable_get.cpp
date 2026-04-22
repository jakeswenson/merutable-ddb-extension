#include "merutable_get.hpp"
#include "handle_cache.hpp"
#include "meru_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

// ── Helpers ───────────────────────────────────────────────────────────────────

static LogicalType ColTypeToLogical(MeruColumnType t) {
	switch (t) {
	case MeruColumnType_Int64:         return LogicalType::BIGINT;
	case MeruColumnType_Int32:         return LogicalType::INTEGER;
	case MeruColumnType_Boolean:       return LogicalType::BOOLEAN;
	case MeruColumnType_Float:         return LogicalType::FLOAT;
	case MeruColumnType_Double:        return LogicalType::DOUBLE;
	case MeruColumnType_FixedLenBytes: return LogicalType::BLOB;
	default:                           return LogicalType::VARCHAR;
	}
}

// ── Bind data ─────────────────────────────────────────────────────────────────
// Stores schema info needed by scan to build MeruValue pk array and map result
// columns. Column types come directly from MeruColumnType (no string parsing).

struct MerutableGetBindData : public TableFunctionData {
	std::string db_path;
	// Per-column info mirrored from MeruManifestInfo (manifest freed after bind)
	std::vector<MeruColumnType> col_types;  // all columns, in schema order
	std::vector<uintptr_t> pk_indices;      // primary key column indices
	std::vector<int> user_col_indices;      // schema indices of user-visible columns
	std::vector<Value> pk_inputs;           // PK values from DuckDB inputs[1..]
};

// ── Init / State ──────────────────────────────────────────────────────────────

struct MerutableGetState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> MerutableGetInit(ClientContext &,
                                                              TableFunctionInitInput &) {
	return make_uniq<MerutableGetState>();
}

// ── Bind ──────────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> MerutableGetBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 2) {
		throw BinderException("merutable_get requires (path, pk_value) arguments");
	}

	std::string db_path = input.inputs[0].GetValue<string>();

	MeruManifestInfo *raw_info = nullptr;
	meru_checked([&](char **e) {
		return meru_manifest_info(GetHandleCache().Runtime(), db_path.c_str(), &raw_info, e);
	});
	// RAII: MeruManifestInfo is freed when this guard goes out of scope
	struct InfoGuard {
		MeruManifestInfo *p;
		~InfoGuard() { if (p) meru_manifest_info_free(p); }
	} guard{raw_info};

	auto bind_data = make_uniq<MerutableGetBindData>();
	bind_data->db_path = db_path;

	for (uintptr_t i = 0; i < raw_info->column_count; i++) {
		bind_data->col_types.push_back(raw_info->columns[i].col_type);
		bind_data->user_col_indices.push_back((int)i);
		names.push_back(raw_info->columns[i].name);
		return_types.push_back(ColTypeToLogical(raw_info->columns[i].col_type));
	}

	for (uintptr_t i = 0; i < raw_info->pk_count; i++) {
		bind_data->pk_indices.push_back(raw_info->primary_key[i]);
	}

	for (size_t i = 1; i < input.inputs.size(); i++) {
		bind_data->pk_inputs.push_back(input.inputs[i]);
	}

	return std::move(bind_data);
}

// ── Scan ──────────────────────────────────────────────────────────────────────

static void MerutableGetScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state    = data.global_state->Cast<MerutableGetState>();
	auto &bind     = data.bind_data->Cast<MerutableGetBindData>();

	if (state.done) { output.SetCardinality(0); return; }
	state.done = true;

	MeruHandle *db = GetHandleCache().Get(bind.db_path);

	// Build PK value array from captured DuckDB inputs
	std::vector<MeruValue> pk_vals(bind.pk_indices.size());
	for (size_t i = 0; i < bind.pk_indices.size(); i++) {
		uintptr_t col_idx = bind.pk_indices[i];
		MeruColumnType tag = bind.col_types[col_idx];
		const Value &dv = bind.pk_inputs[i];

		MeruValue &mv = pk_vals[i];
		memset(&mv, 0, sizeof(mv));
		mv.tag = tag;

		if (dv.IsNull()) {
			mv.is_null = 1;
			continue;
		}
		switch (tag) {
		case MeruColumnType_Int64:   mv.inner.v_int64  = dv.GetValue<int64_t>(); break;
		case MeruColumnType_Int32:   mv.inner.v_int32  = dv.GetValue<int32_t>(); break;
		case MeruColumnType_Boolean: mv.inner.v_bool   = dv.GetValue<bool>() ? 1 : 0; break;
		case MeruColumnType_Float:   mv.inner.v_float  = dv.GetValue<float>(); break;
		case MeruColumnType_Double:  mv.inner.v_double = dv.GetValue<double>(); break;
		default: {
			// ByteArray — Rust copies immediately before meru_get returns
			auto s = dv.GetValue<string>();
			mv.inner.v_bytes.data = (const uint8_t *)s.data();
			mv.inner.v_bytes.len  = s.size();
			break;
		}
		}
	}

	int found = 0;
	MeruRow *row = nullptr;
	meru_checked([&](char **e) {
		return meru_get(db, pk_vals.data(), pk_vals.size(), &found, &row, e);
	});

	MeruRowGuard rg(row);
	if (!rg.found()) { output.SetCardinality(0); return; }

	output.SetCardinality(1);
	for (size_t out_col = 0; out_col < bind.user_col_indices.size(); out_col++) {
		int col_idx = bind.user_col_indices[out_col];
		MeruValue &mv = rg.get()->fields[col_idx];
		auto &vec = output.data[out_col];

		if (mv.is_null) { FlatVector::SetNull(vec, 0, true); continue; }

		switch (mv.tag) {
		case MeruColumnType_Int64:   FlatVector::GetData<int64_t>(vec)[0] = mv.inner.v_int64; break;
		case MeruColumnType_Int32:   FlatVector::GetData<int32_t>(vec)[0] = mv.inner.v_int32; break;
		case MeruColumnType_Boolean: FlatVector::GetData<bool>(vec)[0]    = mv.inner.v_bool != 0; break;
		case MeruColumnType_Float:   FlatVector::GetData<float>(vec)[0]   = mv.inner.v_float; break;
		case MeruColumnType_Double:  FlatVector::GetData<double>(vec)[0]  = mv.inner.v_double; break;
		default: {
			auto bv = mv.inner.v_bytes;
			FlatVector::GetData<string_t>(vec)[0] =
			    StringVector::AddStringOrBlob(vec, (const char *)bv.data, bv.len);
			break;
		}
		}
	}
}

// ── Register ──────────────────────────────────────────────────────────────────

TableFunctionSet MerutableGetFunction::GetTableFunctionSet() {
	TableFunctionSet set("merutable_get");
	for (auto pk_type : {LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::VARCHAR,
	                      LogicalType::DOUBLE, LogicalType::FLOAT, LogicalType::BOOLEAN}) {
		TableFunction tf("merutable_get", {LogicalType::VARCHAR, pk_type},
		                 MerutableGetScan, MerutableGetBind, MerutableGetInit);
		set.AddFunction(tf);
	}
	return set;
}

} // namespace duckdb
