#include "merutable_scan.hpp"
#include "handle_cache.hpp"
#include "meru_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

#include <sstream>

namespace duckdb {

// ── MeruManifestInfoGuard: RAII wrapper for MeruManifestInfo* ────────────────

struct MeruManifestInfoGuard {
	MeruManifestInfo *ptr = nullptr;

	explicit MeruManifestInfoGuard(MeruManifestInfo *p) : ptr(p) {}
	~MeruManifestInfoGuard() { if (ptr) meru_manifest_info_free(ptr); }

	MeruManifestInfoGuard(const MeruManifestInfoGuard &) = delete;
	MeruManifestInfoGuard &operator=(const MeruManifestInfoGuard &) = delete;

	MeruManifestInfo *get() const { return ptr; }
};

// ── Helpers ───────────────────────────────────────────────────────────────────

static LogicalType ColTypeToLogical(MeruColumnType t) {
	switch (t) {
	case MeruColumnType_Int64:         return LogicalType::BIGINT;
	case MeruColumnType_Int32:         return LogicalType::INTEGER;
	case MeruColumnType_Boolean:       return LogicalType::BOOLEAN;
	case MeruColumnType_Float:         return LogicalType::FLOAT;
	case MeruColumnType_Double:        return LogicalType::DOUBLE;
	case MeruColumnType_FixedLenBytes: return LogicalType::BLOB;
	default:                           return LogicalType::VARCHAR; // ByteArray
	}
}

// ── Bind data ─────────────────────────────────────────────────────────────────

struct MerutableScanBindData : public TableFunctionData {
	std::string dedup_sql;
};

// ── Scan state ────────────────────────────────────────────────────────────────

struct MerutableScanState : public GlobalTableFunctionState {
	unique_ptr<Connection>   conn;   // owns the connection that ran the query
	unique_ptr<QueryResult>  result;
	bool done = false;
};

// ── Bind ──────────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> MerutableScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("merutable_scan requires a non-null path argument");
	}
	std::string db_path = input.inputs[0].GetValue<string>();

	// Read manifest via Rust — handles both JSON and protobuf formats
	MeruManifestInfo *raw_info = nullptr;
	meru_checked([&](char **e) {
		return meru_manifest_info(GetHandleCache().Runtime(), db_path.c_str(), &raw_info, e);
	});
	MeruManifestInfoGuard info(raw_info);

	auto bind_data = make_uniq<MerutableScanBindData>();

	if (info.get()->parquet_count == 0) {
		bind_data->dedup_sql = "";
		return std::move(bind_data);
	}

	// Collect user-visible columns and PK column names
	std::vector<std::string> user_cols;
	std::vector<std::string> pk_col_names;

	for (uintptr_t i = 0; i < info.get()->column_count; i++) {
		auto &col = info.get()->columns[i];
		names.push_back(col.name);
		return_types.push_back(ColTypeToLogical(col.col_type));
		user_cols.push_back(col.name);
	}

	for (uintptr_t i = 0; i < info.get()->pk_count; i++) {
		uintptr_t idx = info.get()->primary_key[i];
		pk_col_names.push_back(info.get()->columns[idx].name);
	}

	// Build MVCC dedup SQL
	std::ostringstream sql;
	sql << "SELECT ";
	for (size_t i = 0; i < user_cols.size(); i++) {
		if (i > 0) sql << ", ";
		sql << '"' << user_cols[i] << '"';
	}

	sql << " FROM read_parquet([";
	for (uintptr_t i = 0; i < info.get()->parquet_count; i++) {
		if (i > 0) sql << ", ";
		sql << "'";
		for (const char *c = info.get()->parquet_paths[i]; *c; c++) {
			if (*c == '\'') sql << "''";
			else sql << *c;
		}
		sql << "'";
	}
	sql << "], union_by_name=true)";

	if (!pk_col_names.empty()) {
		sql << " QUALIFY ROW_NUMBER() OVER (PARTITION BY ";
		for (size_t i = 0; i < pk_col_names.size(); i++) {
			if (i > 0) sql << ", ";
			sql << '"' << pk_col_names[i] << '"';
		}
		sql << " ORDER BY _merutable_seq DESC) = 1 AND _merutable_op = 1";
	}

	bind_data->dedup_sql = sql.str();
	return std::move(bind_data);
}

// ── Init ──────────────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> MerutableScanInit(ClientContext &context,
                                                               TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MerutableScanBindData>();
	auto state = make_uniq<MerutableScanState>();

	if (bind_data.dedup_sql.empty()) {
		state->done = true;
		return std::move(state);
	}

	// Execute the dedup SQL via a Connection, not ClientContext::Query.
	// ClientContext::Query re-acquires the context lock which is already held
	// by the calling query — deadlock. A new Connection gets its own context.
	auto conn = make_uniq<Connection>(*context.db);
	state->result = conn->Query(bind_data.dedup_sql);
	state->conn = std::move(conn); // keep alive while result is consumed

	if (state->result->HasError()) {
		throw IOException("merutable_scan query failed: " + state->result->GetError());
	}
	return std::move(state);
}

// ── Scan ──────────────────────────────────────────────────────────────────────

static void MerutableScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<MerutableScanState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto chunk = state.result->Fetch();
	if (!chunk || chunk->size() == 0) {
		state.done = true;
		output.SetCardinality(0);
		return;
	}

	output.Move(*chunk);
}

// ── Register ──────────────────────────────────────────────────────────────────

TableFunction MerutableScanFunction::GetTableFunction() {
	TableFunction tf("merutable_scan", {LogicalType::VARCHAR}, MerutableScanExecute,
	                 MerutableScanBind, MerutableScanInit);
	tf.projection_pushdown = false;
	return tf;
}

} // namespace duckdb
