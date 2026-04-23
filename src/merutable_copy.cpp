#include "merutable_copy.hpp"
#include "handle_cache.hpp"
#include "meru_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/string_util.hpp"

#include <unordered_set>

namespace duckdb {

// ── Bind data ─────────────────────────────────────────────────────────────────

struct MerutableWriteBindData : public TableFunctionData {
	std::string db_path;
	std::string table_name;
	std::vector<std::string> column_names;
	std::vector<LogicalType> column_types;
	std::vector<int> primary_key; // column indices
};

// ── Global state (one handle per COPY statement) ──────────────────────────────

struct MerutableWriteGlobalState : public GlobalFunctionData {
	MeruHandle *db = nullptr;
	std::string db_path;

	~MerutableWriteGlobalState() {
		if (db) {
			// Finalize should have run — this is an error path, best-effort close.
			char *err = nullptr;
			meru_close_free(db, &err);
			meru_free_string(err);
			db = nullptr;
		}
	}
};

// ── Options declaration ───────────────────────────────────────────────────────

static void MerutableListCopyOptions(ClientContext &context, CopyOptionsInput &input) {
	auto &opts = input.options;
	opts["table_name"]  = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
	opts["primary_key"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
}

// ── Bind ──────────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> MerutableWriteBind(ClientContext &context, CopyFunctionBindInput &input,
                                                    const vector<string> &names,
                                                    const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<MerutableWriteBindData>();
	bind_data->db_path = input.info.file_path;
	bind_data->column_names = names;
	bind_data->column_types = sql_types;

	// Parse options
	std::string pk_opt;
	for (auto &opt : input.info.options) {
		auto key = StringUtil::Lower(opt.first);
		if (opt.second.empty()) continue;
		if (key == "table_name") {
			bind_data->table_name = opt.second[0].GetValue<string>();
		} else if (key == "primary_key") {
			pk_opt = opt.second[0].GetValue<string>();
		}
	}

	// Default table_name to last path component
	if (bind_data->table_name.empty()) {
		auto &p = bind_data->db_path;
		auto pos = p.find_last_of("/\\");
		bind_data->table_name = (pos == std::string::npos) ? p : p.substr(pos + 1);
	}

	// Check if db already exists — if so, read PK from manifest
	MeruManifestInfo *raw_info = nullptr;
	char *merr = nullptr;
	int mstatus = meru_manifest_info(GetHandleCache().Runtime(), bind_data->db_path.c_str(), &raw_info, &merr);
	meru_free_string(merr);
	if (mstatus == MeruStatus_Ok && raw_info) {
		struct InfoGuard { MeruManifestInfo *p; ~InfoGuard() { if (p) meru_manifest_info_free(p); } } g{raw_info};
		for (uintptr_t i = 0; i < raw_info->pk_count; i++) {
			bind_data->primary_key.push_back((int)raw_info->primary_key[i]);
		}
	} else {
		// New db: determine PK from option or default to first column
		if (!pk_opt.empty()) {
			// Comma-separated column names
			auto pk_names = StringUtil::Split(pk_opt, ',');
			for (auto &pname : pk_names) {
				std::string trimmed = pname;
				StringUtil::Trim(trimmed);
				bool found = false;
				for (size_t i = 0; i < names.size(); i++) {
					if (StringUtil::Lower(names[i]) == StringUtil::Lower(trimmed)) {
						bind_data->primary_key.push_back((int)i);
						found = true;
						break;
					}
				}
				if (!found) {
					throw BinderException("merutable: PRIMARY_KEY column '" + trimmed + "' not found in query columns");
				}
			}
		} else {
			bind_data->primary_key = {0}; // default: first column
		}
	}

	return std::move(bind_data);
}

// ── Initialize global (open handle) ──────────────────────────────────────────

static unique_ptr<GlobalFunctionData> MerutableInitGlobal(ClientContext &context, FunctionData &bind_data_in,
                                                           const string &file_path) {
	auto &bind_data = bind_data_in.Cast<MerutableWriteBindData>();
	auto gstate = make_uniq<MerutableWriteGlobalState>();
	gstate->db_path = bind_data.db_path;

	// Build schema for meru_open
	std::vector<MeruColumnDef> col_defs(bind_data.column_names.size());
	std::vector<std::string> name_storage(bind_data.column_names.size());
	// PK columns must be non-nullable
	std::unordered_set<int> pk_set(bind_data.primary_key.begin(), bind_data.primary_key.end());
	for (size_t i = 0; i < bind_data.column_names.size(); i++) {
		name_storage[i] = bind_data.column_names[i];
		col_defs[i].name        = name_storage[i].c_str();
		col_defs[i].col_type    = DuckTypeToMeru(bind_data.column_types[i]);
		col_defs[i].fixed_byte_len = 0;
		col_defs[i].nullable    = pk_set.count((int)i) ? 0 : 1;
		col_defs[i].initial_default = nullptr;
		col_defs[i].write_default   = nullptr;
	}

	std::vector<uintptr_t> pk_indices(bind_data.primary_key.begin(), bind_data.primary_key.end());

	MeruSchema schema_c;
	schema_c.table_name      = bind_data.table_name.c_str();
	schema_c.columns         = col_defs.data();
	schema_c.column_count    = col_defs.size();
	schema_c.primary_key     = pk_indices.data();
	schema_c.primary_key_len = pk_indices.size();

	MeruOpenOptions opts;
	memset(&opts, 0, sizeof(opts));
	opts.schema          = schema_c;
	opts.catalog_uri     = bind_data.db_path.c_str();
	opts.wal_dir         = nullptr; // default: db_path/wal
	opts.memtable_size_mb = 0;
	opts.read_only       = 0;

	meru_checked([&](char **e) {
		return meru_open(&opts, GetHandleCache().Runtime(), &gstate->db, e);
	});

	return std::move(gstate);
}

// ── Sink (called per DataChunk) ───────────────────────────────────────────────

static void MerutableSink(ExecutionContext &context, FunctionData &bind_data_in,
                           GlobalFunctionData &gstate_in, LocalFunctionData &lstate_in, DataChunk &input) {
	auto &bind_data = bind_data_in.Cast<MerutableWriteBindData>();
	auto &gstate    = gstate_in.Cast<MerutableWriteGlobalState>();
	WriteRows(gstate.db, input, bind_data.column_types);
}

// ── Combine (merge local → global, no-op since we write directly) ─────────────

static void MerutableCombine(ExecutionContext &context, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
	// All writes already committed in Sink; nothing to merge.
}

// ── Finalize ──────────────────────────────────────────────────────────────────

static void MerutableFinalize(ClientContext &context, FunctionData &bind_data_in, GlobalFunctionData &gstate_in) {
	auto &gstate = gstate_in.Cast<MerutableWriteGlobalState>();
	if (!gstate.db) return;

	meru_checked([&](char **e) { return meru_flush(gstate.db, e); });

	// Export Iceberg JSON in-place so merutable_scan sees the new data.
	// Must pass catalog_path explicitly — NULL resolves to cwd in the Rust impl.
	MeruString cpath(meru_catalog_path(gstate.db));
	meru_checked([&](char **e) {
		return meru_export_iceberg(gstate.db, cpath.empty() ? gstate.db_path.c_str() : cpath.get(), e);
	});

	meru_checked([&](char **e) { return meru_close_free(gstate.db, e); });
	gstate.db = nullptr;

	GetHandleCache().FlushAndEvict(gstate.db_path);
}

// ── Register ──────────────────────────────────────────────────────────────────

CopyFunction MerutableCopyFunction::GetCopyFunction() {
	CopyFunction cf("merutable");
	cf.copy_options           = MerutableListCopyOptions;
	cf.copy_to_bind           = MerutableWriteBind;
	cf.copy_to_initialize_global = MerutableInitGlobal;
	cf.copy_to_sink           = MerutableSink;
	cf.copy_to_combine        = MerutableCombine;
	cf.copy_to_finalize       = MerutableFinalize;
	cf.extension              = "merutable";
	return cf;
}

} // namespace duckdb
