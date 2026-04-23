#include "merutable_storage.hpp"
#include "merutable_scan.hpp"
#include "handle_cache.hpp"
#include "meru_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include <unordered_set>

namespace duckdb {

// ── MerutableTableEntry ───────────────────────────────────────────────────────

MerutableTableEntry::MerutableTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                         CreateTableInfo &info, std::string db_path,
                                         std::vector<LogicalType> col_types,
                                         std::vector<int> pk_indices)
    : TableCatalogEntry(catalog, schema, info),
      db_path_(std::move(db_path)),
      col_types_(std::move(col_types)),
      pk_indices_(std::move(pk_indices)) {}

TableFunction MerutableTableEntry::GetScanFunction(ClientContext &context,
                                                    unique_ptr<FunctionData> &bind_data) {
	vector<LogicalType> return_types;
	vector<string> names;
	bind_data = MerutableScanFunction::BindWithPath(context, db_path_, return_types, names);
	return MerutableScanFunction::GetTableFunction();
}

unique_ptr<BaseStatistics> MerutableTableEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

TableStorageInfo MerutableTableEntry::GetStorageInfo(ClientContext &) {
	TableStorageInfo info;
	info.cardinality = 0;
	return info;
}

// ── MerutableSchema ───────────────────────────────────────────────────────────

MerutableSchema::MerutableSchema(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {}

optional_ptr<CatalogEntry> MerutableSchema::CreateTable(CatalogTransaction transaction,
                                                         BoundCreateTableInfo &bound_info) {
	auto &info = bound_info.base->Cast<CreateTableInfo>();
	auto &cat = this->catalog.Cast<MerutableCatalog>();
	std::string db_path = cat.DbPath();

	std::vector<LogicalType> col_types;
	std::vector<std::string> col_names;
	std::vector<int> pk_indices;

	for (auto &col : info.columns.Logical()) {
		col_names.push_back(col.Name());
		col_types.push_back(col.Type());
	}

	// Extract PRIMARY KEY column names from constraints
	std::unordered_set<std::string> pk_set;
	for (auto &constraint : info.constraints) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto &uc = constraint->Cast<UniqueConstraint>();
			if (uc.IsPrimaryKey()) {
				// Multi-column PK: GetColumnNames() returns the column name list
				for (auto &col_name : uc.GetColumnNames()) {
					pk_set.insert(col_name);
				}
				// Single-column form via index
				if (uc.HasIndex()) {
					pk_set.insert(col_names[uc.GetIndex().index]);
				}
			}
		}
	}

	for (size_t i = 0; i < col_names.size(); i++) {
		if (pk_set.count(col_names[i])) {
			pk_indices.push_back((int)i);
		}
	}
	if (pk_indices.empty()) {
		pk_indices = {0};
	}

	// Build C schema
	std::vector<MeruColumnDef> col_defs(col_names.size());
	std::vector<std::string> name_storage(col_names);
	std::unordered_set<int> pk_idx_set(pk_indices.begin(), pk_indices.end());
	for (size_t i = 0; i < col_names.size(); i++) {
		col_defs[i].name            = name_storage[i].c_str();
		col_defs[i].col_type        = DuckTypeToMeru(col_types[i]);
		col_defs[i].fixed_byte_len  = 0;
		col_defs[i].nullable        = pk_idx_set.count((int)i) ? 0 : 1;
		col_defs[i].initial_default = nullptr;
		col_defs[i].write_default   = nullptr;
	}

	std::vector<uintptr_t> pk_idx_arr(pk_indices.begin(), pk_indices.end());
	MeruSchema schema_c;
	schema_c.table_name      = info.table.c_str();
	schema_c.columns         = col_defs.data();
	schema_c.column_count    = col_defs.size();
	schema_c.primary_key     = pk_idx_arr.data();
	schema_c.primary_key_len = pk_idx_arr.size();

	MeruOpenOptions opts;
	memset(&opts, 0, sizeof(opts));
	opts.schema           = schema_c;
	opts.catalog_uri      = db_path.c_str();
	opts.wal_dir          = nullptr;
	opts.memtable_size_mb = 0;
	opts.read_only        = 0;

	MeruHandle *db = nullptr;
	meru_checked([&](char **e) {
		return meru_open(&opts, GetHandleCache().Runtime(), &db, e);
	});
	// Keep handle open for the lifetime of the catalog (until DETACH).
	cat.SetHandle(db);

	auto entry = make_uniq<MerutableTableEntry>(this->catalog, *this, info,
	                                            db_path, col_types, pk_indices);
	auto *raw = entry.get();
	{
		std::lock_guard<std::mutex> lk(mu_);
		tables_[info.table] = std::move(entry);
	}
	return raw;
}

optional_ptr<CatalogEntry> MerutableSchema::LookupEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup) {
	if (lookup.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	std::lock_guard<std::mutex> lk(mu_);
	auto it = tables_.find(lookup.GetEntryName());
	if (it == tables_.end()) {
		return nullptr;
	}
	return it->second.get();
}

CatalogSet::EntryLookup MerutableSchema::LookupEntryDetailed(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup) {
	auto entry = LookupEntry(transaction, lookup);
	CatalogSet::EntryLookup result;
	result.result = entry;
	result.reason = entry ? CatalogSet::EntryLookup::FailureReason::SUCCESS
	                      : CatalogSet::EntryLookup::FailureReason::NOT_PRESENT;
	return result;
}

void MerutableSchema::Scan(ClientContext &, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	std::lock_guard<std::mutex> lk(mu_);
	for (auto &[name, entry] : tables_) {
		callback(*entry);
	}
}

void MerutableSchema::Scan(CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	std::lock_guard<std::mutex> lk(mu_);
	for (auto &[name, entry] : tables_) {
		callback(*entry);
	}
}

void MerutableSchema::DropEntry(ClientContext &, DropInfo &) {
	throw NotImplementedException("merutable does not support DROP");
}

void MerutableSchema::Alter(CatalogTransaction, AlterInfo &) {
	throw NotImplementedException("merutable does not support ALTER");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateIndex(CatalogTransaction, CreateIndexInfo &,
                                                         TableCatalogEntry &) {
	throw NotImplementedException("merutable does not support CREATE INDEX");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw NotImplementedException("merutable does not support CREATE FUNCTION");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw NotImplementedException("merutable does not support CREATE VIEW");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw NotImplementedException("merutable does not support CREATE SEQUENCE");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw NotImplementedException("merutable does not support CREATE TABLE FUNCTION");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw NotImplementedException("merutable does not support CREATE COPY FUNCTION");
}

optional_ptr<CatalogEntry> MerutableSchema::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw NotImplementedException("merutable does not support CREATE PRAGMA FUNCTION");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw NotImplementedException("merutable does not support CREATE COLLATION");
}

optional_ptr<CatalogEntry> MerutableSchema::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw NotImplementedException("merutable does not support CREATE TYPE");
}

// ── MerutableInsertOperator ───────────────────────────────────────────────────
// Minimal PhysicalOperator sink: writes chunks via WriteRows, flushes on finalize.

class MerutableInsertOperator : public PhysicalOperator {
public:
	MerutableInsertOperator(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                        MerutableCatalog &catalog, std::vector<LogicalType> col_types,
	                        idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types),
	                        estimated_cardinality),
	      catalog_(catalog), col_types_(std::move(col_types)) {}

	MerutableCatalog &catalog_;
	std::vector<LogicalType> col_types_;

	bool IsSource() const override { return true; }

protected:
	SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
	                                  OperatorSourceInput &) const override {
		chunk.SetCardinality(0);
		return SourceResultType::FINISHED;
	}

public:
	bool IsSink() const override { return true; }

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &) const override {
		return make_uniq<GlobalSinkState>();
	}

	SinkResultType Sink(ExecutionContext &, DataChunk &chunk,
	                    OperatorSinkInput &) const override {
		WriteRows(catalog_.Handle(), chunk, col_types_);
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &,
	                          OperatorSinkFinalizeInput &) const override {
		catalog_.FlushAndExport();
		return SinkFinalizeType::READY;
	}

	string GetName() const override { return "MERUTABLE_INSERT"; }

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		return InsertionOrderPreservingMap<string>();
	}
};

// ── MerutableCatalog ──────────────────────────────────────────────────────────

MerutableCatalog::MerutableCatalog(AttachedDatabase &db, std::string db_path)
    : Catalog(db), db_path_(std::move(db_path)) {}

MerutableCatalog::~MerutableCatalog() {
	if (!db_handle_) return;
	// Best-effort flush, export, and close on DETACH.
	char *err = nullptr;
	meru_flush(db_handle_, &err); meru_free_string(err); err = nullptr;
	meru_export_iceberg(db_handle_, db_path_.c_str(), &err); meru_free_string(err); err = nullptr;
	meru_close_free(db_handle_, &err); meru_free_string(err);
	db_handle_ = nullptr;
}

void MerutableCatalog::FlushAndExport() {
	if (!db_handle_) return;
	meru_checked([&](char **e) { return meru_flush(db_handle_, e); });
	meru_checked([&](char **e) { return meru_export_iceberg(db_handle_, db_path_.c_str(), e); });
}

void MerutableCatalog::Initialize(bool) {
	CreateSchemaInfo schema_info;
	schema_info.schema = DEFAULT_SCHEMA;
	schema_info.internal = false;
	schema_ = make_uniq<MerutableSchema>(*this, schema_info);
}

optional_ptr<CatalogEntry> MerutableCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw NotImplementedException("merutable does not support CREATE SCHEMA");
}

optional_ptr<SchemaCatalogEntry> MerutableCatalog::LookupSchema(
    CatalogTransaction transaction, const EntryLookupInfo &lookup,
    OnEntryNotFound if_not_found) {
	auto name = lookup.GetEntryName();
	if (name == DEFAULT_SCHEMA || name.empty()) {
		return schema_.get();
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema '%s' not found in merutable catalog", name);
}

void MerutableCatalog::ScanSchemas(ClientContext &,
                                    std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*schema_);
}

PhysicalOperator &MerutableCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &,
                                                       LogicalCreateTable &, PhysicalOperator &) {
	throw NotImplementedException("merutable does not support CREATE TABLE AS SELECT");
}

PhysicalOperator &MerutableCatalog::PlanInsert(ClientContext &context,
                                               PhysicalPlanGenerator &planner,
                                               LogicalInsert &op,
                                               optional_ptr<PhysicalOperator> plan) {
	auto &table_entry = op.table.Cast<MerutableTableEntry>();
	auto &cat = op.table.catalog.Cast<MerutableCatalog>();
	auto &insert_op = planner.Make<MerutableInsertOperator>(
	    op.types, cat, table_entry.ColTypes(), op.estimated_cardinality);
	if (plan) {
		insert_op.children.push_back(*plan);
	}
	return insert_op;
}

PhysicalOperator &MerutableCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &,
                                               LogicalDelete &, PhysicalOperator &) {
	throw NotImplementedException("merutable does not support DELETE");
}

PhysicalOperator &MerutableCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &,
                                               LogicalUpdate &, PhysicalOperator &) {
	throw NotImplementedException("merutable does not support UPDATE");
}

void MerutableCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw NotImplementedException("merutable does not support DROP SCHEMA");
}

DatabaseSize MerutableCatalog::GetDatabaseSize(ClientContext &) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

// ── MerutableTransaction / MerutableTransactionManager ───────────────────────
// merutable writes are durable at the Rust layer (WAL-backed). DuckDB only
// needs a transaction object to satisfy its internal bookkeeping — no MVCC.

class MerutableTransaction : public Transaction {
public:
	MerutableTransaction(TransactionManager &manager, ClientContext &context)
	    : Transaction(manager, context) {}
	~MerutableTransaction() override = default;
};

class MerutableTransactionManager : public TransactionManager {
public:
	explicit MerutableTransactionManager(AttachedDatabase &db) : TransactionManager(db) {}

	Transaction &StartTransaction(ClientContext &context) override {
		auto txn = make_uniq<MerutableTransaction>(*this, context);
		auto *raw = txn.get();
		std::lock_guard<std::mutex> lk(mu_);
		active_[raw] = std::move(txn);
		return *raw;
	}

	ErrorData CommitTransaction(ClientContext &, Transaction &txn) override {
		std::lock_guard<std::mutex> lk(mu_);
		active_.erase(&txn);
		return ErrorData();
	}

	void RollbackTransaction(Transaction &txn) override {
		std::lock_guard<std::mutex> lk(mu_);
		active_.erase(&txn);
	}

	void Checkpoint(ClientContext &, bool) override {}

private:
	std::mutex mu_;
	std::unordered_map<Transaction *, unique_ptr<MerutableTransaction>> active_;
};

// ── MerutableStorageExtension ─────────────────────────────────────────────────

MerutableStorageExtension::MerutableStorageExtension() {
	attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &,
	            AttachedDatabase &db, const string &,
	            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
		return make_uniq<MerutableCatalog>(db, info.path);
	};

	create_transaction_manager = [](optional_ptr<StorageExtensionInfo>,
	                                AttachedDatabase &db,
	                                Catalog &) -> unique_ptr<TransactionManager> {
		return make_uniq<MerutableTransactionManager>(db);
	};
}

} // namespace duckdb
