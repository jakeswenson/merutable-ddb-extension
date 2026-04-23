#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "merutable.h"

#include <mutex>
#include <unordered_map>

namespace duckdb {

// ── MerutableTableEntry ───────────────────────────────────────────────────────
// One per CREATE TABLE. Owns db_path + schema info; shares handle via HandleCache.

class MerutableTableEntry : public TableCatalogEntry {
public:
	MerutableTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
	                    CreateTableInfo &info, std::string db_path,
	                    std::vector<LogicalType> col_types,
	                    std::vector<int> pk_indices);

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	const std::string &DbPath() const { return db_path_; }
	const std::vector<LogicalType> &ColTypes() const { return col_types_; }

private:
	std::string db_path_;
	std::vector<LogicalType> col_types_;
	std::vector<int> pk_indices_;
};

// ── MerutableSchema ───────────────────────────────────────────────────────────
// One schema ("main") per attached database. Only CreateTable and LookupEntry
// are meaningful; all other DDL throws NotImplementedException.

class MerutableSchema : public SchemaCatalogEntry {
public:
	MerutableSchema(Catalog &catalog, CreateSchemaInfo &info);

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction,
	                                       BoundCreateTableInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
	                                       const EntryLookupInfo &lookup) override;
	CatalogSet::EntryLookup LookupEntryDetailed(CatalogTransaction transaction,
	                                            const EntryLookupInfo &lookup) override;

	void Scan(ClientContext &context, CatalogType type,
	          const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type,
	          const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction, CreateIndexInfo &,
	                                       TableCatalogEntry &) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction, CreateFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction, CreateViewInfo &) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction, CreateSequenceInfo &) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction, CreateCollationInfo &) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction, CreateTypeInfo &) override;

private:
	std::mutex mu_;
	std::unordered_map<std::string, unique_ptr<MerutableTableEntry>> tables_;
};

// ── MerutableCatalog ──────────────────────────────────────────────────────────
// One per ATTACH. Wraps db_path and owns a single MerutableSchema ("main").

class MerutableCatalog : public Catalog {
public:
	MerutableCatalog(AttachedDatabase &db, std::string db_path);
	~MerutableCatalog() override;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override { return "merutable"; }

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction,
	                                        CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
	                                              const EntryLookupInfo &lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context,
	                 std::function<void(SchemaCatalogEntry &)> callback) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                             LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;

	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
	                             LogicalDelete &op, PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
	                             LogicalUpdate &op, PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override { return false; }
	string GetDBPath() override { return db_path_; }

	const std::string &DbPath() const { return db_path_; }

	// Called from CreateTable — takes ownership of the open handle.
	void SetHandle(MeruHandle *db) { db_handle_ = db; }
	MeruHandle *Handle() const { return db_handle_; }

	// Flush memtable and export iceberg metadata (after each INSERT statement).
	void FlushAndExport();

private:
	std::string db_path_;
	unique_ptr<MerutableSchema> schema_;
	MeruHandle *db_handle_ = nullptr;

	void DropSchema(ClientContext &context, DropInfo &info) override;
};

// ── MerutableStorageExtension ─────────────────────────────────────────────────

class MerutableStorageExtension : public StorageExtension {
public:
	MerutableStorageExtension();
};

} // namespace duckdb
