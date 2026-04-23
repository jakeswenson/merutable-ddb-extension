#define DUCKDB_EXTENSION_MAIN

#include "merutable_extension.hpp"
#include "merutable_scan.hpp"
#include "merutable_get.hpp"
#include "merutable_copy.hpp"
#include "merutable_storage.hpp"
#include "handle_cache.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(MerutableScanFunction::GetTableFunction());

	auto get_set = MerutableGetFunction::GetTableFunctionSet();
	for (auto &f : get_set.functions) {
		loader.RegisterFunction(f);
	}

	loader.RegisterFunction(MerutableCopyFunction::GetCopyFunction());

	auto storage_ext = make_shared_ptr<MerutableStorageExtension>();
	StorageExtension::Register(loader.GetDatabaseInstance().config,
	                           "merutable", std::move(storage_ext));
}

void MerutableExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string MerutableExtension::Name() {
	return "merutable";
}

std::string MerutableExtension::Version() const {
#ifdef EXT_VERSION_MERUTABLE
	return EXT_VERSION_MERUTABLE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(merutable, loader) {
	duckdb::LoadInternal(loader);
}
}
