#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct MerutableScanFunction {
	static TableFunction GetTableFunction();

	static unique_ptr<FunctionData> BindWithPath(ClientContext &context,
	                                              const std::string &db_path,
	                                              vector<LogicalType> &return_types,
	                                              vector<string> &names);
};

} // namespace duckdb
