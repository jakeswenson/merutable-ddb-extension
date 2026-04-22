#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct MerutableScanFunction {
	static TableFunction GetTableFunction();
};

} // namespace duckdb
