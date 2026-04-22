#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct MerutableGetFunction {
	// Returns a set of overloads — one per supported PK type (for single-column PKs).
	// Multi-column PKs use a STRUCT argument variant added separately.
	static TableFunctionSet GetTableFunctionSet();
};

} // namespace duckdb
