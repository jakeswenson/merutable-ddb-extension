#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

struct MerutableCopyFunction {
	static CopyFunction GetCopyFunction();
};

} // namespace duckdb
