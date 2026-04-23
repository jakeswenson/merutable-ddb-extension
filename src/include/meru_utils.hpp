#pragma once

#include "merutable.h"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string>
#include <stdexcept>
#include <vector>

namespace duckdb {

// ── meru_checked: turns the C status+err_out convention into a C++ exception.
// Usage: meru_checked([&](char **e){ return meru_foo(..., e); });
template <typename F>
void meru_checked(F &&fn) {
	char *err = nullptr;
	int status = fn(&err);
	if (status != MeruStatus_Ok) {
		std::string msg = err ? err : "unknown error";
		meru_free_string(err);
		throw IOException("merutable: " + msg);
	}
}

// ── MeruString: RAII wrapper for heap-allocated C strings from the API.
// Automatically calls meru_free_string on destruction.
struct MeruString {
	char *ptr = nullptr;

	explicit MeruString(char *p) : ptr(p) {}
	~MeruString() { if (ptr) meru_free_string(ptr); }

	MeruString(const MeruString &) = delete;
	MeruString &operator=(const MeruString &) = delete;

	const char *get() const { return ptr; }
	std::string str()  const { return ptr ? ptr : ""; }
	bool        empty() const { return !ptr || ptr[0] == '\0'; }
};

// ── MeruRowGuard: RAII wrapper for MeruRow* from meru_get.
struct MeruRowGuard {
	MeruRow *ptr = nullptr;

	explicit MeruRowGuard(MeruRow *p) : ptr(p) {}
	~MeruRowGuard() { if (ptr) meru_row_free(ptr); }

	MeruRowGuard(const MeruRowGuard &) = delete;
	MeruRowGuard &operator=(const MeruRowGuard &) = delete;

	MeruRow *get() const { return ptr; }
	bool     found() const { return ptr != nullptr; }
};

// ── DuckTypeToMeru: shared type mapping ───────────────────────────────────────
inline MeruColumnType DuckTypeToMeru(const LogicalType &lt) {
	switch (lt.id()) {
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:    return MeruColumnType_Int64;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::TINYINT:    return MeruColumnType_Int32;
	case LogicalTypeId::BOOLEAN:    return MeruColumnType_Boolean;
	case LogicalTypeId::FLOAT:      return MeruColumnType_Float;
	case LogicalTypeId::DOUBLE:     return MeruColumnType_Double;
	default:                        return MeruColumnType_ByteArray;
	}
}

// ── WriteRows: shared DataChunk → meru_put_batch helper ───────────────────────
// col_types must be the full column type list matching the open MeruHandle schema.
inline void WriteRows(MeruHandle *db, DataChunk &input,
                      const std::vector<LogicalType> &col_types) {
	input.Flatten();
	idx_t n_cols = col_types.size();
	idx_t n_rows = input.size();

	std::vector<std::vector<MeruValue>> field_arrays(n_rows);
	std::vector<MeruRow> rows(n_rows);

	for (idx_t row = 0; row < n_rows; row++) {
		field_arrays[row].resize(n_cols);
		for (idx_t col = 0; col < n_cols; col++) {
			MeruValue &mv = field_arrays[row][col];
			memset(&mv, 0, sizeof(mv));
			mv.tag = DuckTypeToMeru(col_types[col]);

			auto &vec = input.data[col];
			if (FlatVector::IsNull(vec, row)) {
				mv.is_null = 1;
				continue;
			}
			switch (col_types[col].id()) {
			case LogicalTypeId::BIGINT:
				mv.inner.v_int64 = FlatVector::GetData<int64_t>(vec)[row]; break;
			case LogicalTypeId::INTEGER:
				mv.inner.v_int32 = FlatVector::GetData<int32_t>(vec)[row]; break;
			case LogicalTypeId::BOOLEAN:
				mv.inner.v_bool = FlatVector::GetData<bool>(vec)[row] ? 1 : 0; break;
			case LogicalTypeId::FLOAT:
				mv.inner.v_float = FlatVector::GetData<float>(vec)[row]; break;
			case LogicalTypeId::DOUBLE:
				mv.inner.v_double = FlatVector::GetData<double>(vec)[row]; break;
			default: {
				auto s = FlatVector::GetData<string_t>(vec)[row];
				mv.inner.v_bytes.data = (const uint8_t *)s.GetDataUnsafe();
				mv.inner.v_bytes.len  = s.GetSize();
				break;
			}
			}
		}
		rows[row].fields      = field_arrays[row].data();
		rows[row].field_count = n_cols;
	}

	meru_checked([&](char **e) {
		return meru_put_batch(db, rows.data(), (uintptr_t)n_rows, nullptr, e);
	});
}

} // namespace duckdb
