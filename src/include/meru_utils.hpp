#pragma once

#include "merutable.h"
#include "duckdb/common/exception.hpp"

#include <string>
#include <stdexcept>

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

} // namespace duckdb
