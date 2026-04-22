#pragma once

#include "merutable.h"

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace duckdb {

// Thread-safe LRU cache of open MeruHandle* handles, keyed by canonical path.
// Each slot holds a read-write handle opened via meru_open_existing().
// The cache owns a single MeruRuntime* shared across all handles.
// On eviction: meru_export_iceberg (in-place) then meru_close_free.
// On shutdown: same, then meru_runtime_free.
class HandleCache {
public:
	explicit HandleCache(size_t capacity = 8);
	~HandleCache();

	// Returns an open handle for db_path, opening it if not cached.
	// Throws std::runtime_error on failure. Bumps LRU on hit.
	MeruHandle *Get(const std::string &db_path);

	// Export Iceberg JSON in-place, then close + evict a path.
	// Called after COPY finalize so merutable_scan sees fresh data.
	void FlushAndEvict(const std::string &db_path);

	// Drain all handles cleanly. Called from extension Shutdown().
	void Shutdown();

	// Expose the shared runtime so callers that open their own handles
	// (e.g. COPY write path) can share the same thread pool.
	MeruRuntime *Runtime() const { return rt_; }

private:
	using Entry = std::pair<std::string, MeruHandle *>;
	using List  = std::list<Entry>;

	std::mutex  mu_;
	size_t      capacity_;
	MeruRuntime *rt_ = nullptr;
	List        lru_;
	std::unordered_map<std::string, List::iterator> index_;

	// Closes (export + close_free) one entry. Must hold mu_.
	void CloseEntry(List::iterator it);
};

HandleCache &GetHandleCache();

} // namespace duckdb
