#include "handle_cache.hpp"
#include "meru_utils.hpp"

#include <stdexcept>

namespace duckdb {

HandleCache::HandleCache(size_t capacity) : capacity_(capacity) {
	char *err = nullptr;
	rt_ = meru_runtime_new(0, &err);
	if (!rt_) {
		std::string msg = err ? err : "unknown error";
		meru_free_string(err);
		throw std::runtime_error("merutable: failed to create runtime: " + msg);
	}
}

HandleCache::~HandleCache() {
	Shutdown();
}

MeruHandle *HandleCache::Get(const std::string &db_path) {
	std::lock_guard<std::mutex> lock(mu_);

	auto it = index_.find(db_path);
	if (it != index_.end()) {
		lru_.splice(lru_.begin(), lru_, it->second);
		return it->second->second;
	}

	if (lru_.size() >= capacity_) {
		CloseEntry(std::prev(lru_.end()));
	}

	MeruHandle *db = nullptr;
	meru_checked([&](char **e) {
		return meru_open_existing(db_path.c_str(), /*read_only=*/0, rt_, &db, e);
	});

	lru_.emplace_front(db_path, db);
	index_[db_path] = lru_.begin();
	return db;
}

void HandleCache::FlushAndEvict(const std::string &db_path) {
	std::lock_guard<std::mutex> lock(mu_);
	auto it = index_.find(db_path);
	if (it != index_.end()) {
		CloseEntry(it->second);
	}
}

void HandleCache::Shutdown() {
	std::lock_guard<std::mutex> lock(mu_);
	for (auto it = lru_.begin(); it != lru_.end(); ) {
		auto next = std::next(it);
		CloseEntry(it);
		it = next;
	}
	if (rt_) {
		meru_runtime_free(rt_);
		rt_ = nullptr;
	}
}

void HandleCache::CloseEntry(List::iterator it) {
	MeruHandle *db = it->second;

	// Export Iceberg JSON before closing so merutable_scan sees current data.
	// Use catalog_path to ensure we write in-place, not to cwd.
	MeruString cpath(meru_catalog_path(db));
	if (!cpath.empty()) {
		meru_checked([&](char **e) {
			return meru_export_iceberg(db, cpath.get(), e);
		});
	}

	meru_checked([&](char **e) {
		return meru_close_free(db, e);
	});

	index_.erase(it->first);
	lru_.erase(it);
}

HandleCache &GetHandleCache() {
	static HandleCache instance;
	return instance;
}

} // namespace duckdb
