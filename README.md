# merutable DuckDB extension

DuckDB extension for [merutable](https://github.com/merutable/merutable) — an embeddable single-table LSM engine that stores data as Parquet files with Iceberg-compatible metadata.

Provides three capabilities:

| Function | Type | Description |
|---|---|---|
| `merutable_scan(path)` | Table function | Full table scan with MVCC dedup applied automatically |
| `merutable_get(path, key)` | Table function | O(log N) point lookup via the Rust engine (hits live memtable) |
| `COPY ... TO path (FORMAT merutable)` | Copy function | Write rows into a merutable table |

---

## Requirements

- [Nix](https://nixos.org) with flakes enabled
- Rust (stable, managed by the nix shell via `rust-overlay`)
- The [merutable](https://github.com/merutable/merutable) workspace checked out as a sibling directory, or set `MERUTABLE_WORKSPACE` to its path

## Building

```sh
# First-time setup: initialise git submodules (duckdb + extension-ci-tools)
just init

# Build the Rust capi static library, then the DuckDB extension
just build          # debug build
just build-release  # release build

# Verify the nix environment
just check-env
```

The built extension lands at:
```
build/debug/extension/merutable/merutable.duckdb_extension
build/release/extension/merutable/merutable.duckdb_extension
```

### Build caching

[sccache](https://github.com/mozilla/sccache) is wired in automatically. DuckDB is ~500k lines of C++ — the first build takes ~10 minutes. Every subsequent build after `just clean` takes ~30 seconds from cache.

### Workspace path

By default the build looks for merutable at `../../merutable/merutable` relative to this repo. Override:

```sh
MERUTABLE_WORKSPACE=/path/to/merutable just build
# or permanently in your shell:
export MERUTABLE_WORKSPACE=/path/to/merutable
```

---

## Loading

```sh
duckdb -unsigned
```

```sql
LOAD '/path/to/merutable-ddb-extension/build/release/extension/merutable/merutable.duckdb_extension';
```

---

## Usage

### Writing

```sql
COPY (SELECT 1::BIGINT AS id, 'alice'::VARCHAR AS name, 0.95::DOUBLE AS score)
TO '/tmp/mydb' (FORMAT merutable, TABLE_NAME 'users', PRIMARY_KEY 'id');
```

Options:
- `TABLE_NAME` — logical table name (required for new databases, inferred from path otherwise)
- `PRIMARY_KEY` — comma-separated column names for the primary key (default: first column)

After writing, the extension calls `flush` + `export_iceberg` automatically so the data is immediately readable via `merutable_scan`.

### Reading (full scan)

```sql
SELECT * FROM merutable_scan('/tmp/mydb');
SELECT * FROM merutable_scan('/tmp/mydb') WHERE score > 0.9;
SELECT COUNT(*) FROM merutable_scan('/tmp/mydb');
```

### Point lookup

```sql
-- Returns 0 or 1 row
SELECT * FROM merutable_get('/tmp/mydb', 1::BIGINT);

-- Miss returns empty result set
SELECT * FROM merutable_get('/tmp/mydb', 999::BIGINT);
```

`merutable_get` goes through the Rust engine's full read path: memtable → bloom filter → sparse index → Parquet page. It sees unflushed writes that `merutable_scan` cannot.

---

## How `merutable_scan` works

merutable is an LSM engine — at any point multiple Parquet files at different levels (L0, L1, L2…) may each contain versions of the same row. A naive `SELECT *` would return duplicates and deleted rows as valid data.

`merutable_scan` applies the MVCC dedup projection automatically:

```sql
-- What merutable_scan('/tmp/mydb') expands to internally:
SELECT <user_columns>
FROM read_parquet(['/tmp/mydb/data/L0/a.parquet', '/tmp/mydb/data/L1/b.parquet', ...],
                 union_by_name=true)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY <primary_key_columns>
    ORDER BY _merutable_seq DESC
) = 1
  AND _merutable_op = 1
```

- `_merutable_seq` — monotonic sequence number; highest = newest version of a key
- `_merutable_op` — `1` = Put (live row), `0` = Delete tombstone

The `QUALIFY` clause keeps only the newest version of each key and discards tombstones.

### Predicate pushdown

`merutable_scan` executes the dedup SQL via a fresh DuckDB `Connection`. DuckDB's planner treats the inner `read_parquet(...)` as a normal Parquet scan and applies its full pushdown stack to it:

- **Projection pushdown** — only the columns referenced in your query are read from each Parquet file. Columns you don't select are never decoded.
- **Filter pushdown** — `WHERE` predicates on non-PK columns are pushed into the Parquet reader as row-group and page-level filters. Parquet's column statistics (min/max per row group) allow entire row groups to be skipped without reading them.
- **The QUALIFY window function does not prevent pushdown** — DuckDB evaluates filters before the window function where possible, so `WHERE score > 0.9` prunes at the Parquet level before the dedup step runs.

What pushdown does **not** cover: merutable's L0 files use small 8 KiB pages optimised for random KV access, while L1+ files use large 32–128 MiB row groups optimised for columnar scan. DuckDB's pushdown works on both, but L0 files will have more row groups to evaluate statistics across. For analytical queries over large datasets, L1+ files (after compaction) will be significantly faster.

### Why `merutable_get` sees more data

`merutable_scan` reads from flushed Parquet files only. `merutable_get` calls into the live Rust engine which also checks the in-memory memtable — rows written since the last flush are visible immediately via `get` but not yet via `scan`. Call `COPY` (which triggers a flush) to make new data visible to `scan`.

---

## Architecture

```
merutable-ddb-extension (C++)
├── merutable_scan.cpp     — TableFunction: reads manifest via FFI, builds dedup SQL,
│                            executes via a new Connection to avoid context lock deadlock
├── merutable_get.cpp      — TableFunction: reads schema via FFI, delegates to HandleCache
├── merutable_copy.cpp     — CopyToFunction: opens MeruDB via FFI, streams DataChunks as put_batch
├── handle_cache.cpp       — LRU cache of open MeruHandle* (capacity 8), one shared MeruRuntime
└── meru_utils.hpp         — C++ wrappers: meru_checked(), MeruString, MeruRowGuard

merutable-capi (Rust, pre-built static lib)
├── meru_manifest_info()   — reads manifest (JSON or MRUB protobuf), returns schema + file paths
├── meru_open_existing()   — opens a db reading schema from manifest
├── meru_put_batch()       — WAL-synced batch write
├── meru_flush()           — memtable → Parquet
└── meru_export_iceberg()  — writes metadata/vN.metadata.json for scan visibility
```

The Rust static library (`libmerutable_capi.a`) must be built before the DuckDB extension:

```sh
just build-capi   # builds crates/merutable-capi inside the nix shell
just build        # builds capi then the extension
```

---

## Development

```sh
just shell          # enter nix dev shell
just build          # build extension (incremental, sccache)
just clean          # wipe build/ (next build pulls from sccache)
just cache-stats    # show sccache hit/miss counts
just ext-path       # print path to built extension binary
```
