# merutable DuckDB extension

DuckDB extension for [merutable](https://github.com/merutable/merutable) — an embeddable single-table LSM engine that stores data as Parquet files with Iceberg-compatible metadata.

Provides four capabilities:

| Function / Statement | Type | Description |
|---|---|---|
| `merutable_scan(path)` | Table function | Full table scan with MVCC dedup applied automatically |
| `merutable_get(path, key)` | Table function | O(log N) point lookup via the Rust engine (hits live memtable) |
| `COPY ... TO path (FORMAT merutable)` | Copy function | Write rows into a merutable table |
| `ATTACH / CREATE TABLE / INSERT INTO` | StorageExtension | First-class DuckDB table syntax for attach-based workflows |

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

### Attach-based workflow (StorageExtension)

For first-class DuckDB table syntax, attach the database first:

```sql
-- Attach an existing database (or create a new one)
ATTACH '/tmp/mydb' AS mydb (TYPE merutable);

-- Create a table (defines schema; opens the Rust engine handle)
CREATE TABLE mydb.users (id BIGINT PRIMARY KEY, name VARCHAR);

-- Standard DuckDB INSERT INTO
INSERT INTO mydb.users VALUES (1, 'alice');
INSERT INTO mydb.users VALUES (2, 'bob');

-- Standard SELECT
SELECT * FROM mydb.users WHERE name = 'alice';

-- DETACH closes the handle (flush + iceberg export happen automatically)
DETACH mydb;
```

The attached database stays open as a live `MeruHandle` for the duration of the session (ATTACH → DETACH). Inserts write directly to the open handle — no open/close overhead per statement. `FlushAndExport` is called after each INSERT statement so data is immediately visible to `merutable_scan` and `merutable_get`.

The `COPY` path still works on attached databases:

```sql
COPY (SELECT 3::BIGINT AS id, 'carol'::VARCHAR AS name)
TO '/tmp/mydb' (FORMAT merutable, TABLE_NAME 'users', PRIMARY_KEY 'id');
```

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

### How a WHERE clause travels through the stack

When you run `SELECT * FROM merutable_scan('/tmp/mydb') WHERE id > 5 AND id < 100`, here is exactly what happens:

1. **Outer query bind** — DuckDB calls `MerutableScanBind`. It reads the manifest via `meru_manifest_info()`, builds the static dedup SQL string, and stores it in `FunctionData`. No filter information is available at this stage.

2. **Optimizer** — Because `merutable_scan`'s `TableFunction` has `filter_pushdown = false` (the default), DuckDB's pushdown optimizer wraps the scan in a `LogicalFilter` node instead of injecting the predicate into `LogicalGet::table_filters`. The dedup SQL is already frozen; nothing is added to it.

3. **Init** — `MerutableScanInit` receives `TableFunctionInitInput::filters = nullptr`. It creates a fresh `Connection(*context.db)` and executes the dedup SQL as-is, returning **all** current rows.

4. **Post-scan filter** — The `LogicalFilter(id > 5 AND id < 100)` is compiled into a `PhysicalFilter` operator sitting above the scan in the pipeline. DuckDB evaluates the predicate over each chunk emitted by the scan and discards non-matching rows here — **not** at the storage layer.

5. **Inside the sub-connection** — The dedup SQL executes in a separate `Connection` context. Within that inner query, DuckDB *does* push predicates into `read_parquet()` — but only predicates that appear in the inner SQL itself (e.g. from the `QUALIFY` clause). Your outer `WHERE id > 5` never reaches the inner query.

The practical implication: **primary-key range predicates from the outer query do not prune Parquet files or row groups today**. All Parquet files are read, all rows are deduped, and then the range is applied as a post-filter.

### Potential optimisation: direct range scan via `meru_scan()`

The Rust engine exposes a bounded range scan:

```c
meru_scan(handle, start_pk, start_count, end_pk, end_count, &result, &err)
```

Both `start_pk` and `end_pk` can be `NULL` for open-ended scans. This operates on the live `MeruHandle` — it sees both the memtable and all compacted Parquet files, and applies MVCC dedup internally. For a primary-key range query it would skip unrelated data at the engine level rather than reading and post-filtering.

To wire this up, two things would need to change:

1. **Opt in to filter pushdown** — Set `tf.filter_pushdown = true` (or register a `pushdown_complex_filter` callback) on the `TableFunction`. This tells DuckDB's optimizer to place matching predicates into `LogicalGet::table_filters` as `ConstantFilter` objects rather than wrapping them in a post-scan `LogicalFilter`. A range `id > 5 AND id < 100` on the same column arrives as a `ConjunctionAndFilter` containing two `ConstantFilter` children with `comparison_type = COMPARE_GREATERTHAN` / `COMPARE_LESSTHAN`.

2. **Read bounds in `MerutableScanInit`** — Check `input.filters` for `ConstantFilter` entries on the PK column index. Convert the `Value` to a `MeruValue`, call `meru_scan(handle, start, 1, end, 1, &result, &err)`, and stream `result->entries` as DataChunks — bypassing the Parquet SQL path entirely for that query.

The main complication: `meru_scan()` needs a live `MeruHandle`, which is only held by `MerutableCatalog` (the ATTACH path). For `merutable_scan('/path')` (the standalone function path), you would need to acquire a handle from `HandleCache::Get()`. A hybrid is also possible: use `meru_scan()` for the in-memory tier and the filtered Parquet SQL for the durable tier, then union-dedup the results — but that adds complexity.

### Why `merutable_get` sees more data

`merutable_scan` reads from flushed Parquet files only. `merutable_get` calls into the live Rust engine which also checks the in-memory memtable — rows written since the last flush are visible immediately via `get` but not yet via `scan`. Call `COPY` (which triggers a flush) to make new data visible to `scan`.

### SELECT via catalog (`mydb.users`) vs `merutable_scan('/path')`

When you use the ATTACH path and run `SELECT * FROM mydb.users`, the scan goes through `MerutableTableEntry::GetScanFunction`. Under the hood it calls the same `BindWithPath` → dedup SQL path as `merutable_scan`. The cost is identical: one `meru_manifest_info` disk read, one fresh `Connection`, one `read_parquet` execution over all Parquet files.

The attached database holds the `MeruHandle` open, but that handle is not used for SELECT today — the Parquet path reads files directly via the manifest. The handle is only used by INSERT (via `WriteRows`) and closes cleanly on DETACH.

---

## Architecture

```
merutable-ddb-extension (C++)
├── merutable_extension.cpp  — Extension entry point; registers all functions + StorageExtension
├── merutable_scan.cpp       — TableFunction: reads manifest via FFI, builds dedup SQL,
│                              executes via a new Connection to avoid context lock deadlock;
│                              also exposes BindWithPath() for catalog-based SELECT
├── merutable_get.cpp        — TableFunction: reads schema via FFI, delegates to HandleCache
├── merutable_copy.cpp       — CopyToFunction: opens MeruDB via FFI, streams DataChunks as put_batch
├── merutable_storage.cpp    — StorageExtension: MerutableCatalog, MerutableSchema,
│                              MerutableTableEntry, MerutableInsertOperator,
│                              MerutableTransactionManager
├── handle_cache.cpp         — LRU cache of open MeruHandle* (capacity 8), one shared MeruRuntime
└── meru_utils.hpp           — C++ wrappers: meru_checked(), MeruString, MeruRowGuard,
                               DuckTypeToMeru(), WriteRows()

merutable-capi (Rust, pre-built static lib)
├── meru_manifest_info()     — reads manifest (JSON or MRUB protobuf), returns schema + file paths
├── meru_open()              — opens/creates a db with an explicit schema
├── meru_open_existing()     — opens a db reading schema from manifest
├── meru_put_batch()         — WAL-synced batch write
├── meru_scan()              — bounded range scan (start_pk..end_pk); sees memtable + Parquet
├── meru_flush()             — memtable → Parquet
└── meru_export_iceberg()    — writes metadata/vN.metadata.json for scan visibility
```

### StorageExtension lifecycle

```
ATTACH '/path' AS mydb (TYPE merutable)
  └─ MerutableStorageExtension::attach()
       └─ creates MerutableCatalog(db, path)
            └─ Initialize() creates MerutableSchema("main")

CREATE TABLE mydb.users (id BIGINT PRIMARY KEY, name VARCHAR)
  └─ MerutableSchema::CreateTable()
       ├─ builds MeruColumnDef[] + MeruSchema from DuckDB CreateTableInfo
       ├─ meru_open() → MeruHandle* (kept open in MerutableCatalog::db_handle_)
       └─ stores MerutableTableEntry in schema table map

INSERT INTO mydb.users VALUES (...)
  └─ MerutableCatalog::PlanInsert()
       └─ MerutableInsertOperator::Sink()   (per DataChunk)
            └─ WriteRows(catalog.Handle(), chunk, col_types)
                 └─ meru_put_batch()
       └─ MerutableInsertOperator::Finalize()
            └─ MerutableCatalog::FlushAndExport()
                 ├─ meru_flush()           (memtable → Parquet)
                 └─ meru_export_iceberg()  (update manifest)

SELECT * FROM mydb.users
  └─ MerutableTableEntry::GetScanFunction()
       └─ MerutableScanFunction::BindWithPath()
            └─ BindFromPath()
                 ├─ meru_manifest_info()   (manifest disk read)
                 ├─ builds dedup SQL over read_parquet([...])
                 └─ MerutableScanInit() executes SQL via new Connection

DETACH mydb
  └─ ~MerutableCatalog()
       ├─ meru_flush()
       ├─ meru_export_iceberg()
       └─ meru_close_free()
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
