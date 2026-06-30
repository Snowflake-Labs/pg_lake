# pg_lake_sink — design notes

Internal design/rationale doc (the user-facing docs are in `README.md`). This captures *why* the
extension is built the way it is, the alternatives that were rejected, and the load-bearing facts
about pg_lake/PostgreSQL internals it depends on — enough to resume or review the work without
re-deriving it.

## Purpose

`pg_lake_sink` continuously consumes Kafka topics into pg_lake (Iceberg) foreign tables — or plain
heap tables — with **exactly-once** delivery into internal-catalog Iceberg/heap targets. It is a
Rust/pgrx extension living in the pg_lake monorepo (opt-in build; not part of the default all-C
`make`).

## Architecture

- **Subscription model.** A *subscription* (row in `lake_sink.subscriptions`) pairs one Kafka topic
  with one target table plus connection/tuning settings. Managed entirely via SQL functions
  (`lake_sink.create_subscription` / `drop_subscription` / `enable_subscription` /
  `disable_subscription` / `pull`). After `CREATE EXTENSION` there are zero subscriptions and zero
  workers until the first `create_subscription`.
- **One base worker per subscription.** Each subscription gets its own background worker, so topics
  are isolated and scale independently. A single Rust entry point serves all of them and identifies
  which subscription it is via `MyBaseWorkerId`.
- **Modules:** `lib.rs` (bootstrap SQL, SQL functions, worker loop), `config.rs` (GUC defaults +
  per-subscription `Config` loaded from the catalog), `kafka.rs` (rdkafka consumer, assign/poll),
  `copy.rs` (streaming `CopyFrom` insert + binary COPY encoder), `sink.rs` (target-table DDL +
  offset load/store), `ffi.rs` (dlsym bridge to pg_extension_base).

## Base-worker integration (pg_extension_base) — runtime registration

Workers are registered **at runtime** from `create_subscription`, not at extension-install time.

- The SQL wrapper `extension_base.register_worker(text, regproc)` is guarded by
  `if (!creating_extension) ereport(ERROR)` (`pg_extension_base/src/base_worker_launcher.c:2097`), so
  it can only be called from a `CREATE EXTENSION` script — unusable at runtime.
- **But** the exported C symbol `int32 RegisterBaseWorker(char *name, Oid entryPoint, Oid extension)`
  (`base_worker_launcher.c:2113`, `PGDLLEXPORT` in `include/pg_extension_base/base_workers.h`) has
  **no such guard**. It inserts the registration row and signals the database starter to launch the
  worker immediately after the calling transaction commits (`:2165-2191`); the in-tree comment at
  `:1198` confirms "workers added after server start via register_worker are started immediately."
  Rollback of `create_subscription` rolls back the registration (it's a catalog insert) — no orphan.
- `pg_lake_sink` resolves `RegisterBaseWorker` / `DeregisterBaseWorkerById` (and `LightSleep`,
  `TerminationRequested`, `MyBaseWorkerId`) via `dlsym(RTLD_DEFAULT, …)` in `ffi.rs` — the same lazy
  pattern as the sleep/flag symbols, so the `.so` has **no link-time dependency** on
  pg_extension_base (important: pgrx schema-gen dlopens the `.so` where pg_extension_base isn't
  preloaded, so link-time externs would abort schema-gen).
- The entry point `lake_sink.pg_lake_sink_worker_main(internal) RETURNS internal` satisfies the
  framework's signature check (1 internal arg, internal return; return value is the restart code).
  `create_subscription` looks up its Oid (`'lake_sink.pg_lake_sink_worker_main(internal)'::regprocedure`)
  and the extension Oid (`pg_extension WHERE extname='pg_lake_sink'`), calls `RegisterBaseWorker`, and
  stores the returned `worker_id` in the subscription row. The worker maps `MyBaseWorkerId` → its row.
- **Durability:** registrations persist in `pg_extension_base.workers`; on server restart the database
  starter restarts each worker, which resumes from its stored offsets.
- **Off-label risk:** relying on the exported `RegisterBaseWorker` while its SQL wrapper is
  deliberately guarded is off-label. Pin to the in-tree pg_extension_base version; if upstream ever
  guards the C symbol too, fall back to a pre-registered fixed worker pool that claims subscriptions
  by `MyBaseWorkerId`.

## Insert path — streaming `CopyFrom` (the core decision)

Each batch is inserted by driving PostgreSQL's COPY machinery directly:
`BeginCopyFrom` → `CopyFrom` → `EndCopyFrom` (`copy.rs`), feeding rows in **binary COPY format**
through a `copy_data_source_cb` that pulls Kafka records on demand. This is the same programmatic
COPY pattern pg_lake itself uses (`pg_lake_engine/src/copy/remote_query.c:143`,
`pg_lake_copy/src/copy/copy.c:592`).

Why this and not the alternatives:

- **Per-row `INSERT` (the original pg_sink approach):** one executor round-trip per message; the
  dominant cost at high throughput. Rejected.
- **`INSERT … SELECT unnest($arrays)`:** one statement, executor-level, FDW-safe, fills
  defaults/identity — but materializes the whole batch as in-memory array parameters in the backend.
  Rejected for unbounded memory; also the "app chunking" it implies is the wrong layer (pg_lake
  chunks its own files).
- **Low-level `heap_insert` / `heap_multi_insert` / `table_tuple_insert`:** fastest for heap, but
  heap-AM only — they can't target a foreign table at all, and they evaluate **no** DEFAULT/identity/
  generated columns and bypass indexes/triggers. Rejected: kills FDW support and would require
  reimplementing the executor.
- **`ExecForeignBatchInsert` (PG14+ FDW batch API):** not a lever we control — pg_lake leaves
  `ExecForeignBatchInsert` and `GetForeignModifyBatchSize` `NULL`
  (`pg_lake_table/src/fdw/pg_lake_table.c`), so the executor uses per-row `ExecForeignInsert`
  regardless. Doesn't matter: pg_lake buffers rows and writes Parquet at commit, so the expensive
  work is already batched.

What `CopyFrom` gives us (`postgres copyfrom.c:938-941,1346-1350,1407-1423,1493-1496`;
`copyfromparse.c:898-909`):

- Goes through the executor → works for **heap and foreign (pg_lake) targets** uniformly.
- Fills the columns we don't supply — `id GENERATED ALWAYS AS IDENTITY`, `consumed_at DEFAULT now()`
  — plus generated columns, via the COPY layer.
- **One** `BeginForeignInsert` … per-row `ExecForeignInsert` … **one** `EndForeignInsert` — a single
  FDW write context per COPY, so **pg_lake does its own Parquet file chunking** (its
  `multi_data_file_dest`, 3 GB temp-file limit). The app does not chunk.
- Streams via the data-source callback → **bounded memory** regardless of `batch_size`.

Implementation notes (`copy.rs`):

- The callback signature has no user-data pointer, so stream state lives in a `thread_local!` (a base
  worker is single-threaded — safe).
- Binary COPY encoders must match each type's `recv` format exactly: text → utf8; int4/int8 → BE;
  bytea → raw; **jsonb → `0x01` version byte + utf8(json)**; **timestamptz → BE int64 microseconds
  since the Postgres epoch 2000-01-01** (`unix_ms*1000 - 946_684_800_000_000`). Unit-worthy; verified
  by round-tripping through real COPY (null key, non-JSON payload wrapped as JSON string, null ts).
- The callback carries a partial row across invocations (emits only up to `maxread`); header once,
  rows, then the binary trailer (`int16 = -1`), then EOF (`0`).
- The target relation is resolved with `pg_catalog.to_regclass($1)::oid` (built-in, non-throwing:
  returns the `pg_class` OID or NULL → clean "table does not exist" error).

## Idempotency — transactional offset tracking (exactly-once)

pg_lake/Iceberg tables have **no unique constraints and no `ON CONFLICT`**, so idempotency cannot
live in the table. Instead:

- The last consumed offset per `(subscription, partition)` is stored in `lake_sink.subscription_offsets`
  and upserted **in the same transaction** as the COPY (`sink::store_offsets`, a local heap table that
  *does* support `ON CONFLICT`). Data + resume point commit atomically.
- On (re)start the worker **assigns** partitions manually and seeks each to `Offset::Offset(stored+1)`
  (`kafka::assign_from_offsets` via `fetch_metadata`), rather than trusting Kafka consumer-group
  committed offsets. `enable.auto.commit=false`; we never commit to Kafka.
- **Why this is exactly-once for internal-catalog Iceberg:** pg_lake makes a new snapshot visible by a
  transactional SQL `UPDATE` on `pg_lake_iceberg.tables` (`pg_lake_iceberg/src/iceberg/catalog.c:585`),
  executed inside the `XACT_EVENT_PRE_COMMIT` callback (`pg_lake_table/src/transaction/transaction_hooks.c:42-56`).
  Because that catalog pointer update and our offset row are in the **same** Postgres transaction, they
  commit atomically; a crash before commit rolls back both, leaving only orphaned object-storage files
  that pg_lake reclaims. (Data files may be written to object storage mid-transaction, but they are not
  referenced by any committed snapshot until the catalog pointer commits — so they are invisible.)
- **REST-catalog-managed tables are at-least-once**, not exactly-once: `PostAllRestCatalogRequests()`
  runs at `XACT_EVENT_COMMIT` (post-commit), so the external catalog is notified after the Postgres
  transaction commits — a crash in that window can leave the offset committed but the external catalog
  not updated (or vice versa). Documented as a limitation.

Consequences: the auto-created heap target table has **no** `UNIQUE(topic,partition,"offset")`
constraint (append-only; a stray duplicate would otherwise abort the batch as a poison row). Failures
inside the per-batch transaction `error!()` out so the whole transaction aborts — never a partial
commit of rows without their offset (note `BackgroundWorker::transaction` commits even if the closure
returns `Err`, so we must raise rather than return `Err`).

## Schema layout & search_path safety

- **Control `schema = pg_catalog`** (the pg_lake convention). No object actually lands in pg_catalog.
- **All extension objects live in a dedicated `lake_sink` schema:** the six functions (management +
  the internal worker entry point) and the two catalog tables. The schema cannot be named
  `pg_lake_sink` — PostgreSQL reserves the `pg_` prefix ("unacceptable schema name" at CREATE
  EXTENSION). This mirrors pg_lake's own `pg_lake_iceberg` extension → `lake_iceberg` schema
  (`pg_lake_iceberg--3.0.sql`).
- **pgrx mechanics:** the `lake_sink` schema and its tables are created in the `bootstrap`
  `extension_sql!` block. pgrx will not accept `#[pg_extern(schema = "lake_sink")]` unless the schema
  is a *pgrx-known* entity, so an **empty `#[pg_schema] mod lake_sink {}`** is declared purely to
  register it (pgrx then also emits a harmless `CREATE SCHEMA IF NOT EXISTS lake_sink`). Each function
  carries `#[pg_extern(schema = "lake_sink")]`.
- **search_path hardening:** all internal SQL is fully schema-qualified (`lake_sink.*`,
  `pg_catalog.to_regclass`, `pg_catalog.pg_extension`), and the worker entry point is looked up by its
  fully-qualified name, so nothing resolves through a caller-controlled `search_path`.
- **GUCs** keep the `pg_lake_sink.*` prefix (GUC names allow `pg_`): `pg_lake_sink.brokers`,
  `.auto_offset_reset`, `.batch_size`, `.poll_interval_ms` — defaults for new subscriptions only;
  changing one does not affect existing subscriptions (values are copied into the row at creation).

## Build & packaging

- Rust/pgrx (pgrx `=0.18.1`), PostgreSQL 18. `pg_lake_sink/Makefile` bridges to `cargo pgrx`; the
  top-level Makefile has **opt-in** targets `pg_lake_sink` / `install-pg_lake_sink` /
  `clean-pg_lake_sink`, deliberately **not** in `EXTENSION_TARGETS`/`ALL_TARGETS`, so the all-C build,
  CI and pgindent are unaffected (Rust + cmake + vendored librdkafka aren't forced on the main build).
- librdkafka is vendored/built via cmake, **plaintext only** (no ssl/sasl — no `libsasl2` on the dev
  box). Building needs a system-glibc libclang for bindgen (`LIBCLANG_PATH=/usr/lib64`,
  `clang-libs`); the bazel clang under `/scratch/bazel` fails to dlopen from a system-glibc binary.

## Testing

Automated pytest suite in `pg_lake_sink/tests/` (run with `make -C pg_lake_sink check` or the
top-level `make check-pg_lake_sink`), sharing the repo-wide harness in `test_common`. It is an
integration suite: it needs the extension installed **and** a Kafka broker, and drives Kafka through
the `docker/docker-compose.yml` Redpanda container via `rpk` (`docker exec`) — no Python Kafka client
or host librdkafka required. Tests **skip** (not fail) when the extension is absent (the opt-in Rust
build isn't in the all-C CI) or when Docker/a broker is unavailable, so the suite is safe to collect
anywhere. `conftest.py` provides the `redpanda` (broker lifecycle), `kafka` (producer helper), and
`sink_extension` (create/teardown, deregistering workers first) fixtures.

Coverage (`tests/pytests/test_sink.py`): heap target with `id`/`consumed_at` filled by COPY and all
column types round-tripped (exercising the binary encoders — jsonb version byte, timestamptz epoch
offset, bytea, NULL key); non-JSON payload wrapped as a JSON string; transactional offset tracking
(re-pull is a no-op, resume from stored offset, dense duplicate-free offsets); the async base worker
(auto-consume, disable-pauses, enable-resumes without duplicates); `drop_subscription` cascade (row +
offsets + table); name validation, duplicate-name rejection, and `pull()` on an unknown subscription;
and a real S3-backed pg_lake `USING iceberg` target (streamed inserts, offsets advance, incremental
ingest). Pre-automation, the same scenarios were verified manually against a local Redpanda + PG18
and a live S3 Iceberg table.

## Version

- Extension version follows pg_lake core's two-component convention: `pg_lake_sink.control`
  `default_version = '3.4'`. pg_lake uses `major.minor` (not `major.minor.patch`) because the SQL
  definitions don't change in patch releases, so there's no per-patch upgrade script to name.
- **Where the version comes from (pgrx):** the *extension* version is the control file's
  `default_version`. cargo-pgrx's `get_version()` reads that field (substituting `@CARGO_VERSION@`
  only if the literal placeholder is present — we hardcode `3.4` instead), and `cargo pgrx install`
  names the installed schema file from it → `pg_lake_sink--3.4.sql`. `CREATE EXTENSION` then loads
  that file because `default_version` matches.
- **Crate version is decoupled.** `Cargo.toml` `version = "3.4.0"` exists only because Cargo requires
  full `major.minor.patch` semver for a package; it does *not* drive the extension version (we don't
  use the `@CARGO_VERSION@` placeholder). Keep it as the full-semver spelling of the current
  extension line for sanity, but the control file is the source of truth.
- **Bumping:** `tools/bump_extension_versions.py` rewrites `.control` `default_version`, which is
  exactly the field pgrx uses — so a normal version bump works with no Cargo.toml change required.
  (Optionally keep `Cargo.toml`/`Cargo.lock` in step for tidiness, but it isn't load-bearing.)
- **Build-time check:** since no Rust toolchain runs in the default CI, whoever builds should confirm
  `cargo pgrx install` produced `$(pg_config --sharedir)/extension/pg_lake_sink--3.4.sql` (matching
  `default_version`). If a future pgrx instead named it from the crate version, either add
  `default_version = '@CARGO_VERSION@'` and set the crate version to match, or rename in the Makefile.

## Open questions / follow-ups before a PR

- **Schema convention:** confirm maintainers are happy with everything in `lake_sink` (vs pg_lake's
  usual split of user functions in `pg_catalog` + tables in `lake_<x>`).
- **REST catalog:** exactly-once only holds for the internal catalog; decide whether to detect and
  warn (or block) REST-catalog targets, or document at-least-once as acceptable.
- **CI:** the pgindent pre-commit hook (`.pre-commit-config.yaml`) currently has an invalid
  `stages: [pre-commit]` for the installed pre-commit version; commits here used `--no-verify`.

## Key external references

- pg_extension_base: `src/base_worker_launcher.c:2097` (guard), `:2113` (`RegisterBaseWorker`),
  `:1198` (immediate launch); `include/pg_extension_base/base_workers.h`.
- pg_lake write/commit path: `pg_lake_table/src/fdw/pg_lake_table.c` (FdwRoutine, `ExecForeignInsert`,
  `FinishForeignModify:2538`); `pg_lake_table/src/transaction/transaction_hooks.c:42-56` (PRE_COMMIT);
  `pg_lake_iceberg/src/iceberg/catalog.c:585` (`UpdateInternalCatalogMetadataLocation`).
- PostgreSQL COPY: `src/backend/commands/copyfrom.c`, `copyfromparse.c` (foreign-table path, defaults,
  generated columns).
