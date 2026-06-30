# pg_lake_sink

A PostgreSQL extension, written in Rust with [pgrx](https://github.com/pgcentralfoundation/pgrx),
that consumes messages from **Kafka** topics and writes them into Postgres tables.

Each **subscription** pairs one topic with one target table and runs its own continuous
**background worker** on pg_lake's [`pg_extension_base`](../pg_extension_base)
base-worker framework. Subscriptions are created and managed at runtime through SQL; there is
also an on-demand **`lake_sink.pull()`** function for testing and backfill.

## How it works

```
                         ┌─ subscription "a" ─▶ worker ─▶ CopyFrom (stream) ─▶ lake_sink.a
Kafka topics ──▶ librdkafka BaseConsumer(s) ──┤                                + offsets (same txn)
                         └─ subscription "b" ─▶ worker ─▶ CopyFrom (stream) ─▶ my_lake.b (pg_lake)
```

* `lake_sink.create_subscription(...)` records the subscription in `lake_sink.subscriptions`,
  optionally creates the target table, and registers a dedicated base worker **at runtime** by
  calling `pg_extension_base`'s exported `RegisterBaseWorker` over FFI. The worker launches as soon
  as the creating transaction commits (and is restarted automatically on server restart).
* One Rust entry point `lake_sink.pg_lake_sink_worker_main(internal)` serves every subscription. Each
  worker identifies itself by `MyBaseWorkerId` and loads its own row from
  `lake_sink.subscriptions`. Framework symbols (`LightSleep`, `TerminationRequested`,
  `MyBaseWorkerId`, `RegisterBaseWorker`, `DeregisterBaseWorkerById`) are resolved at runtime via
  `dlsym`, so the `.so` carries no link-time dependency on `pg_extension_base`.
* Each batch is inserted by **streaming rows into a single `CopyFrom`** on the target table (driven
  directly via FFI, encoding rows in COPY binary format). This is the FDW-safe bulk path: it goes
  through the executor, so it works for plain heap tables **and** pg_lake foreign tables, fills the
  `id`/`consumed_at` columns we don't supply, and is one write context — so **pg_lake does its own
  Parquet file chunking**. Memory stays bounded regardless of `batch_size`.
* Each message becomes one row: the value is stored as `payload jsonb` (wrapped as a JSON string
  if it is not valid JSON) alongside `topic, partition, "offset", key, headers, kafka_timestamp`.
* **Idempotency is offset tracking, not table constraints.** The worker stores the last consumed
  `(partition → offset)` in `lake_sink.subscription_offsets` **in the same transaction** as the
  inserted rows, and on startup assigns partitions and resumes from those offsets (it does not use
  Kafka consumer-group commits). For internal-catalog pg_lake/Iceberg and heap targets this is
  **exactly-once** — the data and the offset commit atomically; a crash rolls both back, leaving
  only orphaned object-storage files that pg_lake reclaims. (Tables managed by an external **REST**
  Iceberg catalog get at-least-once, since that catalog is notified after the Postgres commit.)
* Each subscription defaults to its own consumer group id (`pg_lake_sink_<name>`).

## Target tables

`create_subscription` makes one table per subscription (default name `lake_sink.<name>`) with
this layout, unless you pass `create_target_table => false` to point at an existing table (e.g. a
pre-created **pg_lake** foreign table with the same columns):

| column          | type          | notes                              |
|-----------------|---------------|------------------------------------|
| id              | bigint        | identity PK (filled by COPY)       |
| topic           | text          |                                    |
| partition       | int           |                                    |
| "offset"        | bigint        |                                    |
| key             | bytea         | message key                        |
| payload         | jsonb         | message value                      |
| headers         | jsonb         | message headers                    |
| kafka_timestamp | timestamptz   | broker/produce timestamp           |
| consumed_at     | timestamptz   | default now() (filled by COPY)     |

The table is append-only (no unique constraint — idempotency comes from offset tracking). The
catalogs `lake_sink.subscriptions` (one row per subscription) and `lake_sink.subscription_offsets`
(per-partition resume points) are created at `CREATE EXTENSION`.

## Managing subscriptions

```sql
-- topic 'events' -> table lake_sink.events, group pg_lake_sink_events, defaults from GUCs
SELECT lake_sink.create_subscription('events', 'events');

-- override any default; point at an existing pg_lake (Iceberg) foreign table
SELECT lake_sink.create_subscription(
    name                => 'orders',
    topic               => 'orders.v2',
    brokers             => 'broker1:9092,broker2:9092',
    target_table        => 'my_lake.orders_raw',
    create_target_table => false,   -- table already exists (e.g. a pg_lake foreign table)
    auto_offset_reset   => 'latest',
    batch_size          => 100000   -- rows per transaction = rows per pg_lake snapshot
);

SELECT lake_sink.disable_subscription('orders');  -- pause (worker idles)
SELECT lake_sink.enable_subscription('orders');   -- resume
SELECT lake_sink.drop_subscription('orders');             -- stop worker, forget subscription
SELECT lake_sink.drop_subscription('orders', drop_table => true);  -- also drop the table
```

Editing a row in `lake_sink.subscriptions` (e.g. `UPDATE ... SET batch_size = 200000`) is picked
up by the worker on its next cycle; changing brokers/topic/group/offset-reset rebuilds and
re-assigns its consumer. `batch_size` bounds the rows per transaction — i.e. per pg_lake Iceberg
snapshot — so for lake targets set it large; pg_lake sizes the Parquet files within that.

## Configuration (defaults for new subscriptions)

These GUCs only supply **defaults** to `create_subscription`; per-subscription values live in the
catalog row once created. Changing a GUC does not affect existing subscriptions.

| GUC                         | default           |
|-----------------------------|-------------------|
| `pg_lake_sink.brokers`           | `localhost:19092` |
| `pg_lake_sink.auto_offset_reset` | `earliest`        |
| `pg_lake_sink.batch_size`        | `500`             |
| `pg_lake_sink.poll_interval_ms`  | `1000`            |

## Build & install

`pg_lake_sink` is an **opt-in** Rust/pgrx extension; it is not part of pg_lake's default `make`
build. It needs the Rust toolchain, `cargo-pgrx` (matching the `pgrx` dep version), and a libclang
that matches the system glibc (`LIBCLANG_PATH=/usr/lib64` on this box — see note below).

From the monorepo root (opt-in target), or from this directory directly:

```bash
export LIBCLANG_PATH=/usr/lib64
make install-pg_lake_sink                 # from the pg_lake root, or:
cargo pgrx install --release --pg-config <path>/bin/pg_config   # from pg_lake_sink/
```

Add the libraries to the cluster and restart:

```
# postgresql.conf
shared_preload_libraries = 'pg_extension_base,pg_lake_sink'
```

```sql
CREATE EXTENSION pg_extension_base;   -- dependency
CREATE EXTENSION pg_lake_sink;             -- creates schema lake_sink + the subscriptions catalog
```

After `CREATE EXTENSION` there are no subscriptions and no workers until the first
`create_subscription` call.

## Local Kafka (Redpanda)

```bash
docker compose -f docker/docker-compose.yml up -d
scripts/produce.sh events 10          # create topic + produce 10 JSON messages
```

## Verify

```sql
SELECT lake_sink.create_subscription('events', 'events');
SELECT * FROM extension_base.list_base_workers();   -- shows pg_lake_sink_events
SELECT count(*) FROM lake_sink.events;
SELECT * FROM lake_sink.subscription_offsets;       -- resume points advance as rows commit

-- on-demand pull for a subscription (resumes from / advances its stored offsets, so disable the
-- worker first to avoid both consuming the same partitions)
SELECT lake_sink.disable_subscription('events');
SELECT lake_sink.pull('events', 100);
```

## Tests

```bash
# Runs the pytest suite (shares the repo-wide harness in ../test_common).
make -C pg_lake_sink check        # or, from the repo root: make check-pg_lake_sink
```

These are integration tests: they need the extension installed **and** a Kafka broker, and drive
Kafka through the `docker/docker-compose.yml` Redpanda container via `rpk` (so no Python Kafka client
is required). Individual tests **skip** rather than fail when `pg_lake_sink` isn't installed (it's an
opt-in Rust build) or when Docker / a broker isn't available. The broker is started/stopped
automatically from the compose file; the default address `localhost:19092` matches
`pg_lake_sink.brokers`. See `tests/pytests/test_sink.py` for coverage.

## Notes / limitations

* `pull()` and a subscription's worker both resume from and advance the subscription's stored
  offsets; disable the worker before an ad-hoc `pull()` so they don't both claim the same partitions.
* Idempotency is **offset tracking**, committed in the same transaction as the rows: exactly-once
  for heap and internal-catalog pg_lake/Iceberg targets; at-least-once for REST-catalog-managed
  tables (their catalog commit is post-`COMMIT`).
* Inserts use a streaming `CopyFrom` directly (COPY binary format) — the FDW-safe bulk path that
  fills default/identity columns and lets pg_lake chunk its own data files. Memory is bounded by the
  COPY buffer regardless of `batch_size`.
* Plaintext only — librdkafka is built without SSL/SASL (no `libsasl2` on this box).
* Subscription names are restricted to `[A-Za-z0-9_]` (≤ 200 chars) so the derived worker name,
  default table name, and group id are all safe identifiers.
* Workers are registered via `pg_extension_base`'s exported `RegisterBaseWorker` C symbol. The
  SQL wrapper `extension_base.register_worker` is intentionally restricted to `CREATE EXTENSION`,
  but the underlying C function is exported and explicitly designed to register-and-launch at
  runtime; pin to the in-tree `pg_extension_base` version.
* **libclang on this box:** the bazel-provided clang under `/scratch/bazel/...` links against
  `/opt/sfc/sysroot` glibc and fails to `dlopen` from a system-glibc binary (bindgen). Install
  `clang-libs` (`sudo dnf install -y clang-libs`) and use `LIBCLANG_PATH=/usr/lib64`.

## Layout

```
DESIGN.md       design notes: rationale, rejected alternatives, pg_lake/PG internals it relies on
src/lib.rs      _PG_init, worker entry point + loop, create/drop/enable/disable_subscription, pull(), catalog SQL
src/config.rs   default GUCs + per-subscription Config loaded from lake_sink.subscriptions
src/ffi.rs      dlsym bridge to pg_extension_base symbols (sleep/flags + Register/Deregister worker)
src/kafka.rs    consumer construction, manual assign-from-stored-offsets, single-message polling
src/copy.rs     streaming CopyFrom insert (COPY binary encoder + data-source callback)
src/sink.rs     target-table DDL + transactional offset load/store
docker/         single-node Redpanda
scripts/        produce sample messages
```
