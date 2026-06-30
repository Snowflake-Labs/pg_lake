// Copyright 2025 Snowflake Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! pg_lake_sink — a Rust/pgrx PostgreSQL extension that consumes Kafka topics into tables.
//!
//! Each *subscription* (a row in `lake_sink.subscriptions`) pairs one Kafka topic with one
//! target table and gets its own continuous background worker, registered at runtime with
//! pg_lake's `pg_extension_base` base-worker framework. Subscriptions are managed entirely
//! through SQL:
//!   * `pg_lake_sink.create_subscription(name, topic, ...)` — make a target table and start a worker;
//!   * `pg_lake_sink.drop_subscription(name)` — stop the worker and forget the subscription;
//!   * `pg_lake_sink.enable_subscription` / `disable_subscription` — pause/resume a worker;
//!   * `pg_lake_sink.pull(subscription, int)` — on-demand consume for testing / backfill.
//!
//! All consume paths stream rows through one `CopyFrom` ([`crate::copy`]) and record Kafka
//! offsets in `lake_sink.subscription_offsets` in the same transaction (exactly-once). The single
//! worker entry point [`pg_lake_sink_worker_main`] serves every subscription, identifying itself by
//! `MyBaseWorkerId`.

use std::ffi::CString;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::datum::Internal;
use pgrx::prelude::*;

mod config;
mod copy;
mod ffi;
mod kafka;
mod sink;

use config::{Config, SubState};

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    config::init_gucs();
}

// The extension's functions live in `pg_catalog` (control `schema = pg_catalog`) so they cannot be
// shadowed via search_path. Its tables live in a dedicated `lake_sink` schema, created here (the
// schema cannot be named `pg_lake_sink` — Postgres reserves the `pg_` prefix). Per-subscription
// target tables are created dynamically by `create_subscription` (extension_sql is static SQL).
extension_sql!(
    r#"
CREATE SCHEMA lake_sink;

CREATE TABLE lake_sink.subscriptions (
    name              text        PRIMARY KEY,
    brokers           text        NOT NULL,
    topic             text        NOT NULL,
    group_id          text        NOT NULL,
    target_table      text        NOT NULL,
    auto_offset_reset text        NOT NULL DEFAULT 'earliest',
    batch_size        int         NOT NULL DEFAULT 500,
    poll_interval_ms  int         NOT NULL DEFAULT 1000,
    enabled           boolean     NOT NULL DEFAULT true,
    worker_id         int,
    created_at        timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE lake_sink.subscriptions IS 'pg_lake_sink Kafka subscriptions; one base worker + one target table each';
CREATE UNIQUE INDEX subscriptions_worker_id_key ON lake_sink.subscriptions (worker_id) WHERE worker_id IS NOT NULL;

CREATE TABLE lake_sink.subscription_offsets (
    subscription text   NOT NULL REFERENCES lake_sink.subscriptions(name) ON DELETE CASCADE,
    partition    int    NOT NULL,
    last_offset  bigint NOT NULL,
    PRIMARY KEY (subscription, partition)
);
COMMENT ON TABLE lake_sink.subscription_offsets IS 'pg_lake_sink per-partition resume points, advanced transactionally with inserts';
"#,
    name = "bootstrap_subscriptions",
    bootstrap,
);

/// Declares the `lake_sink` schema to pgrx so functions can target it via
/// `#[pg_extern(schema = "lake_sink")]`. The schema (and its tables) is actually created by the
/// bootstrap block above; pgrx also emits a harmless `CREATE SCHEMA IF NOT EXISTS lake_sink` here.
#[pg_schema]
mod lake_sink {}

/// Base-worker entry point, in the `lake_sink` schema. Invoked by the `pg_extension_base` launcher
/// as `lake_sink.pg_lake_sink_worker_main(internal) RETURNS internal`. The returned int64 is the
/// restart policy: `-1` restart now, `>0` restart after N ms, `0` no restart.
///
/// One process per subscription: which one is determined by `MyBaseWorkerId` (see [`run_worker`]).
#[pg_extern(schema = "lake_sink")]
fn pg_lake_sink_worker_main(_worker_id: Internal) -> Internal {
    let restart_code = run_worker();
    Internal::from(Some(pg_sys::Datum::from(restart_code)))
}

/// The worker's main loop. Loads its own subscription by `MyBaseWorkerId` each iteration (so a
/// catalog UPDATE is picked up automatically), waits for data, then streams one batch into the
/// target via COPY while advancing offsets in the same transaction. Returns a restart code.
fn run_worker() -> i64 {
    let worker_id = ffi::my_base_worker_id();
    log!("pg_lake_sink: base worker {worker_id} starting");

    // Consumer kept across iterations; rebuilt and re-assigned from stored offsets when the
    // connection config changes.
    let mut consumer: Option<(rdkafka::consumer::BaseConsumer, (String, String, String, String))> =
        None;

    while !ffi::termination_requested() {
        let cfg = match BackgroundWorker::transaction(|| Config::load_by_worker_id(worker_id)) {
            Ok(SubState::Active(c)) => c,
            Ok(SubState::Disabled) => {
                consumer = None; // drop assignment while paused
                ffi::light_sleep(1000);
                continue;
            }
            Ok(SubState::Gone) => {
                log!("pg_lake_sink: worker {worker_id} has no subscription row; exiting");
                return ffi::NO_RESTART;
            }
            Err(e) => {
                warning!("pg_lake_sink: worker {worker_id} cannot load subscription: {e}; backing off");
                ffi::light_sleep(1000);
                continue;
            }
        };

        // (Re)build + assign the consumer when broker/topic/group/offset-reset change.
        let key = cfg.connection_key();
        if consumer.as_ref().map(|(_, k)| *k != key).unwrap_or(true) {
            match build_and_assign(&cfg) {
                Ok(c) => {
                    log!(
                        "pg_lake_sink: worker {worker_id} consuming '{}' on {} into {}",
                        cfg.topic,
                        cfg.brokers,
                        cfg.target_table
                    );
                    consumer = Some((c, key));
                }
                Err(e) => {
                    warning!("pg_lake_sink: cannot start consumer: {e}; backing off");
                    ffi::light_sleep(cfg.poll_interval_ms.max(1000) as i64);
                    continue;
                }
            }
        }
        let consumer_ref = &consumer.as_ref().unwrap().0;

        // Wait (outside any transaction) for the first message of the next batch.
        let first = match kafka::poll_one(consumer_ref, cfg.poll_interval_ms) {
            Some(r) => r,
            None => continue, // idle; loop re-checks termination then polls again
        };

        // One transaction: stream the batch in via COPY and advance offsets atomically. Any
        // failure raises (`error!`), aborting the transaction so neither rows nor offsets commit;
        // the worker then re-polls from the still-committed last offset.
        let target = cfg.target_table.clone();
        let name = cfg.name.clone();
        let max_rows = cfg.batch_size as usize;
        let window = cfg.poll_interval_ms;
        // Capture the consumer as a raw pointer: `BaseConsumer` is not `RefUnwindSafe`, and the
        // closure runs synchronously within this iteration so the pointer stays valid.
        let consumer_ptr: *const rdkafka::consumer::BaseConsumer = consumer_ref;
        let inserted = BackgroundWorker::transaction(move || {
            // SAFETY: `consumer_ptr` outlives this synchronous closure (owned above).
            let consumer = unsafe { &*consumer_ptr };
            let (rows, offsets) = copy::copy_insert(&target, consumer, first, max_rows, window)
                .unwrap_or_else(|e| error!("pg_lake_sink: copy failed: {e}"));
            if rows > 0 {
                sink::store_offsets(&name, &offsets)
                    .unwrap_or_else(|e| error!("pg_lake_sink: store offsets failed: {e}"));
            }
            rows
        });

        if inserted > 0 {
            log!("pg_lake_sink: worker {worker_id} inserted {inserted} row(s)");
        }
    }

    log!("pg_lake_sink: base worker {worker_id} terminating");
    ffi::NO_RESTART
}

/// Build a consumer and assign its partitions from the offsets stored for this subscription.
fn build_and_assign(cfg: &Config) -> Result<rdkafka::consumer::BaseConsumer, String> {
    let consumer = kafka::build_consumer(cfg).map_err(|e| e.to_string())?;
    let stored =
        BackgroundWorker::transaction(|| sink::load_offsets(&cfg.name)).map_err(|e| e.to_string())?;
    kafka::assign_from_offsets(&consumer, cfg, &stored).map_err(|e| e.to_string())?;
    Ok(consumer)
}

/// Create a subscription: make its target table, record it, and start a dedicated base worker.
/// Omitted optional arguments fall back to the `pg_lake_sink.*` default GUCs, a target table named
/// `pg_lake_sink.<name>`, and a consumer group `pg_lake_sink_<name>` (unique per subscription so workers
/// never steal each other's partitions). Returns the assigned base-worker id.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "lake_sink")]
fn create_subscription(
    name: &str,
    topic: &str,
    brokers: default!(Option<&str>, "NULL"),
    target_table: default!(Option<&str>, "NULL"),
    group_id: default!(Option<&str>, "NULL"),
    auto_offset_reset: default!(Option<&str>, "NULL"),
    batch_size: default!(Option<i32>, "NULL"),
    poll_interval_ms: default!(Option<i32>, "NULL"),
    enabled: default!(bool, true),
    create_target_table: default!(bool, true),
) -> i32 {
    validate_subscription_name(name);

    let brokers = brokers
        .map(str::to_string)
        .unwrap_or_else(config::default_brokers);
    let target_table = target_table
        .map(str::to_string)
        .unwrap_or_else(|| format!("lake_sink.{name}"));
    let group_id = group_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("pg_lake_sink_{name}"));
    let auto_offset_reset = auto_offset_reset
        .map(str::to_string)
        .unwrap_or_else(config::default_auto_offset_reset);
    let batch_size = batch_size.unwrap_or_else(config::default_batch_size);
    let poll_interval_ms = poll_interval_ms.unwrap_or_else(config::default_poll_interval_ms);

    // Create the per-subscription target table unless the caller points at an existing one
    // (e.g. a pre-created pg_lake foreign table).
    if create_target_table {
        sink::create_target_table(&target_table)
            .unwrap_or_else(|e| error!("pg_lake_sink: cannot create target table {target_table:?}: {e}"));
    }

    // Record the subscription (worker_id filled in once registered). Duplicate name -> PK error.
    Spi::connect_mut(|client| -> Result<(), spi::Error> {
        client.update(
            "INSERT INTO lake_sink.subscriptions \
                (name, brokers, topic, group_id, target_table, auto_offset_reset, \
                 batch_size, poll_interval_ms, enabled) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            None,
            &[
                name.into(),
                brokers.as_str().into(),
                topic.into(),
                group_id.as_str().into(),
                target_table.as_str().into(),
                auto_offset_reset.as_str().into(),
                batch_size.into(),
                poll_interval_ms.into(),
                enabled.into(),
            ],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| error!("pg_lake_sink: cannot record subscription {name:?}: {e}"));

    // Register a base worker for this subscription. The exported C symbol launches it after
    // this transaction commits; on rollback the registration (a catalog insert) is undone too.
    let worker_name = CString::new(format!("pg_lake_sink_{name}")).expect("worker name has no NUL");
    let worker_id = ffi::register_base_worker(&worker_name, entry_point_oid(), extension_oid());

    Spi::connect_mut(|client| -> Result<(), spi::Error> {
        client.update(
            "UPDATE lake_sink.subscriptions SET worker_id = $1 WHERE name = $2",
            None,
            &[worker_id.into(), name.into()],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| error!("pg_lake_sink: cannot store worker id for {name:?}: {e}"));

    log!("pg_lake_sink: created subscription '{name}' (topic '{topic}' -> {target_table}, worker {worker_id})");
    worker_id
}

/// Stop a subscription's worker and remove it. Optionally drops the target table too.
#[pg_extern(schema = "lake_sink")]
fn drop_subscription(name: &str, drop_table: default!(bool, false)) {
    let row = Spi::connect(|client| -> Result<Option<(Option<i32>, String)>, spi::Error> {
        let table = client.select(
            "SELECT worker_id, target_table FROM lake_sink.subscriptions WHERE name = $1",
            Some(1),
            &[name.into()],
        )?;
        match table.first().get_heap_tuple()? {
            None => Ok(None),
            Some(r) => Ok(Some((r.get::<i32>(1)?, r.get::<String>(2)?.unwrap_or_default()))),
        }
    })
    .unwrap_or_else(|e| error!("pg_lake_sink: cannot look up subscription {name:?}: {e}"));

    let (worker_id, target_table) =
        row.unwrap_or_else(|| error!("pg_lake_sink: no subscription named {name:?}"));

    // Deregister first (missing_ok: tolerate an already-gone worker). On rollback of this
    // transaction the worker is resurrected by the starter, keeping catalog and worker in sync.
    if let Some(wid) = worker_id {
        ffi::deregister_base_worker_by_id(wid, true);
    }

    Spi::connect_mut(|client| -> Result<(), spi::Error> {
        client.update(
            "DELETE FROM lake_sink.subscriptions WHERE name = $1",
            None,
            &[name.into()],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| error!("pg_lake_sink: cannot delete subscription {name:?}: {e}"));

    if drop_table {
        let table = sink::validated_table(&target_table);
        Spi::run(&format!("DROP TABLE IF EXISTS {table}"))
            .unwrap_or_else(|e| error!("pg_lake_sink: cannot drop table {target_table:?}: {e}"));
    }

    log!("pg_lake_sink: dropped subscription '{name}'");
}

/// Resume a paused subscription's worker.
#[pg_extern(schema = "lake_sink")]
fn enable_subscription(name: &str) {
    set_enabled(name, true);
}

/// Pause a subscription's worker (it stays alive but idle and releases its partitions).
#[pg_extern(schema = "lake_sink")]
fn disable_subscription(name: &str) {
    set_enabled(name, false);
}

fn set_enabled(name: &str, enabled: bool) {
    let affected = Spi::connect_mut(|client| -> Result<u64, spi::Error> {
        let table = client.update(
            "UPDATE lake_sink.subscriptions SET enabled = $1 WHERE name = $2",
            None,
            &[enabled.into(), name.into()],
        )?;
        Ok(table.len() as u64)
    })
    .unwrap_or_else(|e| error!("pg_lake_sink: cannot update subscription {name:?}: {e}"));

    if affected == 0 {
        error!("pg_lake_sink: no subscription named {name:?}");
    }
}

/// Consume up to `max_messages` from the given subscription's topic right now and insert them
/// into its target table, advancing stored offsets in the caller's transaction. Returns the
/// number of rows written. Uses the same streaming-COPY + offset-tracking path as the worker, so
/// it resumes from (and advances) the subscription's stored offsets — disable the worker first to
/// avoid the two competing for the same partitions.
#[pg_extern(schema = "lake_sink")]
fn pull(subscription: &str, max_messages: default!(i32, 100)) -> i64 {
    let cfg = Config::load_by_name(subscription)
        .unwrap_or_else(|e| error!("pg_lake_sink: cannot load subscription: {e}"))
        .unwrap_or_else(|| error!("pg_lake_sink: no subscription named {subscription:?}"));
    let max = max_messages.max(0) as usize;
    if max == 0 {
        return 0;
    }

    let consumer = kafka::build_consumer(&cfg)
        .unwrap_or_else(|e| error!("pg_lake_sink: cannot create consumer: {e}"));
    let stored =
        sink::load_offsets(&cfg.name).unwrap_or_else(|e| error!("pg_lake_sink: cannot load offsets: {e}"));
    kafka::assign_from_offsets(&consumer, &cfg, &stored)
        .unwrap_or_else(|e| error!("pg_lake_sink: cannot assign partitions: {e}"));

    let first = match kafka::poll_one(&consumer, cfg.poll_interval_ms) {
        Some(r) => r,
        None => return 0,
    };
    let (rows, offsets) = copy::copy_insert(&cfg.target_table, &consumer, first, max, cfg.poll_interval_ms)
        .unwrap_or_else(|e| error!("pg_lake_sink: copy failed: {e}"));
    if rows > 0 {
        sink::store_offsets(&cfg.name, &offsets)
            .unwrap_or_else(|e| error!("pg_lake_sink: store offsets failed: {e}"));
    }
    rows as i64
}

/// Oid of the worker entry point, looked up by its `(internal)` signature. The function lives in
/// the `lake_sink` schema; referenced fully-qualified so the lookup is search_path-independent.
fn entry_point_oid() -> pg_sys::Oid {
    Spi::get_one::<pg_sys::Oid>(
        "SELECT 'lake_sink.pg_lake_sink_worker_main(internal)'::regprocedure::oid",
    )
    .expect("pg_lake_sink: entry-point lookup failed")
    .expect("pg_lake_sink: entry point lake_sink.pg_lake_sink_worker_main(internal) not found")
}

/// Oid of this extension, needed when registering a base worker.
fn extension_oid() -> pg_sys::Oid {
    Spi::get_one::<pg_sys::Oid>("SELECT oid FROM pg_catalog.pg_extension WHERE extname = 'pg_lake_sink'")
        .expect("pg_lake_sink: extension lookup failed")
        .expect("pg_lake_sink: extension 'pg_lake_sink' not found")
}

/// Reject subscription names that wouldn't make safe identifiers. Keeping names to
/// `[A-Za-z0-9_]` (max 200 chars) guarantees the derived worker name `pg_lake_sink_<name>` stays
/// within the 255-char limit and that the default table/group names are injection-safe.
fn validate_subscription_name(name: &str) {
    let ok = !name.is_empty()
        && name.len() <= 200
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !ok {
        error!("pg_lake_sink: invalid subscription name {name:?}; use [A-Za-z0-9_], max 200 chars");
    }
}