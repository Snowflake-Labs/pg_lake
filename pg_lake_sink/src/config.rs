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
//! GUC defaults and the per-subscription `Config` the worker / pull function read.
//!
//! Config no longer lives in GUCs: each subscription's settings are rows in
//! `lake_sink.subscriptions`. The GUCs that remain are only *defaults* consulted by
//! `pg_lake_sink.create_subscription(...)` when an argument is omitted.

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

pub static BROKERS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"localhost:19092"));
pub static AUTO_OFFSET_RESET: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"earliest"));

pub static BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(500);
pub static POLL_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(1000);

/// Register the `pg_lake_sink.*` default GUCs. Called from `_PG_init`. These supply defaults to
/// `create_subscription`; changing one does not affect existing subscriptions (their values
/// are copied into the catalog row at creation time).
pub fn init_gucs() {
    GucRegistry::define_string_guc(
        c"pg_lake_sink.brokers",
        c"Default Kafka bootstrap brokers for new subscriptions",
        c"Comma-separated host:port list passed to librdkafka 'bootstrap.servers'.",
        &BROKERS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_lake_sink.auto_offset_reset",
        c"Default librdkafka 'auto.offset.reset' (earliest|latest) for new subscriptions",
        c"",
        &AUTO_OFFSET_RESET,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_lake_sink.batch_size",
        c"Default maximum messages consumed and inserted per transaction",
        c"",
        &BATCH_SIZE,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_lake_sink.poll_interval_ms",
        c"Default worker poll/idle wait (ms) for new subscriptions",
        c"",
        &POLL_INTERVAL_MS,
        1,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn cstr_to_string(setting: &GucSetting<Option<CString>>, default: &str) -> String {
    match setting.get() {
        Some(c) => c.to_string_lossy().into_owned(),
        None => default.to_string(),
    }
}

/// Default broker list for new subscriptions.
pub fn default_brokers() -> String {
    cstr_to_string(&BROKERS, "localhost:19092")
}

/// Default `auto.offset.reset` for new subscriptions.
pub fn default_auto_offset_reset() -> String {
    cstr_to_string(&AUTO_OFFSET_RESET, "earliest")
}

/// Default batch size for new subscriptions.
pub fn default_batch_size() -> i32 {
    BATCH_SIZE.get()
}

/// Default poll interval (ms) for new subscriptions.
pub fn default_poll_interval_ms() -> i32 {
    POLL_INTERVAL_MS.get()
}

/// A subscription's effective configuration, loaded from a `lake_sink.subscriptions` row.
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub name: String,
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub target_table: String,
    pub auto_offset_reset: String,
    pub batch_size: i32,
    pub poll_interval_ms: i32,
    pub enabled: bool,
}

/// What a worker found when it looked up its own subscription row.
pub enum SubState {
    /// Row exists and `enabled` is true: consume using this config.
    Active(Config),
    /// Row exists but `enabled` is false: stay alive but idle.
    Disabled,
    /// No row for this worker id: the subscription was dropped; exit cleanly.
    Gone,
}

const SELECT_COLUMNS: &str = "name, brokers, topic, group_id, target_table, \
     auto_offset_reset, batch_size, poll_interval_ms, enabled";

impl Config {
    /// Materialize a `Config` (and the `enabled` flag) from the current SPI row, columns in
    /// the order of [`SELECT_COLUMNS`] (1-based ordinals).
    fn from_row(row: &spi::SpiHeapTupleData<'_>) -> Result<(Config, bool), spi::Error> {
        let cfg = Config {
            name: row.get::<String>(1)?.unwrap_or_default(),
            brokers: row.get::<String>(2)?.unwrap_or_default(),
            topic: row.get::<String>(3)?.unwrap_or_default(),
            group_id: row.get::<String>(4)?.unwrap_or_default(),
            target_table: row.get::<String>(5)?.unwrap_or_default(),
            auto_offset_reset: row.get::<String>(6)?.unwrap_or_default(),
            batch_size: row.get::<i32>(7)?.unwrap_or(500),
            poll_interval_ms: row.get::<i32>(8)?.unwrap_or(1000),
            enabled: row.get::<bool>(9)?.unwrap_or(false),
        };
        let enabled = cfg.enabled;
        Ok((cfg, enabled))
    }

    /// Load the subscription owned by base worker `worker_id`. Used by the worker loop.
    pub fn load_by_worker_id(worker_id: i32) -> Result<SubState, spi::Error> {
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM lake_sink.subscriptions WHERE worker_id = $1"
        );
        Spi::connect(|client| {
            let table = client.select(&sql, Some(1), &[worker_id.into()])?;
            match table.first().get_heap_tuple()? {
                None => Ok(SubState::Gone),
                Some(row) => {
                    let (cfg, enabled) = Config::from_row(&row)?;
                    Ok(if enabled {
                        SubState::Active(cfg)
                    } else {
                        SubState::Disabled
                    })
                }
            }
        })
    }

    /// Load a subscription by name. Used by the on-demand `pull()` function. Returns `None`
    /// if no such subscription exists.
    pub fn load_by_name(name: &str) -> Result<Option<Config>, spi::Error> {
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM lake_sink.subscriptions WHERE name = $1"
        );
        Spi::connect(|client| {
            let table = client.select(&sql, Some(1), &[name.into()])?;
            match table.first().get_heap_tuple()? {
                None => Ok(None),
                Some(row) => Ok(Some(Config::from_row(&row)?.0)),
            }
        })
    }

    /// The connection-defining subset. When this changes the consumer is rebuilt.
    pub fn connection_key(&self) -> (String, String, String, String) {
        (
            self.brokers.clone(),
            self.topic.clone(),
            self.group_id.clone(),
            self.auto_offset_reset.clone(),
        )
    }
}