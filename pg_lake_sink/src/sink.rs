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
//! Target-table DDL and transactional offset persistence (via SPI).
//!
//! The actual row insert lives in [`crate::copy`] (streaming `CopyFrom`); this module owns the
//! target-table bootstrap and the `lake_sink.subscription_offsets` bookkeeping that makes the
//! sink exactly-once: offsets are upserted in the same transaction as the inserted rows.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

/// Create a per-subscription target table with the canonical pg_lake_sink column layout, if it does
/// not already exist. Called from `create_subscription` when it auto-creates the table.
///
/// There is intentionally **no** `UNIQUE (topic, partition, "offset")` constraint: idempotency
/// comes from offset tracking, and a unique constraint would turn a stray duplicate into a
/// batch-aborting error. The table is append-only, like a pg_lake foreign target.
pub fn create_target_table(target_table: &str) -> Result<(), spi::Error> {
    let table = validated_table(target_table);
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
            id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
            topic           text        NOT NULL, \
            partition       int         NOT NULL, \
            \"offset\"        bigint      NOT NULL, \
            key             bytea, \
            payload         jsonb, \
            headers         jsonb, \
            kafka_timestamp timestamptz, \
            consumed_at     timestamptz NOT NULL DEFAULT now()\
         )"
    );
    Spi::run(&ddl)
}

/// Load the stored `(partition, last_offset)` resume points for a subscription.
pub fn load_offsets(subscription: &str) -> Result<Vec<(i32, i64)>, spi::Error> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT partition, last_offset FROM lake_sink.subscription_offsets WHERE subscription = $1",
            None,
            &[subscription.into()],
        )?;
        let mut out = Vec::new();
        for row in table {
            let partition = row.get::<i32>(1)?.unwrap_or_default();
            let last = row.get::<i64>(2)?.unwrap_or_default();
            out.push((partition, last));
        }
        Ok(out)
    })
}

/// Upsert the latest offset per partition for a subscription. Must run in the **same**
/// transaction as the row insert so data and resume point commit atomically. `subscription_offsets`
/// is a local heap table, so `ON CONFLICT` works here even when the target is a foreign table.
pub fn store_offsets(subscription: &str, offsets: &[(i32, i64)]) -> Result<(), spi::Error> {
    if offsets.is_empty() {
        return Ok(());
    }
    Spi::connect_mut(|client| -> Result<(), spi::Error> {
        for (partition, last_offset) in offsets {
            let args: [DatumWithOid; 3] =
                [subscription.into(), (*partition).into(), (*last_offset).into()];
            client.update(
                "INSERT INTO lake_sink.subscription_offsets (subscription, partition, last_offset) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (subscription, partition) DO UPDATE SET last_offset = EXCLUDED.last_offset",
                None,
                &args,
            )?;
        }
        Ok(())
    })
}

/// Guard a table name against SQL injection before interpolating it into DDL / `to_regclass`.
/// Only `schema.table` made of identifier characters is allowed.
pub(crate) fn validated_table(name: &str) -> String {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
    if !ok {
        error!("pg_lake_sink: invalid target table name: {name:?}");
    }
    name.to_string()
}