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
//! librdkafka consumer construction and polling.
//!
//! We use a synchronous `BaseConsumer` and poll it from the worker's own thread, so no
//! PostgreSQL state is ever touched from librdkafka's internal threads.
//!
//! Partitions are assigned manually ([`assign_from_offsets`]) and positioned from offsets the
//! sink stored in Postgres, rather than via consumer-group coordination / Kafka-committed
//! offsets. This is what makes the sink exactly-once: the resume point is the DB, updated in the
//! same transaction as the inserted rows.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::config::Config;

/// One Kafka message flattened into the columns of the target table.
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    /// Always a valid JSON document: the raw value if it parses as JSON, otherwise the
    /// value wrapped as a JSON string.
    pub payload_json: Option<String>,
    pub headers_json: Option<String>,
    pub timestamp_ms: Option<i64>,
}

/// Build a plaintext consumer from the current config. The consumer is **not** subscribed;
/// the caller assigns partitions with [`assign_from_offsets`].
pub fn build_consumer(cfg: &Config) -> KafkaResult<BaseConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
}

/// Manually assign every partition of the topic, positioning each at the offset after the one
/// last durably stored in Postgres (`stored`), or at the `auto_offset_reset` default when no
/// offset is stored yet. Replaces consumer-group `subscribe`, so the resume point is fully
/// controlled by the sink.
pub fn assign_from_offsets(
    consumer: &BaseConsumer,
    cfg: &Config,
    stored: &[(i32, i64)],
) -> KafkaResult<()> {
    let metadata = consumer.fetch_metadata(Some(&cfg.topic), Duration::from_secs(10))?;
    let default_offset = if cfg.auto_offset_reset.eq_ignore_ascii_case("latest") {
        Offset::End
    } else {
        Offset::Beginning
    };
    let stored_map: std::collections::HashMap<i32, i64> = stored.iter().copied().collect();

    let mut tpl = TopicPartitionList::new();
    for topic in metadata.topics() {
        if topic.name() != cfg.topic {
            continue;
        }
        for partition in topic.partitions() {
            let offset = match stored_map.get(&partition.id()) {
                Some(last) => Offset::Offset(last + 1),
                None => default_offset,
            };
            tpl.add_partition_offset(&cfg.topic, partition.id(), offset)?;
        }
    }

    if tpl.count() == 0 {
        pgrx::warning!("pg_lake_sink: topic '{}' has no partitions yet", cfg.topic);
    }
    consumer.assign(&tpl)
}

/// Poll for a single record, waiting up to `timeout_ms`. Returns `None` on timeout (no message
/// available) or on a transient poll error (logged).
pub fn poll_one(consumer: &BaseConsumer, timeout_ms: i32) -> Option<Record> {
    match consumer.poll(Duration::from_millis(timeout_ms.max(0) as u64)) {
        Some(Ok(msg)) => Some(to_record(&msg)),
        Some(Err(e)) => {
            pgrx::warning!("pg_lake_sink: poll error: {e}");
            None
        }
        None => None,
    }
}

fn to_record(msg: &rdkafka::message::BorrowedMessage) -> Record {
    let payload_json = msg.payload().map(normalize_json);

    let headers_json = msg.headers().map(|hs| {
        let mut obj = serde_json::Map::with_capacity(hs.count());
        for h in hs.iter() {
            let value = match h.value {
                Some(bytes) => serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()),
                None => serde_json::Value::Null,
            };
            obj.insert(h.key.to_string(), value);
        }
        serde_json::Value::Object(obj).to_string()
    });

    Record {
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
        key: msg.key().map(|k| k.to_vec()),
        payload_json,
        headers_json,
        timestamp_ms: msg.timestamp().to_millis(),
    }
}

/// Produce a string that is always valid JSON for the `payload jsonb` column.
fn normalize_json(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) if serde_json::from_str::<serde_json::Value>(s).is_ok() => s.to_string(),
        Ok(s) => serde_json::Value::String(s.to_string()).to_string(),
        Err(_) => serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()).to_string(),
    }
}