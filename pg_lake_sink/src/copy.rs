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
//! Streaming bulk insert into the target table via PostgreSQL's `COPY` machinery.
//!
//! Each batch is inserted by driving `BeginCopyFrom`/`CopyFrom`/`EndCopyFrom` directly (the same
//! API pg_lake itself uses, see `pg_lake_engine/src/copy/remote_query.c`). Rows are fed in
//! **binary COPY format** through a data-source callback that pulls Kafka records on demand, so:
//!
//!   * memory is bounded by the COPY buffer + one in-flight record (never the whole batch);
//!   * `DEFAULT`/identity/generated columns we don't supply (`id`, `consumed_at`) are populated by
//!     the COPY layer — unlike low-level `heap_*` inserts;
//!   * the whole batch is one FDW write context (single `BeginForeignInsert`…`EndForeignInsert`),
//!     so pg_lake does its own Parquet file chunking;
//!   * the same path works for heap and foreign (pg_lake) targets.
//!
//! The data-source callback has no user-data argument, so the streaming state lives in a
//! `thread_local!` — safe because a base worker is single-threaded.

use core::ffi::{c_int, c_void};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::ptr;
use std::time::{Duration, Instant};

use pgrx::pg_sys;
use pgrx::prelude::*;
use rdkafka::consumer::BaseConsumer;

use crate::kafka::{self, Record};

/// Columns we provide in the COPY stream. `id` (identity) and `consumed_at` (default now()) are
/// intentionally omitted so the COPY layer fills them.
pub const COLUMNS: [&str; 7] = [
    "topic",
    "partition",
    "offset",
    "key",
    "payload",
    "headers",
    "kafka_timestamp",
];

/// COPY binary file header: 11-byte signature, int32 flags (0), int32 header-extension len (0).
const BINARY_HEADER: &[u8] = &[
    b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0xFF, b'\r', b'\n', 0x00, // signature
    0, 0, 0, 0, // flags
    0, 0, 0, 0, // header extension length
];

/// Microseconds between the Unix epoch (1970-01-01) and the Postgres epoch (2000-01-01).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Cap on a single greedy drain poll, so we re-check the deadline / stay responsive.
const DRAIN_POLL_MS: i32 = 100;

#[derive(PartialEq)]
enum Phase {
    Header,
    Rows,
    Done,
}

/// State the COPY data-source callback streams from. Lives in a `thread_local!` because the
/// callback signature carries no user pointer.
struct StreamState {
    consumer: *const BaseConsumer,
    /// The first record (already polled by the worker) consumed before polling more.
    first: Option<Record>,
    max_rows: usize,
    rows: usize,
    deadline: Instant,
    offsets: HashMap<i32, i64>,
    /// Encoded-but-not-yet-handed-to-COPY bytes (at most one row / the header / the trailer).
    pending: Vec<u8>,
    pending_pos: usize,
    phase: Phase,
}

impl StreamState {
    fn new(consumer: *const BaseConsumer, first: Record, max_rows: usize, window_ms: i32) -> Self {
        StreamState {
            consumer,
            first: Some(first),
            max_rows: max_rows.max(1),
            rows: 0,
            deadline: Instant::now() + Duration::from_millis(window_ms.max(0) as u64),
            offsets: HashMap::new(),
            pending: Vec::with_capacity(1024),
            pending_pos: 0,
            phase: Phase::Header,
        }
    }

    /// Refill `pending` with the next unit of the COPY byte stream. Returns false only when the
    /// stream is exhausted (end of data → callback should report EOF).
    fn produce_next(&mut self) -> bool {
        self.pending.clear();
        self.pending_pos = 0;
        match self.phase {
            Phase::Header => {
                self.pending.extend_from_slice(BINARY_HEADER);
                self.phase = Phase::Rows;
                true
            }
            Phase::Rows => {
                let next = if let Some(rec) = self.first.take() {
                    Some(rec)
                } else if self.rows < self.max_rows && Instant::now() < self.deadline {
                    // SAFETY: the consumer outlives this COPY (owned by the worker loop) and the
                    // callback only runs synchronously during CopyFrom on this thread.
                    let consumer = unsafe { &*self.consumer };
                    kafka::poll_one(consumer, DRAIN_POLL_MS)
                } else {
                    None
                };
                match next {
                    Some(rec) => {
                        encode_row(&mut self.pending, &rec);
                        self.offsets
                            .entry(rec.partition)
                            .and_modify(|o| {
                                if rec.offset > *o {
                                    *o = rec.offset;
                                }
                            })
                            .or_insert(rec.offset);
                        self.rows += 1;
                    }
                    None => {
                        // COPY binary trailer: int16 field count of -1.
                        self.pending.extend_from_slice(&(-1i16).to_be_bytes());
                        self.phase = Phase::Done;
                    }
                }
                true
            }
            Phase::Done => false,
        }
    }
}

thread_local! {
    static STREAM: RefCell<Option<StreamState>> = const { RefCell::new(None) };
}

/// Insert a batch into `target_table`, starting from the already-polled `first` record and
/// greedily draining up to `max_rows` more within `window_ms`. Returns the number of rows
/// inserted and the max offset observed per partition (to persist transactionally).
pub fn copy_insert(
    target_table: &str,
    consumer: &BaseConsumer,
    first: Record,
    max_rows: usize,
    window_ms: i32,
) -> Result<(u64, Vec<(i32, i64)>), spi::Error> {
    let validated = crate::sink::validated_table(target_table);
    let relid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[validated.as_str().into()],
    )?
    .unwrap_or_else(|| error!("pg_lake_sink: target table {target_table:?} does not exist"));

    STREAM.with(|s| {
        *s.borrow_mut() = Some(StreamState::new(consumer as *const _, first, max_rows, window_ms))
    });

    // SAFETY: standard COPY-FROM setup; mirrors pg_lake's own programmatic CopyFrom usage. A
    // Postgres error here longjmps out and aborts the surrounding transaction (so nothing
    // commits); STREAM is reset at the next entry, so the stale consumer pointer is never used.
    let rows = unsafe { run_copy(relid) };

    let offsets = STREAM.with(|s| {
        s.borrow_mut()
            .take()
            .map(|st| st.offsets.into_iter().collect::<Vec<_>>())
            .unwrap_or_default()
    });
    Ok((rows, offsets))
}

unsafe fn run_copy(relid: pg_sys::Oid) -> u64 {
    let rel = pg_sys::relation_open(relid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);

    // A ParseState with the target in its range table + permission info, as CopyFrom expects.
    let pstate = pg_sys::make_parsestate(ptr::null_mut());

    let rte = pg_sys::palloc0(size_of::<pg_sys::RangeTblEntry>()) as *mut pg_sys::RangeTblEntry;
    (*rte).type_ = pg_sys::NodeTag::T_RangeTblEntry;
    (*rte).rtekind = pg_sys::RTEKind::RTE_RELATION;
    (*rte).relid = relid;
    (*rte).relkind = (*(*rel).rd_rel).relkind;
    (*rte).rellockmode = pg_sys::RowExclusiveLock as c_int;
    (*rte).inh = false;

    let perm = pg_sys::palloc0(size_of::<pg_sys::RTEPermissionInfo>()) as *mut pg_sys::RTEPermissionInfo;
    (*perm).type_ = pg_sys::NodeTag::T_RTEPermissionInfo;
    (*perm).relid = relid;
    (*perm).inh = false;
    (*perm).requiredPerms = pg_sys::ACL_INSERT as pg_sys::AclMode;
    (*perm).checkAsUser = pg_sys::GetUserId();

    (*pstate).p_rtable = pg_sys::lappend(ptr::null_mut(), rte as *mut c_void);
    (*pstate).p_rteperminfos = pg_sys::lappend(ptr::null_mut(), perm as *mut c_void);

    // Column list (the 7 we supply) and `(format binary)`.
    let mut attnamelist: *mut pg_sys::List = ptr::null_mut();
    for col in COLUMNS {
        let cstr = CString::new(col).unwrap();
        let node = pg_sys::makeString(pg_sys::pstrdup(cstr.as_ptr()));
        attnamelist = pg_sys::lappend(attnamelist, node as *mut c_void);
    }
    let fmt_name = CString::new("format").unwrap();
    let fmt_value = CString::new("binary").unwrap();
    let binary = pg_sys::makeString(pg_sys::pstrdup(fmt_value.as_ptr()));
    let defelem = pg_sys::makeDefElem(
        pg_sys::pstrdup(fmt_name.as_ptr()),
        binary as *mut pg_sys::Node,
        -1,
    );
    let options = pg_sys::lappend(ptr::null_mut(), defelem as *mut c_void);

    let cstate = pg_sys::BeginCopyFrom(
        pstate,
        rel,
        ptr::null_mut(),
        ptr::null(),
        false,
        Some(data_source_cb),
        attnamelist,
        options,
    );
    let rows = pg_sys::CopyFrom(cstate);
    pg_sys::EndCopyFrom(cstate);
    pg_sys::relation_close(rel, pg_sys::NoLock as pg_sys::LOCKMODE);
    rows
}

/// COPY data-source callback: fills `outbuf` (up to `maxread` bytes) from the stream state,
/// returning the byte count, or 0 at end of stream.
unsafe extern "C-unwind" fn data_source_cb(
    outbuf: *mut c_void,
    _minread: c_int,
    maxread: c_int,
) -> c_int {
    STREAM.with(|s| {
        let mut guard = s.borrow_mut();
        let st = match guard.as_mut() {
            Some(st) => st,
            None => return 0,
        };
        let out = outbuf as *mut u8;
        let max = maxread.max(0) as usize;
        let mut written = 0usize;
        while written < max {
            if st.pending_pos >= st.pending.len() && !st.produce_next() {
                break; // end of stream
            }
            let avail = st.pending.len() - st.pending_pos;
            if avail == 0 {
                continue;
            }
            let n = avail.min(max - written);
            ptr::copy_nonoverlapping(
                st.pending.as_ptr().add(st.pending_pos),
                out.add(written),
                n,
            );
            st.pending_pos += n;
            written += n;
        }
        written as c_int
    })
}

/// Append one row in COPY binary format: int16 field count, then each field as int32 length
/// (or -1 for NULL) followed by its binary `recv` representation.
fn encode_row(buf: &mut Vec<u8>, r: &Record) {
    buf.extend_from_slice(&(COLUMNS.len() as i16).to_be_bytes());
    put_bytes(buf, Some(r.topic.as_bytes())); // topic (text, NOT NULL)
    put_i32(buf, r.partition);
    put_i64(buf, r.offset);
    put_bytes(buf, r.key.as_deref()); // key (bytea)
    put_jsonb(buf, r.payload_json.as_deref());
    put_jsonb(buf, r.headers_json.as_deref());
    put_timestamptz(buf, r.timestamp_ms);
}

fn put_bytes(buf: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        Some(b) => {
            buf.extend_from_slice(&(b.len() as i32).to_be_bytes());
            buf.extend_from_slice(b);
        }
    }
}

fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&4i32.to_be_bytes());
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&8i32.to_be_bytes());
    buf.extend_from_slice(&v.to_be_bytes());
}

/// jsonb binary format: a version byte (1) followed by the JSON text.
fn put_jsonb(buf: &mut Vec<u8>, json: Option<&str>) {
    match json {
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        Some(j) => {
            let len = 1 + j.len();
            buf.extend_from_slice(&(len as i32).to_be_bytes());
            buf.push(1u8); // jsonb version
            buf.extend_from_slice(j.as_bytes());
        }
    }
}

/// timestamptz binary format: int64 microseconds since the Postgres epoch (2000-01-01).
fn put_timestamptz(buf: &mut Vec<u8>, unix_ms: Option<i64>) {
    match unix_ms.and_then(|ms| ms.checked_mul(1000)) {
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        Some(unix_us) => {
            let pg_us = unix_us - PG_EPOCH_OFFSET_US;
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&pg_us.to_be_bytes());
        }
    }
}