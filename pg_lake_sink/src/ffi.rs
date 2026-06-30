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
//! Runtime bridge to the `pg_extension_base` base-worker framework.
//!
//! `pg_extension_base` is a pure-C extension loaded via `shared_preload_libraries`. Rather
//! than declaring its symbols as link-time externs (which would make our `.so` fail to load
//! in contexts where pg_extension_base is absent, e.g. pgrx's schema generator), we resolve
//! them lazily with `dlsym(RTLD_DEFAULT, ...)`. PostgreSQL loads modules with `RTLD_GLOBAL`,
//! so a preloaded pg_extension_base exposes these into the global symbol scope of the worker.
//!
//! See `pg_extension_base/include/pg_extension_base/base_workers.h`.

use core::ffi::{c_char, c_int, c_long, c_void, CStr};
use core::ptr;
use std::sync::OnceLock;

use pgrx::pg_sys;

extern "C" {
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
}

/// glibc `RTLD_DEFAULT`: search the process-global symbol scope.
const RTLD_DEFAULT: *mut c_void = ptr::null_mut();

/// Return value for a base-worker entry point: restart immediately.
#[allow(dead_code)]
pub const RESTART_IMMEDIATELY: i64 = -1;
/// Return value for a base-worker entry point: clean exit, do not restart.
pub const NO_RESTART: i64 = 0;

struct Symbols {
    /// `void LightSleep(long timeoutMs)`
    light_sleep: extern "C" fn(c_long),
    /// `volatile sig_atomic_t TerminationRequested` (sig_atomic_t == int on Linux)
    termination: *const c_int,
    /// `volatile sig_atomic_t ReloadRequested`
    #[allow(dead_code)]
    reload: *const c_int,
    /// `int32 MyBaseWorkerId`
    worker_id: *const i32,
    /// `int32 RegisterBaseWorker(char *workerName, Oid entryPointFunctionId, Oid extensionId)`.
    /// Note: the SQL wrapper `extension_base.register_worker` is guarded by `creating_extension`,
    /// but this exported C symbol is not — it registers the worker and signals the database
    /// starter to launch it immediately after the calling transaction commits. That is exactly
    /// what lets `create_subscription` spin up a worker per subscription at runtime.
    register: extern "C" fn(*const c_char, pg_sys::Oid, pg_sys::Oid) -> i32,
    /// `int32 DeregisterBaseWorkerById(int32 workerId, bool missingOk)`
    deregister_by_id: extern "C" fn(i32, bool) -> i32,
}

// The pointers are into pg_extension_base's static storage, stable for the process lifetime.
unsafe impl Sync for Symbols {}
unsafe impl Send for Symbols {}

static SYMBOLS: OnceLock<Symbols> = OnceLock::new();

fn resolve(name: &[u8]) -> *mut c_void {
    debug_assert_eq!(name.last(), Some(&0), "symbol name must be NUL-terminated");
    // SAFETY: `name` is a NUL-terminated byte string; dlsym is always available via libc.
    let p = unsafe { dlsym(RTLD_DEFAULT, name.as_ptr() as *const c_char) };
    if p.is_null() {
        let pretty = std::str::from_utf8(&name[..name.len() - 1]).unwrap_or("?");
        panic!(
            "pg_lake_sink: could not resolve symbol '{pretty}'. \
             Is 'pg_extension_base' listed in shared_preload_libraries?"
        );
    }
    p
}

fn symbols() -> &'static Symbols {
    SYMBOLS.get_or_init(|| {
        // SAFETY: each symbol resolves to a stable address in the preloaded pg_extension_base.
        unsafe {
            Symbols {
                light_sleep: core::mem::transmute::<*mut c_void, extern "C" fn(c_long)>(resolve(
                    b"LightSleep\0",
                )),
                termination: resolve(b"TerminationRequested\0") as *const c_int,
                reload: resolve(b"ReloadRequested\0") as *const c_int,
                worker_id: resolve(b"MyBaseWorkerId\0") as *const i32,
                register: core::mem::transmute::<
                    *mut c_void,
                    extern "C" fn(*const c_char, pg_sys::Oid, pg_sys::Oid) -> i32,
                >(resolve(b"RegisterBaseWorker\0")),
                deregister_by_id: core::mem::transmute::<
                    *mut c_void,
                    extern "C" fn(i32, bool) -> i32,
                >(resolve(b"DeregisterBaseWorkerById\0")),
            }
        }
    })
}

#[inline]
pub fn termination_requested() -> bool {
    // SAFETY: reading a process-local volatile flag owned by pg_extension_base.
    unsafe { ptr::read_volatile(symbols().termination) != 0 }
}

#[inline]
pub fn my_base_worker_id() -> i32 {
    // SAFETY: simple scalar read of a launcher-set global.
    unsafe { ptr::read_volatile(symbols().worker_id) }
}

#[inline]
pub fn light_sleep(timeout_ms: i64) {
    (symbols().light_sleep)(timeout_ms as c_long)
}

#[allow(dead_code)]
#[inline]
pub fn reload_requested() -> bool {
    // SAFETY: as above. LightSleep already consumes this flag; provided for completeness.
    unsafe { ptr::read_volatile(symbols().reload) != 0 }
}

/// Register a base worker at runtime and return its assigned worker id. `name` must be
/// unique among all registered workers (≤ 255 chars). `entry_point` is the Oid of the
/// `(internal) RETURNS internal` entry function; `extension` is the owning extension's Oid.
///
/// The worker is launched by the database starter once the calling transaction commits; if
/// the transaction aborts, the registration (a catalog insert) rolls back and nothing starts.
pub fn register_base_worker(name: &CStr, entry_point: pg_sys::Oid, extension: pg_sys::Oid) -> i32 {
    (symbols().register)(name.as_ptr(), entry_point, extension)
}

/// Deregister (and stop) a base worker by its id. With `missing_ok`, tolerates a worker that
/// is already gone. Mirrors `register_base_worker`: on transaction abort the worker is
/// resurrected by the starter, so call it from the same transaction that removes the catalog row.
pub fn deregister_base_worker_by_id(worker_id: i32, missing_ok: bool) -> i32 {
    (symbols().deregister_by_id)(worker_id, missing_ok)
}