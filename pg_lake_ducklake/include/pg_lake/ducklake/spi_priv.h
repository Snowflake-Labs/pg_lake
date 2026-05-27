/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Privileged-SPI helpers for pg_lake_ducklake. Equivalent to the
 * SPI_START_EXTENSION_OWNER / SPI_END macros in pg_extension_base, but
 * exposed as functions so multiple SPI sessions can coexist in one
 * function body without macro-local variables (_savedUserId, ...)
 * colliding. Each caller declares its own `DucklakePrivSPIState` on
 * the stack and pairs DucklakeBeginPrivilegedSPI/DucklakeEndPrivilegedSPI
 * around the SPI_exec calls.
 *
 * What this buys versus raw SPI_connect/SPI_finish:
 *   - runs as the pg_lake_ducklake extension owner so users without
 *     write access to lake_ducklake.* still see their writes go through
 *   - locks search_path to pg_catalog,pg_temp during the SPI block so
 *     unqualified operators/functions can't be hijacked
 *   - filters the internal queries out of pg_stat_statements / pgaudit
 *     / auto_explain
 *   - SECURITY_RESTRICTED_OPERATION rejects SET/RESET smuggled in via
 *     parameter values
 */
#pragma once

#include "postgres.h"


typedef struct DucklakePrivSPIState
{
	Oid			savedUserId;
	int			savedSecurityContext;
	int			gucNestLevel;
}			DucklakePrivSPIState;


extern PGDLLEXPORT void DucklakeBeginPrivilegedSPI(DucklakePrivSPIState * state);
extern PGDLLEXPORT void DucklakeEndPrivilegedSPI(DucklakePrivSPIState * state);
