/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Function-form equivalents of pg_extension_base's
 * SPI_START_EXTENSION_OWNER / SPI_END macros. See the header for the
 * threat model. Functions pair with a caller-declared
 * DucklakePrivSPIState on the stack so multiple SPI sessions in one
 * function don't collide on the macro's hidden locals.
 */
#include "postgres.h"

#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_lake/ducklake/spi_priv.h"
#include "pg_lake/extensions/pg_lake_ducklake.h"


void
DucklakeBeginPrivilegedSPI(DucklakePrivSPIState * state)
{
	state->savedUserId = InvalidOid;
	state->savedSecurityContext = 0;

	GetUserIdAndSecContext(&state->savedUserId, &state->savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeDucklake),
						   SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_RESTRICTED_OPERATION);

	state->gucNestLevel = NewGUCNestLevel();
	(void) set_config_option("auto_explain.log_min_duration", "-1",
							 PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE,
							 true, 0, false);
	(void) set_config_option("pgaudit.log", "none",
							 PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE,
							 true, 0, false);
	(void) set_config_option("pg_stat_statements.track", "none",
							 PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE,
							 true, 0, false);
	(void) set_config_option("search_path", "pg_catalog, pg_temp",
							 PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE,
							 true, 0, false);

	SPI_connect();
}


void
DucklakeEndPrivilegedSPI(DucklakePrivSPIState * state)
{
	if (state->savedUserId != InvalidOid)
		SetUserIdAndSecContext(state->savedUserId, state->savedSecurityContext);

	AtEOXact_GUC(true, state->gucNestLevel);

	SPI_finish();
}
