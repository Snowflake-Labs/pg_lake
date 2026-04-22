/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * Postgres extension updater
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "pg_extension_base/base_workers.h"
#include "pg_extension_base/spi_helpers.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;

typedef struct UpdatableExtension
{
	/* name of the extension */
	char	   *extensionName;

	/* default version (usually the latest installed version) */
	char	   *defaultVersion;
} UpdatableExtension;


/* function declarations */
void		_PG_init(void);
static List *GetUpdatableExtensionList(void);
static void UpdateExtension(char *extensionName);

/* UDF declarations */
PG_FUNCTION_INFO_V1(pg_extension_updater_main);

/* settings */
static bool EnableExtensionUpdates = true;


/*
 * _PG_init is the entry-point for pg_extension_updater.
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable(
							 "pg_extension_updater.enable",
							 gettext_noop("Enables the automatic extension updates (default on)"),
							 NULL,
							 &EnableExtensionUpdates,
							 true,
							 PGC_POSTMASTER,
							 GUC_STANDARD,
							 NULL, NULL, NULL);

}


/*
 * pg_extension_updater_main is the main entry-point for the base worker
 * that updates all extensions.
 */
Datum
pg_extension_updater_main(PG_FUNCTION_ARGS)
{
	int32		workerId = PG_GETARG_INT32(0);

	if (!EnableExtensionUpdates)
	{
		PG_RETURN_VOID();
	}

	List	   *extensionList = GetUpdatableExtensionList();

	ListCell   *extensionCell = NULL;

	foreach(extensionCell, extensionList)
	{
		UpdatableExtension *extension = lfirst(extensionCell);

		ereport(LOG, (errmsg("automatically updating extension %s to version %s",
							 extension->extensionName,
							 extension->defaultVersion)));

		UpdateExtension(extension->extensionName);
	}

	PG_RETURN_VOID();
}


/*
 * GetUpdatableExtensionList returns a list of updatable extensions.
 */
static List *
GetUpdatableExtensionList(void)
{
	List	   *extensionList = NIL;

	START_TRANSACTION();
	{
		SPI_connect();

		bool		readOnly = true;
		long		limit = 0;

		/* no limit */

		SPI_execute("select name, default_version "
					"from pg_catalog.pg_available_extensions "
					"where installed_version operator(pg_catalog.<>) default_version",
					readOnly, limit);

		for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
		{
			bool		isNull = false;
			Name		extensionName = GET_SPI_VALUE(NAMEOID, rowIndex, 1, &isNull);
			char	   *defaultVersion = GET_SPI_VALUE(TEXTOID, rowIndex, 2, &isNull);

			MemoryContext spiContext = MemoryContextSwitchTo(OuterContext);

			UpdatableExtension *extension = palloc0(sizeof(UpdatableExtension));

			extension->extensionName = pstrdup(NameStr(*extensionName));
			extension->defaultVersion = pstrdup(defaultVersion);

			extensionList = lappend(extensionList, extension);

			MemoryContextSwitchTo(spiContext);
		}

		SPI_finish();
	}
	END_TRANSACTION();

	return extensionList;
}


/*
 * UpdateExtension updates a given extension in a separate transaction.
 * and logs a warning in case the alter extension fails.
 */
static void
UpdateExtension(char *extensionName)
{
	START_TRANSACTION();
	{
		SPI_connect();

		bool		readOnly = false;
		long		limit = 0;

		char	   *query = psprintf("alter extension %s update",
									 quote_identifier(extensionName));

		SPI_execute(query, readOnly, limit);
		SPI_finish();
	}
	END_TRANSACTION_NO_THROW(WARNING);
}
