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

#include "postgres.h"
#include "miscadmin.h"

#include "access/table.h"
#include "access/xact.h"
#include "access/genam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_class.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "utils/syscache.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/ddl/vacuum.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/iceberg/operations/vacuum.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/transaction/transaction_hooks.h"
#include "pg_lake/util/injection_points.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_extension_base/spi_helpers.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"

#include "pg_extension_base/base_workers.h"

/* managed by a GUC, not exposed to the user, mostly to ensure termination, compact at most 100 times */
int			MaxCompactionsPerVacuum = 100;

/* case insensitive */
#define PG_LAKE_ICEBERG_VACUUM_FLAG "iceberg"

/* managed by a GUC, not exposed to the user, see note in VacuumRemoveInProgressFiles */
int			MaxFileRemovalsPerVacuum = 100000;


PG_FUNCTION_INFO_V1(pg_lake_iceberg_vacuum);


static void VacuumRegisterMissingFieldsForAllTables(MemoryContext outOfTransactionMemoryContext);
static void PgLakeIcebergVacuumForTables(MemoryContext outOfTransactionMemoryContext,
										 bool firstLoop);
static bool IsVacuumLakeTable(VacuumStmt *vacuumStmt);
static List *GetAutoVacuumEnabledTables(List *relationIdList);
static bool IsAutoVacuumEnabled(Oid relationId);
static void VacuumLakeTables(ProcessUtilityParams * utilityParams);
static List *GetPgLakePartitionIds(Oid relationId);
static bool ProcessVacuumPgLakeIcebergFlag(VacuumStmt *vacuumStmt);
static void VacuumCompactDataFiles(Oid relationId, bool isFull, bool isVerbose);
static void VacuumCompactMetadata(Oid relationId, bool isVerbose);
static void VacuumRemoveDeletionQueueRecords(Oid relationId, bool isFull, bool isVerbose);
static void VacuumRemoveInProgressFiles(Oid relationId, bool isFull, bool isVerbose);
static void VacuumRegisterMissingFields(Oid relationId);
static void PgLakeIcebergVacuumForRelation(Oid relationId, bool firstLoop);
static void VacuumTableInSeparateXacts(Oid relationId, bool isFull, bool isVerbose);
static void VacuumDroppedPgLakeIcebergTables(VacuumStmt *vacuumStmt);
static char *GetMetadataLocationPrefixForRelationId(Oid relationId);
static void VacuumConsumeTrackedIcebergMetadataChanges(void);


/*
* pg_lake_iceberg_vacuum is a function that continuously vacuums all iceberg
* tables on the server. This function powers auto-vacuum for iceberg tables via
* base worker registered.
*/
Datum
pg_lake_iceberg_vacuum(PG_FUNCTION_ARGS)
{
	bool		firstLoop = true;

	/* report application_name in pg_stat_activity */
	pgstat_report_appname("pg_lake autovacuum");

	/* report process name in ps (follows "pg_extension_base worker") */
	set_ps_display(psprintf("(pg_lake autovacuum for database %d)", MyDatabaseId));

	/*
	 * Vacuuming iceberg tables starts and commits transactions in a loop. To
	 * avoid memory allocation issues, certain data structures (e.g., relation
	 * IDs that need to be vacuumed) are allocated in a child context of
	 * CacheMemoryContext, which is reset on each loop iteration.
	 */
	MemoryContext outOfTransactionMemoryContext =
		AllocSetContextCreate(CacheMemoryContext, "pg_lake_iceberg vacuum",
							  ALLOCSET_DEFAULT_SIZES);


	START_TRANSACTION();
	{
		/*
		 * Before we start vacuuming, we need to register missing fields for
		 * all iceberg tables. This is a no-op for tables that already have
		 * all fields registered. In other words, only tables that have been
		 * created prior to v2.1 will have missing fields.
		 */
		VacuumRegisterMissingFieldsForAllTables(outOfTransactionMemoryContext);
	}
	END_TRANSACTION_NO_THROW(WARNING);

	/* set up invalidation callbacks */
	InitObjectStoreCatalog();

	TimestampTz lastVacuumTime = GetCurrentTimestamp();
	TimestampTz lastCatalogExportTime = GetCurrentTimestamp();

	while (true)
	{
		TimestampTz currentTime = GetCurrentTimestamp();

		if (IcebergAutovacuumEnabled &&
			TimestampDifferenceExceeds(lastVacuumTime, currentTime,
									   IcebergAutovacuumNaptime * 1000))
		{
			START_TRANSACTION();
			{
				PgLakeIcebergVacuumForTables(outOfTransactionMemoryContext, firstLoop);
			}
			END_TRANSACTION_NO_THROW(WARNING);

			firstLoop = false;

			/* we wait for nap time _after_ vacuum completed */
			lastVacuumTime = GetCurrentTimestamp();
		}

		if (EnableObjectStoreCatalog &&
			TimestampDifferenceExceeds(lastCatalogExportTime, currentTime, 1000))
		{
			/* initiate push at regular cadence */
			lastCatalogExportTime = GetCurrentTimestamp();

			START_TRANSACTION();
			{
				ExportIcebergCatalogIfChanged();
			}
			END_TRANSACTION_NO_THROW(WARNING);
		}

		MemoryContextReset(outOfTransactionMemoryContext);

		LightSleep(1000);
	}
	PG_RETURN_VOID();
}


/*
* PgLakeIcebergVacuumForTables is the work-horse function for auto-vacuuming.
* It is called by pg_lake_iceberg_vacuum.
*
* The memory context `outOfTransactionMemoryContext` is used to allocate memory
* that needs to persist across the multiple transactions opened and closed
* by VACUUM. This ensures that certain data lives longer than the individual
* transactions.
*
* We also pass `firstLoop` to the function to determine if we should log a warning
* for read-only tables.
*/
static void
PgLakeIcebergVacuumForTables(MemoryContext outOfTransactionMemoryContext,
							 bool firstLoop)
{
	MemoryContext oldctx = MemoryContextSwitchTo(outOfTransactionMemoryContext);
	List	   *relationIdList = GetAllInternalIcebergRelationIds();

	List	   *vacuumRelationIdList = GetAutoVacuumEnabledTables(relationIdList);

	MemoryContextSwitchTo(oldctx);

	foreach_oid(relationId, vacuumRelationIdList)
	{
		/* vacuum the iceberg table */
		PgLakeIcebergVacuumForRelation(relationId, firstLoop);

		/*
		 * This loop could take a long time, so we check for interrupts on
		 * each iteration.
		 */
		CHECK_FOR_INTERRUPTS();
	}
}


/*
* VacuumRegisterMissingFieldsForAllTables is a utility function that registers
* missing fields for all iceberg tables.
*/
static void
VacuumRegisterMissingFieldsForAllTables(MemoryContext outOfTransactionMemoryContext)
{
	MemoryContext oldctx = MemoryContextSwitchTo(outOfTransactionMemoryContext);
	List	   *relationIdList = GetAllInternalIcebergRelationIds();

	MemoryContextSwitchTo(oldctx);

	foreach_oid(relationId, relationIdList)
	{
		/* vacuum the iceberg table */
		VacuumRegisterMissingFields(relationId);

		/*
		 * This loop could take a long time, so we check for interrupts on
		 * each iteration.
		 */
		CHECK_FOR_INTERRUPTS();
	}
}


/*
* PgLakeIcebergVacuumForRelation is the work-horse function for auto-vacuuming
* per iceberg table. It is called by pg_lake_iceberg_vacuum.
*/
static void
PgLakeIcebergVacuumForRelation(Oid relationId, bool firstLoop)
{
	if (!ActiveSnapshotSet())
		PushActiveSnapshot(GetTransactionSnapshot());

	TimestampTz startTime = GetCurrentTimestamp();

	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		/* table dropped */
		return;
	}

	if (IsReadOnlyIcebergTable(relationId))
	{
		/*
		 * Only warn for the first loop, always let other tables VACUUMed.
		 */
		if (firstLoop)
		{
			ereport(WARNING,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("skipping autovacuum for iceberg table \"%s\" "
							"as it is read-only", get_rel_name(relationId))));
		}

		/* table read-only */
		return;
	}

	bool		isFull = false;
	bool		isVerbose = false;

	/* main VACUUM logic for the iceberg table */
	VacuumTableInSeparateXacts(relationId, isFull, isVerbose);

	TimestampTz endTime = GetCurrentTimestamp();

	if (IcebergAutovacuumLogMinDuration != -1 &&
		TimestampDifferenceExceeds(startTime, endTime, IcebergAutovacuumLogMinDuration))
	{
		long		secs_dur;
		int			usecs_dur;

		TimestampDifference(startTime, endTime, &secs_dur, &usecs_dur);

		ereport(LOG, (errmsg("Vacuuming iceberg table %s on "
							 "database %s took %ld.%06d s",
							 GetQualifiedRelationName(relationId),
							 get_database_name(MyDatabaseId),
							 secs_dur, usecs_dur)));
	}
}


/*
* GetAutoVacuumEnabledTables returns a list of relation ids that have autovacuum
* enabled.
*/
static List *
GetAutoVacuumEnabledTables(List *relationIdList)
{
	List	   *filteredRelationIdList = NIL;

	foreach_oid(relationId, relationIdList)
	{
		if (IsAutoVacuumEnabled(relationId))
		{
			filteredRelationIdList = lappend_oid(filteredRelationIdList, relationId);
		}
	}

	return filteredRelationIdList;
}

/*
 * IsAutoVacuumEnabled returns whether the given iceberg table has autovacuum
 * enabled.
 */
static bool
IsAutoVacuumEnabled(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	DefElem    *autoVacuumOption = GetOption(options, "autovacuum_enabled");

	return autoVacuumOption != NULL ? defGetBoolean(autoVacuumOption) : true;
}

/*
 * ProcessVacuumPgLakeTable is a utility statement handler for handling
 * VACUUM statements on pg_lake tables
 */
bool
ProcessVacuumPgLakeTable(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, VacuumStmt))
	{
		/* not a vacuum */
		return false;
	}

	VacuumStmt *vacuumStmt =
		(VacuumStmt *) plannedStmt->utilityStmt;

	if (!vacuumStmt->is_vacuumcmd)
	{
		/* we do not have special logic for plain ANALYZE */
		return false;
	}

	/* append all pg_lake_iceberg tables if pg_lake_iceberg flag is used */
	bool		hasIcebergFlag = ProcessVacuumPgLakeIcebergFlag(vacuumStmt);

	if (!hasIcebergFlag && !IsVacuumLakeTable(vacuumStmt))
	{
		/* not a lake table */
		return false;
	}

	VacuumLakeTables(params);

	if (hasIcebergFlag)
	{
		VacuumDroppedPgLakeIcebergTables(vacuumStmt);
	}

	return true;
}


/*
* We introduce a new VACUUM option, "iceberg", which will vacuum all
* pg_lake_iceberg tables.
* We do not let the users to specify a list of relations when
* using this option.
* We return true if the option is used, false otherwise.
*/
static bool
ProcessVacuumPgLakeIcebergFlag(VacuumStmt *vacuumStmt)
{
	foreach_ptr(DefElem, opt, vacuumStmt->options)
	{
		if (strcmp(opt->defname, PG_LAKE_ICEBERG_VACUUM_FLAG) == 0)
		{
			if (vacuumStmt->rels != NIL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("VACUUM (ICEBERG) does not accept a list of relations"),
						 errhint("Use VACUUM (ICEBERG) without any relation. The command "
								 "will vacuum all iceberg tables.")));
			}

			/* now, first remove the pg_lake_iceberg option */
			vacuumStmt->options = list_delete_ptr(vacuumStmt->options, opt);

			/* now, add all pg_lake_iceberg tables to vacuumStmt->rels */
			List	   *relationIdList = GetAllInternalIcebergRelationIds();

			foreach_oid(relationId, relationIdList)
			{
				RangeVar   *rv = makeRangeVar(get_namespace_name(get_rel_namespace(relationId)),
											  get_rel_name(relationId), -1);
				VacuumRelation *vacuumRel = makeVacuumRelation(rv, relationId, NIL);

				vacuumStmt->rels = lappend(vacuumStmt->rels, vacuumRel);
			}

			/* we are done, we don't care about the remaining VACUUM options */
			return true;
		}
	}

	return false;
}


/*
 * IsVacuumLakeTable returns whether the given CREATE FOREIGN TABLE statement
 * will create a lake table.
 */
static bool
IsVacuumLakeTable(VacuumStmt *vacuumStmt)
{
	ListCell   *relationCell = NULL;

	foreach(relationCell, vacuumStmt->rels)
	{
		VacuumRelation *vacuumRel = lfirst(relationCell);
		bool		missingOk = true;
		Oid			relationId = RangeVarGetRelid(vacuumRel->relation, NoLock, missingOk);

		if (IsAnyLakeForeignTableById(relationId))
			return true;
		else if (get_rel_relkind(relationId) == RELKIND_PARTITIONED_TABLE)
			return GetPgLakePartitionIds(relationId) != NIL;
	}

	return false;
}


/*
 * VacuumLakeTables performs a vacuum in lake
 * tables and then proceeds with regular VACUUM for the remaining tables.
 */
static void
VacuumLakeTables(ProcessUtilityParams * utilityParams)
{
	Assert(IsA(utilityParams->plannedStmt->utilityStmt, VacuumStmt));
	VacuumStmt *vacuumStmt = (VacuumStmt *) utilityParams->plannedStmt->utilityStmt;

	/* what we do in VACUUM cannot be rollbacked, so prevent tx block */
	bool		isTopLevel = (utilityParams->context == PROCESS_UTILITY_TOPLEVEL);

	PreventInTransactionBlock(isTopLevel, "VACUUM");

	List	   *regularTables = NIL;
	ListCell   *relationCell = NULL;

	DefElem    *fullOption = GetOption(vacuumStmt->options, "full");
	bool		isFull = fullOption != NULL ? defGetBoolean(fullOption) : false;
	DefElem    *verboseOption = GetOption(vacuumStmt->options, "verbose");
	bool		isVerbose = verboseOption != NULL ? defGetBoolean(verboseOption) : false;
	DefElem    *analyzeOption = GetOption(vacuumStmt->options, "analyze");
	bool		isAnalyze = analyzeOption != NULL ? defGetBoolean(analyzeOption) : false;

	List	   *compactionCandidates = NIL;

	/* create a memory context that survives across transactions */
	MemoryContext vacuumContext = AllocSetContextCreate(PortalContext,
														"pg_lake Vacuum",
														ALLOCSET_DEFAULT_SIZES);

	MemoryContext oldContext = MemoryContextSwitchTo(vacuumContext);

	/*
	 * Create a modifiable copy that we can still pass to ProcessUtility after
	 * running some transactions.
	 */
	vacuumStmt = (VacuumStmt *) CopyUtilityStmt(utilityParams);

	foreach(relationCell, vacuumStmt->rels)
	{
		VacuumRelation *vacuumRel = lfirst(relationCell);
		bool		missingOk = true;
		Oid			relationId = RangeVarGetRelid(vacuumRel->relation, NoLock, missingOk);

		if (IsAnyLakeForeignTableById(relationId))
		{
			if (vacuumRel->va_cols && !isAnalyze)
			{
				/* match the postgres error for vacuuming column names */
				ereport(ERROR, (errmsg("ANALYZE option must be specified when a column list is provided")));
			}

			/* vacuum the table only if we're the owner */
			if (object_ownercheck(RelationRelationId, relationId, GetUserId()) ||
				object_ownercheck(DatabaseRelationId, MyDatabaseId, GetUserId()))
			{
				compactionCandidates = lappend_oid(compactionCandidates, relationId);
			}
			else
			{
				ereport(WARNING, (errmsg("permission denied to vacuum \"%s\", skipping it",
										 get_rel_name(relationId))));
			}

			continue;
		}

		if (get_rel_relkind(relationId) == RELKIND_PARTITIONED_TABLE)
		{
			List	   *partitionRels = GetPgLakePartitionIds(relationId);

			compactionCandidates = list_concat(compactionCandidates, partitionRels);

			/*
			 * We still preserve the partitioned table in the regular list, as
			 * it may have other children, though this might result in some
			 * warnings.
			 */
		}

		regularTables = lappend(regularTables, vacuumRel);
	}

	MemoryContextSwitchTo(oldContext);

	/* do not include foreign tables in regular VACUUM */
	vacuumStmt->rels = regularTables;

	/* run regular vacuum, unless we removed all the tables */
	if (regularTables != NIL)
		PgLakeCommonParentProcessUtility(utilityParams);

	/* do compaction */
	ListCell   *relationIdCell = NULL;

	foreach(relationIdCell, compactionCandidates)
	{
		Oid			relationId = lfirst_oid(relationIdCell);

		/* skip read only lake tables */
		if (!IsWritablePgLakeTable(relationId) && !IsWritableIcebergTable(relationId))
		{
			ereport(WARNING,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lake table \"%s\" is read-only", get_rel_name(relationId))));

			/* let other tables VACUUMed */
			continue;
		}

		VacuumTableInSeparateXacts(relationId, isFull, isVerbose);
	}
}


/*
 * GetPgLakePartitionIds returns the children or other descendants
 * of the given relation that are lake tables.
 */
static List *
GetPgLakePartitionIds(Oid relationId)
{
	List	   *inheritors = find_all_inheritors(relationId, NoLock, NULL);
	ListCell   *inheritorCell = NULL;

	List	   *lakeInheritors = NIL;

	foreach(inheritorCell, inheritors)
	{
		Oid			childId = lfirst_oid(inheritorCell);

		if (IsAnyLakeForeignTableById(childId))
			lakeInheritors = lappend_oid(lakeInheritors, childId);
	}

	return lakeInheritors;
}


/*
 * VacuumTableInSeparateXacts repeatedly tries to compact the table
 * until there is nothing to compact. After each compaction operation,
 * we commit the preceding transaction to release locks and help forward
 * progress.
 *
 * It currently performs the following operations:
 * 1. Compact data files
 * 2. Expire old snapshots & merge manifests
 * 3. Remove unreferenced files
 * 4. For pre-2.1 tables, register fields in field_id_mappings
 */
static void
VacuumTableInSeparateXacts(Oid relationId, bool isFull, bool isVerbose)
{
	VacuumCompactDataFiles(relationId, isFull, isVerbose);
	VacuumCompactMetadata(relationId, isVerbose);
	VacuumRemoveDeletionQueueRecords(relationId, isFull, isVerbose);
	VacuumRemoveInProgressFiles(relationId, isFull, isVerbose);
	VacuumRegisterMissingFields(relationId);
}


/*
 * VacuumCompactDataFiles compacts data files for the given relation.
 */
static void
VacuumCompactDataFiles(Oid relationId, bool isFull, bool isVerbose)
{
	volatile bool continueCompaction = false;
	int			compactionCount = 0;

	/*
	 * Compact files that are created before VACUUM starts. We do this because
	 * compaction happens in multiple iterations, and on each iteration we
	 * process certain number of files. If we do not eliminate the files that
	 * are created before the VACUUM starts, VACUUM may take very long time to
	 * finish. That's not wrong, but we want to make sure that VACUUM finishes
	 * in a reasonable time and wait until more data files are available for
	 * compaction.
	 *
	 * We prefer to use the transaction start time instead of the current time
	 * because otherwise files created during the compaction may be modified
	 * in the same VACUUM.
	 */
	TimestampTz compactionStartTime = GetCurrentTransactionStartTimestamp();

	do
	{
		INJECTION_POINT_COMPAT("compact-files-before-snapshot");

		if (!ActiveSnapshotSet())
			PushActiveSnapshot(GetTransactionSnapshot());

		INJECTION_POINT_COMPAT("compact-files-after-snapshot");

		MemoryContext savedContext = CurrentMemoryContext;

		/*
		 * We may need to rollback the initial state of the database in case
		 * of rollback.
		 */
		BeginInternalSubTransaction(NULL);

		INJECTION_POINT_COMPAT("compact-files-before-try");

		PG_TRY();
		{
			INJECTION_POINT_COMPAT("compact-files-before-compact");

			/* do compaction */
			continueCompaction = CompactDataFiles(relationId, compactionStartTime, isFull, isVerbose);

			VacuumConsumeTrackedIcebergMetadataChanges();

			INJECTION_POINT_COMPAT("compact-files-after-compact");

			ReleaseCurrentSubTransaction();

			INJECTION_POINT_COMPAT("compact-files-after-tx");
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(savedContext);
			ErrorData  *edata = CopyErrorData();

			FlushErrorState();

			RollbackAndReleaseCurrentSubTransaction();

			ResetTrackedIcebergMetadataOperation();
			ResetRestCatalogRequests();

			/* continue unless it was a cancellation */
			if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
				edata->elevel = WARNING;

			ThrowErrorData(edata);

			continueCompaction = false;
		}
		PG_END_TRY();

		/* rotate into a new transaction to release locks and save progress */
		PopActiveSnapshot();
		CommitTransactionCommand();
		StartTransactionCommand();
		compactionCount++;
	}
	while (continueCompaction && compactionCount < MaxCompactionsPerVacuum);
}


/*
 * VacuumCompactMetadata first applies the snapshot retention policy, then it records the
 * files that are not referenced by any snapshot.
 *
 * It also merges manifests if needed.
 */
static void
VacuumCompactMetadata(Oid relationId, bool isVerbose)
{
	if (!ActiveSnapshotSet())
		PushActiveSnapshot(GetTransactionSnapshot());

	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	if (tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		return;
	}

	MemoryContext savedContext = CurrentMemoryContext;

	/*
	 * We may need to rollback the initial state of the database in case of
	 * rollback.
	 */
	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		INJECTION_POINT_COMPAT("expire-snapshots");

		CompactMetadata(relationId, isVerbose);

		VacuumConsumeTrackedIcebergMetadataChanges();

		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();

		ResetTrackedIcebergMetadataOperation();
		ResetRestCatalogRequests();

		/* continue unless it was a cancellation */
		if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
			edata->elevel = WARNING;

		ThrowErrorData(edata);
	}
	PG_END_TRY();

	/* rotate into a new transaction to release locks and save progress */
	PopActiveSnapshot();
	CommitTransactionCommand();
	StartTransactionCommand();
}


/*
* VacuumDroppedPgLakeIcebergTables removes the deletion queue records for
* dropped pg_lake_iceberg tables.
*/
static void
VacuumDroppedPgLakeIcebergTables(VacuumStmt *vacuumStmt)
{
	DefElem    *fullOption = GetOption(vacuumStmt->options, "full");
	bool		isFull = fullOption != NULL ? defGetBoolean(fullOption) : false;
	DefElem    *verboseOption = GetOption(vacuumStmt->options, "verbose");
	bool		isVerbose = verboseOption != NULL ? defGetBoolean(verboseOption) : false;

	Oid			relationId = InvalidOid;

	VacuumRemoveDeletionQueueRecords(relationId, isFull, isVerbose);
	VacuumRemoveInProgressFiles(relationId, isFull, isVerbose);
}


/*
* Looping until we have removed the maximum number of files per vacuum
* or until there are no more files to remove.
*
* When isFull is true, we remove all cleanup records.
*/
static void
VacuumRemoveDeletionQueueRecords(Oid relationId, bool isFull, bool isVerbose)
{
	int			totalFilesRemoved = 0;

	volatile bool hasRemainingFiles = true;
	MemoryContext savedContext = CurrentMemoryContext;

	do
	{
		if (!ActiveSnapshotSet())
			PushActiveSnapshot(GetTransactionSnapshot());

		/*
		 * We may need to rollback the initial state of the database in case
		 * of rollback.
		 */
		BeginInternalSubTransaction(NULL);

		List	   *deletionQueueRecords = NIL;

		PG_TRY();
		{
			INJECTION_POINT_COMPAT("deletion-queue");

			/* do cleanup */
			deletionQueueRecords = GetDeletionQueueRecords(relationId, isFull);

			hasRemainingFiles = RemoveDeletionQueueRecords(deletionQueueRecords, isVerbose);

			VacuumConsumeTrackedIcebergMetadataChanges();

			ReleaseCurrentSubTransaction();
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(savedContext);
			ErrorData  *edata = CopyErrorData();

			FlushErrorState();

			RollbackAndReleaseCurrentSubTransaction();

			ResetTrackedIcebergMetadataOperation();
			ResetRestCatalogRequests();

			/* continue unless it was a cancellation */
			if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
				edata->elevel = WARNING;

			ThrowErrorData(edata);

			/* do not continue in case of failure */
			hasRemainingFiles = false;
		}
		PG_END_TRY();

		totalFilesRemoved += list_length(deletionQueueRecords);
		hasRemainingFiles = hasRemainingFiles ? list_length(deletionQueueRecords) > 0 : false;

		/* rotate into a new transaction to release locks and save progress */
		PopActiveSnapshot();
		CommitTransactionCommand();
		StartTransactionCommand();
	}
	while (!isFull				/* when isFull, we'll remove all files */
		   && totalFilesRemoved < MaxFileRemovalsPerVacuum	/* per-vacuum limit */
		   && hasRemainingFiles /* no more files to remove */ );
}

/*
* Looping until we have removed the maximum number of files per vacuum
* or until there are no more files to remove.
*
* When isFull is true, we remove all cleanup records.
*/
static void
VacuumRemoveInProgressFiles(Oid relationId, bool isFull, bool isVerbose)
{
	if (relationId != InvalidOid && !IsPgLakeIcebergForeignTableById(relationId))
	{
		return;
	}

	int			totalFilesRemoved = 0;

	List	   *removedFiles = NIL;
	volatile bool hasRemainingFiles = true;
	MemoryContext savedContext = CurrentMemoryContext;

	do
	{
		if (!ActiveSnapshotSet())
			PushActiveSnapshot(GetTransactionSnapshot());

		/*
		 * We may need to rollback the initial state of the database in case
		 * of rollback.
		 */
		BeginInternalSubTransaction(NULL);

		PG_TRY();
		{
			INJECTION_POINT_COMPAT("in-progress-files");

			char	   *locationPrefix = GetMetadataLocationPrefixForRelationId(relationId);

			hasRemainingFiles = RemoveInProgressFiles(locationPrefix, isFull, isVerbose, &removedFiles);

			VacuumConsumeTrackedIcebergMetadataChanges();

			ReleaseCurrentSubTransaction();
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(savedContext);
			ErrorData  *edata = CopyErrorData();

			FlushErrorState();

			RollbackAndReleaseCurrentSubTransaction();

			ResetTrackedIcebergMetadataOperation();
			ResetRestCatalogRequests();

			/* continue unless it was a cancellation */
			if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
				edata->elevel = WARNING;

			ThrowErrorData(edata);

			/* do not continue in case of failure */
			hasRemainingFiles = false;
		}
		PG_END_TRY();

		totalFilesRemoved += list_length(removedFiles);

		/* rotate into a new transaction to release locks and save progress */
		PopActiveSnapshot();
		CommitTransactionCommand();
		StartTransactionCommand();
	}
	while (!isFull				/* when isFull, we'll remove all files */
		   && totalFilesRemoved < MaxFileRemovalsPerVacuum	/* per-vacuum limit */
		   && hasRemainingFiles /* no more files to remove */ );
}


/*
* VacuumRegisterMissingFields registers the column mappings for
* the given relation.
* You'd normally do not expect this is required, but it is possible that
* the table was created before we started tracking field IDs.
* We should be able to remove this function in the future, when we have
* column mappings for all tables.
*/
static void
VacuumRegisterMissingFields(Oid relationId)
{
	if (relationId == InvalidOid)
	{
		/* nothing to do for dropped tables */
		return;
	}

	if (!IsInternalIcebergTable(relationId))
	{
		return;
	}

	MemoryContext savedContext = CurrentMemoryContext;

	if (!ActiveSnapshotSet())
		PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * We may need to rollback the initial state of the database in case of
	 * rollback.
	 */
	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		if (GetLargestRegisteredFieldId(relationId) == INVALID_FIELD_ID)
		{
			ereport(LOG,
					(errmsg("Registering missing fields for iceberg table \"%s\"",
							GetQualifiedRelationName(relationId))));

			/*
			 * We have not already registered any fields, so this table is
			 * likely created before we started tracking field IDs.
			 */
			List	   *postgresColumnMappings =
				CreatePostgresColumnMappingsForIcebergTableFromExternalMetadata(relationId);

			RegisterPostgresColumnMappings(postgresColumnMappings);
		}

		VacuumConsumeTrackedIcebergMetadataChanges();

		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();

		ResetTrackedIcebergMetadataOperation();
		ResetRestCatalogRequests();

		/* continue unless it was a cancellation */
		if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
			edata->elevel = WARNING;

		ThrowErrorData(edata);

	}
	PG_END_TRY();

	/* rotate into a new transaction to release locks and save progress */
	PopActiveSnapshot();
	CommitTransactionCommand();
	StartTransactionCommand();
}


/*
* GetMetadataLocationPrefixForRelationId returns the metadata location prefix
* for the given relation. If the relationId is InvalidOid, it returns an empty
* string, which is used to remove all in-progress files.
*/
static char *
GetMetadataLocationPrefixForRelationId(Oid relationId)
{
	if (relationId == InvalidOid)
	{
		/*
		 * InvalidOid is mainly used for the dropped tables. The idea is to
		 * remove the in-progress files that are not associated with any
		 * table. We call VacuumRemoveInProgressFiles with InvalidOid when
		 * user does a VACUUM(ICEBERG). In this case, we want to remove all
		 * in-progress files. We set location to empty string.
		 *
		 * Though, there is a minor gap in our implementation. Per VACUUM on
		 * table, we only allow MaxFileRemovalsPerVacuum files to be removed.
		 *
		 * So, if a non-dropped table has more than MaxFileRemovalsPerVacuum
		 * in-progress files, we might continue removing its files here.
		 *
		 * Given MaxFileRemovalsPerVacuum is a GUC that is not exposed to the
		 * user, we are not too worried about this gap.
		 */
		return "";
	}
	else
	{
		char	   *metadataLocation = GetIcebergMetadataLocation(relationId, false);
		IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataLocation);

		/* cast (const char *) to (char *) */
		return (char *) metadata->location;
	}
}


/*
 * VacuumConsumeTrackedIcebergMetadataChanges
 * Normally, Iceberg metadata changes are applied in the
 * pre-commit hook. That means for this operation they would
 * normally run inside `CommitTransactionCommand()`.
 *
 * VACUUM is treated as a special case: we don’t want it to fail
 * due to metadata errors. To simplify error handling, we apply
 * the Iceberg metadata changes here instead, where error handling
 * is more structured.
 *
 * In theory, we could rely on the pre-commit hook alone. But
 * since error handling is more difficult there than here, keep
 * this in mind before removing this logic.
*/
static void
VacuumConsumeTrackedIcebergMetadataChanges(void)
{
	ConsumeTrackedIcebergMetadataChanges();
}
