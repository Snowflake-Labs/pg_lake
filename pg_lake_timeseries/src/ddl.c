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

/*
 * DDL for tiered tables (DESIGN.md section 5).
 *
 * A tiered table is made with one statement:
 *
 *   CREATE TABLE metrics (ts timestamptz NOT NULL, device int, value float8)
 *   USING timeseries WITH (partition_interval = '1 hour',
 *                          hot_retention = '2 hours',
 *                          location = 's3://bucket/metrics');
 *
 * which this file turns into the partitioned heap the user asked for, an Iceberg
 * table next to it holding the same columns, a registration, and the first
 * partitions. Everything else -- CREATE INDEX, ALTER TABLE, DROP TABLE -- is
 * ordinary DDL on the user's own relation. The two tiers only have to agree about
 * the *shape* of the table, so exactly the subcommands that change a column's
 * name, type or existence are forwarded to the Iceberg tier, where pg_lake
 * applies Iceberg's own rules; the rest is a hot-tier matter and is left alone.
 *
 * There is deliberately no function that marks an existing pair of tables as
 * tiered. The invariants the planner hook relies on (DESIGN.md section 7) are all
 * established here by construction rather than validated after the fact.
 *
 * Statements are intercepted through pg_lake's utility handler framework
 * (RegisterUtilityStatementHandler), which the iceberg access method uses the
 * same way. A handler that has something to add executes the user's statement
 * itself, through ExecuteUserStatement(), and returns true:
 *
 *   - CREATE TABLE is rewritten first, so PostgreSQL never sees
 *     "USING timeseries", and the Iceberg tier is built from the heap afterwards;
 *   - ALTER TABLE / RENAME work out what the Iceberg tier has to be told from the
 *     statement before it runs -- a column being dropped still exists at that
 *     point -- and tell it afterwards, in the same transaction.
 */
#include "postgres.h"

#include "access/attnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/access_method/access_method.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/query/execute.h"

#include "pg_lake_timeseries/ddl.h"
#include "pg_lake_timeseries/metadata.h"
#include "pg_lake_timeseries/registry.h"


/*
 * TimeseriesOptions holds the WITH options of CREATE TABLE ... USING timeseries:
 * the ones this extension understands, parsed, and every other one left as it was
 * written so that it can be handed to the Iceberg tier.
 */
typedef struct TimeseriesOptions
{
	char	   *timeColumn;
	Interval   *partitionInterval;
	Interval   *hotRetention;
	Interval   *coldRetention;
	int32		precreateAhead;
	List	   *coldOptions;
}			TimeseriesOptions;


PG_FUNCTION_INFO_V1(timeseries_am_handler);


/*
 * The statement a handler below is having PostgreSQL execute, if any.
 *
 * Executing it starts the handler chain over from the first handler, so this is
 * how a handler tells its own re-entry -- where the work of keeping the tiers
 * together is already done or still to do -- from a statement it has not seen.
 */
static Node *statementInProgress = NULL;


static bool ProcessTimeseriesUtility(ProcessUtilityParams * params, void *arg);
static void ExecuteUserStatement(ProcessUtilityParams * params);
static bool ProcessCreateTimeseriesTable(ProcessUtilityParams * params);
static void ErrorIfCreateTableAsTimeseries(ProcessUtilityParams * params);

static void ParseTimeseriesOptions(CreateStmt *createStmt,
								   TimeseriesOptions * options);
static Interval *ParseIntervalOption(DefElem *option);
static int32 ParseInt32Option(DefElem *option);
static Interval *DefaultInterval(const char *value);
static void EnsureValidPartitionInterval(Interval *partitionInterval);
static void EnsureCreateTimeseriesTableSupported(CreateStmt *createStmt);
static char *TimeColumnForCreateStmt(CreateStmt *createStmt,
									 TimeseriesOptions * options);
static void SetRangePartitionSpec(CreateStmt *createStmt, const char *timeColumn);
static void EnsureValidTimeColumn(Oid relationId, const char *timeColumn);
static void CreateColdTier(Oid relationId, const char *timeColumn,
						   TimeseriesOptions * options, Oid *coldTableId);
static List *ColumnDefListForRelation(Relation relation);
static List *ColdTierOptions(List *coldOptions, const char *timeColumn,
							 Interval *partitionInterval);
static void RecordColdTierDependency(Oid relationId, Oid coldTableId);
static void CreateInitialPartitions(Oid relationId);

static bool ProcessAlterTieredTable(ProcessUtilityParams * params);
static void ErrorIfSettingTimeseriesAccessMethod(AlterTableStmt *alterTableStmt);
static void ErrorIfSettingAccessMethodOfTieredTable(TieredTable * tieredTable,
													AlterTableStmt *alterTableStmt);
static bool AlterTieredTableSettings(TieredTable * tieredTable,
									 AlterTableStmt *alterTableStmt);
static bool IsTimeseriesSetting(const char *name);
static void ApplyTimeseriesSetting(TieredTable * tieredTable, DefElem *option,
								   bool reset);
static void EnsureValidRetention(const char *name, Interval *retention);
static AlterTableStmt *ColdTierAlterTableStmt(TieredTable * tieredTable,
											  AlterTableStmt *alterTableStmt);
static AlterTableCmd *ColdTierAlterTableCmd(TieredTable * tieredTable,
											AlterTableCmd *command);
static ColumnDef *ColdTierColumnDef(ColumnDef *columnDef);
static bool ProcessRenameTieredColumn(ProcessUtilityParams * params);
static void ErrorIfTruncateTieredTable(ProcessUtilityParams * params);
static void ErrorIfTimeColumn(TieredTable * tieredTable, const char *columnName,
							  const char *action);
static RangeVar *RelationRangeVar(Oid relationId);


/*
 * InitializeTimeseriesDDL registers the utility statement handler. Called from
 * _PG_init, so this runs whether or not the extension exists in the database;
 * every entry point below therefore starts by asking whether it does.
 */
void
InitializeTimeseriesDDL(void)
{
	RegisterUtilityStatementHandler(ProcessTimeseriesUtility, NULL);
}


/*
 * timeseries_am_handler is never called: a CREATE TABLE naming this access
 * method is rewritten before PostgreSQL resolves it, and nothing else can name
 * it. It exists so that "USING timeseries" resolves at all -- and so that the
 * error is about the access method, not about a missing one, if this module is
 * not in shared_preload_libraries.
 */
Datum
timeseries_am_handler(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s access method is a placeholder and should not be used",
					TIMESERIES_AM),
			 errhint("It is only valid in CREATE TABLE ... USING %s, and needs "
					 "pg_lake_timeseries in shared_preload_libraries.",
					 TIMESERIES_AM)));
}


/*
 * ProcessTimeseriesUtility is the entry point for every utility statement in the
 * database, so it has to be cheap for the ones that are not ours.
 */
static bool
ProcessTimeseriesUtility(ProcessUtilityParams * params, void *arg)
{
	Node	   *utilityStmt = params->plannedStmt->utilityStmt;

	if (PgLakeTimeseries == NULL || !IsExtensionCreated(PgLakeTimeseries))
	{
		/*
		 * Without the extension there is nowhere to register anything, and
		 * "USING timeseries" fails on its own: the access method does not
		 * exist either.
		 */
		return false;
	}

	if (utilityStmt == statementInProgress)
	{
		/* a statement of ours coming back around; PostgreSQL owns it now */
		return false;
	}

	switch (nodeTag(utilityStmt))
	{
		case T_CreateStmt:
			return ProcessCreateTimeseriesTable(params);

		case T_CreateTableAsStmt:
			ErrorIfCreateTableAsTimeseries(params);
			return false;

		case T_AlterTableStmt:
			return ProcessAlterTieredTable(params);

		case T_RenameStmt:
			return ProcessRenameTieredColumn(params);

		case T_TruncateStmt:
			ErrorIfTruncateTieredTable(params);
			return false;

		default:
			return false;
	}
}


/*
 * ExecuteUserStatement has PostgreSQL execute the statement the handler was
 * called for, unchanged, and returns once it has.
 *
 * The handler chain starts over from the beginning, so the statement is
 * remembered for as long as that takes and recognised on the way back through
 * ProcessTimeseriesUtility. Restoring the outer statement rather than clearing it
 * keeps that true of a statement that ran DDL of its own, and restoring it on the
 * way out of an error too keeps a statement that raised from being remembered by a
 * transaction that caught it.
 */
static void
ExecuteUserStatement(ProcessUtilityParams * params)
{
	Node	   *volatile outerStatement = statementInProgress;

	statementInProgress = params->plannedStmt->utilityStmt;

	PG_TRY();
	{
		PgLakeCommonProcessUtility(params);
	}
	PG_CATCH();
	{
		statementInProgress = outerStatement;
		PG_RE_THROW();
	}
	PG_END_TRY();

	statementInProgress = outerStatement;
}


/*
 * ErrorIfCreateTableAsTimeseries rejects CREATE TABLE ... USING timeseries AS
 * SELECT.
 *
 * A tiered table is empty when it is created and gets its history from the
 * writes that follow: the partition frontier only reaches back one hot_retention,
 * so a query result reaching further back has nowhere to land.
 */
static void
ErrorIfCreateTableAsTimeseries(ProcessUtilityParams * params)
{
	CreateTableAsStmt *createTableAsStmt =
		(CreateTableAsStmt *) params->plannedStmt->utilityStmt;
	IntoClause *into = createTableAsStmt->into;

	if (into == NULL || into->accessMethod == NULL ||
		strcmp(into->accessMethod, TIMESERIES_AM) != 0)
	{
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("USING %s is not supported in CREATE TABLE AS",
					TIMESERIES_AM),
			 errhint("Create the table first, then INSERT into it.")));
}


/*
 * ProcessCreateTimeseriesTable handles CREATE TABLE ... USING timeseries.
 *
 * The heap is created from the user's own statement, with the access method
 * replaced and the range partition specification filled in; the Iceberg tier is
 * then built from the heap that PostgreSQL actually created rather than from the
 * parse tree, so that LIKE clauses, serial columns, domains and defaults all
 * arrive as the columns they became.
 */
static bool
ProcessCreateTimeseriesTable(ProcessUtilityParams * params)
{
	CreateStmt *createStmt = (CreateStmt *) params->plannedStmt->utilityStmt;

	/*
	 * Only an explicit USING timeseries counts. Unlike the iceberg access
	 * method this one is never honoured as default_table_access_method: every
	 * CREATE TABLE in the database creating a second table and a registration
	 * is not a useful default, and it would catch extensions' own metadata
	 * tables.
	 */
	if (createStmt->accessMethod == NULL ||
		strcmp(createStmt->accessMethod, TIMESERIES_AM) != 0)
	{
		return false;
	}

	/* we rewrite the statement, so we need our own copy of it */
	if (params->readOnlyTree)
		createStmt = (CreateStmt *) CopyUtilityStmt(params);

	EnsureCreateTimeseriesTableSupported(createStmt);

	TimeseriesOptions options = {0};

	ParseTimeseriesOptions(createStmt, &options);

	char	   *timeColumn = TimeColumnForCreateStmt(createStmt, &options);

	SetRangePartitionSpec(createStmt, timeColumn);

	/*
	 * The heap is a plain PostgreSQL table. Name the access method rather
	 * than clearing it, so that a default_table_access_method of iceberg
	 * cannot turn the hot tier into a second lake table.
	 */
	createStmt->accessMethod = DEFAULT_TABLE_ACCESS_METHOD;

	/*
	 * A partitioned table accepts no storage parameters at all, so every
	 * option that is left belongs to the Iceberg tier.
	 */
	createStmt->options = NIL;

	params->plannedStmt->utilityStmt = (Node *) createStmt;

	/* create the heap, from the statement the user wrote */
	ExecuteUserStatement(params);

	Oid			relationId = RangeVarGetRelid(createStmt->relation, NoLock, false);

	EnsureValidTimeColumn(relationId, timeColumn);

	Oid			coldTableId = InvalidOid;

	CreateColdTier(relationId, timeColumn, &options, &coldTableId);
	RecordColdTierDependency(relationId, coldTableId);

	TieredTable tieredTable = {0};

	tieredTable.relationId = relationId;
	tieredTable.coldTableId = coldTableId;
	namestrcpy(&tieredTable.timeColumn, timeColumn);
	tieredTable.partitionInterval = *options.partitionInterval;

	/*
	 * A new tiered table's Iceberg tier is empty, so it is authoritative for
	 * nothing and the boundary starts where it can never exclude a row.
	 */
	tieredTable.boundary = DT_NOBEGIN;
	tieredTable.hotRetention = *options.hotRetention;
	tieredTable.coldRetentionIsNull = (options.coldRetention == NULL);
	if (options.coldRetention != NULL)
		tieredTable.coldRetention = *options.coldRetention;
	tieredTable.precreateAhead = options.precreateAhead;

	RegisterTieredTable(&tieredTable);

	/*
	 * Make the table writable immediately. Without this the first INSERT
	 * would have to wait for the maintenance worker to extend the frontier.
	 */
	CreateInitialPartitions(relationId);

	return true;
}


/*
 * EnsureCreateTimeseriesTableSupported rejects the CREATE TABLE variants that a
 * tiered table cannot be.
 */
static void
EnsureCreateTimeseriesTableSupported(CreateStmt *createStmt)
{
	if (createStmt->relation->relpersistence != RELPERSISTENCE_PERMANENT)
	{
		/*
		 * The cold tier is a foreign table over object storage: it outlives
		 * the session, and it is not crash-safe to skip WAL for the hot tier
		 * of something whose contents get copied into Iceberg.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a %s table cannot be temporary or unlogged",
						TIMESERIES_AM)));
	}

	if (createStmt->partbound != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a partition cannot use the %s access method",
						TIMESERIES_AM),
				 errhint("The partitions of a tiered table are managed for you.")));
	}

	if (createStmt->inhRelations != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a %s table cannot inherit from another table",
						TIMESERIES_AM)));
	}
}


/*
 * ParseTimeseriesOptions splits the WITH options into the ones this extension
 * consumes and the ones the Iceberg tier gets, applying the defaults for the
 * former.
 *
 * Unknown options are not an error here: location, partition_by and anything
 * else pg_lake understands belongs to the cold tier, and pg_lake rejects what it
 * does not know.
 */
static void
ParseTimeseriesOptions(CreateStmt *createStmt, TimeseriesOptions * options)
{
	ListCell   *optionCell = NULL;

	options->timeColumn = NULL;
	options->partitionInterval = NULL;
	options->hotRetention = NULL;
	options->coldRetention = NULL;
	options->precreateAhead = TIMESERIES_DEFAULT_PRECREATE_AHEAD;
	options->coldOptions = NIL;

	foreach(optionCell, createStmt->options)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, TIMESERIES_OPTION_TIME_COLUMN) == 0)
			options->timeColumn = defGetString(option);
		else if (strcmp(option->defname, TIMESERIES_OPTION_PARTITION_INTERVAL) == 0)
			options->partitionInterval = ParseIntervalOption(option);
		else if (strcmp(option->defname, TIMESERIES_OPTION_HOT_RETENTION) == 0)
			options->hotRetention = ParseIntervalOption(option);
		else if (strcmp(option->defname, TIMESERIES_OPTION_COLD_RETENTION) == 0)
			options->coldRetention = ParseIntervalOption(option);
		else if (strcmp(option->defname, TIMESERIES_OPTION_PRECREATE_AHEAD) == 0)
			options->precreateAhead = ParseInt32Option(option);
		else
			options->coldOptions = lappend(options->coldOptions, option);
	}

	if (options->partitionInterval == NULL)
	{
		options->partitionInterval =
			DefaultInterval(TIMESERIES_DEFAULT_PARTITION_INTERVAL);
	}

	if (options->hotRetention == NULL)
		options->hotRetention = DefaultInterval(TIMESERIES_DEFAULT_HOT_RETENTION);

	EnsureValidPartitionInterval(options->partitionInterval);

	if (options->precreateAhead < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must not be negative",
						TIMESERIES_OPTION_PRECREATE_AHEAD)));
	}
}


/*
 * ParseIntervalOption reads an option value as an interval.
 */
static Interval *
ParseIntervalOption(DefElem *option)
{
	char	   *value = defGetString(option);

	return DatumGetIntervalP(DirectFunctionCall3(interval_in,
												 CStringGetDatum(value),
												 ObjectIdGetDatum(InvalidOid),
												 Int32GetDatum(-1)));
}


/*
 * ParseInt32Option reads an option value as an integer, written either as one or
 * quoted. Both spellings reach a table option, and the interval options next to
 * this one have to be quoted, so requiring the bare form here would be a trap.
 */
static int32
ParseInt32Option(DefElem *option)
{
	if (option->arg != NULL && IsA(option->arg, String))
		return pg_strtoint32(strVal(option->arg));

	return defGetInt32(option);
}


/*
 * DefaultInterval reads one of the interval defaults of this file, which are
 * written the way the documentation writes them.
 */
static Interval *
DefaultInterval(const char *value)
{
	return DatumGetIntervalP(DirectFunctionCall3(interval_in,
												 CStringGetDatum(value),
												 ObjectIdGetDatum(InvalidOid),
												 Int32GetDatum(-1)));
}


/*
 * EnsureValidPartitionInterval rejects the intervals that cannot be a partition
 * granularity.
 *
 * Fixed-length only: partition bounds are computed by flooring an epoch to a
 * multiple of the interval (timeseries.partition_start), which a month or a year
 * has no single length for.
 */
static void
EnsureValidPartitionInterval(Interval *partitionInterval)
{
	if (partitionInterval->month != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be a fixed-length interval",
						TIMESERIES_OPTION_PARTITION_INTERVAL),
				 errhint("Use hour, day or week granularities; month and year "
						 "are not supported yet.")));
	}

	if (partitionInterval->day < 0 || partitionInterval->time < 0 ||
		(partitionInterval->day == 0 && partitionInterval->time == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be positive",
						TIMESERIES_OPTION_PARTITION_INTERVAL)));
	}
}


/*
 * TimeColumnForCreateStmt decides which column splits the tiers.
 *
 * The time column is the partition key, so a user-written PARTITION BY names it;
 * otherwise it is the table's one timestamptz column, and time_column settles it
 * when there is more than one.
 */
static char *
TimeColumnForCreateStmt(CreateStmt *createStmt, TimeseriesOptions * options)
{
	if (options->timeColumn != NULL)
		return options->timeColumn;

	if (createStmt->partspec != NULL &&
		list_length(createStmt->partspec->partParams) == 1)
	{
		PartitionElem *partitionElem =
			(PartitionElem *) linitial(createStmt->partspec->partParams);

		if (partitionElem->name != NULL)
			return partitionElem->name;
	}

	char	   *timeColumn = NULL;
	ListCell   *tableElementCell = NULL;

	foreach(tableElementCell, createStmt->tableElts)
	{
		if (!IsA(lfirst(tableElementCell), ColumnDef))
			continue;

		ColumnDef  *columnDef = (ColumnDef *) lfirst(tableElementCell);

		/*
		 * missing_ok, because a type this does not resolve is PostgreSQL's
		 * error to report on the CREATE TABLE itself, in the right words and
		 * with a position.
		 */
		if (LookupTypeNameOid(NULL, columnDef->typeName, true) != TIMESTAMPTZOID)
			continue;

		if (timeColumn != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("a %s table has more than one timestamp with time "
							"zone column", TIMESERIES_AM),
					 errhint("Add WITH (%s = '<column>').",
							 TIMESERIES_OPTION_TIME_COLUMN)));
		}

		timeColumn = columnDef->colname;
	}

	if (timeColumn == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("a %s table needs a timestamp with time zone column",
						TIMESERIES_AM),
				 errhint("Add WITH (%s = '<column>') if it is of a domain or "
						 "another timestamp type.",
						 TIMESERIES_OPTION_TIME_COLUMN)));
	}

	return timeColumn;
}


/*
 * SetRangePartitionSpec makes the statement partition by range on the time
 * column, or checks that it already does.
 *
 * Range partitioning on the time column is what lets a range of time be handed
 * to the other tier by dropping whole partitions, and what makes the boundary
 * predicate prune to one tier (DESIGN.md section 7.4).
 */
static void
SetRangePartitionSpec(CreateStmt *createStmt, const char *timeColumn)
{
	if (createStmt->partspec == NULL)
	{
		PartitionElem *partitionElem = makeNode(PartitionElem);

		partitionElem->name = pstrdup(timeColumn);
		partitionElem->location = -1;

		PartitionSpec *partitionSpec = makeNode(PartitionSpec);

		partitionSpec->strategy = PARTITION_STRATEGY_RANGE;
		partitionSpec->partParams = list_make1(partitionElem);
		partitionSpec->location = -1;

		createStmt->partspec = partitionSpec;

		return;
	}

	PartitionSpec *partitionSpec = createStmt->partspec;

	if (partitionSpec->strategy != PARTITION_STRATEGY_RANGE ||
		list_length(partitionSpec->partParams) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a %s table must be partitioned by range on a single "
						"column", TIMESERIES_AM)));
	}

	PartitionElem *partitionElem =
		(PartitionElem *) linitial(partitionSpec->partParams);

	if (partitionElem->name == NULL ||
		strcmp(partitionElem->name, timeColumn) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a %s table must be partitioned by range on %s",
						TIMESERIES_AM, quote_identifier(timeColumn))));
	}
}


/*
 * EnsureValidTimeColumn checks the time column of the heap PostgreSQL created.
 *
 * timestamptz only: the boundary, the partition bounds and the predicates the
 * planner hook adds are all timestamptz, and reading a timestamp bound as one
 * would shift it by the session's time zone. This compares the type OID rather
 * than the type name, so a domain over timestamptz is rejected too -- its typmod
 * and its constraints are not the planner's to reason about.
 *
 * NOT NULL, because a NULL time belongs to no partition, so to no tier.
 */
static void
EnsureValidTimeColumn(Oid relationId, const char *timeColumn)
{
	Relation	relation = table_open(relationId, AccessShareLock);
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	AttrNumber	attributeNumber = get_attnum(relationId, timeColumn);

	/* PostgreSQL rejected the partition specification if it were missing */
	Assert(attributeNumber != InvalidAttrNumber);

	Form_pg_attribute attributeForm =
		TupleDescAttr(tupleDescriptor, attributeNumber - 1);
	Oid			attributeTypeId = attributeForm->atttypid;
	bool		attributeNotNull = attributeForm->attnotnull;

	table_close(relation, AccessShareLock);

	if (attributeTypeId != TIMESTAMPTZOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("time column %s must be timestamp with time zone, not %s",
						quote_identifier(timeColumn),
						format_type_be(attributeTypeId))));
	}

	if (!attributeNotNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("time column %s must be NOT NULL",
						quote_identifier(timeColumn)),
				 errhint("A row with no time belongs to neither tier.")));
	}
}


/*
 * CreateColdTier creates the Iceberg tier of a tiered table next to the heap and
 * returns its OID.
 *
 * Its columns come from the heap's tuple descriptor, which is what makes the two
 * tiers interchangeable by position for the planner hook. Only the shape is
 * copied: defaults, identity, generation expressions and constraints are hot-tier
 * concerns, and Iceberg reads a field its data files do not have as NULL, so a
 * NOT NULL there would be a promise the format cannot keep.
 */
static void
CreateColdTier(Oid relationId, const char *timeColumn,
			   TimeseriesOptions * options, Oid *coldTableId)
{
	Relation	relation = table_open(relationId, AccessShareLock);
	char	   *coldName = makeObjectName(RelationGetRelationName(relation),
										  NULL, TIMESERIES_COLD_SUFFIX);
	RangeVar   *coldRangeVar =
		makeRangeVar(get_namespace_name(RelationGetNamespace(relation)),
					 coldName, -1);
	List	   *tableElements = ColumnDefListForRelation(relation);

	table_close(relation, AccessShareLock);

	CreateStmt *coldCreateStmt = makeNode(CreateStmt);

	coldCreateStmt->relation = coldRangeVar;
	coldCreateStmt->tableElts = tableElements;
	coldCreateStmt->accessMethod = PG_LAKE_ICEBERG_AM_ALIAS;
	coldCreateStmt->options = ColdTierOptions(options->coldOptions, timeColumn,
											  options->partitionInterval);
	coldCreateStmt->oncommit = ONCOMMIT_NOOP;

	ExecuteInternalDDL((Node *) coldCreateStmt,
					   "CREATE TABLE ... USING iceberg /* pg_lake_timeseries */");

	*coldTableId = RangeVarGetRelid(coldRangeVar, NoLock, false);
}


/*
 * ColumnDefListForRelation describes a relation's live columns as they would be
 * written in a CREATE TABLE.
 */
static List *
ColumnDefListForRelation(Relation relation)
{
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	List	   *columnDefList = NIL;

	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm =
			TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attisdropped)
			continue;

		ColumnDef  *columnDef = makeColumnDef(NameStr(attributeForm->attname),
											  attributeForm->atttypid,
											  attributeForm->atttypmod,
											  attributeForm->attcollation);

		columnDefList = lappend(columnDefList, columnDef);
	}

	return columnDefList;
}


/*
 * ColdTierOptions returns the options of the Iceberg tier, defaulting
 * partition_by from the partition granularity of the heap.
 *
 * The cold tier is written a range of time at a time and expired a range at a
 * time, so its data files should not straddle those ranges: a delete that lines
 * up with the Iceberg partitioning is metadata-only, and one that does not
 * rewrites files. Day granularity unless the heap partitions are finer than a
 * day, in which case hour.
 */
static List *
ColdTierOptions(List *coldOptions, const char *timeColumn,
				Interval *partitionInterval)
{
	ListCell   *optionCell = NULL;

	foreach(optionCell, coldOptions)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		/* the user knows better */
		if (strcmp(option->defname, TIMESERIES_OPTION_PARTITION_BY) == 0)
			return coldOptions;
	}

	int64		intervalSeconds = partitionInterval->day * SECS_PER_DAY +
		partitionInterval->time / USECS_PER_SEC;
	const char *transform = intervalSeconds < SECS_PER_DAY ? "hour" : "day";
	char	   *partitionBy = psprintf("%s(%s)", transform, timeColumn);

	return lappend(coldOptions,
				   makeDefElem(pstrdup(TIMESERIES_OPTION_PARTITION_BY),
							   (Node *) makeString(partitionBy), -1));
}


/*
 * RecordColdTierDependency ties the Iceberg tier to the heap.
 *
 * An internal dependency is exactly the relationship: DROP TABLE on the heap
 * drops the cold tier with it -- through pg_lake's object access hook, so its
 * data files and Iceberg metadata go too -- and DROP TABLE on the cold tier
 * alone is refused, with PostgreSQL pointing at the table to drop instead.
 *
 * The registration row is not covered by this. A regclass column carries no
 * dependency, so it is swept away afterwards by timeseries.forget_dropped()
 * (DESIGN.md section 11.1).
 */
static void
RecordColdTierDependency(Oid relationId, Oid coldTableId)
{
	ObjectAddress coldTableAddress = {RelationRelationId, coldTableId, 0};
	ObjectAddress relationAddress = {RelationRelationId, relationId, 0};

	recordDependencyOn(&coldTableAddress, &relationAddress, DEPENDENCY_INTERNAL);
}


/*
 * CreateInitialPartitions gives a new tiered table the partitions covering its
 * hot window, so that it is writable the moment CREATE TABLE returns.
 */
static void
CreateInitialPartitions(Oid relationId)
{
	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	SPI_EXECUTE("SELECT timeseries.add_partitions($1)", false);

	SPI_END();
}


/*
 * ProcessAlterTieredTable handles ALTER TABLE on a tiered table.
 *
 * There are two kinds of ALTER TABLE here. One changes a setting of the tiered
 * table itself -- SET (hot_retention = '3 days') and friends -- and is absorbed:
 * PostgreSQL never sees it, because it is not a relation option. Everything else
 * is an ordinary alteration of the user's heap, and the only thing to do about it
 * is to keep the Iceberg tier the same shape.
 *
 * What the tier has to be told is worked out from the statement before it runs,
 * so that a DROP COLUMN is still looking at a column that exists, and applied
 * after the user's statement has gone through the rest of the handler chain. The
 * two are one transaction, so the tiers cannot end up disagreeing: pg_lake
 * validates the Iceberg side against Iceberg's rules, and a type change it
 * refuses takes the heap's rewrite down with it.
 */
static bool
ProcessAlterTieredTable(ProcessUtilityParams * params)
{
	AlterTableStmt *alterTableStmt =
		(AlterTableStmt *) params->plannedStmt->utilityStmt;

	if (alterTableStmt->objtype != OBJECT_TABLE)
		return false;

	ErrorIfSettingTimeseriesAccessMethod(alterTableStmt);

	Oid			relationId = AlterTableLookupRelation(alterTableStmt, NoLock);
	TieredTable tieredTable = {0};

	if (!OidIsValid(relationId) || !GetTieredTable(relationId, &tieredTable))
		return false;

	ErrorIfSettingAccessMethodOfTieredTable(&tieredTable, alterTableStmt);

	if (AlterTieredTableSettings(&tieredTable, alterTableStmt))
		return true;

	AlterTableStmt *coldAlterTableStmt =
		ColdTierAlterTableStmt(&tieredTable, alterTableStmt);

	if (coldAlterTableStmt == NULL)
	{
		/* nothing the tier shares is changing, so this is an ordinary ALTER */
		return false;
	}

	/* alter the heap, by running the statement the user wrote */
	ExecuteUserStatement(params);

	ExecuteInternalDDL((Node *) coldAlterTableStmt,
					   "ALTER TABLE ... /* pg_lake_timeseries */");

	return true;
}


/*
 * ErrorIfSettingTimeseriesAccessMethod rejects ALTER TABLE ... SET ACCESS METHOD
 * timeseries.
 *
 * Setting the access method only writes pg_class.relam, and a tiered table is not
 * a relation with a particular relam: it is two tables and a registration. The
 * table would end up naming an access method whose handler exists only to raise.
 */
static void
ErrorIfSettingTimeseriesAccessMethod(AlterTableStmt *alterTableStmt)
{
	ListCell   *commandCell = NULL;

	foreach(commandCell, alterTableStmt->cmds)
	{
		AlterTableCmd *command = lfirst_node(AlterTableCmd, commandCell);

		if (command->subtype != AT_SetAccessMethod || command->name == NULL)
			continue;

		if (strcmp(command->name, TIMESERIES_AM) == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot set the %s access method on an existing table",
							TIMESERIES_AM),
					 errhint("Create the table with CREATE TABLE ... USING %s.",
							 TIMESERIES_AM)));
		}
	}
}


/*
 * ErrorIfSettingAccessMethodOfTieredTable rejects ALTER TABLE ... SET ACCESS
 * METHOD on a tiered table.
 *
 * The hot tier is a partitioned heap by construction: the maintenance functions
 * create and drop its partitions, and sealing hands a range over by dropping one.
 * Naming another access method would change what the partitions are made of, or
 * -- with iceberg -- leave a table that has an Iceberg tier of its own next to the
 * one the registration points at. Neither is a table this extension can go on
 * maintaining, so it is refused rather than half-supported.
 */
static void
ErrorIfSettingAccessMethodOfTieredTable(TieredTable * tieredTable,
										AlterTableStmt *alterTableStmt)
{
	ListCell   *commandCell = NULL;

	foreach(commandCell, alterTableStmt->cmds)
	{
		AlterTableCmd *command = lfirst_node(AlterTableCmd, commandCell);

		if (command->subtype != AT_SetAccessMethod)
			continue;

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change the access method of tiered table %s",
						get_rel_name(tieredTable->relationId)),
				 errdetail("A tiered table is a partitioned heap with an Iceberg "
						   "tier next to it, not a table with a single access "
						   "method.")));
	}
}


/*
 * AlterTieredTableSettings applies ALTER TABLE ... SET/RESET (<setting> = ...)
 * and returns whether it consumed the whole statement.
 *
 * The settings live in timeseries.tables rather than in pg_class.reloptions, so
 * this is where they are written; it is also the only way to change them, now that
 * they are given on CREATE TABLE. Everything the maintenance worker reads can be
 * changed here except the two tiers, which are what the table is made of, and the
 * boundary, which only seal() may move.
 *
 * A statement that mixes settings with anything else is refused rather than split.
 * Half of it would be executed here and half by PostgreSQL, and the two halves
 * cannot fail together.
 *
 * A partitioned table has no storage parameters of its own, so SET (...) on a
 * tiered table is entirely this extension's namespace and a name it does not know
 * is refused here. PostgreSQL's own answer would be to suggest setting the
 * parameter on the leaf partitions, which is not what the user was reaching for.
 */
static bool
AlterTieredTableSettings(TieredTable * tieredTable,
						 AlterTableStmt *alterTableStmt)
{
	int			settingCount = 0;
	int			otherCount = 0;
	ListCell   *commandCell = NULL;

	foreach(commandCell, alterTableStmt->cmds)
	{
		AlterTableCmd *command = lfirst_node(AlterTableCmd, commandCell);

		if (command->subtype != AT_SetRelOptions &&
			command->subtype != AT_ResetRelOptions)
		{
			otherCount++;
			continue;
		}

		ListCell   *optionCell = NULL;

		foreach(optionCell, (List *) command->def)
		{
			DefElem    *option = lfirst_node(DefElem, optionCell);

			if (!IsTimeseriesSetting(option->defname))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized parameter \"%s\"", option->defname),
						 errhint("A tiered table has the settings %s, %s, %s and %s.",
								 TIMESERIES_OPTION_PARTITION_INTERVAL,
								 TIMESERIES_OPTION_HOT_RETENTION,
								 TIMESERIES_OPTION_COLD_RETENTION,
								 TIMESERIES_OPTION_PRECREATE_AHEAD)));
			}

			settingCount++;
		}
	}

	if (settingCount == 0)
		return false;

	if (otherCount > 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change the settings of a tiered table together "
						"with other changes"),
				 errhint("Use a separate ALTER TABLE ... SET (...) statement.")));
	}

	/*
	 * Keep the relation from being dropped underneath the settings, which is
	 * the lock ALTER TABLE would otherwise have been holding by now.
	 */
	LockRelationOid(tieredTable->relationId, ShareUpdateExclusiveLock);

	foreach(commandCell, alterTableStmt->cmds)
	{
		AlterTableCmd *command = lfirst_node(AlterTableCmd, commandCell);
		bool		reset = (command->subtype == AT_ResetRelOptions);
		ListCell   *optionCell = NULL;

		foreach(optionCell, (List *) command->def)
		{
			ApplyTimeseriesSetting(tieredTable,
								   lfirst_node(DefElem, optionCell), reset);
		}
	}

	UpdateTieredTableSettings(tieredTable);

	return true;
}


/*
 * IsTimeseriesSetting returns whether an option name is one of ours.
 *
 * time_column is in the list although it cannot be changed, so that saying so is
 * this extension's error rather than PostgreSQL's about an unrecognized parameter.
 */
static bool
IsTimeseriesSetting(const char *name)
{
	return strcmp(name, TIMESERIES_OPTION_TIME_COLUMN) == 0 ||
		strcmp(name, TIMESERIES_OPTION_PARTITION_INTERVAL) == 0 ||
		strcmp(name, TIMESERIES_OPTION_HOT_RETENTION) == 0 ||
		strcmp(name, TIMESERIES_OPTION_COLD_RETENTION) == 0 ||
		strcmp(name, TIMESERIES_OPTION_PRECREATE_AHEAD) == 0;
}


/*
 * ApplyTimeseriesSetting changes one setting of a tiered table in place, either to
 * a given value (SET) or back to the default it would have had on CREATE TABLE
 * (RESET).
 */
static void
ApplyTimeseriesSetting(TieredTable * tieredTable, DefElem *option, bool reset)
{
	const char *name = option->defname;

	if (strcmp(name, TIMESERIES_OPTION_TIME_COLUMN) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s of a tiered table cannot be changed", name),
				 errhint("It is the partition key of the table and the column "
						 "both tiers are divided on.")));
	}
	else if (strcmp(name, TIMESERIES_OPTION_PARTITION_INTERVAL) == 0)
	{
		Interval   *partitionInterval = reset ?
			DefaultInterval(TIMESERIES_DEFAULT_PARTITION_INTERVAL) :
			ParseIntervalOption(option);

		EnsureValidPartitionInterval(partitionInterval);

		tieredTable->partitionInterval = *partitionInterval;
	}
	else if (strcmp(name, TIMESERIES_OPTION_HOT_RETENTION) == 0)
	{
		Interval   *hotRetention = reset ?
			DefaultInterval(TIMESERIES_DEFAULT_HOT_RETENTION) :
			ParseIntervalOption(option);

		EnsureValidRetention(name, hotRetention);

		tieredTable->hotRetention = *hotRetention;
	}
	else if (strcmp(name, TIMESERIES_OPTION_COLD_RETENTION) == 0)
	{
		/* the default is to keep Iceberg history forever */
		tieredTable->coldRetentionIsNull = reset;

		if (!reset)
		{
			Interval   *coldRetention = ParseIntervalOption(option);

			EnsureValidRetention(name, coldRetention);

			tieredTable->coldRetention = *coldRetention;
		}
	}
	else if (strcmp(name, TIMESERIES_OPTION_PRECREATE_AHEAD) == 0)
	{
		int32		precreateAhead = reset ?
			TIMESERIES_DEFAULT_PRECREATE_AHEAD : ParseInt32Option(option);

		if (precreateAhead < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must not be negative", name)));
		}

		tieredTable->precreateAhead = precreateAhead;
	}
	else
	{
		/* IsTimeseriesSetting() and this have to agree */
		elog(ERROR, "unexpected timeseries setting \"%s\"", name);
	}
}


/*
 * EnsureValidRetention rejects a retention that would put the boundary in the
 * future. Unlike the partition interval, a retention may be month- or
 * year-length: it is only ever subtracted from now(), which timestamptz knows
 * how to do.
 */
static void
EnsureValidRetention(const char *name, Interval *retention)
{
	if (retention->month < 0 || retention->day < 0 || retention->time < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must not be negative", name)));
	}
}


/*
 * ColdTierAlterTableStmt returns the ALTER TABLE to run on the Iceberg tier for
 * the user's ALTER TABLE, or NULL if the tier is not concerned by any of it.
 *
 * Shape is all the two tiers have to agree about: the planner substitutes one for
 * the other by position (DESIGN.md section 7.2), so a column added, dropped,
 * retyped or renamed on one side has to happen on the other. Constraints,
 * defaults, identity, storage and statistics are hot-tier matters and stay where
 * the user put them -- an Iceberg data file that does not have a field reads as
 * NULL, so NOT NULL there could not be honoured anyway.
 */
static AlterTableStmt *
ColdTierAlterTableStmt(TieredTable * tieredTable,
					   AlterTableStmt *alterTableStmt)
{
	List	   *coldCommands = NIL;
	ListCell   *commandCell = NULL;

	foreach(commandCell, alterTableStmt->cmds)
	{
		AlterTableCmd *command = lfirst_node(AlterTableCmd, commandCell);
		AlterTableCmd *coldCommand = ColdTierAlterTableCmd(tieredTable, command);

		if (coldCommand != NULL)
			coldCommands = lappend(coldCommands, coldCommand);
	}

	if (coldCommands == NIL)
		return NULL;

	AlterTableStmt *coldAlterTableStmt = makeNode(AlterTableStmt);

	coldAlterTableStmt->relation = RelationRangeVar(tieredTable->coldTableId);
	coldAlterTableStmt->objtype = OBJECT_TABLE;
	coldAlterTableStmt->cmds = coldCommands;

	return coldAlterTableStmt;
}


/*
 * ColdTierAlterTableCmd returns the subcommand to run on the Iceberg tier for one
 * subcommand of the user's ALTER TABLE, or NULL if the tier is not concerned.
 *
 * A subcommand naming a column that the table does not have is left alone, so that
 * the error the user gets is PostgreSQL's about their own table rather than ours
 * about a tier they did not name.
 */
static AlterTableCmd *
ColdTierAlterTableCmd(TieredTable * tieredTable, AlterTableCmd *command)
{
	AlterTableCmd *coldCommand = makeNode(AlterTableCmd);

	coldCommand->subtype = command->subtype;
	coldCommand->missing_ok = command->missing_ok;

	switch (command->subtype)
	{
		case AT_AddColumn:
			{
				ColumnDef  *columnDef = castNode(ColumnDef, command->def);

				coldCommand->def = (Node *) ColdTierColumnDef(columnDef);

				return coldCommand;
			}

		case AT_DropColumn:
			{
				if (get_attnum(tieredTable->relationId, command->name) ==
					InvalidAttrNumber)
				{
					return NULL;
				}

				ErrorIfTimeColumn(tieredTable, command->name, "drop");

				coldCommand->name = command->name;
				coldCommand->behavior = command->behavior;

				return coldCommand;
			}

		case AT_AlterColumnType:
			{
				if (get_attnum(tieredTable->relationId, command->name) ==
					InvalidAttrNumber)
				{
					return NULL;
				}

				ErrorIfTimeColumn(tieredTable, command->name,
								  "change the type of");

				ColumnDef  *columnDef = castNode(ColumnDef, command->def);

				if (columnDef->raw_default != NULL)
				{
					/*
					 * The USING expression of an ALTER COLUMN TYPE rewrites
					 * the values of a column, and Iceberg promotes the type
					 * of a field without touching its data files. Applying it
					 * to the heap alone would leave the same column computed
					 * one way above the boundary and another way below it.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot change the type of column \"%s\" of %s "
									"with a USING expression",
									command->name,
									get_rel_name(tieredTable->relationId)),
							 errdetail("The expression would only reach the rows "
									   "PostgreSQL still owns; the Iceberg tier "
									   "promotes the type of a field rather than "
									   "rewriting its values.")));
				}

				coldCommand->name = command->name;
				coldCommand->def = (Node *) ColdTierColumnDef(columnDef);

				return coldCommand;
			}

		case AT_ChangeOwner:
			{
				/*
				 * Both tiers stay one table's worth of responsibility:
				 * writing the metadata of a tiered table requires owning
				 * both.
				 */
				coldCommand->newowner = command->newowner;

				return coldCommand;
			}

		default:

			/*
			 * Everything else is about how the heap stores or constrains its
			 * rows, which the Iceberg tier neither shares nor can express.
			 */
			return NULL;
	}
}


/*
 * ColdTierColumnDef strips a ColumnDef down to the column's shape: its name, its
 * type and its collation. In particular a DEFAULT is dropped, because Iceberg has
 * no stored default: a field its data files do not have reads as NULL, and the
 * rows the heap fills in are the ones the default was written for.
 */
static ColumnDef *
ColdTierColumnDef(ColumnDef *columnDef)
{
	ColumnDef  *coldColumnDef = makeNode(ColumnDef);

	coldColumnDef->colname = columnDef->colname;
	coldColumnDef->typeName = columnDef->typeName;
	coldColumnDef->collClause = columnDef->collClause;
	coldColumnDef->collOid = InvalidOid;
	coldColumnDef->is_local = true;
	coldColumnDef->location = -1;

	return coldColumnDef;
}


/*
 * ProcessRenameTieredColumn renames a column of the Iceberg tier along with the
 * heap's. It returns whether it took the statement over, which it does whenever
 * the tier has to follow.
 *
 * The time column is not one of them: it is the field the Iceberg tier is
 * partitioned on, and pg_lake refuses to rename a field that is used in a
 * partition spec, so the tiers could only come out of it disagreeing.
 *
 * Renaming the table itself is deliberately not propagated: a registration names
 * relations by OID, so nothing depends on the cold tier still being called
 * <table>_cold, and renaming it could collide with a table that already is.
 */
static bool
ProcessRenameTieredColumn(ProcessUtilityParams * params)
{
	RenameStmt *renameStmt = (RenameStmt *) params->plannedStmt->utilityStmt;

	if (renameStmt->renameType != OBJECT_COLUMN ||
		renameStmt->relationType != OBJECT_TABLE ||
		renameStmt->relation == NULL)
	{
		return false;
	}

	Oid			relationId = RangeVarGetRelid(renameStmt->relation, NoLock, true);
	TieredTable tieredTable = {0};

	if (!OidIsValid(relationId) || !GetTieredTable(relationId, &tieredTable))
		return false;

	/* let PostgreSQL be the one to complain about a column that is not there */
	if (get_attnum(relationId, renameStmt->subname) == InvalidAttrNumber)
		return false;

	ErrorIfTimeColumn(&tieredTable, renameStmt->subname, "rename");

	RenameStmt *coldRenameStmt = makeNode(RenameStmt);

	coldRenameStmt->renameType = OBJECT_COLUMN;
	coldRenameStmt->relationType = OBJECT_TABLE;
	coldRenameStmt->relation = RelationRangeVar(tieredTable.coldTableId);
	coldRenameStmt->subname = renameStmt->subname;
	coldRenameStmt->newname = renameStmt->newname;
	coldRenameStmt->behavior = renameStmt->behavior;

	/* rename in the heap, by running the statement the user wrote */
	ExecuteUserStatement(params);

	ExecuteInternalDDL((Node *) coldRenameStmt,
					   "ALTER TABLE ... RENAME COLUMN /* pg_lake_timeseries */");

	return true;
}


/*
 * ErrorIfTruncateTieredTable refuses TRUNCATE on a tiered table.
 *
 * TRUNCATE would empty the heap and leave the Iceberg tier holding everything
 * below the boundary, so the table would still return the history the user was
 * trying to remove. Emptying both tiers is not something TRUNCATE can do either:
 * the boundary would have to move back to -infinity, and the ranges Iceberg has
 * been handed would have to be forgotten.
 *
 * Truncating one partition is still allowed. That is a range of the hot tier,
 * which is exactly what a DELETE over the same range would have done.
 */
static void
ErrorIfTruncateTieredTable(ProcessUtilityParams * params)
{
	TruncateStmt *truncateStmt = (TruncateStmt *) params->plannedStmt->utilityStmt;
	ListCell   *relationCell = NULL;

	foreach(relationCell, truncateStmt->relations)
	{
		RangeVar   *rangeVar = lfirst_node(RangeVar, relationCell);
		Oid			relationId = RangeVarGetRelid(rangeVar, NoLock, true);

		if (!OidIsValid(relationId) || !IsTieredTable(relationId))
			continue;

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot truncate tiered table %s",
						get_rel_name(relationId)),
				 errdetail("Its history is authoritative in Iceberg, which "
						   "TRUNCATE does not reach."),
				 errhint("Use DROP TABLE, or DELETE over the range to remove.")));
	}
}


/*
 * ErrorIfTimeColumn refuses an alteration of the time column.
 *
 * The time column is the partition key of the heap, the column both tiers are
 * divided on, and the type of the boundary. Dropping it or changing its type
 * would leave the registration describing a table that no longer exists in that
 * shape.
 */
static void
ErrorIfTimeColumn(TieredTable * tieredTable, const char *columnName,
				  const char *action)
{
	if (namestrcmp(&tieredTable->timeColumn, columnName) != 0)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot %s column \"%s\" of %s", action, columnName,
					get_rel_name(tieredTable->relationId)),
			 errdetail("It is the time column of a tiered table.")));
}


/*
 * RelationRangeVar names a relation the way a statement would.
 */
static RangeVar *
RelationRangeVar(Oid relationId)
{
	char	   *schemaName = get_namespace_name(get_rel_namespace(relationId));
	char	   *relationName = get_rel_name(relationId);

	return makeRangeVar(schemaName, relationName, -1);
}
