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
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/data_file_stats_catalog.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/util/array_utils.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_extension_base/spi_helpers.h"

#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * Transient hash mapping data file path -> TableDataFile *. Built by
 * LoadColumnStats from the input list so that each SPI row can find its
 * target TableDataFile in O(1) regardless of list size. Caller-owned
 * dataFiles are never copied; we just store pointers into them.
 */
typedef struct PathToTableDataFileEntry
{
	char		path[MAX_S3_PATH_LENGTH];
	TableDataFile *dataFile;
}			PathToTableDataFileEntry;


static HTAB *BuildPathToTableDataFileHash(List *dataFiles);
static bool ColumnStatAlreadyAdded(List *columnStats, int64 fieldId);
static DataFileColumnStats * CreateDataFileColumnStats(int fieldId, PGType pgType,
													   char *lowerBoundText,
													   char *upperBoundText);


/*
 * AddDataFileColumnStatsToCatalog inserts column stats for a data file into
 * lake_table.data_file_column_stats.
 */
void
AddDataFileColumnStatsToCatalog(Oid relationId, const char *path, List *columnStatsList)
{
	ListCell   *columnStatsCell = NULL;

	foreach(columnStatsCell, columnStatsList)
	{
		DataFileColumnStats *columnStats = lfirst(columnStatsCell);

		/*
		 * do not insert stats if bounds are empty. (we do not write them to
		 * iceberg metadata as well)
		 */
		if (columnStats->lowerBoundText == NULL)
		{
			Assert(columnStats->upperBoundText == NULL);
			continue;
		}

		/* switch to schema owner, we assume callers checked permissions */
		Oid			savedUserId = InvalidOid;
		int			savedSecurityContext = 0;

		GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
		SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

		char	   *query =
			"insert into " DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " "
			"(table_name, path, field_id, lower_bound, upper_bound) "
			"values ($1,$2,$3,$4,$5)";

		DECLARE_SPI_ARGS(5);
		SPI_ARG_VALUE(1, OIDOID, relationId, false);
		SPI_ARG_VALUE(2, TEXTOID, path, false);
		SPI_ARG_VALUE(3, INT8OID, columnStats->leafField.fieldId, false);
		SPI_ARG_VALUE(4, TEXTOID, columnStats->lowerBoundText, columnStats->lowerBoundText == NULL);
		SPI_ARG_VALUE(5, TEXTOID, columnStats->upperBoundText, columnStats->upperBoundText == NULL);

		SPI_START();

		bool		readOnly = false;

		SPI_EXECUTE(query, readOnly);

		SPI_END();

		SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	}
}


 /*
  * DataFileColumnStatsCatalogExists checks if the
  * lake_table.data_file_column_stats catalog exists.
  */
bool
DataFileColumnStatsCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(PG_LAKE_TABLE_DATA_FILE_COLUMN_STATS_TABLE_NAME, namespaceId) != InvalidOid;
}


/*
 * LoadColumnStats fills in per-column min/max stats for the given
 * dataFiles. The SPI query filters by `path = ANY($2)` so the catalog
 * read is bounded by the input list rather than the total file count of
 * the table; the leading PK column (table_name) is also pinned so the
 * planner can index-scan straight to the relevant rows.
 *
 * The dataFiles list must contain TableDataFile pointers whose columnStats
 * are NIL on entry. Stats are appended to each dataFile->stats.columnStats.
 * A small transient path -> TableDataFile* hash makes the per-row lookup
 * O(1).
 */
void
LoadColumnStats(Oid relationId, List *dataFiles)
{
	if (dataFiles == NIL)
		return;

	MemoryContext callerContext = CurrentMemoryContext;

	HTAB	   *filesByPath = BuildPathToTableDataFileHash(dataFiles);

	List	   *pathList = NIL;
	ListCell   *fileCell = NULL;

	foreach(fileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(fileCell);

		pathList = lappend(pathList, dataFile->path);
	}

	char	   *query =
		"select "
		 /* 1 */ "s.path, "
		 /* 2 */ "s.field_id, "
		 /* 3 */ "m.field_pg_type, "
		 /* 4 */ "m.field_pg_typemod, "
		 /* 5 */ "s.lower_bound, "
		 /* 6 */ "s.upper_bound "
		"from " DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " s "
		"JOIN " MAPPING_TABLE_NAME " m USING (table_name, field_id) "
		"JOIN pg_attribute a ON (a.attrelid OPERATOR(pg_catalog.=) m.table_name "
		"                        AND a.attnum   OPERATOR(pg_catalog.=) m.pg_attnum "
		"                        AND NOT a.attisdropped) "
		"where s.table_name OPERATOR(pg_catalog.=) $1 "
		"  AND s.path       OPERATOR(pg_catalog.=) ANY($2)";

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	DECLARE_SPI_ARGS(2);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTARRAYOID, StringListToArray(pathList), false);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		bool		isPathNull = false;
		char	   *path = GET_SPI_VALUE(TEXTOID, rowIndex, 1, &isPathNull);

		if (isPathNull)
		{
			MemoryContextSwitchTo(spiContext);
			continue;
		}

		bool		isFieldIdNull = false;
		int64		fieldId = GET_SPI_VALUE(INT8OID, rowIndex, 2, &isFieldIdNull);

		if (isFieldIdNull)
		{
			MemoryContextSwitchTo(spiContext);
			continue;
		}

		bool		isPgTypeNull = false;
		bool		isPgTypeModNull = false;

		PGType		pgType = {
			.postgresTypeOid = GET_SPI_VALUE(OIDOID, rowIndex, 3, &isPgTypeNull),
			.postgresTypeMod = GET_SPI_VALUE(INT4OID, rowIndex, 4, &isPgTypeModNull)
		};

		char	   *lowerBoundText = NULL;
		char	   *upperBoundText = NULL;

		bool		isLowerBoundNull = false;
		Datum		lowerBoundDatum = GET_SPI_DATUM(rowIndex, 5, &isLowerBoundNull);

		if (!isLowerBoundNull)
			lowerBoundText = TextDatumGetCString(lowerBoundDatum);

		bool		isUpperBoundNull = false;
		Datum		upperBoundDatum = GET_SPI_DATUM(rowIndex, 6, &isUpperBoundNull);

		if (!isUpperBoundNull)
			upperBoundText = TextDatumGetCString(upperBoundDatum);

		bool		found = false;
		PathToTableDataFileEntry *entry =
			hash_search(filesByPath, path, HASH_FIND, &found);

		if (found && !ColumnStatAlreadyAdded(entry->dataFile->stats.columnStats, fieldId))
		{
			DataFileColumnStats *columnStats =
				CreateDataFileColumnStats(fieldId, pgType, lowerBoundText, upperBoundText);

			entry->dataFile->stats.columnStats =
				lappend(entry->dataFile->stats.columnStats, columnStats);
		}

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	hash_destroy(filesByPath);
}


/*
 * BuildPathToTableDataFileHash creates a transient hash mapping each file
 * path in `dataFiles` to its TableDataFile pointer.
 */
static HTAB *
BuildPathToTableDataFileHash(List *dataFiles)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = MAX_S3_PATH_LENGTH;
	hashCtl.entrysize = sizeof(PathToTableDataFileEntry);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *filesByPath = hash_create("path->TableDataFile* hash",
										  list_length(dataFiles),
										  &hashCtl,
										  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	ListCell   *cell = NULL;

	foreach(cell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(cell);
		bool		found = false;

		PathToTableDataFileEntry *entry =
			hash_search(filesByPath, dataFile->path, HASH_ENTER, &found);

		if (!found)
			entry->dataFile = dataFile;
	}

	return filesByPath;
}


/*
 * ColumnStatAlreadyAdded checks if the given field id is already present in
 * the column stats list.
 */
static bool
ColumnStatAlreadyAdded(List *columnStats, int64 fieldId)
{
	ListCell   *cell = NULL;

	foreach(cell, columnStats)
	{
		DataFileColumnStats *columnStat = (DataFileColumnStats *) lfirst(cell);

		if (columnStat->leafField.fieldId == fieldId)
			return true;
	}

	return false;
}


/*
 * CreateDataFileColumnStats creates a new DataFileColumnStats from the given
 * parameters.
 */
static DataFileColumnStats *
CreateDataFileColumnStats(int fieldId, PGType pgType, char *lowerBoundText, char *upperBoundText)
{
	DataFileColumnStats *columnStats = palloc0(sizeof(DataFileColumnStats));

	columnStats->leafField.fieldId = fieldId;
	columnStats->lowerBoundText = lowerBoundText;
	columnStats->upperBoundText = upperBoundText;
	columnStats->leafField.pgType = pgType;

	bool		forAddColumn = false;
	int			subFieldIndex = fieldId;

	Field	   *field = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

	Assert(field->type == FIELD_TYPE_SCALAR);

	columnStats->leafField.field = field;

	const char *duckTypeName = IcebergTypeNameToDuckdbTypeName(field->field.scalar.typeName);

	columnStats->leafField.duckTypeName = duckTypeName;

	return columnStats;
}
