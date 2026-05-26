/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * DucklakeBuildDataFileSchema constructs a DataFileSchema for a DuckLake
 * foreign table. Lifted from pg_lake_table/src/fdw/schema_operations/
 * register_field_ids.c so DuckLake-specific catalog reads don't bloat
 * pg_lake_table.
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/table.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/ducklake/catalog.h"
#include "pg_lake/ducklake/data_file_schema.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/type.h"


DataFileSchema *
DucklakeBuildDataFileSchema(Oid relationId)
{
	Relation	rel = relation_open(relationId, AccessShareLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	/* Count non-dropped columns first */
	int			nonDroppedCount = 0;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (!attr->attisdropped && attr->attnum > 0)
			nonDroppedCount++;
	}

	schema->fields = palloc0(sizeof(DataFileSchemaField) * nonDroppedCount);
	schema->nfields = 0;

	/*
	 * Build per-attnum lookups for (column_id, initial_default) from
	 * lake_ducklake.column for the live (end_snapshot IS NULL) rows of
	 * this table. We pull this once into palloc'd arrays indexed by
	 * attnum so the per-attribute loop below can attach both without an
	 * SPI call per column.
	 */
	int64	   *columnIdsByAttnum = NULL;
	char	  **defaultStrings = NULL;
	int			maxAttnum = 0;
	{
		bool		pushedSnapshot = false;

		if (!ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			pushedSnapshot = true;
		}

		DucklakeTableMetadata *metadata = DucklakeGetTableMetadata(relationId);

		if (metadata != NULL)
		{
			for (int i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

				if (!attr->attisdropped && attr->attnum > maxAttnum)
					maxAttnum = attr->attnum;
			}

			if (maxAttnum > 0)
			{
				columnIdsByAttnum = palloc0(sizeof(int64) * (maxAttnum + 1));
				defaultStrings = palloc0(sizeof(char *) * (maxAttnum + 1));

				MemoryContext callerContext = CurrentMemoryContext;

				StringInfoData q;

				/*
				 * Match catalog rows to pg_attribute by NAME, not by
				 * column_order. After DROP+ADD column the new live
				 * row's column_order (allocated sequentially in the
				 * catalog) no longer matches pg_attribute.attnum (which
				 * skips dropped slots), so order-based lookup would
				 * miss the column entirely and stamp field_id=0 on it.
				 */
				initStringInfo(&q);
				appendStringInfo(&q,
								 "SELECT a.attnum, c.column_id, c.initial_default "
								 "  FROM pg_attribute a "
								 "  JOIN lake_ducklake.column c "
								 "    ON c.column_name = a.attname::text "
								 " WHERE a.attrelid = %u "
								 "   AND a.attnum > 0 AND NOT a.attisdropped "
								 "   AND c.table_id = %ld "
								 "   AND c.end_snapshot IS NULL "
								 "   AND c.parent_column IS NULL",
								 relationId, metadata->tableId);

				SPI_connect();
				int			ret = SPI_exec(q.data, 0);

				if (ret == SPI_OK_SELECT)
				{
					for (uint64 row = 0; row < SPI_processed; row++)
					{
						bool		isnull;
						int32		attno = DatumGetInt32(SPI_getbinval(
													SPI_tuptable->vals[row],
													SPI_tuptable->tupdesc, 1, &isnull));

						if (isnull || attno <= 0 || attno > maxAttnum)
							continue;

						int64		colId = DatumGetInt64(SPI_getbinval(
													SPI_tuptable->vals[row],
													SPI_tuptable->tupdesc, 2, &isnull));

						if (!isnull)
							columnIdsByAttnum[attno] = colId;

						Datum		d = SPI_getbinval(SPI_tuptable->vals[row],
													  SPI_tuptable->tupdesc, 3, &isnull);

						if (!isnull)
						{
							MemoryContext oldcxt =
								MemoryContextSwitchTo(callerContext);

							defaultStrings[attno] = TextDatumGetCString(d);
							MemoryContextSwitchTo(oldcxt);
						}
					}
				}

				SPI_finish();
			}

			if (metadata->tableName)
				pfree(metadata->tableName);
			if (metadata->schemaName)
				pfree(metadata->schemaName);
			if (metadata->path)
				pfree(metadata->path);
			pfree(metadata);
		}

		if (pushedSnapshot)
			PopActiveSnapshot();
	}

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped || attr->attnum <= 0)
			continue;

		DataFileSchemaField *field = &schema->fields[schema->nfields];

		int64		columnId = (columnIdsByAttnum != NULL && attr->attnum <= maxAttnum)
									? columnIdsByAttnum[attr->attnum]
									: 0;

		field->id = (columnId > 0) ? (int) columnId : attr->attnum;
		field->name = pstrdup(NameStr(attr->attname));
		field->required = attr->attnotnull;
		field->doc = NULL;
		field->writeDefault = NULL;
		field->initialDefault = NULL;
		field->duckSerializedInitialDefault = NULL;

		PGType		pgType = MakePGType(attr->atttypid, attr->atttypmod);
		bool		forAddColumn = false;
		int			subFieldIndex = field->id;

		field->type = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

		if (defaultStrings != NULL && attr->attnum <= maxAttnum &&
			defaultStrings[attr->attnum] != NULL)
		{
			field->initialDefault = defaultStrings[attr->attnum];
			field->duckSerializedInitialDefault = defaultStrings[attr->attnum];
		}

		schema->nfields++;
	}

	relation_close(rel, AccessShareLock);

	return schema;
}
