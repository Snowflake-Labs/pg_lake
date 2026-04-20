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
 * partition_pushdown.c
 *
 * Utilities for pushing down partitioned Iceberg writes to DuckDB using
 * the PARTITION_BY clause in COPY TO. Supports identity and temporal
 * (year, month, day, hour) partition transforms.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "pg_lake/fdw/partition_pushdown.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/write_data.h"


static char *PartitionTransformToDuckDBExpression(IcebergPartitionTransform * transform);


/*
 * GetPartitionByExpressionsForRelation returns a list of DuckDB SQL expressions
 * for partition pushdown, or NIL if the table has no partition spec or any
 * transform is not pushdownable.
 *
 * This is the single entry point for deciding whether partitioned writes can
 * be pushed down to DuckDB via PARTITION_BY.
 */
List *
GetPartitionByExpressionsForRelation(Oid relationId)
{
	List	   *transforms = CurrentPartitionTransformList(relationId);

	if (transforms == NIL)
		return NIL;

	List	   *exprs = NIL;
	ListCell   *cell = NULL;

	foreach(cell, transforms)
	{
		IcebergPartitionTransform *transform = lfirst(cell);
		char	   *expr = PartitionTransformToDuckDBExpression(transform);

		if (expr == NULL)
			return NIL;

		exprs = lappend(exprs, makeString(expr));
	}

	return exprs;
}


/*
 * PartitionTransformToDuckDBExpression returns a DuckDB SQL expression that
 * computes the Iceberg partition value for the given transform.
 *
 * Returns NULL for transforms that cannot be pushed down (bucket, truncate, void).
 *
 * The expressions produce Iceberg-compatible partition values:
 * - year: integer years since 1970
 * - month: integer months since Jan 1970
 * - day: integer days since 1970-01-01
 * - hour: integer hours since 1970-01-01T00:00:00
 * - identity: the column value as-is (whitelisted types only)
 */
static char *
PartitionTransformToDuckDBExpression(IcebergPartitionTransform * transform)
{
	const char *col = quote_identifier(transform->columnName);
	Oid			typeOid = transform->pgType.postgresTypeOid;

	switch (transform->type)
	{
		case PARTITION_TRANSFORM_IDENTITY:
			{
				/*
				 * Only push down identity partitions for types whose DuckDB
				 * VARCHAR representation can be parsed by PG's type input
				 * function. Types like bytea are excluded because DuckDB's
				 * BLOB-to-VARCHAR cast uses a format PG cannot parse (same
				 * issue as column_statistics, which skips bytea via
				 * ShouldSkipStatistics).
				 *
				 * No UTC conversion is needed for timestamptz/timetz here
				 * because identity passes the value as-is (no temporal
				 * arithmetic). UTC matters only for temporal transforms
				 * (year, month, day, hour) that compute epoch-based integers.
				 */
				if (typeOid == INT2OID || typeOid == INT4OID ||
					typeOid == INT8OID || typeOid == FLOAT4OID ||
					typeOid == FLOAT8OID || typeOid == NUMERICOID ||
					typeOid == BOOLOID || typeOid == TEXTOID ||
					typeOid == VARCHAROID || typeOid == BPCHAROID ||
					typeOid == DATEOID || typeOid == TIMESTAMPOID ||
					typeOid == TIMESTAMPTZOID || typeOid == UUIDOID ||
					typeOid == TIMEOID || typeOid == TIMETZOID)
					return psprintf("%s", col);
				else
					return NULL;
			}

		case PARTITION_TRANSFORM_YEAR:
			{
				/*
				 * Iceberg spec requires UTC for timestamptz. PG stores
				 * timestamptz internally in UTC, so the non-pushdown path
				 * works correctly. In DuckDB, year() uses session timezone,
				 * so we must convert to UTC first.
				 */
				if (typeOid == TIMESTAMPTZOID)
					return psprintf("(year(timezone('UTC', %s)) - 1970)", col);
				else
					return psprintf("(year(%s) - 1970)", col);
			}

		case PARTITION_TRANSFORM_MONTH:
			{
				if (typeOid == TIMESTAMPTZOID)
					return psprintf("((year(timezone('UTC', %s)) - 1970) * 12 + "
									"month(timezone('UTC', %s)) - 1)", col, col);
				else
					return psprintf("((year(%s) - 1970) * 12 + month(%s) - 1)",
									col, col);
			}

		case PARTITION_TRANSFORM_DAY:
			{
				/*
				 * Iceberg spec requires UTC for day transforms. For
				 * timestamptz, convert to UTC before computing the day.
				 */
				if (typeOid == TIMESTAMPTZOID)
					return psprintf("datediff('day', date '1970-01-01', "
									"timezone('UTC', %s)::date)", col);
				else
					return psprintf("datediff('day', date '1970-01-01', %s::date)", col);
			}

		case PARTITION_TRANSFORM_HOUR:
			{
				/*
				 * Only TIMESTAMP and TIMESTAMPTZ are pushdownable for hour
				 * transforms. TIME/TIMETZ fall back to row-by-row processing.
				 * Iceberg spec requires UTC for timestamptz.
				 */
				if (typeOid == TIMESTAMPTZOID)
					return psprintf("datediff('hour', timestamp '1970-01-01', "
									"timezone('UTC', %s)::timestamp)", col);
				else if (typeOid == TIMESTAMPOID)
					return psprintf("datediff('hour', timestamp '1970-01-01', "
									"%s::timestamp)", col);
				else
					return NULL;
			}

		case PARTITION_TRANSFORM_BUCKET:
		case PARTITION_TRANSFORM_TRUNCATE:
		case PARTITION_TRANSFORM_VOID:
			return NULL;
	}

	return NULL;
}



/*
 * NormalizeDuckDBTextToPGText converts a DuckDB text representation of a value
 * to PostgreSQL's canonical text format by roundtripping through PG's type I/O.
 *
 * DuckDB may format values differently from PG (e.g. "1.0" vs "1" for numeric,
 * "-0.0" vs "-0" for float8). This normalization ensures the text matches what
 * DeserializePartitionValueFromPGText expects for its roundtrip assertion.
 */
static char *
NormalizeDuckDBTextToPGText(const char *duckdbText, Oid resultTypeOid,
							int32 resultTypeMod)
{
	Oid			typoinput;
	Oid			typioparam;
	Oid			typoutput;
	bool		typIsVarlena;

	getTypeInputInfo(resultTypeOid, &typoinput, &typioparam);
	Datum		parsedDatum = OidInputFunctionCall(typoinput, (char *) duckdbText,
												   typioparam, resultTypeMod);

	getTypeOutputInfo(resultTypeOid, &typoutput, &typIsVarlena);
	return OidOutputFunctionCall(typoutput, parsedDatum);
}


/*
 * ParsePartitionValuesFromPartitionKeys extracts partition values from the
 * partition_keys MAP(VARCHAR, VARCHAR) returned by DuckDB's COPY TO with
 * return_stats.
 *
 * The partition_keys map has entries like:
 *   {__pglake_part_0=54, __pglake_part_1=us-east}
 *
 * Each value is converted to the proper Iceberg binary format using the
 * partition transforms.
 */
Partition *
ParsePartitionValuesFromPartitionKeys(char *partitionKeysText, List *transforms)
{
	if (partitionKeysText == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("partition_keys is NULL for partitioned write")));

	int			numTransforms = list_length(transforms);
	Partition  *partition = palloc0(sizeof(Partition));

	partition->fields = palloc0(sizeof(PartitionField) * numTransforms);
	partition->fields_length = numTransforms;

	/* parse the MAP(TEXT,TEXT) text into a datum */
	Oid			mapTypeOid = GetOrCreatePGMapType("MAP(TEXT,TEXT)");
	Oid			typoinput;
	Oid			typioparam;

	getTypeInputInfo(mapTypeOid, &typoinput, &typioparam);
	Datum		mapDatum = OidInputFunctionCall(typoinput, partitionKeysText,
												typioparam, -1);

	/*
	 * Build an array of value texts indexed by partition index. We iterate
	 * the map entries and match PARTITION_COLUMN_PREFIX keys to their
	 * indices.
	 */
	char	  **valueTexts = palloc0(sizeof(char *) * numTransforms);
	bool	   *valueIsNull = palloc0(sizeof(bool) * numTransforms);

	ArrayType  *elementsArray = DatumGetArrayTypeP(mapDatum);
	ArrayIterator arrayIterator = array_create_iterator(elementsArray, 0, NULL);
	Datum		elemDatum;
	bool		isNull = false;

	while (array_iterate(arrayIterator, &elemDatum, &isNull))
	{
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected NULL element in partition_keys map")));

		HeapTupleHeader tupleHeader = DatumGetHeapTupleHeader(elemDatum);
		bool		keyIsNull = false;
		bool		valIsNull = false;

		Datum		keyDatum = GetAttributeByNum(tupleHeader, 1, &keyIsNull);
		Datum		valDatum = GetAttributeByNum(tupleHeader, 2, &valIsNull);

		if (keyIsNull)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected NULL key in partition_keys map")));

		char	   *key = TextDatumGetCString(keyDatum);

		/* parse PARTITION_COLUMN_PREFIX + N to get the partition index */
		int			prefixLen = strlen(PARTITION_COLUMN_PREFIX);

		if (strncmp(key, PARTITION_COLUMN_PREFIX, prefixLen) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected partition key name \"%s\" "
							"(expected " PARTITION_COLUMN_PREFIX "N)", key)));

		int			partIndex = pg_strtoint32(key + prefixLen);

		if (partIndex < 0 || partIndex >= numTransforms)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected partition key %s (expected 0..%d)",
							key, numTransforms - 1)));
		}

		if (valIsNull)
		{
			valueIsNull[partIndex] = true;
		}
		else
		{
			valueTexts[partIndex] = TextDatumGetCString(valDatum);
		}
	}

	array_free_iterator(arrayIterator);

	/* convert each partition value to Iceberg binary format */
	for (int partIndex = 0; partIndex < numTransforms; partIndex++)
	{
		IcebergPartitionTransform *transform = list_nth(transforms, partIndex);
		PartitionField *field = &partition->fields[partIndex];

		field->field_id = transform->partitionFieldId;
		field->field_name = pstrdup(transform->partitionFieldName);
		field->value_type = GetTransformResultAvroType(transform);

		if (valueIsNull[partIndex] || valueTexts[partIndex] == NULL)
		{
			/* NULL partition value */
			field->value = NULL;
			field->value_length = 0;
		}
		else
		{
			/*
			 * Normalize DuckDB text to PG canonical format (e.g. "1.0" -> "1"
			 * for numeric) so the roundtrip assertion in
			 * DeserializePartitionValueFromPGText passes.
			 */
			char	   *normalizedText =
				NormalizeDuckDBTextToPGText(valueTexts[partIndex],
											transform->resultPgType.postgresTypeOid,
											transform->resultPgType.postgresTypeMod);

			field->value = DeserializePartitionValueFromPGText(
															   transform, normalizedText,
															   &field->value_length);
		}
	}

	return partition;
}
