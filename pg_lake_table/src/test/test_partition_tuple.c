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
#include "funcapi.h"
#include "access/relation.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/temporal_utils.h"


static TupleTableSlot *make_tuple_slot_from_record(Oid relationId, HeapTupleHeader rowHeader);
static void EnsureTupleDescMatchTransforms(TupleDesc tupledesc, List *transforms);
static Datum AdjustFieldSummaryTextToSpark(const char *fieldSummaryText, PGType sourceType,
										   IcebergPartitionTransformType transformType);


PG_FUNCTION_INFO_V1(get_partition_tuple);
PG_FUNCTION_INFO_V1(get_partition_summary);


/*
 * get_partition_tuple
 *
 * This function takes a HeapTupleHeader and returns the partition tuple
 * after applying available transforms on the input tuple.
 *
 * partition_by clause is expected to be defined on the table.
 */
Datum
get_partition_tuple(PG_FUNCTION_ARGS)
{
	HeapTupleHeader row = PG_GETARG_HEAPTUPLEHEADER(0);

	Oid			tupType = HeapTupleHeaderGetTypeId(row);
	Oid			relationId = get_typ_typrelid(tupType);

	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	if (tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("get_partition_tuple is only supported for iceberg tables")));
	}

	List	   *transforms = CurrentPartitionTransformList(relationId);

	if (transforms == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("empty partition transforms found for table %u", relationId)));

	/* compute partition tuple */
	TupleTableSlot *slot = make_tuple_slot_from_record(relationId, row);

	Partition  *partition = ComputePartitionTupleForTuple(transforms, slot);

	/* init result tuplestore */
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	EnsureTupleDescMatchTransforms(rsinfo->setDesc, transforms);

	/* fill result tuplestore */
	Datum	   *values = palloc0(sizeof(Datum) * partition->fields_length);
	bool	   *nulls = palloc0(sizeof(bool) * partition->fields_length);

	for (int i = 0; i < partition->fields_length; i++)
	{
		PartitionField *field = &partition->fields[i];

		/* corresponding transform for partition field */
		IcebergPartitionTransform *transform = list_nth(transforms, i);

		AttrNumber	attnum = get_attnum(relationId, transform->columnName);

		if (attnum == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column %s does not exist in relation %u",
							transform->columnName, relationId)));
		}

		PGType		pgType = transform->resultPgType;

		values[i] = PartitionValueToDatum(transform->type, field->value, field->value_length, pgType, &nulls[i]);
	}

	ExecDropSingleTupleTableSlot(slot);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	PG_RETURN_VOID();
}


/*
 * get_partition_summary
 *
 * This function takes a table name and returns the partition summary
 * for all current manifests of the table.
 */
Datum
get_partition_summary(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);

	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	if (tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("get_partition_summary is only supported for iceberg tables")));
	}

	bool		forUpdate = false;
	char	   *metadataUri = GetIcebergMetadataLocation(relationId, forUpdate);

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);

	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, false);

	List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);

	List	   *allTransforms = AllPartitionTransformList(relationId);

	if (allTransforms == NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no partition transforms found for table %u", relationId)));
	}

	/* init result tuplestore */
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* fill result tuplestore */
	Datum	   *values = palloc0(sizeof(Datum) * 4);
	bool	   *nulls = palloc0(sizeof(bool) * 4);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = (IcebergManifest *) lfirst(manifestCell);

		List	   *manifestPartitionFields = GetIcebergSpecPartitionFieldsFromCatalog(relationId, manifest->partition_spec_id);

		List	   *manifestTransforms = GetPartitionTransformsFromSpecFields(relationId, manifestPartitionFields);

		for (int summaryIdx = 0; summaryIdx < manifest->partitions_length; summaryIdx++)
		{
			FieldSummary *partitionSummary = &manifest->partitions[summaryIdx];

			/* corresponding partition spec field for partition summary */
			IcebergPartitionSpecField *specField = list_nth(manifestPartitionFields, summaryIdx);

			/* corresponding transform for partition spec field */
			IcebergPartitionTransform *transform = FindPartitionTransformById(manifestTransforms, specField->field_id);

			PGType		sourceType = transform->pgType;

			values[0] = Int32GetDatum(manifest->sequence_number);

			values[1] = Int32GetDatum(specField->field_id);

			if (partitionSummary->lower_bound != NULL)
			{
				const char *lowerBoundText = SerializePartitionValueToPGText(partitionSummary->lower_bound,
																			 partitionSummary->lower_bound_length,
																			 transform);

				values[2] = AdjustFieldSummaryTextToSpark(lowerBoundText, sourceType, transform->type);
				nulls[2] = false;
			}
			else
			{
				nulls[2] = true;
			}

			if (partitionSummary->upper_bound != NULL)
			{
				const char *upperBoundText = SerializePartitionValueToPGText(partitionSummary->upper_bound,
																			 partitionSummary->upper_bound_length,
																			 transform);

				values[3] = AdjustFieldSummaryTextToSpark(upperBoundText, sourceType, transform->type);
				nulls[3] = false;
			}
			else
			{
				nulls[3] = true;
			}

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PG_RETURN_VOID();
}


static TupleTableSlot *
make_tuple_slot_from_record(Oid relationId, HeapTupleHeader rowHeader)
{
	/* 1) Open the relation to get its TupleDesc */
	Relation	rel = relation_open(relationId, AccessShareLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	/* 2) Create a TupleTableSlot based on that descriptor */
	TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);


	/* Fill in a stack HeapTupleData */
	HeapTupleData tmptup;

	MemSet(&tmptup, 0, sizeof(HeapTupleData));

	tmptup.t_len = HeapTupleHeaderGetDatumLength(rowHeader);
	tmptup.t_data = rowHeader;
	tmptup.t_tableOid = relationId;
	ItemPointerSetInvalid(&tmptup.t_self);

	/* Copy it into allocated memory */
	HeapTuple	newtup = heap_copytuple(&tmptup);

	/*
	 * Now store the allocated HeapTuple in the slot. The 'true' means the
	 * slot will eventually free newtup.
	 */
	ExecStoreHeapTuple(newtup, slot, true);

	/* 6) Close the relation */
	relation_close(rel, NoLock);

	/* 7) Return the slot */
	return slot;
}


static void
EnsureTupleDescMatchTransforms(TupleDesc tupledesc, List *transforms)
{
	if (tupledesc->natts != list_length(transforms))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of fields in partition tuple does not match number of transforms")));
	}

	for (int i = 0; i < tupledesc->natts; i++)
	{
		IcebergPartitionTransform *transform = list_nth(transforms, i);

		Form_pg_attribute column = TupleDescAttr(tupledesc, i);

		Assert(!column->attisdropped);

		const char *attname = NameStr(column->attname);

		PGType		transformResultPgType = transform->resultPgType;

		if (column->atttypid != transformResultPgType.postgresTypeOid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("attribute %s's type %s in tupledesc does not match transform %s's type %s",
							attname, format_type_be(column->atttypid),
							transform->columnName, format_type_be(transformResultPgType.postgresTypeOid))));

		if (column->atttypmod != transformResultPgType.postgresTypeMod)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("attribute %s's typmod %d in tupledesc does not match transform %s's typmod %d",
								   attname, column->atttypmod, transform->columnName,
								   transformResultPgType.postgresTypeMod)));
	}
}


/*
 * AdjustFieldSummaryTextToSpark adjust partition field summary value text representation
 * according to the view that we use from Spark (different than how it looks like
 * in the manifest list)
 */
static Datum
AdjustFieldSummaryTextToSpark(const char *fieldSummaryText, PGType sourceType,
							  IcebergPartitionTransformType transformType)
{
	if (sourceType.postgresTypeOid == BYTEAOID &&
		(transformType == PARTITION_TRANSFORM_IDENTITY || transformType == PARTITION_TRANSFORM_TRUNCATE))
	{
		/* spark prints base64 encoded value for binary */
		Datum		byteaDatum = DirectFunctionCall1(byteain, CStringGetDatum(fieldSummaryText));

		const char *encodingName = "base64";

		return DirectFunctionCall2(binary_encode, byteaDatum, CStringGetTextDatum(encodingName));
	}
	else if (sourceType.postgresTypeOid == BOOLOID)
	{
		/* spark prints false instead of f */
		const char *adjustedBool = (strcmp(fieldSummaryText, "f") == 0) ? "false" : "true";

		return CStringGetTextDatum(adjustedBool);
	}
	else if (sourceType.postgresTypeOid == TIMESTAMPOID && transformType == PARTITION_TRANSFORM_IDENTITY)
	{
		/*
		 * spark prints '2020-01-01T20:00:00' whereas pg prints '2020-01-01
		 * 20:00:00'
		 */
		Datum		tsDatum = DirectFunctionCall3(timestamp_in,
												  CStringGetDatum(fieldSummaryText),
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(sourceType.postgresTypeMod));

		Datum		numericSecondsDatum = DirectFunctionCall2(extract_timestamp,
															  CStringGetTextDatum("second"),
															  tsDatum);
		float8		seconds = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, numericSecondsDatum));

		const char *formatStr = (seconds == 0.0) ? "YYYY-MM-DD\"T\"HH24:MI" : "YYYY-MM-DD\"T\"HH24:MI:SS";

		return DirectFunctionCall2(timestamp_to_char, tsDatum, CStringGetTextDatum(formatStr));
	}
	else if (sourceType.postgresTypeOid == TIMESTAMPTZOID && transformType == PARTITION_TRANSFORM_IDENTITY)
	{
		/*
		 * spark prints '2020-01-01T20:00:00' whereas pg prints '2020-01-01
		 * 20:00:00'
		 */
		Datum		tsDatum = DirectFunctionCall3(timestamptz_in,
												  CStringGetDatum(fieldSummaryText),
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(sourceType.postgresTypeMod));

		Datum		numericSecondsDatum = DirectFunctionCall2(extract_timestamp,
															  CStringGetTextDatum("second"),
															  tsDatum);
		float8		seconds = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, numericSecondsDatum));

		const char *formatStr = (seconds == 0.0) ? "YYYY-MM-DD\"T\"HH24:MI\"Z\"" : "YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"";

		return DirectFunctionCall2(timestamptz_to_char, tsDatum, CStringGetTextDatum(formatStr));
	}
	else if ((sourceType.postgresTypeOid == DATEOID || sourceType.postgresTypeOid == TIMESTAMPOID || sourceType.postgresTypeOid == TIMESTAMPTZOID) && transformType == PARTITION_TRANSFORM_YEAR)
	{
		/* spark prints 2 years as '1972' */
		int			years = DirectFunctionCall1(int4in, CStringGetDatum(fieldSummaryText));

		Timestamp	ts = YearsFromEpochToTimestamp(years);

		return DirectFunctionCall2(timestamp_to_char, TimestampGetDatum(ts), CStringGetTextDatum("YYYY"));
	}
	else if ((sourceType.postgresTypeOid == DATEOID || sourceType.postgresTypeOid == TIMESTAMPOID || sourceType.postgresTypeOid == TIMESTAMPTZOID) && transformType == PARTITION_TRANSFORM_MONTH)
	{
		/* spark prints 15 months as '1971-03' */
		int			months = DirectFunctionCall1(int4in, CStringGetDatum(fieldSummaryText));

		Timestamp	ts = MonthsFromUnixEpochToTimestamp(months);

		return DirectFunctionCall2(timestamp_to_char, TimestampGetDatum(ts), CStringGetTextDatum("YYYY-MM"));
	}
	else if ((sourceType.postgresTypeOid == DATEOID || sourceType.postgresTypeOid == TIMESTAMPOID || sourceType.postgresTypeOid == TIMESTAMPTZOID) && transformType == PARTITION_TRANSFORM_DAY)
	{
		/* spark prints 370 days as '1971-01-05' */
		int			days = DirectFunctionCall1(int4in, CStringGetDatum(fieldSummaryText));

		Timestamp	ts = DaysFromUnixEpochToTimestamp(days);

		return DirectFunctionCall2(timestamp_to_char, TimestampGetDatum(ts), CStringGetTextDatum("YYYY-MM-DD"));
	}
	else if ((sourceType.postgresTypeOid == TIMEOID || sourceType.postgresTypeOid == TIMESTAMPOID || sourceType.postgresTypeOid == TIMESTAMPTZOID) && transformType == PARTITION_TRANSFORM_HOUR)
	{
		/* spark prints 30 hours as '1970-01-02-06' */
		int			hours = DirectFunctionCall1(int4in, CStringGetDatum(fieldSummaryText));

		Timestamp	ts = HoursFromUnixEpochToTimestamp(hours);

		return DirectFunctionCall2(timestamp_to_char, TimestampGetDatum(ts), CStringGetTextDatum("YYYY-MM-DD-HH24"));
	}
	else
	{
		return CStringGetTextDatum(fieldSummaryText);
	}
}
