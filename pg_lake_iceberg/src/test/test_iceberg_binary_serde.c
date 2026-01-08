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

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/util/numeric.h"

#include "utils/builtins.h"
#include "utils/json.h"


static ColumnBound * FindColumnBoundByColumnId(ColumnBound * bounds, int numBounds, int columnId);
static LeafField * GetLeafFieldByColumnId(List *leafFields, int columnId);
static List *DeserializeDataFileColumnBounds(List *leafFields, ColumnBound * columnBounds,
											 int boundsLength, List **columnTypes, List **columnIdDatums);
static List *SerializeDataFileColumnBounds(List *leafFields, List *columnIdDatums, List *boundDatums, List **columnTypes);
static Datum DeserializeColumnBound(ColumnBound * bound, LeafField * leafField);
static Datum DataFileColumnBoundsToJsonDatum(List *boundDatums, List *columnIdDatums, List *columnTypes);

PG_FUNCTION_INFO_V1(pg_lake_read_data_file_stats);
PG_FUNCTION_INFO_V1(pg_lake_reserialize_data_file_stats);
PG_FUNCTION_INFO_V1(pg_lake_serde_value);

/*
 * FindColumnBoundByColumnId finds ColumnBound by column id.
 */
static ColumnBound *
FindColumnBoundByColumnId(ColumnBound * bounds, int numBounds, int columnId)
{
	for (int i = 0; i < numBounds; i++)
	{
		if (bounds[i].column_id == columnId)
		{
			return &bounds[i];
		}
	}

	return NULL;
}

/*
 * GetLeafFieldByColumnId finds LeafField by field id.
 */
static LeafField *
GetLeafFieldByColumnId(List *leafFields, int columnId)
{
	ListCell   *leafFieldCell = NULL;

	foreach(leafFieldCell, leafFields)
	{
		LeafField  *leafField = lfirst(leafFieldCell);

		if (leafField->fieldId == columnId)
		{
			return leafField;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("leaf field not found for field id %d", columnId)));
}


/*
 * DeserializeColumnBound converts ColumnBound to datum.
 */
static Datum
DeserializeColumnBound(ColumnBound * bound, LeafField * leafField)
{
	Field	   *field = leafField->field;

	PGType		pgType = IcebergFieldToPostgresType(field);

	return PGIcebergBinaryDeserialize((unsigned char *) bound->value, bound->value_length,
									  field, pgType);
}

/*
 * DeserializeDataFileColumnBounds deserializes ColumnBounds to datums.
 */
static List *
DeserializeDataFileColumnBounds(List *leafFields, ColumnBound * columnBounds,
								int boundsLength, List **columnTypes, List **columnIdDatums)
{
	List	   *boundDatums = NIL;

	ListCell   *leafFieldCell = NULL;

	foreach(leafFieldCell, leafFields)
	{
		LeafField  *leafField = lfirst(leafFieldCell);

		int			columnId = leafField->fieldId;

		ColumnBound *bound = FindColumnBoundByColumnId(columnBounds,
													   boundsLength,
													   columnId);

		if (bound == NULL)
		{
			continue;
		}

		*columnIdDatums = lappend(*columnIdDatums, (void *) Int32GetDatum(columnId));

		PGType		pgType = IcebergFieldToPostgresType(leafField->field);

		Oid			columnType = pgType.postgresTypeOid;

		Datum		boundDatum = DeserializeColumnBound(bound, leafField);

		boundDatums = lappend(boundDatums, (void *) boundDatum);

		*columnTypes = lappend_oid(*columnTypes, columnType);
	}

	return boundDatums;
}

/*
 * SerializeDataFileColumnBounds serializes ColumnBounds datums, previously deserialized from metadata,
 * to bytea datums.
 */
static List *
SerializeDataFileColumnBounds(List *leafFields, List *columnIdDatums, List *boundDatums, List **columnTypes)
{
	Assert(list_length(boundDatums) == list_length(columnIdDatums));
	Assert(list_length(boundDatums) == list_length(*columnTypes));

	List	   *byteaColumnTypes = NIL;

	List	   *serializedBoundDatums = NIL;

	for (int boundIdx = 0; boundIdx < list_length(boundDatums); boundIdx++)
	{
		Datum		boundDatum = (Datum) list_nth(boundDatums, boundIdx);

		Datum		columnIdDatum = (Datum) list_nth(columnIdDatums, boundIdx);

		int			columnId = DatumGetInt32(columnIdDatum);

		LeafField  *leafField = GetLeafFieldByColumnId(leafFields, columnId);

		Field	   *field = leafField->field;
		PGType		pgType = IcebergFieldToPostgresType(field);

		size_t		binaryLen = 0;

		unsigned char *binaryValue = PGIcebergBinarySerializeBoundValue(boundDatum, field,
																		pgType, &binaryLen);

		bytea	   *columnBoundValue = (bytea *) palloc0(binaryLen + VARHDRSZ);

		SET_VARSIZE(columnBoundValue, binaryLen + VARHDRSZ);

		memcpy(VARDATA(columnBoundValue), binaryValue, binaryLen);

		serializedBoundDatums = lappend(serializedBoundDatums, (void *) PointerGetDatum(columnBoundValue));

		byteaColumnTypes = lappend_oid(byteaColumnTypes, BYTEAOID);
	}

	*columnTypes = byteaColumnTypes;

	return serializedBoundDatums;
}


/*
 * DataFileColumnBoundsToJsonDatum converts column bounds to json datum.
 */
static Datum
DataFileColumnBoundsToJsonDatum(List *boundDatums, List *columnIdDatums, List *columnTypes)
{
	Assert(list_length(boundDatums) == list_length(columnIdDatums));
	Assert(list_length(boundDatums) == list_length(columnTypes));

	int			nargs = 2 * list_length(boundDatums);

	Datum	   *args = palloc0(nargs * sizeof(Datum));
	bool	   *nulls = palloc0(nargs * sizeof(bool));
	Oid		   *types = palloc0(nargs * sizeof(Oid));

	for (int boundIdx = 0; boundIdx < list_length(boundDatums); boundIdx++)
	{
		types[2 * boundIdx] = INT4OID;
		types[2 * boundIdx + 1] = list_nth_oid(columnTypes, boundIdx);

		args[2 * boundIdx] = (Datum) list_nth(columnIdDatums, boundIdx);
		args[2 * boundIdx + 1] = (Datum) list_nth(boundDatums, boundIdx);
	}

	return json_build_object_worker(nargs, args, nulls, types, false, true);
}

/*
 * pg_lake_read_data_file_stats reads data file stats from the given metadata
 * as json in human readable format.
 */
Datum
pg_lake_read_data_file_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	Datum		values[4];
	bool		nulls[4];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);

	List	   *manifests = FetchManifestsFromSnapshot(GetCurrentSnapshot(metadata, false), NULL);

	IcebergTableSchema *schema = GetCurrentIcebergTableSchema(metadata);

	List	   *leafFields = GetLeafFieldsForIcebergSchema(schema);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		List	   *dataFiles = FetchDataFilesFromManifest(manifest, false, IsManifestEntryStatusScannable, NULL);

		ListCell   *dataFileCell = NULL;

		foreach(dataFileCell, dataFiles)
		{
			DataFile   *dataFile = lfirst(dataFileCell);

			Assert(dataFile->lower_bounds_length == dataFile->upper_bounds_length);

			List	   *lowerBoundColumnIdDatums = NIL;
			List	   *lowerBoundColumnTypes = NIL;
			List	   *lowerBoundDatums = DeserializeDataFileColumnBounds(leafFields,
																		   dataFile->lower_bounds,
																		   dataFile->lower_bounds_length,
																		   &lowerBoundColumnTypes,
																		   &lowerBoundColumnIdDatums);

			Datum		lowerBoundsJsonDatum = DataFileColumnBoundsToJsonDatum(lowerBoundDatums,
																			   lowerBoundColumnIdDatums,
																			   lowerBoundColumnTypes);

			List	   *upperBoundColumnIdDatums = NIL;
			List	   *upperBoundColumnTypes = NIL;
			List	   *upperBoundDatums = DeserializeDataFileColumnBounds(leafFields,
																		   dataFile->upper_bounds,
																		   dataFile->upper_bounds_length,
																		   &upperBoundColumnTypes,
																		   &upperBoundColumnIdDatums);

			Datum		upperBoundsJsonDatum = DataFileColumnBoundsToJsonDatum(upperBoundDatums,
																			   upperBoundColumnIdDatums,
																			   upperBoundColumnTypes);

			values[0] = CStringGetTextDatum(dataFile->file_path);
			values[1] = DatumGetInt64(manifest->sequence_number);
			values[2] = lowerBoundsJsonDatum;
			values[3] = upperBoundsJsonDatum;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PG_RETURN_VOID();
}

/*
 * pg_lake_reserialize_data_file_stats reserializes data file stats from the given metadata.
 * It first deserializes the column bounds to datums, then serializes them back to bytea datums
 * according to the Iceberg spec.
 */
Datum
pg_lake_reserialize_data_file_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	Datum		values[4];
	bool		nulls[4];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);

	List	   *manifests = FetchManifestsFromSnapshot(GetCurrentSnapshot(metadata, false), NULL);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		int64		addedSnapshotId = manifest->added_snapshot_id;

		IcebergSnapshot *snapshot = GetIcebergSnapshotViaId(metadata, addedSnapshotId);

		/*
		 * we need to use the schema of the snapshot that added the datafiles
		 * under the manifest
		 */
		IcebergTableSchema *schema = GetIcebergTableSchemaByIdFromTableMetadata(metadata, snapshot->schema_id);

		List	   *leafFields = GetLeafFieldsForIcebergSchema(schema);

		List	   *dataFiles = FetchDataFilesFromManifest(manifest, false, IsManifestEntryStatusScannable, NULL);

		ListCell   *dataFileCell = NULL;

		foreach(dataFileCell, dataFiles)
		{
			DataFile   *dataFile = lfirst(dataFileCell);

			Assert(dataFile->lower_bounds_length == dataFile->upper_bounds_length);

			List	   *lowerBoundColumnIdDatums = NIL;
			List	   *lowerBoundColumnTypes = NIL;
			List	   *lowerBoundDatums = DeserializeDataFileColumnBounds(leafFields,
																		   dataFile->lower_bounds,
																		   dataFile->lower_bounds_length,
																		   &lowerBoundColumnTypes,
																		   &lowerBoundColumnIdDatums);

			List	   *lowerBoundSerializedDatums = SerializeDataFileColumnBounds(leafFields,
																				   lowerBoundColumnIdDatums,
																				   lowerBoundDatums,
																				   &lowerBoundColumnTypes);

			Datum		lowerBoundsJsonDatum = DataFileColumnBoundsToJsonDatum(lowerBoundSerializedDatums,
																			   lowerBoundColumnIdDatums,
																			   lowerBoundColumnTypes);

			List	   *upperBoundColumnIdDatums = NIL;
			List	   *upperBoundColumnTypes = NIL;
			List	   *upperBoundDatums = DeserializeDataFileColumnBounds(leafFields,
																		   dataFile->upper_bounds,
																		   dataFile->upper_bounds_length,
																		   &upperBoundColumnTypes,
																		   &upperBoundColumnIdDatums);

			List	   *upperBoundSerializedDatums = SerializeDataFileColumnBounds(leafFields,
																				   upperBoundColumnIdDatums,
																				   upperBoundDatums,
																				   &upperBoundColumnTypes);

			Datum		upperBoundsJsonDatum = DataFileColumnBoundsToJsonDatum(upperBoundSerializedDatums,
																			   upperBoundColumnIdDatums,
																			   upperBoundColumnTypes);

			values[0] = CStringGetTextDatum(dataFile->file_path);
			values[1] = DatumGetInt64(manifest->sequence_number);
			values[2] = lowerBoundsJsonDatum;
			values[3] = upperBoundsJsonDatum;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PG_RETURN_VOID();
}


/*
 * pg_lake_serde_value serializes and deserializes any value of iceberg types
 * that are supported by the single value binary serde.
 * See https://iceberg.apache.org/spec/#binary-single-value-serialization
 */
Datum
pg_lake_serde_value(PG_FUNCTION_ARGS)
{
	Datum		datum = PG_GETARG_DATUM(0);
	const char *icebergScalarTypeName = text_to_cstring(PG_GETARG_TEXT_P(1));

	Oid			typoid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	int32		typmod = -1;

	Field	   *field = palloc0(sizeof(Field));

	field->type = FIELD_TYPE_SCALAR;
	field->field.scalar.typeName = pstrdup(icebergScalarTypeName);

	PGType		pgType = MakePGType(typoid, typmod);

	if (pgType.postgresTypeOid == NUMERICOID)
	{
		/* set typmod properly for numeric types */
		int			precision = 0;
		int			scale = 0;

		if (sscanf(icebergScalarTypeName, "decimal(%d,%d)", &precision, &scale) == 2)
		{
			pgType.postgresTypeMod = make_numeric_typmod(precision, scale);
		}
	}

	/* check if iceberg type and pg type matches */
	PGType		pgTypeFromIceberg = IcebergFieldToPostgresType(field);

	if (pgTypeFromIceberg.postgresTypeOid != pgType.postgresTypeOid &&
		!PGTypeRequiresConversionToIcebergString(field, pgType))
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("expected %s type, got %s type",
							   format_type_be(pgTypeFromIceberg.postgresTypeOid),
							   format_type_be(pgType.postgresTypeOid))));
	}

	size_t		binaryLen = 0;

	unsigned char *binaryValue = PGIcebergBinarySerializeBoundValue(datum, field, pgType, &binaryLen);

	if (binaryValue == NULL)
	{
		PG_RETURN_NULL();
	}

	return PGIcebergBinaryDeserialize(binaryValue, binaryLen, field, pgType);
}
