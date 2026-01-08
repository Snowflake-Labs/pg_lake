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
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_json_serde.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/util/spi_helpers.h"

#include "access/tupdesc.h"
#include "nodes/makefuncs.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

static const char *PGScalarIcebergJsonSerialize(Datum scalarDatum, Field * field, PGType pgType);
static const char *PGArrayIcebergJsonSerialize(Datum arrayDatum, Field * field, PGType pgType);
static const char *PGStructIcebergJsonSerialize(Datum structDatum, Field * field, PGType pgType);
static const char *PGMapIcebergJsonSerialize(Datum mapDatum, Field * field, PGType pgType);
static Datum PGScalarIcebergJsonDeserialize(const char *scalarJson, Field * field, PGType pgType);
static Datum PGArrayIcebergJsonDeserialize(const char *arrayJson, Field * field, PGType pgType);
static Datum PGStructIcebergJsonDeserialize(const char *structJson, Field * field, PGType pgType);
static Datum PGMapIcebergJsonDeserialize(const char *mapJson, Field * field, PGType pgType);

/*
 * PGIcebergJsonSerialize converts PG datum to json serialized string according to
 * Iceberg spec.
 * https://iceberg.apache.org/spec/#json-single-value-serialization
 */
const char *
PGIcebergJsonSerialize(Datum datum, Field * field, PGType pgType, bool *isNull)
{
	if (*isNull)
	{
		return "null";
	}

	if (field->type == FIELD_TYPE_STRUCT)
	{
		return PGStructIcebergJsonSerialize(datum, field, pgType);
	}
	else if (field->type == FIELD_TYPE_LIST)
	{
		return PGArrayIcebergJsonSerialize(datum, field, pgType);
	}
	else if (field->type == FIELD_TYPE_MAP)
	{
		return PGMapIcebergJsonSerialize(datum, field, pgType);
	}
	else
	{
		Assert(field->type == FIELD_TYPE_SCALAR);

		if (PGTypeRequiresConversionToIcebergString(field, pgType))
		{
			Oid			typoutput;
			bool		typIsVarlena;

			getTypeOutputInfo(pgType.postgresTypeOid, &typoutput, &typIsVarlena);
			const char *datumAsString = OidOutputFunctionCall(typoutput, datum);

			datum = CStringGetTextDatum(datumAsString);

			pgType.postgresTypeOid = TEXTOID;
			pgType.postgresTypeMod = -1;
		}

		return PGScalarIcebergJsonSerialize(datum, field, pgType);
	}
}

/*
 * PGScalarIcebergJsonSerialize converts scalar datum to json serialized string according to
 * Iceberg spec. We use Postgres' `to_json` function to serialize scalar values since it
 * serializes scalar values in the same way as Iceberg spec.
 */
static const char *
PGScalarIcebergJsonSerialize(Datum scalarDatum, Field * field, PGType pgType)
{
	int16		typLen;
	bool		typByVal;

	get_typlenbyval(pgType.postgresTypeOid, &typLen, &typByVal);

	/*
	 * we need to build funcExpr since "to_json" calls get_fn_expr_argtype
	 * because of its "any" type argument
	 */
	Const	   *constNode = makeConst(pgType.postgresTypeOid, pgType.postgresTypeMod,
									  InvalidOid, typLen, scalarDatum, false, typByVal);

	FmgrInfo	toJsonFmgrInfo;

	fmgr_info(F_TO_JSON, &toJsonFmgrInfo);

	toJsonFmgrInfo.fn_expr = (Node *) makeFuncExpr(F_TO_JSON, JSONOID, list_make1(constNode),
												   InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);

	LOCAL_FCINFO(fcinfo, 1);

	InitFunctionCallInfoData(*fcinfo, &toJsonFmgrInfo, 1, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = scalarDatum;
	fcinfo->args[0].isnull = false;

	Datum		jsonDatum = FunctionCallInvoke(fcinfo);

	/* json datum is represented as text */
	const char *jsonString = TextDatumGetCString(jsonDatum);

	if (pgType.postgresTypeOid == BYTEAOID)
	{
		/*
		 * strip '\\x' prefix from binary value (e.g. '"\x00125a"') since
		 * Iceberg spec does not have it
		 */
		Assert(strncmp(jsonString, "\"\\\\x", 4) == 0);
		jsonString = psprintf("\"%s", jsonString + 4);
	}

	return jsonString;
}

/*
 * PGArrayIcebergJsonSerialize converts array datum to json serialized string according to
 * Iceberg spec.
 */
static const char *
PGArrayIcebergJsonSerialize(Datum arrayDatum, Field * field, PGType pgType)
{
	ArrayType  *array = DatumGetArrayTypeP(arrayDatum);

	Oid			elementOid = get_element_type(pgType.postgresTypeOid);

	int16		elemTypLen;
	bool		elemTypByVal;
	char		elemTypAlign;

	get_typlenbyvalalign(elementOid,
						 &elemTypLen, &elemTypByVal, &elemTypAlign);

	PGType		elementPGType = MakePGType(get_element_type(pgType.postgresTypeOid), pgType.postgresTypeMod);

	Field	   *elementField = field->field.list.element;

	/* build json string for array by iterating each element */
	StringInfo	jsonString = makeStringInfo();

	appendStringInfoChar(jsonString, '[');

	int			arrayLength = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	Datum	   *elementDatums = palloc0(sizeof(Datum) * arrayLength);
	bool	   *elementNulls = palloc0(sizeof(bool) * arrayLength);

	deconstruct_array(array, elementOid, elemTypLen, elemTypByVal, elemTypAlign, &elementDatums, &elementNulls, &arrayLength);

	for (int elementIndex = 0; elementIndex < arrayLength; elementIndex++)
	{
		bool		isNull = elementNulls[elementIndex];
		Datum		elementDatum = elementDatums[elementIndex];

		const char *elementValue = PGIcebergJsonSerialize(elementDatum, elementField, elementPGType, &isNull);

		appendStringInfoString(jsonString, elementValue);

		if (elementIndex < arrayLength - 1)
		{
			appendStringInfo(jsonString, ", ");
		}
	}

	appendStringInfoChar(jsonString, ']');

	return jsonString->data;
}

/*
 * PGStructIcebergJsonSerialize converts struct datum to json serialized string according to
 * Iceberg spec. Keys are field ids and values are serialized values of the fields.
 *
 * e.g. {"1": <val1>, "2": <val2>, ...}
 */
static const char *
PGStructIcebergJsonSerialize(Datum structDatum, Field * field, PGType pgType)
{
	TupleDesc	tupleDesc = lookup_rowtype_tupdesc(pgType.postgresTypeOid, pgType.postgresTypeMod);

	HeapTupleHeader tuple = DatumGetHeapTupleHeader(structDatum);

	int			tupleFieldCount = tupleDesc->natts;

	/* build json string for struct by iterating each field */
	StringInfo	jsonString = makeStringInfo();

	appendStringInfoChar(jsonString, '{');

	for (int fieldIndex = 0; fieldIndex < tupleFieldCount; fieldIndex++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, fieldIndex);

		FieldStructElement *structElementField = &field->field.structType.fields[fieldIndex];

		PGType		attrPGType = MakePGType(attr->atttypid, attr->atttypmod);

		bool		isNull = false;
		Datum		attrDatum = GetAttributeByNum(tuple, fieldIndex + 1, &isNull);
		const char *attrJsonString = PGIcebergJsonSerialize(attrDatum, structElementField->type, attrPGType, &isNull);

		appendStringInfo(jsonString, "\"%d\": %s", structElementField->id, attrJsonString);

		if (fieldIndex < tupleFieldCount - 1)
		{
			appendStringInfo(jsonString, ", ");
		}
	}

	appendStringInfoChar(jsonString, '}');

	ReleaseTupleDesc(tupleDesc);

	return jsonString->data;
}

/*
 * PGMapIcebergJsonSerialize converts map datum to json serialized string according to
 * Iceberg spec. Keys and values are array of serialized keys and values.
 *
 * e.g. {"keys": [<key1>, <key2>, ...], "values": [<val1>, <val2>, ...]}
 */
static const char *
PGMapIcebergJsonSerialize(Datum mapDatum, Field * field, PGType pgType)
{
	/* mapDatum is a domain over an array of a 2-col composite type */

	ArrayType  *pairs = DatumGetArrayTypeP(mapDatum);

	TypeCacheEntry *pairTupleLookup = lookup_type_cache(pairs->elemtype, TYPECACHE_TUPDESC);
	TupleDesc	pairTupleDesc = pairTupleLookup->tupDesc;

	/*
	 * Ensure the composite type has exactly 2 attributes (keys and values
	 * arrays)
	 */
	if (pairTupleDesc->natts != 2)
	{
		ereport(ERROR, (errmsg("input type must have exactly 2 attributes")));
	}

	int			nitems = ArrayGetNItems(ARR_NDIM(pairs), ARR_DIMS(pairs));

	if (nitems == 0)
		return pstrdup("{\"keys\": [], \"values\": []}");

	Form_pg_attribute keyAttribute = TupleDescAttr(pairTupleDesc, 0);
	Form_pg_attribute valAttribute = TupleDescAttr(pairTupleDesc, 1);

	Field	   *keyField = field->field.map.key;
	Field	   *valueField = field->field.map.value;

	PGType		keysPGType = MakePGType(keyAttribute->atttypid, keyAttribute->atttypmod);
	PGType		valuesPGType = MakePGType(valAttribute->atttypid, valAttribute->atttypmod);

	ArrayIterator pairIterator = array_create_iterator(pairs, 0, NULL);

	Datum		pairDatum;
	bool		pairIsNull;

	const char **keyJsonStrings = palloc0(sizeof(const char *) * nitems);
	const char **valueJsonStrings = palloc0(sizeof(const char *) * nitems);

	int			currentElementIndex = 0;

	/* collect keys and values as json strings */
	while (array_iterate(pairIterator, &pairDatum, &pairIsNull))
	{
		if (pairIsNull)
			ereport(ERROR, (errmsg("cannot have NULL for map entry value")));

		/* decompose the pairDatum into the two Datums */
		HeapTupleHeader pairTupleHeader = DatumGetHeapTupleHeader(pairDatum);

		HeapTupleData pairTuple;
		Datum		pairValues[2];
		bool		nulls[2];

		pairTuple.t_len = HeapTupleHeaderGetDatumLength(pairTupleHeader);
		ItemPointerSetInvalid(&(pairTuple.t_self));
		pairTuple.t_tableOid = InvalidOid;
		pairTuple.t_data = pairTupleHeader;

		heap_deform_tuple(&pairTuple, pairTupleDesc, pairValues, nulls);

		const char *serializedKey;
		const char *serializedValue;

		if (nulls[0])
			ereport(ERROR, (errmsg("cannot have NULL for map key entry")));

		serializedKey = PGIcebergJsonSerialize(pairValues[0], keyField, keysPGType, &nulls[0]);

		keyJsonStrings[currentElementIndex] = serializedKey;

		serializedValue = PGIcebergJsonSerialize(pairValues[1], valueField, valuesPGType, &nulls[1]);

		valueJsonStrings[currentElementIndex] = serializedValue;

		currentElementIndex += 1;
	}

	StringInfoData mapJsonString;

	initStringInfo(&mapJsonString);

	appendStringInfoCharMacro(&mapJsonString, '{');

	/* append keys */
	appendStringInfoString(&mapJsonString, "\"keys\": [");

	for (int i = 0; i < nitems; i++)
	{
		appendStringInfoString(&mapJsonString, keyJsonStrings[i]);

		if (i < nitems - 1)
		{
			appendStringInfoString(&mapJsonString, ", ");
		}
	}

	/* append values */
	appendStringInfoString(&mapJsonString, "], \"values\": [");

	for (int i = 0; i < nitems; i++)
	{
		appendStringInfoString(&mapJsonString, valueJsonStrings[i]);

		if (i < nitems - 1)
		{
			appendStringInfoString(&mapJsonString, ", ");
		}
	}

	appendStringInfoString(&mapJsonString, "]}");

	return mapJsonString.data;
}

/*
 * PGIcebergJsonDeserialize converts json serialized Iceberg type to datum.
 */
Datum
PGIcebergJsonDeserialize(const char *jsonString, Field * field, PGType pgType, bool *isNull)
{
	if (strcasecmp(jsonString, "null") == 0)
	{
		*isNull = true;
		return 0;
	}

	if (field->type == FIELD_TYPE_LIST)
	{
		return PGArrayIcebergJsonDeserialize(jsonString, field, pgType);
	}
	else if (field->type == FIELD_TYPE_STRUCT)
	{
		return PGStructIcebergJsonDeserialize(jsonString, field, pgType);
	}
	else if (field->type == FIELD_TYPE_MAP)
	{
		return PGMapIcebergJsonDeserialize(jsonString, field, pgType);
	}
	else
	{
		Assert(field->type == FIELD_TYPE_SCALAR);

		/*
		 * We use "to_json" while serializing scalars which returns an escaped
		 * json string. For deserializing the scalars, we rely on their input
		 * functions so we need to unescape them.
		 */
		jsonString = UnEscapeJson(jsonString);

		return PGScalarIcebergJsonDeserialize(jsonString, field, pgType);
	}
}

/*
 * PGScalarIcebergJsonDeserialize converts json serialized Iceberg scalar to datum.
 * Input functions of scalar types are used to convert json string to scalar datum.
 */
static Datum
PGScalarIcebergJsonDeserialize(const char *scalarJson, Field * field, PGType pgType)
{
	if (pgType.postgresTypeOid == BYTEAOID)
	{
		/*
		 * add '\\x' prefix to hex string since Iceberg spec stores binary
		 * without hex prefix
		 */
		StringInfo	hexString = makeStringInfo();

		appendStringInfo(hexString, "\\x%s", scalarJson);

		scalarJson = hexString->data;
	}

	Oid			typinput;
	Oid			typioparam;

	getTypeInputInfo(pgType.postgresTypeOid, &typinput, &typioparam);

	return OidInputFunctionCall(typinput, pstrdup(scalarJson),
								typioparam, pgType.postgresTypeMod);
}

/*
 * PGArrayIcebergJsonDeserialize converts json serialized Iceberg array to datum.
 */
static Datum
PGArrayIcebergJsonDeserialize(const char *arrayJson, Field * field, PGType pgType)
{
	MemoryContext callerContext = CurrentMemoryContext;

	Oid			elementOid = get_element_type(pgType.postgresTypeOid);

	PGType		elementPGType = MakePGType(elementOid, pgType.postgresTypeMod);

	Field	   *elementField = field->field.list.element;

	const char *query = "SELECT json_array_elements($1)";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, JSONOID, arrayJson, false);

	SPI_START();

	bool		readOnly = true;

	SPI_EXECUTE(query, readOnly);

	int			numRows = SPI_processed;

	MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

	Datum	   *elementDatums = palloc0(sizeof(Datum) * numRows);
	bool	   *elementNulls = palloc0(sizeof(bool) * numRows);

	for (int rowIdx = 0; rowIdx < numRows; rowIdx++)
	{
		bool		isNull = false;
		const char *elementJson = GET_SPI_VALUE(JSONOID, rowIdx, 1, &isNull);

		if (isNull)
		{
			elementNulls[rowIdx] = true;

			continue;
		}

		elementDatums[rowIdx] = PGIcebergJsonDeserialize(elementJson, elementField, elementPGType, &isNull);
		elementNulls[rowIdx] = isNull;
	}

	MemoryContextSwitchTo(spiContext);

	SPI_END();

	int16		elemTypLen;
	bool		elemTypByVal;
	char		elemTypAlign;

	get_typlenbyvalalign(elementOid,
						 &elemTypLen, &elemTypByVal, &elemTypAlign);

	int			ndims = 1;
	int			dims[1] = {numRows};
	int			lbs[1] = {1};

	ArrayType  *array = construct_md_array(elementDatums, elementNulls, ndims, dims, lbs,
										   elementOid, elemTypLen, elemTypByVal,
										   elemTypAlign);

	return PointerGetDatum(array);
}

/*
 * PGStructIcebergJsonDeserialize converts json serialized Iceberg struct to datum.
 * Keys are field ids and values are serialized values of the fields.
 *
 * e.g. {"1": <val1>, "2": <val2>, ...}
 */
static Datum
PGStructIcebergJsonDeserialize(const char *structJson, Field * field, PGType pgType)
{
	MemoryContext callerContext = CurrentMemoryContext;

	TupleDesc	tupleDesc = lookup_rowtype_tupdesc(pgType.postgresTypeOid, pgType.postgresTypeMod);

	const char *jsonEachQuery = "SELECT * from json_each($1)";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, JSONOID, structJson, false);

	SPI_START();

	bool		readOnly = true;

	SPI_EXECUTE(jsonEachQuery, readOnly);

	size_t		fieldsLen = SPI_processed;

	MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

	Datum	   *attrDatums = palloc0(sizeof(Datum) * fieldsLen);
	bool	   *attrNulls = palloc0(sizeof(bool) * fieldsLen);

	for (size_t fieldIdx = 0; fieldIdx < fieldsLen; fieldIdx++)
	{
		bool		isNull = false;

		FieldStructElement *structElementField = &field->field.structType.fields[fieldIdx];

		/* we need only the value (key is unused) to build tuple */
		const char *attrJsonString = GET_SPI_VALUE(JSONOID, fieldIdx, 2, &isNull);

		if (isNull)
		{
			attrNulls[fieldIdx] = true;

			continue;
		}

		/* find the attribute from tuple's descriptor */
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, fieldIdx);

		PGType		attrPGType = MakePGType(attr->atttypid, attr->atttypmod);

		attrDatums[fieldIdx] = PGIcebergJsonDeserialize(attrJsonString, structElementField->type, attrPGType, &isNull);
		attrNulls[fieldIdx] = isNull;
	}

	MemoryContextSwitchTo(spiContext);

	SPI_END();

	HeapTuple	tuple = heap_form_tuple(tupleDesc, attrDatums, attrNulls);

	Datum		tupleDatum = HeapTupleGetDatum(tuple);

	ReleaseTupleDesc(tupleDesc);

	return tupleDatum;
}

/*
 * PGMapIcebergJsonDeserialize converts json serialized Iceberg map to datum.
 * Keys and values are array of serialized keys and values.
 *
 * e.g. {"keys": [<key1>, <key2>, ...], "values": [<val1>, <val2>, ...]}
 */
static Datum
PGMapIcebergJsonDeserialize(const char *mapJson, Field * field, PGType pgType)
{
	MemoryContext callerContext = CurrentMemoryContext;

	Field	   *keyField = field->field.map.key;

	Field	   *valueField = field->field.map.value;

	/* map is a domain over an array of tuple(key, value) pairs */
	Oid			pairOid = get_base_element_type(pgType.postgresTypeOid);
	TupleDesc	pairTupleDesc = lookup_rowtype_tupdesc(pairOid, pgType.postgresTypeMod);

	Form_pg_attribute keyAttribute = TupleDescAttr(pairTupleDesc, 0);

	PGType		keysPGType = MakePGType(keyAttribute->atttypid, keyAttribute->atttypmod);

	Form_pg_attribute valAttribute = TupleDescAttr(pairTupleDesc, 1);

	PGType		valuesPGType = MakePGType(valAttribute->atttypid, valAttribute->atttypmod);

	/* collect keys and values as datums */
	const char *keysJsonQuery = "SELECT json_array_elements($1->'keys'), json_array_elements($1->'values')";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, JSONOID, mapJson, false);

	SPI_START();

	bool		readOnly = true;

	SPI_EXECUTE(keysJsonQuery, readOnly);

	int			numPairs = SPI_processed;

	MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

	Datum	   *keyValuePairDatums = palloc0(sizeof(Datum) * numPairs);
	bool	   *keyValuePairNulls = palloc0(sizeof(bool) * numPairs);

	for (int pairIdx = 0; pairIdx < numPairs; pairIdx++)
	{
		bool		isNullKey = false;

		const char *keyJsonString = GET_SPI_VALUE(JSONOID, pairIdx, 1, &isNullKey);

		if (isNullKey)
		{
			ereport(ERROR, (errmsg("cannot have NULL for map key entry")));
		}

		bool		isNullValue = false;

		const char *valueJsonString = GET_SPI_VALUE(JSONOID, pairIdx, 2, &isNullValue);


		Datum		keyDatum = PGIcebergJsonDeserialize(keyJsonString, keyField, keysPGType, &isNullKey);
		Datum		valueDatum = PGIcebergJsonDeserialize(valueJsonString, valueField, valuesPGType, &isNullValue);

		Datum		pairDatum[2] = {keyDatum, valueDatum};
		bool		pairNulls[2] = {isNullKey, isNullValue};

		HeapTuple	tuple = heap_form_tuple(pairTupleDesc, pairDatum, pairNulls);

		keyValuePairDatums[pairIdx] = HeapTupleGetDatum(tuple);
		keyValuePairNulls[pairIdx] = false;
	}

	MemoryContextSwitchTo(spiContext);

	SPI_END();

	ReleaseTupleDesc(pairTupleDesc);

	int16		pairTypLen;
	bool		pairTypByVal;
	char		pairTypAlign;

	get_typlenbyvalalign(pairOid,
						 &pairTypLen, &pairTypByVal, &pairTypAlign);

	int			ndims = 1;
	int			dims[1] = {numPairs};
	int			lbs[1] = {1};

	ArrayType  *array = construct_md_array(keyValuePairDatums, keyValuePairNulls, ndims, dims, lbs,
										   pairOid, pairTypLen, pairTypByVal, pairTypAlign);

	return PointerGetDatum(array);
}
