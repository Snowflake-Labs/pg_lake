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
#include "catalog/pg_type_d.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/pgduck/array_conversion.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/map_conversion.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/struct_conversion.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"


static char *ByteAOutForPGDuck(Datum value);


/*
 * Serialize a Datum in a PGDuck-compatible way; a central hook for
 * special-purpose conversion of PG datatypes in preparation for PGDuck.
 *
 * By default, we call the supplied OutputFunction as provided, however we want
 * the ability to override this serialization on a type-by-type basis, in
 * particular for arrays and records, which get special handling to convert to
 * DuckDB-compatible text format.
 */
char *
PGDuckSerialize(FmgrInfo *flinfo, Oid columnType, Datum value,
				CopyDataFormat format)
{
	if (flinfo->fn_oid == ARRAY_OUT_OID)
	{
		/* maps are a type of array */
		if (IsMapTypeOid(columnType))
			return MapOutForPGDuck(value);

		/*
		 * For Iceberg, interval arrays need struct serialization.
		 */
		if (get_element_type(columnType) == INTERVALOID &&
			format == DATA_FORMAT_ICEBERG)
			return IntervalArrayOutForPGDuck(value);

		return ArrayOutForPGDuck(DatumGetArrayTypeP(value));
	}

	if (flinfo->fn_oid == RECORD_OUT_OID)
		return StructOutForPGDuck(value, format);

	/*
	 * For Iceberg, intervals are serialized as struct(months, days, microseconds).
	 */
	if (columnType == INTERVALOID && format == DATA_FORMAT_ICEBERG)
		return IntervalOutForPGDuck(value);

	if (columnType == BYTEAOID)
		return ByteAOutForPGDuck(value);

	if (IsGeometryOutFunctionId(flinfo->fn_oid))
	{
		/*
		 * Postgis emits HEX WKB by default, which DuckDB does not accept.
		 * Hence, we emit as WKT.
		 */
		Datum		geomAsText = OidFunctionCall1(ST_AsTextFunctionId(), value);

		return TextDatumGetCString(geomAsText);
	}

	return OutputFunctionCall(flinfo, value);
}


/*
 * IsPGDuckSerializeRequired returns whether the given type requires PGDuckSerialize
 * if passed to DuckDB.
 */
bool
IsPGDuckSerializeRequired(PGType postgresType)
{
	Oid			typeId = postgresType.postgresTypeOid;

	if (typeId == BYTEAOID)
		return true;

	/* also covers map */
	if (type_is_array(typeId))
		return true;

	if (get_typtype(typeId) == TYPTYPE_COMPOSITE)
		return true;

	if (IsGeometryType(postgresType))
		return true;

	return false;
}

char *
ByteAOutForPGDuck(Datum value)
{
	bytea	   *bytes = DatumGetByteaP(value);

	Size		arrayLength = VARSIZE_ANY_EXHDR(bytes);

	/* output is 4x number of input bytes plus terminator */
	char	   *outputBuffer = palloc(arrayLength * 4 + 1);
	char	   *currentPointer = outputBuffer;

	char	   *hexLookup = "0123456789abcdef";

	for (Size byteIndex = 0; byteIndex < arrayLength; byteIndex++)
	{
		char		currentByte = bytes->vl_dat[byteIndex];

		*currentPointer++ = '\\';
		*currentPointer++ = 'x';
		*currentPointer++ = hexLookup[(currentByte >> 4) & 0xF];	/* high nibble */
		*currentPointer++ = hexLookup[currentByte & 0xF];	/* low nibble */
	}

	*currentPointer++ = '\0';

	return outputBuffer;
}

/*
 * IntervalOutForPGDuck serializes a PostgreSQL interval as a DuckDB struct:
 * {'months': M, 'days': D, 'microseconds': U}
 */
char *
IntervalOutForPGDuck(Datum value)
{
	Interval   *interval = DatumGetIntervalP(value);

#if PG_VERSION_NUM >= 170000
	if (INTERVAL_NOT_FINITE(interval))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("+-Infinity intervals are not allowed in iceberg tables"),
				 errhint("Delete or replace +-Infinity values.")));
#endif

	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "{'months': %d, 'days': %d, 'microseconds': " INT64_FORMAT "}",
					 interval->month,
					 interval->day,
					 interval->time);

	return buf.data;
}


/*
 * IntervalArrayOutForPGDuck serializes a PostgreSQL interval[] as a DuckDB
 * list of structs: [{'months': M, 'days': D, 'microseconds': U}, ...]
 *
 * This is used for Iceberg where intervals are stored as structs.
 */
char *
IntervalArrayOutForPGDuck(Datum value)
{
	ArrayType  *array = DatumGetArrayTypeP(value);
	int			nitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');

	if (nitems > 0)
	{
		ArrayMetaState *my_extra = palloc0(sizeof(ArrayMetaState));

		get_type_io_data(INTERVALOID, IOFunc_output,
						 &my_extra->typlen, &my_extra->typbyval,
						 &my_extra->typalign, &my_extra->typdelim,
						 &my_extra->typioparam, &my_extra->typiofunc);
		my_extra->element_type = INTERVALOID;

		ArrayIterator iter = array_create_iterator(array, 0, my_extra);
		Datum		itemvalue;
		bool		isnull;
		bool		first = true;

		while (array_iterate(iter, &itemvalue, &isnull))
		{
			if (!first)
				appendStringInfoString(&buf, ", ");
			first = false;

			if (isnull)
				appendStringInfoString(&buf, "NULL");
			else
				appendStringInfoString(&buf, IntervalOutForPGDuck(itemvalue));
		}

		array_free_iterator(iter);
	}

	appendStringInfoChar(&buf, ']');

	return buf.data;
}


/* Helper to see if we are a "container" type oid */
bool
IsContainerType(Oid typeId)
{
	/* also covers map */
	if (type_is_array(typeId))
		return true;

	if (get_typtype(typeId) == TYPTYPE_COMPOSITE)
		return true;

	return false;
}
