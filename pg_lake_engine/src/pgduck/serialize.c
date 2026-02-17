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
static char *IntervalOutForPGDuck(Datum value);


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
PGDuckSerialize(FmgrInfo *flinfo, Oid columnType, Datum value)
{
	if (flinfo->fn_oid == ARRAY_OUT_OID)
	{
		/* maps are a type of array */
		if (IsMapTypeOid(columnType))
			return MapOutForPGDuck(value);

		return ArrayOutForPGDuck(DatumGetArrayTypeP(value));
	}

	if (flinfo->fn_oid == RECORD_OUT_OID)
		return StructOutForPGDuck(value);

	if (columnType == BYTEAOID)
		return ByteAOutForPGDuck(value);

	if (columnType == INTERVALOID)
		return IntervalOutForPGDuck(value);

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

	if (typeId == INTERVALOID)
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
static char *
IntervalOutForPGDuck(Datum value)
{
	Interval   *interval = DatumGetIntervalP(value);
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "{'months': %d, 'days': %d, 'microseconds': " INT64_FORMAT "}",
					 interval->month,
					 interval->day,
					 interval->time);

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
