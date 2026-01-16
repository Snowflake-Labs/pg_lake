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
#include "libpq-fe.h"

#include "pg_lake/pgduck/parse_struct.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/map.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/pg_map.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/util/numeric.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "common/string.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* mapping between DuckDB types and PostgreSQL types */
typedef struct DuckDBTypeMap
{
	DuckDBType	duckDBType;
	const char *duckDBTypeName;
	Oid			postgresTypeId;
	Oid			postgresArrayTypeId;
	const char *duckDBTypeAliases;
}			DuckDBTypeMap;


/* prototypes */

/* We are exposing this routine generally, but declared non-static so
 * parse_struct.c can use it too. */
Oid			GetPGTypeForDuckDBTypeNameBuiltin(const char *name, int *typeMod, bool isArray);


/* Support for type name aliases */
#define TYPE_ALIAS_DELIM '#'
#define TYPE_ALIAS_DELIM_STR "#"
#define MAKE_TYPE_ALIAS1(t1) (t1)
#define MAKE_TYPE_ALIAS2(t1,t2) (t1 TYPE_ALIAS_DELIM_STR t2)
#define MAKE_TYPE_ALIAS3(t1,t2,t3) (t1 TYPE_ALIAS_DELIM_STR t2 TYPE_ALIAS_DELIM_STR t3)
#define MAKE_TYPE_ALIAS4(t1,t2,t3,t4) (t1 TYPE_ALIAS_DELIM_STR t2 TYPE_ALIAS_DELIM_STR t3 TYPE_ALIAS_DELIM_STR t4)


/* This routine tests the given type entry against a string to see if it is the
 * canonical name or an alias. */

static bool
IsMatchingTypeNameOrAlias(const char *inputType, DuckDBTypeMap * typeMap)
{
	Assert(inputType != NULL && typeMap != NULL);

	/*
	 * Before we can check our type we need to find the end of the "base"
	 * type; since we have multi-word types ("TIMESTAMP WITH TIME ZONE"),
	 * types that are substrings of each other "TIMESTAMP" vs "TIMESTAMPTZ",
	 * and types that include extra information that we don't care about in
	 * terms of identifications "MAP(INTEGER,VARCHAR)" or "STRUCT(x INTEGER, y
	 * FLOAT)" we will scan the type until we find the first non-word,
	 * non-space character, then back up from any trailing space.
	 */

#define TYPE_NAME_BUFFER_SIZE 100

	char		checkType[TYPE_NAME_BUFFER_SIZE];
	const char *currentCharPointer = inputType;

	/* skip spaces and alpha-numeric */
	while (isspace(*currentCharPointer) || isalnum(*currentCharPointer))
		currentCharPointer++;

	/* now back up across any trailing spaces */
	while (isspace(*currentCharPointer))
		currentCharPointer--;

	/*
	 * We probably shouldn't ever see this, so no ereport, and really anything
	 * longer than the longest type is going to be invalid so our size could
	 * be cut off here.
	 */
	int			typeLen = (currentCharPointer - inputType);

	if (typeLen >= TYPE_NAME_BUFFER_SIZE)
	{
		elog(WARNING, "internal error: input type name exceeded our buffer size"
			 " (harmless, but means you're passing in a type that is too long)");
		return false;
	}

	/* make our local buffer copy */
	memcpy(checkType, inputType, typeLen);
	checkType[typeLen] = '\0';

	/* first we check against the main type name */
	if (strcasecmp(checkType, typeMap->duckDBTypeName) == 0)
		return true;

	/* if it's not the main type, then check against our alias string, if any */

	if (!typeMap->duckDBTypeAliases)
		return false;

	const char *foundTypeLocation = typeMap->duckDBTypeAliases;

	/*
	 * There are a couple cases:
	 *
	 * 1) we match at the beginning and there is only a single type (so we
	 * match the entire string).
	 *
	 * 2) we match somewhere in the string, but don't know if we are at the
	 * start of the word or not.
	 *
	 * 3) we match somewhere in the string, but the first match isn't a full
	 * type.  This would indicate that we need to probably continue searching
	 * the string until we get NULL return from strcasestr().
	 *
	 * We know the end is valid if the next char after this found substring is
	 * the type delimiter or the null byte, so if it's not we can return
	 * false.
	 *
	 * We know we're at a valid beginning char if our found character is
	 * either the start of the string or the previous character is our
	 * delimiter character.
	 */

	/*
	 * pick up our search at the next location found, or the start of the
	 * string
	 */
	while ((foundTypeLocation = strcasestr(foundTypeLocation, checkType)))
	{
		/*
		 * We know the string exists, but we need to consider that we're a
		 * substring of the given type not the full type.
		 */

		int			checkTypeLength = strlen(checkType);

		/*
		 * Check the end character first, since those are common to both
		 * cases.
		 */
		char		nextChar = foundTypeLocation[checkTypeLength];

		/*
		 * if this is true then we are a substring, not the full type, so next
		 * loop
		 */
		if (nextChar != '\0' && nextChar != TYPE_ALIAS_DELIM)
		{
			foundTypeLocation += checkTypeLength + 1;
			continue;
		}

		/*
		 * at this point, the end of the string is a valid ending character,
		 * so we need to check the beginning of the string.  If it's a
		 * boundary then we're done.
		 */

		if (foundTypeLocation == typeMap->duckDBTypeAliases ||
			foundTypeLocation[-1] == TYPE_ALIAS_DELIM)
			return true;

		/* otherwise, let's advance our pointer and try again */
		foundTypeLocation += checkTypeLength + 1;
	}

	/*
	 * If we got this far, then we definitely didn't see the string in all its
	 * substring glory.
	 */

	return false;
}

/*
 * Should follow DuckDBType order to be able to use it as
 * an index
 */
static DuckDBTypeMap TypeMap[] =
{
	{
		DUCKDB_TYPE_INVALID, "invalid", TEXTOID, TEXTARRAYOID, NULL
	},
	/* bool */
	{
		DUCKDB_TYPE_BOOLEAN, "BOOLEAN", BOOLOID, BOOLARRAYOID, MAKE_TYPE_ALIAS2("BOOL", "LOGICAL")
	},
	/* int8_t */
	{
		DUCKDB_TYPE_TINYINT, "TINYINT", INT2OID, INT2ARRAYOID, MAKE_TYPE_ALIAS1("INT1")
	},
	/* int16_t */
	{
		DUCKDB_TYPE_SMALLINT, "SMALLINT", INT2OID, INT2ARRAYOID, MAKE_TYPE_ALIAS2("INT2", "SHORT")
	},
	/* int32_t */
	{
		DUCKDB_TYPE_INTEGER, "INTEGER", INT4OID, INT4ARRAYOID, MAKE_TYPE_ALIAS3("INT4", "INT", "SIGNED")
	},
	/* int64_t */
	{
		DUCKDB_TYPE_BIGINT, "BIGINT", INT8OID, INT8ARRAYOID, MAKE_TYPE_ALIAS2("INT8", "LONG")
	},
	/* uint8_t */
	{
		DUCKDB_TYPE_UTINYINT, "UTINYINT", INT2OID, INT2ARRAYOID, NULL
	},
	/* uint16_t */
	{
		DUCKDB_TYPE_USMALLINT, "USMALLINT", INT4OID, INT4ARRAYOID, NULL
	},
	/* uint32_t */
	{
		DUCKDB_TYPE_UINTEGER, "UINTEGER", INT8OID, INT8ARRAYOID, NULL
	},
	/* uint64_t */
	{
		DUCKDB_TYPE_UBIGINT, "UBIGINT", INT8OID, INT8ARRAYOID, NULL
	},
	/* float */
	{
		DUCKDB_TYPE_FLOAT, "REAL", FLOAT4OID, FLOAT4ARRAYOID, MAKE_TYPE_ALIAS2("FLOAT", "FLOAT4")
	},
	/* double */
	{
		DUCKDB_TYPE_DOUBLE, "DOUBLE", FLOAT8OID, FLOAT8ARRAYOID, MAKE_TYPE_ALIAS1("FLOAT8")
	},
	/* duckdb_timestamp, in microseconds */
	{
		DUCKDB_TYPE_TIMESTAMP, "TIMESTAMP", TIMESTAMPOID, TIMESTAMPARRAYOID, MAKE_TYPE_ALIAS2("DATETIME", "TIMESTAMP WITHOUT TIME ZONE")
	},
	/* duckdb_date */
	{
		DUCKDB_TYPE_DATE, "DATE", DATEOID, DATEARRAYOID, NULL
	},
	/* duckdb_time */
	{
		DUCKDB_TYPE_TIME, "TIME", TIMEOID, TIMEARRAYOID, MAKE_TYPE_ALIAS1("TIME WITHOUT TIME ZONE")
	},
	/* duckdb_interval */
	{
		DUCKDB_TYPE_INTERVAL, "INTERVAL", INTERVALOID, INTERVALARRAYOID, NULL
	},
	/* duckdb_hugeint */
	{
		DUCKDB_TYPE_HUGEINT, "HUGEINT", TEXTOID, TEXTARRAYOID, NULL
	},

	/*
	 * const char* - technically, these can support a typmod variation like
	 * decimal, but it has no effect so could be read in but not written out,
	 * say
	 */
	{
		DUCKDB_TYPE_VARCHAR, "VARCHAR", TEXTOID, TEXTARRAYOID, MAKE_TYPE_ALIAS4("CHAR", "BPCHAR", "TEXT", "STRING")
	},
	/* duckdb_blob */
	{
		DUCKDB_TYPE_BLOB, "BLOB", BYTEAOID, BYTEAARRAYOID, MAKE_TYPE_ALIAS3("BYTEA", "BINARY", "VARBINARY")
	},
	/* decimal */
	{
		DUCKDB_TYPE_DECIMAL, "DECIMAL", NUMERICOID, NUMERICARRAYOID, MAKE_TYPE_ALIAS1("NUMERIC")
	},
	/* duckdb_timestamp, in seconds */
	{
		DUCKDB_TYPE_TIMESTAMP_S, "TIMESTAMP_S", TIMESTAMPOID, TIMESTAMPARRAYOID, NULL
	},
	/* duckdb_timestamp, in milliseconds */
	{
		DUCKDB_TYPE_TIMESTAMP_MS, "TIMESTAMP_MS", TIMESTAMPOID, TIMESTAMPARRAYOID, NULL
	},
	/* duckdb_timestamp, in nanoseconds */
	{
		DUCKDB_TYPE_TIMESTAMP_NS, "TIMESTAMP_NS", TIMESTAMPOID, TIMESTAMPARRAYOID, NULL
	},
	/* enum type, only useful as logical type */
	{
		DUCKDB_TYPE_ENUM, "ENUM", TEXTOID, TEXTARRAYOID, NULL
	},
	/* list type, only useful as logical type */
	{
		DUCKDB_TYPE_LIST, "LIST", ANYARRAYOID, ANYARRAYOID, NULL
	},
	/* struct type, only useful as logical type */
	{
		DUCKDB_TYPE_STRUCT, "STRUCT", RECORDOID, RECORDARRAYOID, NULL
	},
	/* map type, only useful as logical type */
	{
		DUCKDB_TYPE_MAP, "MAP", RECORDOID, RECORDARRAYOID, NULL
	},
	/* duckdb_hugeint */
	{
		DUCKDB_TYPE_UUID, "UUID", UUIDOID, UUIDARRAYOID, NULL
	},
	/* union type, only useful as logical type */
	{
		DUCKDB_TYPE_UNION, "UNION", TEXTOID, TEXTARRAYOID, NULL
	},
	/* duckdb_bit */
	{
		DUCKDB_TYPE_BIT, "BIT", BITOID, BITARRAYOID, MAKE_TYPE_ALIAS1("BITSTRING")
	},
	/* duckdb_time_tz */
	{
		DUCKDB_TYPE_TIME_TZ, "TIMETZ", TIMETZOID, TIMETZARRAYOID, MAKE_TYPE_ALIAS1("TIME WITH TIME ZONE")
	},
	/* duckdb_timestamp_tz */
	{
		DUCKDB_TYPE_TIMESTAMP_TZ, "TIMESTAMP WITH TIME ZONE", TIMESTAMPTZOID, TIMESTAMPTZARRAYOID, MAKE_TYPE_ALIAS1("TIMESTAMPTZ")
	},
	/* assumes JSON extension is loaded (pgduck_server loads it on start up) */
	{
		DUCKDB_TYPE_JSON, "JSON", JSONBOID, JSONBARRAYOID, NULL
	},

	/*
	 * Queries using duckdb spatial may return GEOMETRY in their describe,
	 * though they are internally represented as blob. In Parquet, they are
	 * stored as blob an explicit conversion (e.g. via ST_GeomFromWKB) is
	 * necessary to interpret Parquet data as geometry.
	 *
	 * OIDs are retrieved dynamically in GetPGTypeForDuckDBTypeNameBuiltin
	 */
	{
		DUCKDB_TYPE_GEOMETRY, "GEOMETRY", InvalidOid, InvalidOid, NULL
	},
	/* last entry */
	{
		DUCKDB_TYPE_INVALID, NULL, InvalidOid, InvalidOid, NULL
	},
};

/*
 * ParseDecimalTypeModFromTypeName parses the type modifier from a decimal type
 * name, if present. Returns -1 if no type modifier is present.
 */
static int
ParseDecimalTypeModFromTypeName(const char *typeName)
{
	int			typeMod = -1;

	/* decimal(10,2) or decimal */
	const char *beginParen = strchr(typeName, '(');

	if (beginParen)
	{
		const char *typeModStr = beginParen;

		int			precision = 0;
		int			scale = 0;

		if (sscanf(typeModStr, "(%d,%d)", &precision, &scale) != 2)
		{
			elog(ERROR, "could not parse decimal type modifier from %s", typeName);
		}

		typeMod = make_numeric_typmod(precision, scale);
	}

	return typeMod;
}

/*
 * GetDuckDBTypeForPGType gets the DuckDB type for a given
 * Postgres type OID.
 */
DuckDBType
GetDuckDBTypeForPGType(PGType postgresType)
{
	Oid			pgTypeId = postgresType.postgresTypeOid;
	Oid			elementType = get_element_type(pgTypeId);
	char		typtype = get_typtype(pgTypeId);

	if (OidIsValid(elementType))
	{
		/*
		 * DuckDB doesn't have JSONB type, so we map it to JSON type. However,
		 * in case of JSONB[], we have not done the mapping yet, so we prevent
		 * pushdown of JSONB[].
		 */
		return elementType == JSONBOID ? DUCKDB_TYPE_INVALID : DUCKDB_TYPE_LIST;
	}

	if (IsMapTypeOid(pgTypeId))
		return DUCKDB_TYPE_MAP;

	if (typtype == TYPTYPE_COMPOSITE)
	{
		return DUCKDB_TYPE_STRUCT;
	}

	switch (pgTypeId)
	{

			/*
			 * For "variadic"/"any" types, the parameter types are not known,
			 * so Postgres uses UNKNOWNOID. DuckDB does not have an equivalent
			 * type, so we map it to VARCHAR, and DuckDB will cast it to the
			 * correct type.
			 */
		case UNKNOWNOID:
			return DUCKDB_TYPE_VARCHAR;

		case BOOLOID:
			return DUCKDB_TYPE_BOOLEAN;

		case BITOID:
			/* not supported in Parquet */
			/* return DUCKDB_TYPE_BIT; */
			return DUCKDB_TYPE_VARCHAR;

		case CHAROID:
			/* no equivalent type */
			return DUCKDB_TYPE_VARCHAR;

		case BYTEAOID:
			return DUCKDB_TYPE_BLOB;

		case DATEOID:
			return DUCKDB_TYPE_DATE;

		case FLOAT4OID:
			return DUCKDB_TYPE_FLOAT;

		case FLOAT8OID:
			return DUCKDB_TYPE_DOUBLE;

		case INT2OID:
			return DUCKDB_TYPE_SMALLINT;

		case INT4OID:
			return DUCKDB_TYPE_INTEGER;

		case INT8OID:
			return DUCKDB_TYPE_BIGINT;

		case INTERVALOID:
			return DUCKDB_TYPE_INTERVAL;

		case NUMERICOID:
			return DUCKDB_TYPE_DECIMAL;

		case OIDOID:
			return DUCKDB_TYPE_UINTEGER;

		case TIMESTAMPOID:
			return DUCKDB_TYPE_TIMESTAMP;

		case TIMESTAMPTZOID:
			return DUCKDB_TYPE_TIMESTAMP_TZ;

		case TIMETZOID:
			return DUCKDB_TYPE_TIME_TZ;

		case TIMEOID:
			return DUCKDB_TYPE_TIME;

		case UUIDOID:
			return DUCKDB_TYPE_UUID;

		case JSONOID:
		case JSONBOID:
			return DUCKDB_TYPE_JSON;

		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case NAMEOID:
			return DUCKDB_TYPE_VARCHAR;

		default:
			if (IsGeometryType(postgresType))
				return DUCKDB_TYPE_GEOMETRY;

			return DUCKDB_TYPE_INVALID;
	}
}

/*
 * This function differs from others by returning full type names for composite
 * types or array types; it assists in mapping from Postgres types to the
 * underlying duckdb type strings, for instance for defining the "columns={}"
 * struct in the CSV imports.
 */
const char *
GetFullDuckDBTypeNameForPGType(PGType postgresType)
{
	DuckDBType	myType = GetDuckDBTypeForPGType(postgresType);

	if (myType == DUCKDB_TYPE_MAP)
	{
		return GetDuckDBMapDefinitionForPGType(postgresType.postgresTypeOid);
	}

	if (myType == DUCKDB_TYPE_LIST)
	{
		/* get the element type and return [] after */
		Oid			elementType = get_element_type(postgresType.postgresTypeOid);

		return psprintf("%s[]", GetFullDuckDBTypeNameForPGType(MakePGTypeOid(elementType)));
	}

	if (myType == DUCKDB_TYPE_STRUCT)
	{
		return GetDuckDBStructDefinitionForPGType(postgresType.postgresTypeOid);
	}

	if (myType == DUCKDB_TYPE_INVALID)
		myType = DUCKDB_TYPE_VARCHAR;

	const char *typeName = GetDuckDBTypeName(myType);

	if (myType == DUCKDB_TYPE_DECIMAL && postgresType.postgresTypeMod != -1)
	{
		StringInfo	typeNameWithMod = makeStringInfo();

		appendStringInfo(typeNameWithMod, "%s(%d,%d)", typeName,
						 numeric_typmod_precision(postgresType.postgresTypeMod),
						 numeric_typmod_scale(postgresType.postgresTypeMod));

		return typeNameWithMod->data;
	}

	return typeName;
}

/*
 * GetDuckDBTypeName returns the DuckDB type name for the
 * given DuckDB type.
 */
const char *
GetDuckDBTypeName(DuckDBType duckType)
{
	return TypeMap[duckType].duckDBTypeName;
}


/*
 * GetOrCreatePGTypeForDuckDBTypeName returns the PostgreSQL type ID that we
 * map the given DuckDB type to, and sets the typeModifier, if any.
 */
Oid
GetOrCreatePGTypeForDuckDBTypeName(const char *name, int *typeMod)
{
	*typeMod = -1;

	if (IsStructType(name))
	{
		/*
		 * We are going to create a Postgres composite type for the STRUCT
		 * type. We skip creating a new type if it already exists.
		 */
		return GetOrCreatePGStructType(name);
	}
	else if (IsMapType(name))
	{
		/*
		 * We are going to create a Postgres composite type for the MAP type.
		 * We skip creating a new type if it already exists.
		 */
		return GetOrCreatePGMapType(name);
	}
	else
		return GetPGTypeForDuckDBTypeNameBuiltin(name, typeMod, IsArrayType(name));

}

/*
 * GetPGTypeForDuckDBTypeNameBuiltin performs a simple lookup of builtin types only;
 * this ensures that we do not fall into the full STRUCT parsing for nested
 * STRUCT types.
 */
Oid
GetPGTypeForDuckDBTypeNameBuiltin(const char *name, int *typeMod, bool isArray)
{
	for (int typeIndex = 1; TypeMap[typeIndex].duckDBTypeName != NULL; typeIndex++)
	{
		DuckDBTypeMap *typeMapEntry = &TypeMap[typeIndex];

		if (IsMatchingTypeNameOrAlias(name, typeMapEntry))
		{
			/* geometry type is not built-in, need to get current OID */
			if (typeMapEntry->duckDBType == DUCKDB_TYPE_GEOMETRY)
				return isArray ? GeometryArrayTypeId() : GeometryTypeId();
			else
			{
				/* TODO: typemod, compare remainder */
				if (typeMapEntry->duckDBType == DUCKDB_TYPE_DECIMAL)
				{
					*typeMod = ParseDecimalTypeModFromTypeName(name);
				}

				return isArray ? typeMapEntry->postgresArrayTypeId : typeMapEntry->postgresTypeId;
			}
		}
	}

	return InvalidOid;
}


/*
 * GetDuckDBTypeByName returns the DuckDB type for a given DuckDB type name,
 * without type modifiers.
 */
DuckDBType
GetDuckDBTypeByName(const char *name)
{
	for (int typeIndex = 1; TypeMap[typeIndex].duckDBTypeName != NULL; typeIndex++)
	{
		DuckDBTypeMap *typeMapEntry = &TypeMap[typeIndex];

		if (IsMatchingTypeNameOrAlias(name, typeMapEntry))
		{
			return typeMapEntry->duckDBType;
		}
	}

	return DUCKDB_TYPE_INVALID;
}

/* Return whether the given postgres typeid looks like a map type */
bool
IsMapTypeOid(Oid typeId)
{
	HeapTuple	tp;
	Form_pg_type typtup;
	bool		isMapType = false;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeId));
	if (!HeapTupleIsValid(tp))
		return false;
	typtup = (Form_pg_type) GETSTRUCT(tp);

	/*
	 * map types live in a specific schema and all have a name pattern of
	 * `key_<type>_val_<type>`; basic prefix check first, then validate
	 * namespace name
	 */
#define MAP_TYPE_PREFIX "key_"

	if (strncmp(NameStr(typtup->typname), MAP_TYPE_PREFIX, strlen(MAP_TYPE_PREFIX)) == 0)
	{
		isMapType = strcmp(get_namespace_name(typtup->typnamespace), MAP_TYPES_SCHEMA) == 0;
	}

	ReleaseSysCache(tp);
	return isMapType;
}


/* Helper to test for leading type strings followed by either ' ' or '(' */
static bool
CheckForComplexTypePrefix(const char *typeName, const char *typePrefix)
{
	if (!typeName || strlen(typeName) < strlen(typePrefix))
		return false;

	char		nextChar = typeName[strlen(typePrefix)];

	return strncasecmp(typeName, typePrefix, strlen(typePrefix)) == 0 && \
		(nextChar == ' ' || nextChar == '(');
}


/*
 * IsMapType looks at the DuckDB type name and returns whether it is a valid map
 * type.
 */
bool
IsMapType(const char *typeName)
{
	return CheckForComplexTypePrefix(typeName, DUCKDB_MAP_TYPE_PREFIX);
}


/*
 * IsStructType looks at the DuckDB type name and returns whether it is a valid struct
 * type.
 */
bool
IsStructType(const char *typeName)
{
	return CheckForComplexTypePrefix(typeName, DUCKDB_STRUCT_TYPE_PREFIX);
}


/*
* IsArrayType looks at the DuckDB type name and returns whether it is an array
* type.
*/
bool
IsArrayType(const char *typeName)
{
	return pg_str_endswith(typeName, "[]");
}


/*
* GetAttributePGType - get the type of a column in a relation
*/
PGType
GetAttributePGType(Oid relationId, AttrNumber attrNo)
{
	Oid			typeOid;
	int32		typeMod;
	Oid			collationId;

	get_atttypetypmodcoll(relationId, attrNo, &typeOid, &typeMod, &collationId);

	return MakePGType(typeOid, typeMod);
}
