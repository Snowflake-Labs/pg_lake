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

#include "catalog/namespace.h"
#include "pg_lake/extensions/pg_map.h"
#include "catalog/pg_namespace.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/type.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


char	   *ParseDuckDBFieldType(char **sourceString, bool *isArray);

static Oid	GetMapCreateFunctionOid(void);
static Oid	PgMapSchemaOwner(void);

/*
 * GetOrCreatePGMapType takes a DuckDB map type name and decomposes it,
 * until we have all known types, and then creates the underlying pieces as
 * necessary.
 */
Oid
GetOrCreatePGMapType(const char *inputName)
{
	char	   *name = (char *) inputName;

	if (!IsMapType(name))
		ereport(ERROR, (errmsg("must be called with a valid MAP type name"),
						errcode(ERRCODE_INVALID_PARAMETER_VALUE)));


	/*
	 * we need to parse our the types; strings are of the form MAP(keytype,
	 * valtype).
	 */

	/* above assert checks that we are pointing at a MAP string */
	name += 3;

	while (isspace(*name))
		name++;

	if (*name != '(')
	{
		ereport(ERROR, (errmsg("missing expected '(' in MAP type definition"),
						errcode(ERRCODE_SYNTAX_ERROR)));
		return InvalidOid;
	}

	/* skip char as well as whitespace */
	name++;
	while (isspace(*name))
		name++;

	bool		isArray = false;
	char	   *keyType = ParseDuckDBFieldType(&name, &isArray);

	if (!keyType)
	{
		ereport(ERROR, (errmsg("couldn't parse key field type in MAP type definition"),
						errcode(ERRCODE_SYNTAX_ERROR)));
		return InvalidOid;
	}

	if (isArray)
	{
		ereport(ERROR, (errmsg("unsupported array type as key type in MAP definition"),
						errcode(ERRCODE_SYNTAX_ERROR)));
		return InvalidOid;
	}

	/* skip whitespace */
	while (isspace(*name))
		name++;

	/* skip comma */
	if (*name != ',')
	{
		ereport(ERROR, (errmsg("missing expected ',' in MAP type definition"),
						errcode(ERRCODE_SYNTAX_ERROR)));
		return InvalidOid;
	}
	name++;

	/* skip whitespace */
	while (isspace(*name))
		name++;

	/* parse the value type */
	char	   *valType = ParseDuckDBFieldType(&name, &isArray);

	if (!valType)
	{
		ereport(ERROR, (errmsg("couldn't parse value field type in MAP type definition"),
						errcode(ERRCODE_SYNTAX_ERROR)));
		return InvalidOid;
	}

	/* skip whitespace */
	while (isspace(*name))
		name++;

	/* skip closing paren */
	if (*name != ')')
	{
		ereport(ERROR, (errmsg("missing expected closing ')' in MAP type definition"),
						errcode(ERRCODE_SYNTAX_ERROR)));
		return InvalidOid;
	}

	ereport(DEBUG1, (errmsg("deconstructing MAP type with key type: '%s' and value type: '%s'", keyType, valType)));

	/*
	 * At this point we have the key and value types as strings, so we need to
	 * turn them into type oids, which we can then use to create the
	 * underlying map types.
	 */

	int			typeMod = -1;
	Oid			keyOid = GetOrCreatePGTypeForDuckDBTypeName(keyType, &typeMod);
	Oid			valOid = GetOrCreatePGTypeForDuckDBTypeName(valType, &typeMod);

	if (!OidIsValid(keyOid) || !OidIsValid(valOid))
	{
		ereport(ERROR, (errmsg("could not resolve key or value MAP type as postgres type"),
						errcode(ERRCODE_INTERNAL_ERROR)));
		return InvalidOid;
	}

	/*
	 * If we originally had a value array, convert the value type oid to the
	 * corresponding array type.  We do this only after we've validated
	 * everything else and have the Postgres Oid in hand.
	 */

	if (isArray)
		valOid = get_array_type(valOid);

	/*
	 * Now that we have Oids, we can run our map.create() function; we assume
	 * this is idempotent so aren't checking for existing map types; that's
	 * the job of the "pg_map" extension.
	 */

	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(PgMapSchemaOwner(), SECURITY_LOCAL_USERID_CHANGE);

	Oid			mapCreateFunctionOid = GetMapCreateFunctionOid();

	/*
	 * We need to pass in NULL for last argument, so can't use
	 * OidFunctionCall3() directly; instead setup and call FunctionCallInvoke
	 * directly.
	 */

	FmgrInfo	flinfo;

	fmgr_info(mapCreateFunctionOid, &flinfo);
	LOCAL_FCINFO(fcinfo, 3);

	InitFunctionCallInfoData(*fcinfo, &flinfo, 3, InvalidOid, NULL, NULL);

	fcinfo->args[0].value = ObjectIdGetDatum(keyOid);
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = ObjectIdGetDatum(valOid);
	fcinfo->args[1].isnull = false;
	fcinfo->args[2].value = (Datum) 0;
	fcinfo->args[2].isnull = true;

	Datum		createMapTypeResult = FunctionCallInvoke(fcinfo);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return DatumGetObjectId(createMapTypeResult);
}


/* This helper will identify the creation function for maps */
static Oid
GetMapCreateFunctionOid()
{
	const Oid	argTypes[3] = {REGTYPEOID, REGTYPEOID, TEXTOID};

	/* could probably cache this */
	Oid			mapCreateFunctionOid = LookupFuncName(list_make2(makeString(MAP_TYPES_SCHEMA), makeString("create")), 3, argTypes, true);

	if (!OidIsValid(mapCreateFunctionOid))
	{
		ereport(ERROR, (errmsg("could not find internal map creation function"),
						errhint("is the pg_map extension installed?"),
						errcode(ERRCODE_INTERNAL_ERROR)));
		return InvalidOid;
	}

	return mapCreateFunctionOid;
}


/*
 * GetDuckDBMapDefinitionForPGType returns a type definition string for a DuckDB map type.
 */
char *
GetDuckDBMapDefinitionForPGType(Oid postgresTypeId, CopyDataFormat format)
{
	Assert(IsMapTypeOid(postgresTypeId));

	/* Dereference the domain to get the underlying array type */
	postgresTypeId = getBaseType(postgresTypeId);

	/* Find the underlying element type from the array type */
	postgresTypeId = get_element_type(postgresTypeId);
	Assert(postgresTypeId != InvalidOid);

	/* Now retrieve the underlying pair type */
	TypeCacheEntry *typEntry = lookup_type_cache(postgresTypeId, TYPECACHE_TUPDESC);

	/*
	 * If for some reason we aren't able to find the type in the typecache
	 * it's an error.
	 */

	if (!typEntry)
		return NULL;

	TupleDesc	tupDesc = typEntry->tupDesc;

	if (!tupDesc)
		return NULL;

	/*
	 * A map type is composed of an array of keys and an array of values, so a
	 * 2-column composite type.
	 */

	Assert(tupDesc->natts == 2);

	Form_pg_attribute keysAttribute = TupleDescAttr(tupDesc, 0);
	Form_pg_attribute valuesAttribute = TupleDescAttr(tupDesc, 1);

	Assert(strcmp(NameStr(keysAttribute->attname), "key") == 0);
	Assert(strcmp(NameStr(valuesAttribute->attname), "val") == 0);

	/* We need to use the element type of the attribute for our lookups */
	return psprintf("MAP(%s,%s)",
					GetFullDuckDBTypeNameForPGType(MakePGType(keysAttribute->atttypid, keysAttribute->atttypmod),
												   format),
					GetFullDuckDBTypeNameForPGType(MakePGType(valuesAttribute->atttypid, valuesAttribute->atttypmod),
												   format)
		);
}


/*
 * GetMapKeyType returns the key type of the given map type.
 */
PGType
GetMapKeyType(Oid mapOid)
{
	/*
	 * mapOid is a domain over an array of key-value tuples. We need to
	 * extract key-value tuple type info.
	 */

	Assert(type_is_array_domain(mapOid));

	Oid			mapPairOid = get_base_element_type(mapOid);

	TypeCacheEntry *tupleLookup = lookup_type_cache(mapPairOid, TYPECACHE_TUPDESC);
	TupleDesc	tupDesc = tupleLookup->tupDesc;

	if (tupDesc->natts != 2)
	{
		ereport(ERROR, (errmsg("input type must have exactly 2 attributes")));
	}

	Form_pg_attribute keyAttribute = TupleDescAttr(tupDesc, 0);

	return MakePGType(keyAttribute->atttypid, keyAttribute->atttypmod);
}


/*
 * GetMapValueType returns the value type of the given map type.
 */
PGType
GetMapValueType(Oid mapOid)
{
	/*
	 * mapOid is a domain over an array of key-value tuples. We need to
	 * extract key-value tuple type info.
	 */

	Assert(type_is_array_domain(mapOid));

	Oid			mapPairOid = get_base_element_type(mapOid);

	TypeCacheEntry *tupleLookup = lookup_type_cache(mapPairOid, TYPECACHE_TUPDESC);
	TupleDesc	tupDesc = tupleLookup->tupDesc;

	if (tupDesc->natts != 2)
	{
		ereport(ERROR, (errmsg("input type must have exactly 2 attributes")));
	}

	Form_pg_attribute valAttribute = TupleDescAttr(tupDesc, 1);

	return MakePGType(valAttribute->atttypid, valAttribute->atttypmod);
}


/*
 * MapTypeSchemaOwner returns the OID of the map_type schema
 * owner.
 */
static Oid
PgMapSchemaOwner(void)
{
	const char *schemaName = "map_type";
	HeapTuple	schemaTuple = SearchSysCache1(NAMESPACENAME, CStringGetDatum(schemaName));

	if (!HeapTupleIsValid(schemaTuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", schemaName)));

	Form_pg_namespace schemaForm = (Form_pg_namespace) GETSTRUCT(schemaTuple);
	Oid			ownerId = schemaForm->nspowner;

	ReleaseSysCache(schemaTuple);

	return ownerId;
}
