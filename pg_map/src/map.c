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
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language_d.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/* Check PgSQL version */
#if PG_VERSION_NUM < 100000
#error PostgreSQL 10 or newer is required
#endif

#define ARRNELEMS(x)  ArrayGetNItems(ARR_NDIM(x), ARR_DIMS(x))

#define EXTENSION_NAME "pg_map"
#define EXTENSION_SCHEMA "map_type"

/* postgres 14/15 do not have this routine */
#if PG_VERSION_NUM < 160000
/*
 * Like construct_array(), where elmtype must be a built-in type, and
 * elmlen/elmbyval/elmalign is looked up from hardcoded data.  This is often
 * useful when manipulating arrays from/for system catalogs.
 */
static ArrayType *
construct_array_builtin(Datum *elems, int nelems, Oid elmtype)
{
	int			elmlen;
	bool		elmbyval;
	char		elmalign;

	switch (elmtype)
	{
		case CHAROID:
			elmlen = 1;
			elmbyval = true;
			elmalign = TYPALIGN_CHAR;
			break;

		case CSTRINGOID:
			elmlen = -2;
			elmbyval = false;
			elmalign = TYPALIGN_CHAR;
			break;

		case FLOAT4OID:
			elmlen = sizeof(float4);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case INT2OID:
			elmlen = sizeof(int16);
			elmbyval = true;
			elmalign = TYPALIGN_SHORT;
			break;

		case INT4OID:
			elmlen = sizeof(int32);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case INT8OID:
			elmlen = sizeof(int64);
			elmbyval = FLOAT8PASSBYVAL;
			elmalign = TYPALIGN_DOUBLE;
			break;

		case NAMEOID:
			elmlen = NAMEDATALEN;
			elmbyval = false;
			elmalign = TYPALIGN_CHAR;
			break;

		case OIDOID:
		case REGTYPEOID:
			elmlen = sizeof(Oid);
			elmbyval = true;
			elmalign = TYPALIGN_INT;
			break;

		case TEXTOID:
			elmlen = -1;
			elmbyval = false;
			elmalign = TYPALIGN_INT;
			break;

		case TIDOID:
			elmlen = sizeof(ItemPointerData);
			elmbyval = false;
			elmalign = TYPALIGN_SHORT;
			break;

		default:
			elog(ERROR, "type %u not supported by construct_array_builtin()", elmtype);
			/* keep compiler quiet */
			elmlen = 0;
			elmbyval = false;
			elmalign = 0;
	}

	return construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);
}
#endif

PG_MODULE_MAGIC;


/*
 * The 'map' type is a kind of meta-structure, but we know that it is a domain
 * over an array of composite type with a 'key' and 'val' attribute
 *
 * This function checks that the map type is valid and sends back some
 * information about the types of the keys and vals.
 */
static bool
typcache_is_map_type(TypeCacheEntry *typeCacheEntry, Oid *keytype, Oid *valtype)
{
	Form_pg_attribute attr0,
				attr1;
	TupleDesc	tupleDesc;
	Oid			keyElmType,
				valElmType,
				arrayType;

	/* verify this is a domain type */
	if (typeCacheEntry->typtype != TYPTYPE_DOMAIN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not a domain type", format_type_be(typeCacheEntry->type_id))));
	}

	arrayType = getBaseType(typeCacheEntry->type_id);

	/* verify this is an array type */
	if (!OidIsValid(get_element_type(arrayType)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not an array type", format_type_be(arrayType))));
	}

	/* now check out the composite type */
	typeCacheEntry = lookup_type_cache(get_element_type(arrayType), TYPECACHE_TUPDESC);

	if (typeCacheEntry == NULL ||
		typeCacheEntry->typtype != TYPTYPE_COMPOSITE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not a composite type", format_type_be(typeCacheEntry->type_id))));
	}

	tupleDesc = typeCacheEntry->tupDesc;
	if (tupleDesc->natts != 2)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("argument is not a map type")));
	}

	/* Keys */
	attr0 = TupleDescAttr(tupleDesc, 0);
	/* Values */
	attr1 = TupleDescAttr(tupleDesc, 1);

	if (strcmp(NameStr(attr0->attname), "key") != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("'key' must be first attribute of pair type")));
	}

	if (strcmp(NameStr(attr1->attname), "val") != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("'val' must be second attribute of pair type")));
	}

	keyElmType = attr0->atttypid;
	valElmType = attr1->atttypid;

	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got key element type = %s", __func__, format_type_be(keyElmType))));

	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got val element type = %s", __func__, format_type_be(valElmType))));

	if (keytype)
		*keytype = keyElmType;
	if (valtype)
		*valtype = valElmType;

	return true;
}


/*
 * Lookup the cmp() function for an arbitrary type.
 * We can use this to sort arrays, and to do a bsearch into
 * sorted arrays (though we do not do that yet).
 */
static FmgrInfo *
type_cmp_fmgr(Oid dataTypeOid)
{
	TypeCacheEntry *typCache;

	typCache = lookup_type_cache(dataTypeOid, TYPECACHE_CMP_PROC_FINFO);

	if (!typCache->cmp_proc)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("map member types must have a cmp function defined"),
				 errdetail("member type '%s' does not have a cmp function", format_type_be(dataTypeOid))
				 ));
	}

	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got TYPECACHE_CMP_PROC = %u (%s)",
					__func__,
					typCache->cmp_proc,
					get_func_name(typCache->cmp_proc)
					)));

	return &(typCache->cmp_proc_finfo);
}


/*
 * concise_type_name returns a short-form type name to be used as part of a
 * map type name.
 *
 * Whereas format_type_be tries hard to return a long type name alias, we
 * prefer a short, familiar aliasa
 */
static char *
concise_type_name(Oid typeId)
{
	switch (typeId)
	{
		case BOOLOID:
			return pstrdup("bool");

		case FLOAT4OID:
			return pstrdup("float4");

		case FLOAT8OID:
			return pstrdup("float8");

		case INT4OID:
			return pstrdup("int");

		case TIMEOID:
			return pstrdup("time");

		case TIMETZOID:
			return pstrdup("timetz");

		case TIMESTAMPOID:
			return pstrdup("timestamp");

		case TIMESTAMPTZOID:
			return pstrdup("timestamptz");

		case VARBITOID:
			return pstrdup("varbit");

		case VARCHAROID:
			return pstrdup("varchar");
	}

	return format_type_be(typeId);
}


/* helper for the following use-cases */
static char *
type_name_helper(Oid keyType, const char *keyPrefix, Oid valType, const char *valPrefix)
{
	char	   *typeName,
			   *typeSchemaName;
	Oid			typId,
				typIdSch;
	bool		keyIsArray = (get_array_type(keyType) == InvalidOid);
	bool		valIsArray = (get_array_type(valType) == InvalidOid);
	StringInfoData si;

	initStringInfo(&si);

	/*
	 * This threshold is the length of the longest built-in type name that
	 * we'd support here without exceeding the longest possible type name.
	 */
#define MAX_TYPE_NAME_LENGTH 23

	appendStringInfoString(&si, keyPrefix);

	/* Use Oid as type name for non-built-in types */
	if (keyType < FirstNormalObjectId)
	{
		char	   *keystring = concise_type_name(keyIsArray ? get_element_type(keyType) : keyType);

		if (strlen(keystring) <= MAX_TYPE_NAME_LENGTH)
		{
			/* replace ' ' with '_' */
			for (char *theChar = keystring; *theChar; theChar++)
				if (*theChar == ' ')
					*theChar = '_';

			appendStringInfoString(&si, keystring);
		}
		else
			appendStringInfo(&si, "%u", keyType);
	}
	else
		appendStringInfo(&si, "%u", keyType);

	if (keyIsArray)
		appendStringInfoString(&si, "_array");

	appendStringInfoCharMacro(&si, '_');
	appendStringInfoString(&si, valPrefix);

	/* Use Oid as type name for non-built-in types */
	if (valType < FirstNormalObjectId)
	{
		char	   *valstring = concise_type_name(valIsArray ? get_element_type(valType) : valType);

		if (strlen(valstring) <= MAX_TYPE_NAME_LENGTH)
		{
			/* replace ' ' with '_' */
			for (char *theChar = valstring; *theChar; theChar++)
				if (*theChar == ' ')
					*theChar = '_';

			appendStringInfoString(&si, valstring);
		}
		else
			appendStringInfo(&si, "%u", valType);
	}
	else
		appendStringInfo(&si, "%u", valType);

	if (valIsArray)
		appendStringInfoString(&si, "_array");

	typeName = si.data;
	typeSchemaName = psprintf("%s.%s", EXTENSION_SCHEMA, typeName);

	if (strlen(typeName) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("%s: type name %s is too long", __func__, typeName)));

	typId = TypenameGetTypid(typeName);

	typIdSch = TypenameGetTypid(typeSchemaName);
	pfree(typeSchemaName);

	if (typId || typIdSch)
		elog(ERROR, "map type %s already exists", typeName);

	ereport(DEBUG4,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: using %s as map type name", __func__, typeName)));

	return typeName;
}


/*
 * Return a type name for a composite type for pairs of the given key/value
 * types */
static char *
pair_type_name(Oid keyType, Oid valType)
{
	return type_name_helper(keyType, "pair_", valType, "");
}

/*
 * Create the custom map type that will support a
 * particular key/value type pair.
 * e.g. 'key_text_val_int4'
 */
static char *
map_type_name(Oid keyType, Oid valType)
{
	return type_name_helper(keyType, "key_", valType, "val_");
}


/*
 * Check for existing map type that matches the same arguments; if map type
 * exists by that name but different arguments then we throw an error.  A
 * different approach would be to just make map_create_type() and other creation
 * bits skip if a corresponding type/function/operator exists, but that adds
 * more complexity, since this extension should be the only one creating types
 * in this extension we can assume that if the type doesn't exist then none of
 * the function/operators will either.
 */
static Oid
find_existing_map_type(char *typeName, Oid keyElementType, Oid valElementType)
{
	TypeName   *typenameStruct = makeTypeNameFromNameList(list_make2(makeString(pstrdup(EXTENSION_SCHEMA)),
																	 makeString(typeName)));
	HeapTuple	typeTuple = LookupTypeName(NULL, typenameStruct, NULL, true);
	Oid			mapKeyType,
				mapValType,
				existingTypeId;
	bool		isMap;
	TypeCacheEntry *typeCacheEntry;

	/* no type by this name found */
	if (!typeTuple)
		return InvalidOid;

	existingTypeId = typeTypeId(typeTuple);
	ReleaseSysCache(typeTuple);

	/*
	 * The type itself should be a domain over an array of pair-wise structs;
	 * make sure that is in fact the case.  We do have a type by this name,
	 * but let's check that the args match
	 */
	typeCacheEntry = lookup_type_cache(
									   existingTypeId,
									   TYPECACHE_TUPDESC);

	/*
	 * Validate that we look like a map type and retrieve the existing typids
	 * for keys/values arrays
	 */

	isMap = typcache_is_map_type(typeCacheEntry, &mapKeyType, &mapValType);

	if (!isMap)
	{
		ereport(ERROR, (errmsg("existing type '%s' does not look like a map type",
							   typeName)));
	}

	if (mapKeyType != keyElementType)
	{
		ereport(ERROR, (errmsg("existing map type '%s' has mismatched key types; expected: %s, got: %s",
							   typeName,
							   format_type_be(keyElementType),
							   format_type_be(mapKeyType))));
	}

	if (mapValType != valElementType)
	{
		ereport(ERROR, (errmsg("existing map type '%s' has mismatched value types; expected: %s, got: %s",
							   typeName,
							   format_type_be(valElementType),
							   format_type_be(mapValType))));
	}

	return existingTypeId;
}


/*
 * Create new map type and also register it as a
 * dependency to the extension.
 */
static Oid
map_create_type(char *typeName, Oid keyElementType, Oid valElementType)
{
	/* first we create the composite pair type */
	char	   *keyValPairTypeName = pair_type_name(keyElementType, valElementType);

	int			typeMod = -1;
	ColumnDef  *keyCol = makeColumnDef("key", keyElementType, typeMod, InvalidOid);
	ColumnDef  *valCol = makeColumnDef("val", valElementType, typeMod, InvalidOid);

	List	   *columnDefList = list_make2(keyCol, valCol);
	char	   *nspStr = pstrdup(EXTENSION_SCHEMA);
	RangeVar   *typevar = makeRangeVar(nspStr, keyValPairTypeName, -1);

	ObjectAddress keyValPairTypeAddr = DefineCompositeType(typevar, columnDefList);
	ObjectAddress mapTypeAddr;
	CreateDomainStmt *newDomain;
	TypeName   *arrayOfCompositeTypes;

	CommandCounterIncrement();

	/*
	 * Now that we have a composite type we must create the map type as the
	 * domain over an array of this object.
	 */

	arrayOfCompositeTypes = makeNode(TypeName);
	arrayOfCompositeTypes->typeOid = get_array_type(keyValPairTypeAddr.objectId);
	arrayOfCompositeTypes->typemod = -1;

	newDomain = makeNode(CreateDomainStmt);
	newDomain->domainname = list_make2(makeString(EXTENSION_SCHEMA), makeString(typeName));
	newDomain->typeName = arrayOfCompositeTypes;

#if PG_VERSION_NUM >= 180000
	{
		ParseState *pstate = make_parsestate(NULL);

		mapTypeAddr = DefineDomain(pstate, newDomain);
	}
#else
	mapTypeAddr = DefineDomain(newDomain);
#endif

	/* map pair depend on map to make sure they are always dropped together */
	recordDependencyOn(&keyValPairTypeAddr, &mapTypeAddr, DEPENDENCY_NORMAL);

	return mapTypeAddr.objectId;
}


static bool
function_exists(const char *proname, oidvector *proargtypes, Oid nspOid)
{
	Oid			procOid = SearchSysCacheExists3(
												PROCNAMEARGSNSP,
												CStringGetDatum(proname),
												PointerGetDatum(proargtypes),
												ObjectIdGetDatum(nspOid));

	if (procOid)
	{
		ereport(DEBUG3,
				(errcode(ERRCODE_DUPLICATE_FUNCTION),
				 errmsg("%s: found duplicate function \"%s\" in schema \"%s\"",
						__func__,
						funcname_signature_string(proname, proargtypes->dim1,
												  NIL, proargtypes->values),
						get_namespace_name(nspOid))));
		return procOid;
	}
	return InvalidOid;
}

/*
 * Create a function to support a custom map type.
 * Register the function as a dependency to the extension.
 * Functions are created strict.
 */
static Oid
map_create_function(const char *sqlFuncName,
					const char *cFuncName,
					Oid returnOid,
					List *argNamesList,
					List *inArgTypes,
					List *outArgTypes)
{
	ObjectAddress funcObjAddr;
	Oid			funcOid;
	Oid			nsOid = get_namespace_oid(EXTENSION_SCHEMA, false);

	int			argCount = list_length(inArgTypes);
	int			allArgCount = argCount + list_length(outArgTypes);

	oidvector  *argTypesVector = NULL;
	ArrayType  *allArgTypesArray = NULL;
	ArrayType  *argModesArray = NULL;
	ArrayType  *argNamesArray = NULL;

	Oid		   *argTypes = palloc0(sizeof(Oid) * argCount);
	Datum	   *allArgTypes = palloc0(sizeof(Datum) * allArgCount);
	Datum	   *argModes = palloc0(sizeof(Datum) * allArgCount);
	Datum	   *argNames = palloc0(sizeof(Datum) * allArgCount);

	int			argIndex = 0;

	/* Write arg oids array to feed to buildoidvector() */
	ListCell   *argCell = NULL;

	foreach(argCell, inArgTypes)
	{
		Oid			argType = lfirst_oid(argCell);

		if (!argType)
			elog(ERROR, "%s: Invalid oid (%u) in arguments", __func__, argType);

		argTypes[argIndex] = argType;

		if (outArgTypes != NIL)
		{
			allArgTypes[argIndex] = ObjectIdGetDatum(argType);
			argModes[argIndex] = CharGetDatum(FUNC_PARAM_IN);
		}

		argIndex++;
	}

	/* prepare parameter arrays */
	argTypesVector = buildoidvector(argTypes, argCount);

	if (outArgTypes != NIL)
	{
		foreach(argCell, outArgTypes)
		{
			Oid			argType = lfirst_oid(argCell);

			if (!argType)
				elog(ERROR, "%s: Invalid oid (%u) in arguments", __func__, argType);

			allArgTypes[argIndex] = ObjectIdGetDatum(argType);
			argModes[argIndex] = CharGetDatum(FUNC_PARAM_OUT);
			argIndex++;
		}

		allArgTypesArray = construct_array_builtin(allArgTypes, allArgCount, OIDOID);
		argModesArray = construct_array_builtin(argModes, allArgCount, CHAROID);
	}

	if (argNamesList != NIL)
	{
		ListCell   *argNameCell = NULL;

		argIndex = 0;

		foreach(argNameCell, argNamesList)
		{
			char	   *argName = lfirst(argNameCell);

			argNames[argIndex] = CStringGetTextDatum(argName);
			argIndex++;
		}

		argNamesArray = construct_array_builtin(argNames, allArgCount, TEXTOID);
	}

	funcOid = function_exists(sqlFuncName, argTypesVector, nsOid);
	if (funcOid)
		return funcOid;

	/* Monster function to create the function! */
	funcObjAddr = ProcedureCreate(
								  sqlFuncName, //const char *procedureName,
								  nsOid, //Oid procNamespace
								  false, //bool replace
								  outArgTypes != NIL, //bool returnsSet
								  returnOid, //Oid returnType
								  GetUserId(), //Oid proowner
								  ClanguageId, //Oid languageObjectId
								  InvalidOid, //Oid languageValidator
								  cFuncName, //const char *prosrc
								  "$libdir/" EXTENSION_NAME, //const char *probin
								  NULL, //Node *prosqlbody
								  PROKIND_FUNCTION, //char prokind
								  false, //bool security_definer
								  false, //bool isLeakProof
								  true, //bool isStrict
								  PROVOLATILE_IMMUTABLE, //char volatility
								  PROPARALLEL_SAFE, //char parallel
								  argTypesVector, //oidvector *parameterTypes
								  PointerGetDatum(allArgTypesArray), //Datum allParameterTypes
								  PointerGetDatum(argModesArray), //Datum parameterModes
								  PointerGetDatum(argNamesArray), //Datum parameterNames
								  NIL, //List *parameterDefaults
								  PointerGetDatum(NULL), //Datum trftypes
#if PG_VERSION_NUM >= 180000
								  NIL, //List *trfoids
#endif
								  PointerGetDatum(NULL), //Datum proconfig
								  InvalidOid, //Oid prosupport
								  1.0, //float4 procost
								  0.0 // float4 prorows
		);

	/* Make sure new function is visible to other calls in this transaction */
	CommandCounterIncrement();

	ereport(DEBUG4,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: created function %s (%u)",
					__func__,
					format_procedure_extended(funcObjAddr.objectId, FORMAT_PROC_INVALID_AS_NULL),
					funcObjAddr.objectId
					)));

	return funcObjAddr.objectId;
}

/*
 * Create a function to support a custom map type.
 * Register the function as a dependency to the extension.
 * Set-returning functions are not currently supported.
 */
static Oid
map_create_operator(const char *operName,
					Oid leftTypeId,
					Oid rightTypeId,
					Oid funcOid)
{

	ObjectAddress opObjAddr = OperatorCreate(
											 operName, //const char *operatorName,
											 PG_CATALOG_NAMESPACE, //Oid operatorNamespace,
											 leftTypeId, //Oid leftTypeId,
											 rightTypeId, //Oid rightTypeId,
											 funcOid, //Oid procedureId,
											 NIL, //List *commutatorName,
											 NIL, //List *negatorName,
											 InvalidOid, //Oid restrictionId,
											 InvalidOid, //Oid joinId,
											 false, //bool canMerge,
											 false // bool canHash
		);

	/* Make sure new function is visible to other */
	/* calls in this transaction */
	CommandCounterIncrement();

	ereport(DEBUG4,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: created operator %s (%u)",
					__func__,
					format_operator_extended(opObjAddr.objectId, FORMAT_OPERATOR_INVALID_AS_NULL),
					opObjAddr.objectId
					)));

	return opObjAddr.objectId;
}


/*
 * map_create(keysdatatype text,
 *            valsdatatype text,
 *            typename text DEFAULT NULL)
 *            RETURNS text
 *
 * Create a new map data type, as a composite type which
 * has two attributes, a 'keys' array of the key type, and
 * a 'vals' array of the value type. The type strings used
 * as arguments must resolve to types via regtype,
 * eg: 'int4'::regtype::oid.
 *
 * The new type will have a type name of 'map_type.key_<type>_val_<type>'
 * if not otherwise provided, and will be registered as a
 * dependency of the map extension.
 *
 * In addition, versions of the map_type.extract, map_type.cardinality, etc
 * functions that use the new map type will be created, and registered
 * as dependencies of the map extension.
 *
 * The on-demand creation of functions solves the problem
 * of "how can we have functions that handle any arbitrary
 * map type". We don't: we create new functions for
 * the actual arbitrary combinations in use.
 */
PG_FUNCTION_INFO_V1(map_create);
Datum
map_create(PG_FUNCTION_ARGS)
{
	Oid			keyType,
				valType,
				mapTypeOid;
	char	   *mapTypeStr = NULL;
	Oid			extractFuncOid = InvalidOid;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		elog(ERROR, "null arguments are not valid for the map(regtype, regtype) function");

	/* User-provided type name */
	if (!PG_ARGISNULL(2))
		mapTypeStr = text_to_cstring(PG_GETARG_TEXT_P(2));

	keyType = PG_GETARG_OID(0);
	valType = PG_GETARG_OID(1);

	/* Let's disallow using arrays as keys */
	if (get_element_type(keyType) != InvalidOid)
		elog(ERROR, "arrays cannot be used as the key type");

	/*
	 * Create the new custom type and the support functions
	 */

	/* Get the new map type name */
	if (!mapTypeStr)
		mapTypeStr = map_type_name(keyType, valType);

	/* Check for existing map type with the given name */
	mapTypeOid = find_existing_map_type(mapTypeStr, keyType, valType);

	if (!OidIsValid(mapTypeOid))
	{
		/* Register that type */
		mapTypeOid = map_create_type(mapTypeStr, keyType, valType);

		/* Create functions that use the new map/key/vals argument types */
		extractFuncOid = map_create_function("extract", "map_extract",
											 valType,
											 list_make2("map", "key"),
											 list_make2_oid(mapTypeOid, keyType), NIL);
		map_create_function("cardinality", "map_cardinality",
							INT4OID,
							list_make1("map"),
							list_make1_oid(mapTypeOid), NIL);
		map_create_function("entries", "map_entries",
							RECORDOID,
							list_make3("map", "key", "value"),
							list_make1_oid(mapTypeOid),
							list_make2_oid(keyType, valType));

		/* Create utility operator */
		map_create_operator("->", mapTypeOid, keyType, extractFuncOid);
	}

	PG_RETURN_OID(mapTypeOid);
}

/*
 * map_extract(maptype map, keytype key) returns value
 *
 * For any "map" type (an array of key/value pairs with a custom composite
 * type), return the value that corresponds to the provided key, or NULL
 * otherwise.
 *
 * Multiple SQL definitions of this function, with specific
 * input types can be supported by this one C function.
 */
PG_FUNCTION_INFO_V1(map_extract);
Datum
map_extract(PG_FUNCTION_ARGS)
{
	Datum		mapArgDatum = PG_GETARG_DATUM(0);
	Datum		keyArgDatum = PG_GETARG_DATUM(1);

	/* Get the types for the map and key arguments */
	Oid			argMapType = get_fn_expr_argtype(fcinfo->flinfo, 0);
	Oid			argKeyType = get_fn_expr_argtype(fcinfo->flinfo, 1);

	Oid			mapKeyType = InvalidOid;
	Oid			mapValType = InvalidOid;

	Oid			mapArrayType;

	ArrayType  *elementsArray;
	ArrayIterator arrayIterator;
	uint32_t	arrayPos;

	bool		isMap,
				isNull;
	TypeCacheEntry *typeCacheEntry;
	HeapTupleHeader tupleHeader;
	bool		keysIsNull,
				valsIsNull;
	Datum		keyDatum,
				valDatum,
				elemDatum;
	FmgrInfo   *keyCmpFmgrInfo;

	/* short-circuit for null maps */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}

	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got map type = %s", __func__, format_type_be(argMapType))));
	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got key type = %s", __func__, format_type_be(argKeyType))));

	/* What is the structure of the map? */
	typeCacheEntry = lookup_type_cache(
									   argMapType,
									   TYPECACHE_TUPDESC);

	isMap = typcache_is_map_type(typeCacheEntry, &mapKeyType, &mapValType);
	if (!isMap)
		elog(ERROR, "map argument is the wrong structure");

	/*
	 * Does the type of the provided key match the structure of the map? (It
	 * always should, but if someone bound this function to an unexpected
	 * signature, it might not.)
	 */
	if (mapKeyType != argKeyType)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("key argument does not match key type in map")));
	}

	/* Get the base array from the type */
	mapArrayType = get_element_type(getBaseType(argMapType));

	/* lookup the composite type */
	typeCacheEntry = lookup_type_cache(mapArrayType, TYPECACHE_TUPDESC);

	elementsArray = DatumGetArrayTypeP(mapArgDatum);
	keyCmpFmgrInfo = type_cmp_fmgr(argKeyType);

	arrayIterator = array_create_iterator(elementsArray, 0, NULL);
	tupleHeader = NULL;
	arrayPos = 0;

	while (array_iterate(arrayIterator, &elemDatum, &isNull))
	{
		tupleHeader = DatumGetHeapTupleHeader(elemDatum);

		keyDatum = GetAttributeByNum(tupleHeader, 1, &keysIsNull);

		if (!isNull)
		{
			int			cmp = DatumGetInt32(FunctionCall2Coll(
															  keyCmpFmgrInfo,
															  PG_GET_COLLATION(),
															  keyArgDatum,
															  keyDatum));

			if (cmp == 0)
			{
				ereport(DEBUG4,
						(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
						 errmsg("%s: got key match at array position = %u", __func__, arrayPos)));
				break;
			}
		}
		arrayPos++;
	}
	array_free_iterator(arrayIterator);

	/* Did not find key! Return null. */
	if (arrayPos >= ARRNELEMS(elementsArray))
		PG_RETURN_NULL();

	/* Pull out the corresponding value */
	valDatum = GetAttributeByNum(tupleHeader, 2, &valsIsNull);
	if (valsIsNull)
	{
		PG_RETURN_NULL();
	}
	PG_RETURN_DATUM(valDatum);
}


/*
 * map_cardinality(map maptype) returns int4
 *
 * Returns the size of the map (aka the number of entries in the map).
 */
PG_FUNCTION_INFO_V1(map_cardinality);
Datum
map_cardinality(PG_FUNCTION_ARGS)
{
	Datum		mapArgDatum = PG_GETARG_DATUM(0);
	Oid			argMapType = get_fn_expr_argtype(fcinfo->flinfo, 0);
	Oid			mapKeyType = InvalidOid;
	Oid			mapValType = InvalidOid;
	TypeCacheEntry *typeCacheEntry;

	bool		isMap;

	/*
	 * short-circuit for null maps; we could return 0, but this matches DuckDB
	 * (and strict) behavior
	 */
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}

	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got map type = %s", __func__, format_type_be(argMapType))));

	typeCacheEntry = lookup_type_cache(
									   argMapType,
									   TYPECACHE_TUPDESC);

	/* Check that the map has expected structure */
	isMap = typcache_is_map_type(typeCacheEntry, &mapKeyType, &mapValType);
	if (!isMap)
		elog(ERROR, "map argument is the wrong structure");

	/* Return the length */
	PG_RETURN_INT32(ARRNELEMS(DatumGetArrayTypeP(mapArgDatum)));
}

/*
 * Supporting map_keys and map_values, returns a full copy of
 * the array in the requested attribute (1 = keys, 2 = vals).
 */
PG_FUNCTION_INFO_V1(map_entries);
Datum
map_entries(PG_FUNCTION_ARGS)
{
	Datum		mapArgDatum = PG_GETARG_DATUM(0);
	Oid			argMapType = get_fn_expr_argtype(fcinfo->flinfo, 0);

	TypeCacheEntry *typeCacheEntry;
	ArrayType  *elementsArray;
	ArrayIterator arrayIterator;
	Oid			keyTypeOid,
				valTypeOid;
	uint32		numElements;
	Datum		elemDatum;
	bool		isNull;

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
#if PG_VERSION_NUM >= 150000
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
#else
	{
		TupleDesc	tupdesc;
		Tuplestorestate *tupstore;
		MemoryContext per_query_ctx;
		MemoryContext oldcontext;

		/* check to see if caller supports us returning a tuplestore */
		if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));
		if (!(rsinfo->allowedModes & SFRM_Materialize))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialize mode required, but it is not allowed in this context")));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		/* Build tuplestore to hold the result rows */
		per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
		oldcontext = MemoryContextSwitchTo(per_query_ctx);

		tupstore = tuplestore_begin_heap(true, false, work_mem);
		rsinfo->returnMode = SFRM_Materialize;
		rsinfo->setResult = tupstore;
		rsinfo->setDesc = tupdesc;

		MemoryContextSwitchTo(oldcontext);
	}
#endif

	ereport(DEBUG3,
			(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
			 errmsg("%s: got map type = %s", __func__, format_type_be(argMapType))));

	typeCacheEntry = lookup_type_cache(
									   argMapType,
									   TYPECACHE_TUPDESC);

	/* Check that the map has expected structure */
	if (!typcache_is_map_type(typeCacheEntry, &keyTypeOid, &valTypeOid))
		elog(ERROR, "map argument is the wrong structure");

	elementsArray = DatumGetArrayTypeP(mapArgDatum);

	numElements = ARRNELEMS(elementsArray);
	if (numElements == 0)
		PG_RETURN_VOID();

	arrayIterator = array_create_iterator(elementsArray, 0, NULL);

	while (array_iterate(arrayIterator, &elemDatum, &isNull))
	{
		HeapTupleHeader tupleHeader = DatumGetHeapTupleHeader(elemDatum);
		bool		keyIsNull = false;
		bool		valIsNull = false;

		Datum		values[] = {
			GetAttributeByNum(tupleHeader, 1, &keyIsNull),
			GetAttributeByNum(tupleHeader, 2, &valIsNull)
		};

		bool		nulls[] = {
			keyIsNull,
			valIsNull
		};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	array_free_iterator(arrayIterator);
	PG_RETURN_VOID();
}
