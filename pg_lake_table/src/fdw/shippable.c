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

/*-------------------------------------------------------------------------
 *
 * shippable.c
 *	  Determine which database objects are shippable to pgduck_server
 *
 * We need to determine whether particular functions, operators, and indeed
 * data types are shippable to a remote server for execution --- that is,
 * do they exist and have the same behavior remotely as they do locally?
 *
 * Remember that the pgduck_server is a PostgreSQL server backed by DuckDB,
 * so we cannot even assume the internal functions of PostgreSQL are shippable.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_d.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


#include "pg_lake/extensions/pg_map.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/fdw/shippable.h"
#include "pg_lake/pgduck/shippable_builtin_functions.h"
#include "pg_lake/pgduck/shippable_spatial_functions.h"
#include "pg_lake/pgduck/shippable_builtin_operators.h"
#include "pg_lake/pgduck/shippable_spatial_operators.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/extensions/postgis.h"


/* pg_lake_table.enable_strict_pushdown setting */
bool		EnableStrictPushdown = true;

/* Hash table for caching the results of shippability lookups */
static HTAB *ShippableCacheHash = NULL;

/* if we cached postgis functions, what was their extension ID? */
static Oid	CachedPostgisId = InvalidOid;

/*
 * Hash key for shippability lookups.
 */
typedef struct
{
	/*
	 * OID of the object itself
	 */
	Oid			objid;

	/* OID of its catalog (pg_proc, etc) */
	Oid			classid;
} ShippableCacheKey;

typedef struct
{
	ShippableCacheKey key;		/* hash key - must be first */
	bool		shippable;

	/* function to determine whether the expression is shippable */
	IsShippableFunction isShippableFunc;
} ShippableCacheEntry;

static bool IsShippableToQueryEngine(Oid objectId, Oid classId, Node *expr);
static bool LookupShippableObject(Oid objectId, Oid classId);
static Oid	GetCatalogFunctionOid(char *functionName, int argCount, Oid *argTypes);
static Oid *ParameterOidArray(char **parameterTypes, int parameterCount);
static ShippableCacheEntry *EnterShippableCacheEntry(Oid objectId, Oid classId, bool shippable);
static void LoadShippableBuiltinFunctions(void);
static void LoadShippableSpatialFunctions(void);
static void LoadShippablePgduckFunctions(const PGDuckShippableFunction *functions, int functionCount);
static void LoadShippableBuiltinOperators(void);
static void LoadShippableSpatialOperators(void);
static void LoadShippablePgduckOperators(const PGDuckShippableOperator *operators, int operatorCount);
static bool IsShippableStructType(Oid objectId);
static char *GetObjectDescription(Oid classId, Oid objectId);

/*
 * Initialize the backend-lifespan cache of shippability decisions.
 */
static void
InitializeShippableCache(void)
{
	HASHCTL		ctl;

	/* Create the hash table. */
	ctl.keysize = sizeof(ShippableCacheKey);
	ctl.entrysize = sizeof(ShippableCacheEntry);
	ShippableCacheHash =
		hash_create("pgduck_server shippability cache", 512, &ctl, HASH_ELEM | HASH_BLOBS);
}


/*
* Returns true if given type is supported by pgduck_server.
*/
static bool
ShippableType(PGType postgresType)
{
	DuckDBType	duckdbType = GetDuckDBTypeForPGType(postgresType);

	if (duckdbType == DUCKDB_TYPE_STRUCT)
		return IsShippableStructType(postgresType.postgresTypeOid);

	if (duckdbType == DUCKDB_TYPE_GEOMETRY)
		ErrorIfPgLakeSpatialNotEnabled();

	return duckdbType != DUCKDB_TYPE_INVALID;
}


/*
* Returns true if given function or operator is supported by pgduck_server.
*/
static bool
ShippableProc(Oid objectId, Oid classId)
{
	/* Find the underlying function for the given operator, check that */

	if (classId == OperatorRelationId)
	{
		objectId = get_opcode(objectId);

		if (objectId == InvalidOid)
			return false;
	}

	/*
	 * At this point, objectId is the underlying function, which should live
	 * in the map_type schema.  Validation is verifying the schema name and
	 * function name.  (Right now we only have a single function/operator we
	 * are pushing down, so this is a trivial check, but we can come up with
	 * something more sophisticated in the future if we need it.)
	 */

	return IsMapExtractFunction(objectId);
}


/*
 * Returns true if given object (operator/function/type) is shippable
 * to pgduck_server.
 */
static bool
LookupShippableObject(Oid objectId, Oid classId)
{
	bool		shippable = false;

	switch (classId)
	{
		case OperatorRelationId:
		case ProcedureRelationId:

			/*
			 * Most functions and operators are already accounted for, however
			 * for Map types the functions and operators can be created
			 * separately so would not be picked up in our initial cache.
			 * Given that, we can check if the function/operator lives in the
			 * map_type schema and if so we can push it down, otherwise we
			 * assume it is not shippable.
			 */

			shippable = ShippableProc(objectId, classId);
			break;

		case TypeRelationId:
			shippable = ShippableType(MakePGTypeOid(objectId));
			break;

		default:

			/*
			 * As long as we support operators, procedures, and types, we
			 * should be able to ship any other object to pgduck_server. These
			 * include operator classes or families, access methods, etc.
			 */
			shippable = true;
			break;
	}

	if (!shippable && message_level_is_interesting(LOG))
	{
		ereport(LOG, (errmsg("pg_lake_table: non-shippable object %s (classid=%d objid=%d)",
							 GetObjectDescription(classId, objectId),
							 classId, objectId)));
	}

	return shippable;
}


/*
 * IsShippableToQueryEngine
 *	   Is this object (function/operator/type) shippable to pgduck_server?
 *
 * We also include an optional expression because sometimes whether an expression
 * is shippable depends on the value of the parameters (e.g. we only ship concat
 * if the parameters are base types, because DuckDB would serialize differently).
 */
static bool
IsShippableToQueryEngine(Oid objectId, Oid classId, Node *expr)
{
	ShippableCacheKey key;
	ShippableCacheEntry *entry;

	/* Initialize cache if first time through. */
	if (!ShippableCacheHash)
	{
		InitializeShippableCache();

		/*
		 * On first access to the cache, we load all the pgduck shippable
		 * operators and functions.
		 */
		LoadShippableBuiltinOperators();
		LoadShippableBuiltinFunctions();
	}

	/*
	 * Add postgis functions to cache if postgis is created or if the OID
	 * changed (extension was recreated). We leak into the cache in the latter
	 * case, but that seems not consequential except in extremely unlikely
	 * scenarios where OIDs are reused during the lifetime of this process, or
	 * when we drop and recreate so often that it causes OOM.
	 */
	if (IsExtensionCreated(Postgis) && ExtensionId(Postgis) != CachedPostgisId)
	{
		LoadShippableSpatialFunctions();
		LoadShippableSpatialOperators();

		CachedPostgisId = ExtensionId(Postgis);
	}

	/* Set up cache hash key */
	key.objid = objectId;
	key.classid = classId;

	/* See if we already cached the result. */
	entry = (ShippableCacheEntry *)
		hash_search(ShippableCacheHash, &key, HASH_FIND, NULL);

	if (!entry)
	{
		/* Not found in cache, so perform shippability lookup. */
		bool		shippable = LookupShippableObject(objectId, classId);

		/*
		 * Don't create a new hash entry until *after* we have the shippable
		 * result in hand, as the underlying catalog lookups might trigger a
		 * cache invalidation.
		 */
		entry = (ShippableCacheEntry *)
			hash_search(ShippableCacheHash, &key, HASH_ENTER, NULL);

		entry->shippable = shippable;

		/* we do not have shippable functions for objects found this way */
		entry->isShippableFunc = NULL;
	}

	if (!entry->shippable)
	{
		return false;
	}

	if (entry->isShippableFunc != NULL && expr != NULL && !entry->isShippableFunc(expr))
	{
		/*
		 * there is an IsShippable function, but it returned false for the
		 * expression
		 */

		if (message_level_is_interesting(LOG))
		{
			ereport(LOG, (errmsg("pg_lake_table: non-shippable expression for %s",
								 GetObjectDescription(classId, objectId))));
		}

		return false;
	}

	return true;
}


/*
* LoadShippableBuiltinOperators - Load all the shippable operators
* to the pgduck_server.
*/
static void
LoadShippableBuiltinOperators(void)
{
	int			numTypes = 0;
	const PGDuckShippableOperatorsByType *operatorTypes =
		GetPGDuckShippableOperatorsByType(&numTypes);

	for (int typeIndex = 0; typeIndex < numTypes; typeIndex++)
	{
		const PGDuckShippableOperatorsByType shippableOperatorsByType =
			operatorTypes[typeIndex];

		LoadShippablePgduckOperators(shippableOperatorsByType.oprs,
									 shippableOperatorsByType.oprsLen);
	}
}


/*
 * LoadShippableSpatialOperators - Load all the shippable spatial operators.
 */
static void
LoadShippableSpatialOperators(void)
{
	int			operatorCount = 0;
	const PGDuckShippableOperator *operators = GetShippableSpatialOperators(&operatorCount);

	LoadShippablePgduckOperators(operators, operatorCount);
}


/*
 * LoadShippablePgduckOperators loads all the shippable operators from the given
 * array into the shippable cache.
 */
static void
LoadShippablePgduckOperators(const PGDuckShippableOperator *operators, int operatorCount)
{
	for (int oprIndex = 0; oprIndex < operatorCount; oprIndex++)
	{
		PGDuckShippableOperator oprInfo = operators[oprIndex];
		char	   *namespace = oprInfo.oprnamespace;

		/*
		 * Postgis types exist in the relocatable extension schema. We use a
		 * special prefix for that.
		 */
		if (strcmp(namespace, POSTGIS_SCHEMA) == 0)
		{
			namespace = get_namespace_name(ExtensionSchemaId(Postgis));
		}

		List	   *nameList = list_make2(makeString(namespace), makeString(oprInfo.oprname));
		int			argCount = oprInfo.oprcodeargcount;
		Oid		   *oidArray = ParameterOidArray(oprInfo.oprcodeargtypes, argCount);

		/* for unary operators, only the oprright is set */
		Oid			leftOid = argCount == 2 ? oidArray[0] : InvalidOid;
		Oid			rightOid = argCount == 2 ? oidArray[1] : oidArray[0];

		Oid			operatorId = OpernameGetOprid(nameList, leftOid, rightOid);

		if (!OidIsValid(operatorId))
		{
			/*
			 * We normally do not expect to be here. But let's prefer errors
			 * over silent failures and assertions.
			 */
			elog(ERROR, "Operator %s with oprcode %s not found", oprInfo.oprname, oprInfo.oprcode);
		}

		ShippableCacheEntry *entry =
			EnterShippableCacheEntry(operatorId, OperatorRelationId, true);

		entry->isShippableFunc = oprInfo.isShippable;
	}
}


/*
* LoadShippableBuiltinFunctions - Load all the shippable built-in functions.
*/
static void
LoadShippableBuiltinFunctions(void)
{
	int			functionCount = 0;
	const PGDuckShippableFunction *functions = GetShippableBuiltinFunctions(&functionCount);

	LoadShippablePgduckFunctions(functions, functionCount);
}


/*
 * LoadShippableSpatialFunctions - Load all the shippable spatial functions.
 */
static void
LoadShippableSpatialFunctions(void)
{
	int			functionCount = 0;
	const PGDuckShippableFunction *functions = GetShippableSpatialFunctions(&functionCount);

	LoadShippablePgduckFunctions(functions, functionCount);
}


/*
 * LoadShippablePgduckFunctions loads an array of functions into the
 * shippable cache.
 */
static void
LoadShippablePgduckFunctions(const PGDuckShippableFunction *functions, int functionCount)
{
	for (size_t procIndex = 0; procIndex < functionCount; procIndex++)
	{
		PGDuckShippableFunction functionInfo = functions[procIndex];
		int			argCount = functionInfo.proargcount;

		Oid		   *parameterOidList = ParameterOidArray(functionInfo.proargtypes, argCount);
		Oid			functionOid = GetCatalogFunctionOid(functionInfo.proname, argCount, parameterOidList);

		if (!OidIsValid(functionOid))
		{
			/*
			 * We normally do not expect to be here. But let's prefer errors
			 * over silent failures and assertions.
			 */
			elog(ERROR, "Function %s with arg types not found", functionInfo.proname);
		}

		ShippableCacheEntry *entry =
			EnterShippableCacheEntry(functionOid, ProcedureRelationId, true);

		entry->isShippableFunc = functionInfo.isShippable;
	}

}


/*
 * EnterShippableCacheEntry - Add a new entry to the pgduck
 * shippability cache.
 */
static ShippableCacheEntry *
EnterShippableCacheEntry(Oid objectId, Oid classId, bool shippable)
{
	ShippableCacheKey key;

	key.objid = objectId;
	key.classid = classId;

	bool		found = false;
	ShippableCacheEntry *entry =
		(ShippableCacheEntry *) hash_search(ShippableCacheHash, &key, HASH_ENTER, &found);

	if (found)
	{
		elog(ERROR, "Shippable cache entry already exists for object %u", objectId);
	}

	entry->shippable = shippable;
	entry->isShippableFunc = NULL;

	return entry;
}


/*
 * ParameterOidArray
 *		Returns an array of OIDs for the given parameter types.
 */
static Oid *
ParameterOidArray(char **parameterTypes, int parameterCount)
{
	Oid		   *parameterOidVector = palloc0(sizeof(Oid) * parameterCount);

	for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		char	   *inputArgType = parameterTypes[parameterIndex];
		Oid			namespaceId = PG_CATALOG_NAMESPACE;

		/*
		 * Postgis types exist in the relocatable extension schema. We use a
		 * special prefix for that.
		 */
		if (strncmp(inputArgType, POSTGIS_SCHEMA, strlen(POSTGIS_SCHEMA)) == 0)
		{
			namespaceId = ExtensionSchemaId(Postgis);
			inputArgType += strlen(POSTGIS_SCHEMA);
		}

		Oid			parameterOid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
												   CStringGetDatum(inputArgType),
												   ObjectIdGetDatum(namespaceId));

		if (parameterOid == InvalidOid)
			elog(ERROR, "type %s not found", inputArgType);

		parameterOidVector[parameterIndex] = parameterOid;
	}

	return parameterOidVector;
}


/*
 * GetCatalogFunctionOid
 *		Returns the OID of the function with the given name, argument count,
 *		and argument types.
 */
static Oid
GetCatalogFunctionOid(char *functionName, int argCount, Oid *argTypes)
{
	oidvector  *oidVector = buildoidvector(argTypes, argCount);
	Oid			namespaceId = PG_CATALOG_NAMESPACE;

	/*
	 * Postgis types exist in the relocatable extension schema. We use a
	 * special prefix for that.
	 */
	if (strncmp(functionName, POSTGIS_SCHEMA, strlen(POSTGIS_SCHEMA)) == 0)
	{
		namespaceId = ExtensionSchemaId(Postgis);
		functionName += strlen(POSTGIS_SCHEMA);
	}

	Oid			functionId = GetSysCacheOid3(PROCNAMEARGSNSP, Anum_pg_proc_oid,
											 CStringGetDatum(functionName),
											 PointerGetDatum(oidVector),
											 ObjectIdGetDatum(namespaceId));

	if (functionId == InvalidOid)
		elog(ERROR, "cache lookup failed for function %s in namespace %d",
			 functionName, namespaceId);

	return functionId;
}


/*
 * Returns true if the given composite type is itself shippable; walk the struct
 * itself and recursively determine if all fields are themselves shippable.
 * Note that only pure composite types are considered shippable structs; table
 * types cause problems related with how they cast into columns.
 */
static bool
IsShippableStructType(Oid objectId)
{
	/*
	 * our struct type is shippable if all of the composite elements are
	 * shippable
	 */
	TypeCacheEntry *typentry;
	TupleDesc	tupdesc;

	/* Fetch the type cache entry for the composite type */
	typentry = lookup_type_cache(objectId, TYPECACHE_TUPDESC);

	if (typentry == NULL || typentry->tupDesc == NULL)
	{
		elog(ERROR, "could not find composite type %u", objectId);
	}

	tupdesc = typentry->tupDesc;

	/* verify that this particular type corresponds to a pure type */
	if (get_rel_relkind(typentry->typrelid) != RELKIND_COMPOSITE_TYPE)
		return false;

	/* Iterate through the attributes */
	for (int attributeIndex = 0; attributeIndex < tupdesc->natts; attributeIndex++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attributeIndex);

		if (attr->attisdropped)
			continue;

		/* Print attribute name, type OID, and typmod */

		bool		attributeIsShippable = ShippableType(MakePGType(attr->atttypid, attr->atttypmod));

		if (!attributeIsShippable)
			return false;
	}

	return true;
}


/*
 * GetObjectDescription gets a human-readable description of a database object.
 */
static char *
GetObjectDescription(Oid classId, Oid objectId)
{
	ObjectAddress address;

	address.classId = classId;
	address.objectId = objectId;
	address.objectSubId = 0;

	/*
	 * Get a human-readable object identity.
	 */
	bool		missingOk = true;
	char	   *objectDescription = getObjectIdentity(&address, missingOk);

	if (objectDescription == NULL)
		objectDescription = "";

	return objectDescription;
}


/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstGenbkiObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else deparse_type_name might incorrectly fail to schema-qualify their names.
 * Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
bool
is_builtin(Oid objectId)
{
	return (objectId < FirstGenbkiObjectId);
}

/*
 * is_shippable
 *	   Is this object (function/operator/type) shippable to foreign server?
 */
bool
is_shippable(Oid objectId, Oid classId, Node *expr)
{
	/*
	 * pgduck_server run DuckDB underneath, so we cannot even ship builtin
	 * objects before checking if that exists and behaves exactly the same on
	 * pgduck_server / DuckDB.
	 */
	if (IsShippableToQueryEngine(objectId, classId, expr))
		return true;

	/* do not allow any other functions in strict mode */
	if (EnableStrictPushdown)
		return false;

	/* Built-in objects are presumed shippable in non-strict mode */
	return is_builtin(objectId);
}


/*
 * GetNotShippableDescription returns a human-readable description of not shippable
 * SQL construct.
 */
const char *
GetNotShippableDescription(NotShippableReason reason, Oid classId, Oid objectId)
{
	if (classId != InvalidOid && objectId != InvalidOid)
		return GetObjectDescription(classId, objectId);

	switch (reason)
	{
		case NOT_SHIPPABLE_SQL_FOR_UPDATE:
			return "FOR UPDATE clause";
		case NOT_SHIPPABLE_SQL_LIMIT_WITH_TIES:
			return "LIMIT with TIES clause";
		case NOT_SHIPPABLE_SQL_EMPTY_TARGET_LIST:
			return "Empty target list";
		case NOT_SHIPPABLE_SQL_WITH_ORDINALITY:
			return "WITH ORDINALITY clause";
		case NOT_SHIPPABLE_SQL_JOIN_MERGED_COLUMNS_ALIAS:
			return "JOIN with merged columns and alias";
		case NOT_SHIPPABLE_SQL_UNNEST_GROUP_BY_OR_WINDOW:
			return "UNNEST with GROUP BY or window function";
		case NOT_SHIPPABLE_INFINITE_CONST_VALUE:
			return "infinite interval constant";
		default:
			return NULL;
	}
}


/*
 * In some contexts, user-defined types (primarily composites) are allowed to be
 * pushed down (say for FieldSelect), but some uses (such as casting) are
 * prohibited due to there being no equivalent behavior on the DuckDB side.
 *
 * This shared routine encapsulates the logic of for which nodes UDTs are not
 * pushdownable.  It is very likely we will need to expand the list of node
 * types to check here.
 *
 * Note that returning false here is not sufficient to guarantee that the
 * expression will still be pushdownable, only that we don't know that it isn't.
 */
bool
is_non_shippable_udt_context(Node *node)
{
	int			nodeType = nodeTag(node);

	/*
	 * Not all nodes are expression types, so exprType() can fail; since this
	 * can be called on any node, just early exit if this node type is not one
	 * of our candidate node types to check (which *are* all expression
	 * nodes).
	 */

	if (nodeType != T_RowExpr &&
		nodeType != T_Const &&
		nodeType != T_CoerceViaIO)
		return false;

	Oid			typeId = exprType(node);

	/*
	 * We are a candidate node, so we will be non-shippable if are a
	 * user-defined type.  Right now this is any non-built-in type or a
	 * Geometry type.
	 *
	 * If we start to support other non-builtin types for pushdown, we would
	 * need to expand this list.
	 */

	return (typeId >= FirstNormalObjectId && !IsGeometryTypeId(typeId));
}
