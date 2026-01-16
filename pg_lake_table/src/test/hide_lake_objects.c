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

#include "access/genam.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_index.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "common/hashfn.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg_lake/extensions/btree_gist.h"
#include "pg_lake/extensions/pg_lake_benchmark.h"
#include "pg_lake/extensions/pg_extension_base.h"
#include "pg_lake/extensions/pg_lake_copy.h"
#include "pg_lake/extensions/pg_lake.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/extensions/pg_map.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/pg_parquet.h"
#include "pg_lake/extensions/pg_lake_replication.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_extension_base/pg_extension_base_ids.h"
#include "pg_lake/test/hide_lake_objects.h"


static bool CheckObjectCreatedByLake(ObjectAddress address);
static bool CheckObjectCreatedByLakeInternal(HTAB *referencedObjectsMap, ObjectAddress referencingAddress);
static bool IsExtensionObjectCreatedByLake(ObjectAddress extensionAddress);
static HTAB *CreateReferencedObjectsMap(void);
static Node *CreateObjectCreatedByLakeExpr(Oid catalogTableId, int catalogTableVarNo);
static AttrNumber GetAttributeNumberForCatalogObjectsUsedByPgDepend(Oid catalogTableId);
static List *GetObjectCreatedByLakeFuncArgs(Oid catalogTableId, int catalogTableVarNo);


typedef struct ReferencedObject
{
	ObjectAddress referenced;
}			ReferencedObject;

/*
 * GUC hides any objects, which is created by pg_lake_table extension, from catalog queries,
 * it is intended to be used in postgres tests to not show our objects in catalog queries.
 */
bool		HideObjectsCreatedByLake = false;


PG_FUNCTION_INFO_V1(is_object_created_by_lake);

/*
 * is_object_created_by_lake checks if any pg_lake_table extension
 * created the given object by inspecting pg_depend table.
 */
Datum
is_object_created_by_lake(PG_FUNCTION_ARGS)
{
	Assert(HideObjectsCreatedByLake);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_BOOL(false);

	Oid			classId = PG_GETARG_OID(0);
	Oid			objectId = PG_GETARG_OID(1);

	if (!OidIsValid(classId) || !OidIsValid(objectId))
		PG_RETURN_BOOL(false);

	ObjectAddress address = {classId, objectId, 0};

	switch (classId)
	{
		case AccessMethodOperatorRelationId:
		case AccessMethodProcedureRelationId:
		case AccessMethodRelationId:
		case AttrDefaultRelationId:
		case ConstraintRelationId:
		case EventTriggerRelationId:
		case ForeignServerRelationId:
		case ForeignDataWrapperRelationId:
		case LanguageRelationId:
		case NamespaceRelationId:
		case OperatorClassRelationId:
		case OperatorFamilyRelationId:
		case OperatorRelationId:
		case ProcedureRelationId:
		case RewriteRelationId:
		case RelationRelationId:
		case TriggerRelationId:
		case TSConfigRelationId:
		case TSDictionaryRelationId:
		case TSTemplateRelationId:
		case TypeRelationId:
			{
				/*
				 * noop pg_depend tracks type catalog via their own oid (first
				 * attribute)
				 */
				break;
			}

		case EnumRelationId:
			{
				/* pg_depend tracks enum catalog via pg_type oid */
				address.classId = TypeRelationId;
				break;
			}

		case AttributeRelationId:
		case IndexRelationId:
		case SequenceRelationId:
		case StatisticRelationId:
			{
				/* pg_depend tracks these catalog tables via pg_class oid */
				address.classId = RelationRelationId;
				break;
			}

		case AggregateRelationId:
			{
				/* pg_depend tracks aggregate catalog via pg_proc oid */
				address.classId = ProcedureRelationId;
				break;
			}

		default:
			{
				/* we do not hide objects of this catalog table */
				PG_RETURN_BOOL(false);
				break;
			}
	}

	PG_RETURN_BOOL(CheckObjectCreatedByLake(address));
}


/*
 * HideObjectsCreatedByLakeFromCatalogTables adds a NOT is_object_created_by_lake(regclass, oid) expr
 * to the quals of catalog tables that we are interested in.
 */
bool
HideObjectsCreatedByLakeFromCatalogTables(Node *node, void *context)
{
	if (!HideObjectsCreatedByLake || creating_extension || !IsExtensionCreated(PgLakeTable) || node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		MemoryContext queryContext = GetMemoryChunkContext(query);

		int			varno = 0;
		ListCell   *entryCell = NULL;

		foreach(entryCell, query->rtable)
		{
			RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(entryCell);

			varno++;

			if (rangeTableEntry->rtekind != RTE_RELATION)
				continue;

			Oid			catalogTableOid = InvalidOid;

			/*
			 * add NOT is_object_created_by_lake(regclass, oid) for the
			 * catalog table when it is a catalog table that we are interested
			 * in.
			 */
			switch (rangeTableEntry->relid)
			{
				case AccessMethodRelationId:
				case AccessMethodOperatorRelationId:
				case AccessMethodProcedureRelationId:
				case AggregateRelationId:
				case AttrDefaultRelationId:
				case AttributeRelationId:
				case ConstraintRelationId:
				case EnumRelationId:
				case EventTriggerRelationId:
				case ForeignDataWrapperRelationId:
				case ForeignServerRelationId:
				case IndexRelationId:
				case LanguageRelationId:
				case NamespaceRelationId:
				case OperatorClassRelationId:
				case OperatorFamilyRelationId:
				case OperatorRelationId:
				case ProcedureRelationId:
				case RelationRelationId:
				case RewriteRelationId:
				case SequenceRelationId:
				case StatisticRelationId:
				case TriggerRelationId:
				case TSConfigRelationId:
				case TSDictionaryRelationId:
				case TSTemplateRelationId:
				case TypeRelationId:
					{
						catalogTableOid = rangeTableEntry->relid;
						break;
					}

				default:
					{
						/* we do not hide objects of this catalog table */
						catalogTableOid = InvalidOid;
						break;
					}
			}

			if (OidIsValid(catalogTableOid))
			{
				Assert(query->jointree != NULL);
				Assert(query->jointree->fromlist != NULL);

				/* allocate our filter in the right memory context */
				MemoryContext originalContext = MemoryContextSwitchTo(queryContext);

				Node	   *createdByLakeExpr =
					CreateObjectCreatedByLakeExpr(catalogTableOid, varno);

				/*
				 * we add the new expr to the existing quals
				 */
				query->jointree->quals = make_and_qual(query->jointree->quals, createdByLakeExpr);

				MemoryContextSwitchTo(originalContext);
			}
		}

		return query_tree_walker((Query *) node,
								 HideObjectsCreatedByLakeFromCatalogTables,
								 context, 0);
	}

	return expression_tree_walker(node, HideObjectsCreatedByLakeFromCatalogTables,
								  context);
}


/*
 * DisableLocallyHideObjectsCreatedByLakeWhenAlreadyEnabled disables
 * the GUC HideObjectsCreatedByLake if only it is enabled for local transaction.
 */
void
DisableLocallyHideObjectsCreatedByLakeWhenAlreadyEnabled(void)
{
	if (!HideObjectsCreatedByLake)
	{
		return;
	}

	set_config_option("pg_lake_table.hide_objects_created_by_lake", "false",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


/*
 * CheckObjectCreatedByLake checks if any pg_lake extension created
 * the given object, by recursing into pg_depend table.
 */
static bool
CheckObjectCreatedByLake(ObjectAddress address)
{
	/* Create a map to store referenced objects of the given object */
	HTAB	   *referencedObjectsMap = CreateReferencedObjectsMap();

	return CheckObjectCreatedByLakeInternal(referencedObjectsMap, address);
}


/*
 * CheckObjectCreatedByLakeInternal recursively checks if any referenced object
 * is an extension created by pg_lake extensions by inspecting pg_depend table.
 */
static bool
CheckObjectCreatedByLakeInternal(HTAB *referencedObjectsMap, ObjectAddress referencingAddress)
{
	if (referencingAddress.classId == ExtensionRelationId &&
		IsExtensionObjectCreatedByLake(referencingAddress))
		return true;

	Relation	pgDependRel = NULL;
	ScanKeyData scanKey[2];
	SysScanDesc scanDesc = NULL;
	HeapTuple	tuple = NULL;

	pgDependRel = table_open(DependRelationId, AccessShareLock);

	/* set up scan key to find referenced objects for the given object */
	ScanKeyInit(&scanKey[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(referencingAddress.classId));
	ScanKeyInit(&scanKey[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(referencingAddress.objectId));

	scanDesc = systable_beginscan(pgDependRel, InvalidOid, false,
								  NULL, 2, scanKey);

	bool		createdByLake = false;

	while (HeapTupleIsValid(tuple = systable_getnext(scanDesc)))
	{
		Form_pg_depend dependForm = (Form_pg_depend) GETSTRUCT(tuple);

		ObjectAddress *referenced = palloc0(sizeof(ObjectAddress));

		referenced->classId = dependForm->refclassid;
		referenced->objectId = dependForm->refobjid;

		bool		found = false;

		ReferencedObject *referencedObject = hash_search(referencedObjectsMap, referenced, HASH_ENTER, &found);

		if (!found)
		{
			referencedObject->referenced = *referenced;

			if (CheckObjectCreatedByLakeInternal(referencedObjectsMap, referencedObject->referenced))
			{
				createdByLake = true;
				break;
			}
		}
	}

	systable_endscan(scanDesc);
	table_close(pgDependRel, AccessShareLock);

	return createdByLake;
}


/*
 * CreateReferencedObjectsMap creates a hash table to store referenced objects.
 */
static HTAB *
CreateReferencedObjectsMap(void)
{
	HASHCTL		hashCtl;

	MemSet(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(ObjectAddress);
	hashCtl.entrysize = sizeof(ReferencedObject);
	hashCtl.hash = tag_hash;
	hashCtl.hcxt = CurrentMemoryContext;

	return hash_create("Referenced Objects Map", 128, &hashCtl,
					   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}


/*
 * CreateObjectCreatedByLakeExpr creates a bool expression as shown below:
 * NOT __lake__internal__nsp__.is_object_created_by_lake(regclass, oid)
 */
static Node *
CreateObjectCreatedByLakeExpr(Oid catalogTableId, int catalogTableVarNo)
{
	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = IsObjectCreatedByPgLakeFuncId();
	funcExpr->args = GetObjectCreatedByLakeFuncArgs(catalogTableId, catalogTableVarNo);
	funcExpr->location = -1;

	BoolExpr   *notExpr = makeNode(BoolExpr);

	notExpr->boolop = NOT_EXPR;
	notExpr->args = list_make1(funcExpr);
	notExpr->location = -1;

	return (Node *) notExpr;
}


/*
 * GetAttributeNumberForCatalogObjectsUsedByPgDepend returns the attribute number, for the
 * catalog table, that is used by the pg_depend table to track dependencies of
 * the objects of that catalog table.
 */
static AttrNumber
GetAttributeNumberForCatalogObjectsUsedByPgDepend(Oid catalogTableId)
{
	AttrNumber	oidAttNum = 1;

	switch (catalogTableId)
	{
			/* enumtypid column is used at pg_depend */
		case EnumRelationId:
			{
				oidAttNum = Anum_pg_enum_enumtypid;
				break;
			}

			/* indrelid column is used at pg_depend */
		case IndexRelationId:
			{
				oidAttNum = Anum_pg_index_indrelid;
				break;
			}

		default:
			{
				/* noop */
				break;
			}
	}

	return oidAttNum;
}


/*
 * GetObjectCreatedByLakeFuncArgs returns func arguments for
 * __lake__internal__nsp__.is_object_created_by_lake
 */
static List *
GetObjectCreatedByLakeFuncArgs(Oid catalogTableId, int catalogTableVarNo)
{
	AttrNumber	catalogAttNumForPgDepend = GetAttributeNumberForCatalogObjectsUsedByPgDepend(catalogTableId);

	/* create const for catalog table regclass */
	Const	   *catalogTableOid = makeConst(REGCLASSOID, -1, InvalidOid, sizeof(Oid),
											ObjectIdGetDatum(catalogTableId),
											false, true);

	/*
	 * create Var for catalog table's object oid
	 */
	Oid			varType = (catalogTableId == AggregateRelationId) ? REGPROCOID : OIDOID;
	Var		   *oidVar = makeVar(catalogTableVarNo, catalogAttNumForPgDepend,
								 varType, -1, InvalidOid, 0);

	return list_make2((Node *) catalogTableOid, (Node *) oidVar);
}


/*
 * IsExtensionObjectCreatedByLake checks if the given object is an extension object
 * created by pg_lake extensions.
 */
static bool
IsExtensionObjectCreatedByLake(ObjectAddress extensionAddress)
{
	Assert(extensionAddress.classId == ExtensionRelationId);

	if (IsExtensionCreated(PgLakeBenchmark) && extensionAddress.objectId == ExtensionId(PgLakeBenchmark))
	{
		return true;
	}
	else if (IsExtensionCreated(PgExtensionBase) && extensionAddress.objectId == ExtensionId(PgExtensionBase))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLake) && extensionAddress.objectId == ExtensionId(PgLake))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLakeTable) && extensionAddress.objectId == ExtensionId(PgLakeTable))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLakeCopy) && extensionAddress.objectId == ExtensionId(PgLakeCopy))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLakeIceberg) && extensionAddress.objectId == ExtensionId(PgLakeIceberg))
	{
		return true;
	}
	else if (IsExtensionCreated(PgMap) && extensionAddress.objectId == ExtensionId(PgMap))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLakeEngine) && extensionAddress.objectId == ExtensionId(PgLakeEngine))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLakeSpatial) && extensionAddress.objectId == ExtensionId(PgLakeSpatial))
	{
		return true;
	}
	else if (IsExtensionCreated(PgParquet) && extensionAddress.objectId == ExtensionId(PgParquet))
	{
		return true;
	}
	else if (IsExtensionCreated(Postgis) && extensionAddress.objectId == ExtensionId(Postgis))
	{
		return true;
	}
	else if (IsExtensionCreated(BtreeGist) && extensionAddress.objectId == ExtensionId(BtreeGist))
	{
		return true;
	}
	else if (IsExtensionCreated(PgLakeReplication) && extensionAddress.objectId == ExtensionId(PgLakeReplication))
	{
		return true;
	}

	return false;
}
