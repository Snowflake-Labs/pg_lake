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

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "pg_lake/pgduck/map.h"
#include "pg_lake/util/case_insensitive_names.h"
#include "pg_lake/util/string_utils.h"

static void CheckType(Oid typeOid, int32 typmod);
static void CheckTupleDesc(TupleDesc tupleDesc, const char *typeName);


/*
 * Reject sibling names DuckDB treats as the same identifier.
 */
void
ErrorIfCaseInsensitiveDuplicateNames(Oid relationId)
{
	Relation	relation = table_open(relationId, AccessShareLock);

	CheckTupleDesc(RelationGetDescr(relation), NULL);
	table_close(relation, AccessShareLock);
}


static void
CheckTupleDesc(TupleDesc tupleDesc, const char *typeName)
{
	for (int outerIndex = 0; outerIndex < tupleDesc->natts; outerIndex++)
	{
		Form_pg_attribute outerAttr = TupleDescAttr(tupleDesc, outerIndex);

		if (outerAttr->attisdropped)
			continue;

		for (int innerIndex = outerIndex + 1; innerIndex < tupleDesc->natts; innerIndex++)
		{
			Form_pg_attribute innerAttr = TupleDescAttr(tupleDesc, innerIndex);

			if (innerAttr->attisdropped)
				continue;

			if (PgLakeAsciiStrcasecmp(NameStr(outerAttr->attname),
									  NameStr(innerAttr->attname)) != 0)
				continue;

			if (typeName != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("field name \"%s\" conflicts with field \"%s\" in type %s",
								NameStr(innerAttr->attname),
								NameStr(outerAttr->attname),
								typeName)));

			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("column name \"%s\" conflicts with column \"%s\"",
							NameStr(innerAttr->attname),
							NameStr(outerAttr->attname))));
		}
	}

	for (int attnum = 0; attnum < tupleDesc->natts; attnum++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc, attnum);

		if (!attribute->attisdropped)
			CheckType(attribute->atttypid, attribute->atttypmod);
	}
}


/* Arrays, domains, composites and maps, same walk as the Iceberg field builder. */
static void
CheckType(Oid typeOid, int32 typmod)
{
	typeOid = ResolveDomainBaseTypeAndTypmod(typeOid, &typmod);

	if (type_is_array(typeOid))
	{
		CheckType(get_element_type(typeOid), typmod);
		return;
	}

	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupleDesc = lookup_rowtype_tupdesc(typeOid, typmod);

		CheckTupleDesc(tupleDesc, format_type_be(typeOid));
		ReleaseTupleDesc(tupleDesc);
		return;
	}

	if (!IsMapTypeOid(typeOid))
		return;

	Oid			elementType = get_element_type(getBaseType(typeOid));
	TypeCacheEntry *typeEntry = lookup_type_cache(elementType, TYPECACHE_TUPDESC);

	if (typeEntry == NULL || typeEntry->tupDesc == NULL)
		return;

	CheckType(TupleDescAttr(typeEntry->tupDesc, 0)->atttypid,
			  TupleDescAttr(typeEntry->tupDesc, 0)->atttypmod);
	CheckType(TupleDescAttr(typeEntry->tupDesc, 1)->atttypid,
			  TupleDescAttr(typeEntry->tupDesc, 1)->atttypmod);
}
