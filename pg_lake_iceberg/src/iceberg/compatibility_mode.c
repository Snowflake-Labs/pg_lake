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

/*
 * compatibility_mode.c
 *  Per-table Iceberg "compatibility mode" type rewrites.
 *
 * Iceberg itself supports the full PostgreSQL type surface we map, but some
 * downstream engines that read our Iceberg tables have narrower limitations.
 * When a table is created (or altered) with the `compatibility_mode` option,
 * pg_lake rewrites the affected column TYPES so the Iceberg schema stays
 * consumable by that engine.
 *
 * Currently the only mode is 'snowflake'.  Snowflake cannot store a UUID
 * inside a semi-structured or structured type, so a uuid that appears nested
 * inside an array, map, or composite is rewritten to text.  A top-level uuid
 * column is left untouched (Snowflake's native UUID handles that fine).
 *
 * The structural recursion (array / map / domain / composite) lives in
 * ConvertTypeTree, shared with the numeric->double pass; this module only
 * supplies the per-mode leaf rule.  Out-of-range value handling, timetz
 * normalization, and numeric->double live elsewhere and compose on top of
 * this pass (each leaves the fields it does not touch alone).
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/table_type.h"


/*
 * ParseIcebergCompatibilityMode maps an option string to the enum.  NULL (the
 * option being absent) and 'auto' both mean ICEBERG_COMPAT_AUTO, the default.
 * An unrecognized value is a hard error so the accepted set lives in exactly
 * one place; option validation in pg_lake_table calls straight into this.
 */
IcebergCompatibilityMode
ParseIcebergCompatibilityMode(const char *optionValue)
{
	if (optionValue == NULL)
		return ICEBERG_COMPAT_AUTO;

	if (strcmp(optionValue, "auto") == 0)
		return ICEBERG_COMPAT_AUTO;

	if (strcmp(optionValue, "snowflake") == 0)
		return ICEBERG_COMPAT_SNOWFLAKE;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid %s option: \"%s\"",
					ICEBERG_COMPATIBILITY_MODE_OPTION, optionValue),
			 errhint("Valid values are \"auto\" and \"snowflake\".")));

	return ICEBERG_COMPAT_AUTO; /* keep the compiler happy */
}


/*
 * IcebergCompatibilityModeFromCreateOptions reads the option out of a CREATE
 * statement's WITH (...) DefElem list.
 */
IcebergCompatibilityMode
IcebergCompatibilityModeFromCreateOptions(List *options)
{
	return ParseIcebergCompatibilityMode(
										 GetStringOption(options, ICEBERG_COMPATIBILITY_MODE_OPTION, false));
}


/*
 * IcebergCompatibilityModeFromRelation reads the option from an existing
 * relation's stored foreign-table options.  Returns AUTO for non-iceberg
 * relations (e.g. during ALTER on an unrelated table).
 */
IcebergCompatibilityMode
IcebergCompatibilityModeFromRelation(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return ICEBERG_COMPAT_AUTO;

	ForeignTable *foreignTable = GetForeignTable(relationId);

	return ParseIcebergCompatibilityMode(
										 GetStringOption(foreignTable->options, ICEBERG_COMPATIBILITY_MODE_OPTION, false));
}


/*
 * NestedUuidToText is the snowflake compatibility leaf rule for ConvertTypeTree.
 * A uuid is rewritten to text only when nested (level > 0): Snowflake forbids a
 * UUID inside a semi-structured or structured type, but a top-level uuid column
 * maps to Snowflake's native UUID and is left untouched.  All container
 * traversal (arrays, maps, domains, composites, dropped-attribute handling) is
 * done by ConvertTypeTree, so this rule only classifies scalar leaves.
 */
static bool
NestedUuidToText(Oid typeOid, int32 typeMod, int level, void *context,
				 Oid *outOid, int32 *outMod)
{
	if (typeOid == UUIDOID && level > 0)
	{
		*outOid = TEXTOID;
		*outMod = -1;
		return true;
	}

	return false;
}


/*
 * ApplyIcebergTableCompatibilityModeForSchema rewrites the column TYPES of a
 * ColumnDef list in place according to the compatibility mode.  Top-level
 * columns are processed at level 0; nested types are rewritten as needed.
 * AUTO currently resolves to no rewrites, so only 'snowflake' does any work.
 *
 * The structural recursion is shared with the numeric->double pass via
 * ConvertTypeTree; this pass only supplies the per-mode leaf rule.  The two run
 * as independent passes on the same list and compose (each leaves untouched the
 * fields the other rewrote).
 */
void
ApplyIcebergTableCompatibilityModeForSchema(List *columnDefList,
											IcebergCompatibilityMode mode)
{
	ListCell   *cell;
	TypeLeafConverter leafConv;

	switch (mode)
	{
		case ICEBERG_COMPAT_SNOWFLAKE:
			leafConv = NestedUuidToText;
			break;

		case ICEBERG_COMPAT_AUTO:
		default:
			/* AUTO currently resolves to no rewrites */
			return;
	}

	foreach(cell, columnDefList)
	{
		if (!IsA(lfirst(cell), ColumnDef))
			continue;

		ColumnDef  *columnDef = (ColumnDef *) lfirst(cell);

		if (columnDef->typeName == NULL)
			continue;

		int32		typmod = 0;

		/*
		 * missing_ok lookup: pseudo-types like serial are not resolvable
		 * before transformColumnDefinition() runs, and they are never uuid.
		 */
		Type		tup = LookupTypeName(NULL, columnDef->typeName, &typmod, true);

		if (!HeapTupleIsValid(tup))
			continue;

		Oid			typeOid = ((Form_pg_type) GETSTRUCT(tup))->oid;

		ReleaseSysCache(tup);

		Oid			convertedOid;
		int32		convertedMod;

		if (ConvertTypeTree(typeOid, typmod, 0, leafConv, NULL,
							&convertedOid, &convertedMod))
			columnDef->typeName = makeTypeNameFromOid(convertedOid, convertedMod);
	}
}
