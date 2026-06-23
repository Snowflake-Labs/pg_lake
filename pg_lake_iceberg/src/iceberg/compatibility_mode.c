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
 *
 * Rewriting a column means swapping only its typeName, which is sound only if
 * nothing else on the column is bound to the original type.  Any column we
 * actually rewrite is therefore screened by ErrorIfColumnRewriteUnsafe, which
 * refuses columns carrying a DEFAULT / GENERATED / IDENTITY expression (there
 * is no implicit uuid->text cast for core to re-coerce them through).  This is
 * reachable only from user-authored DDL: callers that build Iceberg tables
 * programmatically construct each column from type/typmod/collation alone and
 * never attach these clauses, so the check there is a defensive invariant, not
 * an error path users of those callers can hit.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
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
 * ErrorIfColumnRewriteUnsafe refuses to rewrite a column whose declared type
 * carries a clause whose meaning is bound to that type.
 *
 * Changing a column's type underneath a DEFAULT or GENERATED expression is only
 * safe if the expression's result can be coerced to the new type the way the
 * core CREATE/ALTER path expects.  numeric->double gets away with it because
 * numeric->float8 is an implicit cast; but there is NO implicit or assignment
 * cast from uuid to text, so a uuid-typed DEFAULT/GENERATED on a column we
 * rewrite to text would either change the stored semantics or (more often) fail
 * later with a confusing "default expression is of type uuid[]" error from
 * core.  We fail fast with an actionable message instead.
 *
 * Identity is impossible here (it requires an integer type, never a column that
 * contains a uuid) but is covered defensively so future leaf rules stay safe.
 *
 * Only user-authored DDL can reach this: callers that generate Iceberg tables
 * programmatically build columns from type/typmod/collation alone and never
 * attach a DEFAULT / GENERATED / IDENTITY, so for them this is unreachable.
 *
 * At this pre-transform stage these clauses live as Constraint nodes in
 * columnDef->constraints; transformColumnDefinition() only fills the cooked
 * raw_default/generated/identity fields later, but we check those too in case
 * the pass is ever fed an already-transformed ColumnDef.
 */
static void
ErrorIfColumnRewriteUnsafe(ColumnDef *columnDef)
{
	const char *clause = NULL;
	ListCell   *lc;

	if (columnDef->raw_default != NULL || columnDef->cooked_default != NULL)
		clause = "a DEFAULT";
	else if (columnDef->generated != '\0')
		clause = "a GENERATED";
	else if (columnDef->identity != '\0')
		clause = "an IDENTITY";

	foreach(lc, columnDef->constraints)
	{
		Constraint *con = lfirst_node(Constraint, lc);

		if (con->contype == CONSTR_DEFAULT)
			clause = "a DEFAULT";
		else if (con->contype == CONSTR_GENERATED)
			clause = "a GENERATED";
		else if (con->contype == CONSTR_IDENTITY)
			clause = "an IDENTITY";
	}

	if (clause == NULL)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("compatibility_mode cannot rewrite column \"%s\"",
					columnDef->colname),
			 errdetail("The column has %s expression bound to its original type, "
					   "which would no longer match once a nested uuid is "
					   "rewritten to text.", clause),
			 errhint("Drop the expression, or keep the uuid at the top level "
					 "of the column (top-level uuid columns are left unchanged).")));
}


/*
 * ApplyIcebergTableTypeConversions rewrites the column TYPES of a
 * ColumnDef list in place according to the compatibility mode.  Top-level
 * columns are processed at level 0; nested types are rewritten as needed.
 * AUTO currently resolves to no rewrites, so only 'snowflake' does any work.
 *
 * The structural recursion is shared with the numeric->double pass via
 * ConvertTypeTree; this pass only supplies the per-mode leaf rule.  The two run
 * as independent passes on the same list and compose (each leaves untouched the
 * fields the other rewrote).
 *
 * Swapping only typeName is sound only for columns that carry nothing else
 * bound to the original type, so any column we actually rewrite is first run
 * through ErrorIfColumnRewriteUnsafe.
 */
void
ApplyIcebergTableTypeConversions(List *columnDefList,
								 IcebergCompatibilityMode mode)
{
	ListCell   *cell;

	/* AUTO currently resolves to no rewrites; only snowflake does any work */
	if (mode != ICEBERG_COMPAT_SNOWFLAKE)
		return;

	TypeLeafConverter leafConv = NestedUuidToText;

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
		{
			ErrorIfColumnRewriteUnsafe(columnDef);

			ereport(NOTICE,
					(errmsg("column \"%s\" type %s rewritten to %s for "
							"compatibility_mode 'snowflake'",
							columnDef->colname,
							format_type_be(typeOid),
							format_type_be(convertedOid)),
					 errdetail("Snowflake cannot store a uuid inside an array, map, "
							   "or composite; a top-level uuid column is left "
							   "unchanged.")));

			SetColumnDefTypeNameFromOid(columnDef, convertedOid, convertedMod);
		}
	}
}
