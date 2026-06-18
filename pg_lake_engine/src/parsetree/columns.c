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

#include "pg_extension_base/pg_compat.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"

#include "pg_lake/parsetree/columns.h"

/*
 * BuildColumnDefListForTupleDesc returns a list of ColumnDef nodes for a given TupleDesc.
 */
List *
BuildColumnDefListForTupleDesc(TupleDesc tupleDesc)
{
	List	   *columns = NIL;

	for (int attributeNumber = 1; attributeNumber <= tupleDesc->natts; attributeNumber++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc, attributeNumber - 1);

		char	   *columnName = NameStr(attribute->attname);
		Oid			typeId = attribute->atttypid;
		int32		typeMod = attribute->atttypmod;
		Oid			collationId = attribute->attcollation;

		ColumnDef  *column = makeColumnDef(columnName, typeId, typeMod, collationId);

		columns = lappend(columns, column);
	}

	return columns;
}


/*
 * BuildTupleDescriptorForTargetList builds a TupleDesc from the
 * target list of a query.
 */
TupleDesc
BuildTupleDescriptorForTargetList(List *targetList)
{
	ListCell   *targetEntryCell = NULL;
	int			columnCount = 0;

	columnCount = CountNonJunkColumns(targetList);

	TupleDesc	tupleDesc = CreateTemplateTupleDesc(columnCount);
	AttrNumber	attributeNumber = 1;

	/* add non-resjunk columns to TupleDesc */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node	   *targetExpr = (Node *) targetEntry->expr;

		if (targetEntry->resjunk)
			continue;

		TupleDescInitEntry(tupleDesc, attributeNumber, targetEntry->resname,
						   exprType(targetExpr), exprTypmod(targetExpr), 0);
		TupleDescInitEntryCollation(tupleDesc, attributeNumber,
									exprCollation(targetExpr));

		attributeNumber++;
	}

	TupleDescFinalize(tupleDesc);

	return tupleDesc;
}


/*
 * Returns the number of non-resjunk columns in the given list.  We assume we are passed a list of TargetEntries.
 */
int
CountNonJunkColumns(List *targetList)
{
	ListCell   *targetEntryCell = NULL;
	int			columnCount = 0;

	/* count non-resjunk columns */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		if (!targetEntry->resjunk)
			columnCount++;
	}

	return columnCount;
}
