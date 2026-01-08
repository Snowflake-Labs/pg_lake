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

#include "common/int.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/util/string_utils.h"

/*
 * DeepCopyLeafField deep copies a LeafField.
 */
LeafField
DeepCopyLeafField(const LeafField * leafField)
{
	LeafField  *copiedLeafField = palloc0(sizeof(LeafField));

	copiedLeafField->fieldId = leafField->fieldId;
	copiedLeafField->field = DeepCopyField(leafField->field);
	copiedLeafField->duckTypeName = pstrdup(leafField->duckTypeName);
	copiedLeafField->level = leafField->level;
	copiedLeafField->pgType = leafField->pgType;

	return *copiedLeafField;
}


/*
 * FindLeafField finds the leaf field with the given fieldId.
 */
LeafField *
FindLeafField(List *leafFieldList, int fieldId)
{
	ListCell   *cell = NULL;

	foreach(cell, leafFieldList)
	{
		LeafField  *leafField = (LeafField *) lfirst(cell);

		if (leafField->fieldId == fieldId)
		{
			return leafField;
		}
	}

	return NULL;
}
