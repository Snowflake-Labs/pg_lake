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

#include "pg_lake/util/array_utils.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static List *ArrayToList(ArrayType *array, Oid element_type, void *(*process_element) (Datum element_datum));
static ArrayType *ListToArray(List *stringList, Oid elementType);
static void *ProcessInt64Element(Datum element_datum);
static void *ProcessStringElement(Datum element_datum);

/*
 * Int64ArrayToList converts an int8 array to a list of int64.
 * The return list contains int64 * values.
 */
List *
Int64ArrayToList(ArrayType *array)
{
	return ArrayToList(array, INT8OID, ProcessInt64Element);
}

/*
 * StringArrayToList converts a text array to a list of strings.
 */
List *
StringArrayToList(ArrayType *array)
{
	return ArrayToList(array, TEXTOID, ProcessStringElement);
}


/*
* StringListToArray converts a list of strings to a text array.
*/
ArrayType *
StringListToArray(List *stringList)
{
	return ListToArray(stringList, TEXTOID);
}

/*
* INT16ListToArray converts a list of int16 values to an int2 array.
*/
ArrayType *
INT16ListToArray(List *stringList)
{
	return ListToArray(stringList, INT2OID);
}

/*
* OidListToArray converts a list of OIDs to an oid array.
*/
ArrayType *
OidListToArray(List *oidList)
{
	return ListToArray(oidList, OIDOID);
}


/*
* Generic function to convert a list to an array.
* The 'elementType' should be the type of the elements in the
* list. Make sure the type is supported by the function.
*/
static ArrayType *
ListToArray(List *list, Oid elementType)
{
	int			listLen = list_length(list);
	Datum	   *datums = (Datum *) palloc(listLen * sizeof(Datum));
	bool	   *nulls = (bool *) palloc(listLen * sizeof(bool));

	ListCell   *cell;
	int			datumIndex = 0;

	foreach(cell, list)
	{

		if (elementType == INT2OID)
		{
			int16		val = lfirst_int(cell);

			datums[datumIndex] = Int16GetDatum(val);
		}
		else if (elementType == TEXTOID)
		{
			char	   *val = (char *) lfirst(cell);

			datums[datumIndex] = CStringGetTextDatum(val);
		}
		else if (elementType == OIDOID)
		{
			Oid			val = lfirst_oid(cell);

			datums[datumIndex] = ObjectIdGetDatum(val);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported type for conversion to array")));
		}

		nulls[datumIndex] = false;
		datumIndex++;
	}

	ArrayType  *arr = construct_array_builtin(datums, listLen, elementType);

	return arr;
}


/*
 * Generic function to convert an array to a list.
 * The 'process_element' function should allocate and return a value to be added to the list.
 */
static List *
ArrayToList(ArrayType *array, Oid element_type, void *(*process_element) (Datum element_datum))
{
	if (array == NULL)
	{
		return NIL;
	}

	List	   *list = NIL;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &elmlen, &elmbyval, &elmalign);

	Datum	   *arrayElement;
	bool	   *nulls;
	int			elementCount;

	deconstruct_array(array, element_type, elmlen, elmbyval, elmalign,
					  &arrayElement, &nulls, &elementCount);

	for (int i = 0; i < elementCount; i++)
	{
		if (!nulls[i])
		{
			void	   *value = process_element(arrayElement[i]);

			list = lappend(list, value);
		}
	}

	return list;
}

/*
 * Process function for text elements.
 */

static void *
ProcessStringElement(Datum element_datum)
{
	char	   *value = TextDatumGetCString(element_datum);

	return pstrdup(value);
}


/*
 * Process function for int64 elements.
 */
static void *
ProcessInt64Element(Datum element_datum)
{
	int64	   *value = (int64 *) palloc(sizeof(int64));

	*value = DatumGetInt64(element_datum);
	return value;
}
