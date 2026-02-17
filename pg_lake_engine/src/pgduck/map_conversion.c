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

#include "access/htup_details.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/map_conversion.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/typcache.h"

/*
 * MapOutForPGDuck serializes pg_map.map-based types in the serialized
 * format that DuckDB MAP expected.  Since the only caller of this routine is
 * PGDuckSerialize() we presume that the Datum has already been validated to be
 * a MAP via IsMapTypeOid().
 */

char *
MapOutForPGDuck(Datum myMap)
{
	/* myMap is a domain over an array of a 2-col composite type */

	ArrayType  *elements = DatumGetArrayTypeP(myMap);
	TypeCacheEntry *tupleLookup = lookup_type_cache(elements->elemtype, TYPECACHE_TUPDESC);
	TupleDesc	tupDesc = tupleLookup->tupDesc;

	/*
	 * Ensure the composite type has exactly 2 attributes (keys and values
	 * arrays)
	 */
	if (tupDesc->natts != 2)
	{
		ereport(ERROR, (errmsg("input type must have exactly 2 attributes")));
	}

	int			nitems = ArrayGetNItems(ARR_NDIM(elements), ARR_DIMS(elements));

	if (nitems == 0)
		return pstrdup("{}");

	Form_pg_attribute keyAttribute = TupleDescAttr(tupDesc, 0);
	Form_pg_attribute valAttribute = TupleDescAttr(tupDesc, 1);

	Oid			keysElementType = keyAttribute->atttypid;
	Oid			valuesElementType = valAttribute->atttypid;

	ArrayMetaState *keysExtra = palloc0(sizeof(ArrayMetaState));

	get_type_io_data(keysElementType, IOFunc_output,
					 &keysExtra->typlen, &keysExtra->typbyval,
					 &keysExtra->typalign, &keysExtra->typdelim,
					 &keysExtra->typioparam, &keysExtra->typiofunc);
	fmgr_info(keysExtra->typiofunc, &keysExtra->proc);
	keysExtra->element_type = keysElementType;

	ArrayMetaState *valuesExtra = palloc0(sizeof(ArrayMetaState));

	get_type_io_data(valuesElementType, IOFunc_output,
					 &valuesExtra->typlen, &valuesExtra->typbyval,
					 &valuesExtra->typalign, &valuesExtra->typdelim,
					 &valuesExtra->typioparam, &valuesExtra->typiofunc);
	fmgr_info(valuesExtra->typiofunc, &valuesExtra->proc);
	valuesExtra->element_type = valuesElementType;

	ArrayIterator elementIterator = array_create_iterator(elements, 0, NULL);

	/* phew, that was a lot of setup, but now we are able to do our iteration */

	Datum		elemValue;
	bool		elemIsNull,
				needComma = false;

	StringInfoData outputBuffer;

	initStringInfo(&outputBuffer);

	appendStringInfoCharMacro(&outputBuffer, '{');

	while (array_iterate(elementIterator, &elemValue, &elemIsNull))
	{
		if (elemIsNull)
			ereport(ERROR, (errmsg("cannot have NULL for map entry value")));

		/* decompose the elemValue into the two Datums */
		HeapTupleHeader pairTuple = DatumGetHeapTupleHeader(elemValue);
		TupleDesc	pairTupleDesc =
			lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(pairTuple),
								   HeapTupleHeaderGetTypMod(pairTuple));
		HeapTupleData tuple;
		Datum		pairValues[2];
		bool		nulls[2];

		tuple.t_len = HeapTupleHeaderGetDatumLength(pairTuple);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_data = pairTuple;

		heap_deform_tuple(&tuple, pairTupleDesc, pairValues, nulls);

		char	   *serializedKey;
		char	   *serializedValue;

		if (nulls[0])
			ereport(ERROR, (errmsg("cannot have NULL for map key entry")));;

		serializedKey = PGDuckSerialize(&keysExtra->proc, keysElementType, pairValues[0],
									DATA_FORMAT_INVALID);
		if (!IsContainerType(keysElementType))
			serializedKey = (char *) quote_literal_cstr(serializedKey);

		if (nulls[1])
			serializedValue = "NULL";
		else
		{
			serializedValue = PGDuckSerialize(&valuesExtra->proc, valuesElementType, pairValues[1],
											  DATA_FORMAT_INVALID);
			if (!IsContainerType(valuesElementType))
				serializedValue = (char *) quote_literal_cstr(serializedValue);
		}

		if (needComma)
		{
			appendStringInfoCharMacro(&outputBuffer, ',');
			appendStringInfoCharMacro(&outputBuffer, ' ');
		}
		needComma = true;

		appendStringInfoString(&outputBuffer, serializedKey);
		appendStringInfoCharMacro(&outputBuffer, '=');
		appendStringInfoString(&outputBuffer, serializedValue);

		ReleaseTupleDesc(pairTupleDesc);
	}

	appendStringInfoCharMacro(&outputBuffer, '}');

	return outputBuffer.data;
}
