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
#include "utils/builtins.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/struct_conversion.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

/*
 * structure to cache metadata needed for record I/O
 */
typedef struct ColumnIOData
{
	Oid			column_type;
	Oid			typiofunc;
	Oid			typioparam;
	bool		typisvarlena;
	FmgrInfo	proc;
} ColumnIOData;

typedef struct RecordIOData
{
	Oid			record_type;
	int32		record_typmod;
	int			ncolumns;
	ColumnIOData columns[FLEXIBLE_ARRAY_MEMBER];
} RecordIOData;

/*
 * StructOutForPGDuck is a modified version of C<record_out> that emits the
 * DuckDB STRUCT format.  Compared to C<record_out>, we use the names of the
 * attribute keys as text literals, and surround the output with curly braces.
 */

char *
StructOutForPGDuck(Datum myStruct, CopyDataFormat format)
{
	HeapTupleHeader rec = DatumGetHeapTupleHeader(myStruct);
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	RecordIOData *my_extra;
	bool		needComma = false;
	int			ncolumns;
	int			i;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData buf;

	check_stack_depth();		/* recurses for record-type columns */

	/* Extract type info from the tuple itself */
	tupType = HeapTupleHeaderGetTypeId(rec);
	tupTypmod = HeapTupleHeaderGetTypMod(rec);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	my_extra = palloc(offsetof(RecordIOData, columns) +
					  ncolumns * sizeof(ColumnIOData));
	my_extra->record_type = InvalidOid;
	my_extra->record_typmod = 0;

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   offsetof(RecordIOData, columns) +
			   ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/* Break down the tuple into fields */
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	/* And build the result string */
	initStringInfo(&buf);

	appendStringInfoCharMacro(&buf, '{');

	for (i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = att->atttypid;
		Datum		attr;
		char	   *value;
		char	   *tmp;

		/* Ignore dropped columns in datatype */
		if (att->attisdropped)
			continue;

		if (needComma)
		{
			appendStringInfoCharMacro(&buf, ',');
			appendStringInfoCharMacro(&buf, ' ');
		}
		needComma = true;

		/* emit column name and delimiter */
		appendStringInfoString(&buf, quote_literal_cstr(NameStr(att->attname)));
		appendStringInfoCharMacro(&buf, ':');
		appendStringInfoCharMacro(&buf, ' ');

		if (nulls[i])
		{
			appendStringInfoString(&buf, "NULL");
			continue;
		}

		/*
		 * Convert the column value to text
		 */
		if (column_info->column_type != column_type)
		{
			getTypeOutputInfo(column_type,
							  &column_info->typiofunc,
							  &column_info->typisvarlena);
			fmgr_info(column_info->typiofunc, &column_info->proc);
			column_info->column_type = column_type;
		}

		attr = values[i];
		value = PGDuckSerialize(&column_info->proc, column_type, attr, format);

		/* Detect whether we need double quotes for this value */
		bool		needQuotes = !IsContainerType(column_type);

		/* And emit the string */
		if (needQuotes)
			appendStringInfoCharMacro(&buf, '"');
		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (needQuotes && (ch == '"' || ch == '\\'))
				appendStringInfoCharMacro(&buf, ch);
			appendStringInfoCharMacro(&buf, ch);
		}
		if (needQuotes)
			appendStringInfoCharMacro(&buf, '"');
	}

	appendStringInfoChar(&buf, '}');

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	return buf.data;
}
