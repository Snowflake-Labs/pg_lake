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

#include "pg_lake/pgduck/array_conversion.h"
#include "pg_lake/pgduck/serialize.h"
#include "utils/arrayaccess.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

/*
 * ArrayOutForPGDuck is a modified version of array_out that emits the DuckDB
 * array format.
 *
 * We've only made small adjustments to the code, namely:
 * - use [ ] as brackets
 * - error when there are custom lower bounds
 * - we do not use fcinfo (not in a UDF) or AnyArrayType
 */
char *
ArrayOutForPGDuck(ArrayType *array)
{
	Oid			element_type = ARR_ELEMTYPE(array);
	char	   *p,
			   *tmp,
			   *retval,
			  **values;
	bool	   *needquotes;
	size_t		overall_length;
	int			nitems,
				i,
				j,
				k,
				index[MAXDIM];
	int			ndim,
			   *dims,
			   *lb;

	ArrayMetaState *my_extra = palloc0(sizeof(ArrayMetaState));

	get_type_io_data(element_type, IOFunc_output,
					 &my_extra->typlen, &my_extra->typbyval,
					 &my_extra->typalign, &my_extra->typdelim,
					 &my_extra->typioparam, &my_extra->typiofunc);
	fmgr_info(my_extra->typiofunc, &my_extra->proc);
	my_extra->element_type = element_type;

	ndim = ARR_NDIM(array);
	dims = ARR_DIMS(array);
	lb = ARR_LBOUND(array);
	nitems = ArrayGetNItems(ndim, dims);

	if (nitems == 0)
	{
		retval = pstrdup("[]");
		return retval;
	}

	/*
	 * we will need to add explicit dimensions if any dimension has a lower
	 * bound other than one
	 */
	for (i = 0; i < ndim; i++)
	{
		if (lb[i] != 1)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot push down array with custom lower bounds")));
		}
	}

	/*
	 * Convert all values to string form, count total space needed (including
	 * any overhead such as escaping backslashes), and detect whether each
	 * item needs double quotes.
	 */
	values = (char **) palloc(nitems * sizeof(char *));
	needquotes = (bool *) palloc(nitems * sizeof(bool));
	overall_length = 0;

	ArrayIterator iter = array_create_iterator(array, 0, my_extra);

	Datum		itemvalue;
	bool		isnull;

	for (i = 0; i < nitems && array_iterate(iter, &itemvalue, &isnull); i++)
	{
		bool		needquote;

		/* Get source element, checking for NULL */
		if (isnull)
		{
			values[i] = pstrdup("NULL");
			overall_length += 4;
			needquote = false;
		}
		else
		{
			values[i] = PGDuckSerialize(&my_extra->proc, element_type, itemvalue,
										DATA_FORMAT_INVALID);

			/* count data plus backslashes; detect chars needing quotes */
			needquote = !IsContainerType(element_type);

			for (tmp = values[i]; *tmp != '\0'; tmp++)
			{
				char		ch = *tmp;

				overall_length += 1;
				if (needquote && (ch == '"' || ch == '\\'))
					overall_length += 1;
			}
		}

		needquotes[i] = needquote;

		/* Count the pair of double quotes, if needed */
		if (needquote)
			overall_length += 2;
		/* and the comma (or other typdelim delimiter) */
		overall_length += 1;
	}

	/*
	 * The very last array element doesn't have a typdelim delimiter after it,
	 * but that's OK; that space is needed for the trailing '\0'.
	 *
	 * Now count total number of curly brace pairs in output string.
	 */
	for (i = j = 0, k = 1; i < ndim; i++)
	{
		j += k, k *= dims[i];
	}
	overall_length += 2 * j;

	/* Now construct the output string */
	retval = (char *) palloc(overall_length);
	p = retval;

#define APPENDSTR(str)	(strcpy(p, (str)), p += strlen(p))
#define APPENDCHAR(ch)	(*p++ = (ch), *p = '\0')

	APPENDCHAR('[');
	for (i = 0; i < ndim; i++)
		index[i] = 0;
	j = 0;
	k = 0;
	do
	{
		for (i = j; i < ndim - 1; i++)
			APPENDCHAR('[');

		if (needquotes[k])
		{
			APPENDCHAR('"');
			for (tmp = values[k]; *tmp; tmp++)
			{
				char		ch = *tmp;

				if (ch == '"' || ch == '\\')
					*p++ = '\\';
				*p++ = ch;
			}
			*p = '\0';
			APPENDCHAR('"');
		}
		else
			APPENDSTR(values[k]);
		pfree(values[k++]);

		for (i = ndim - 1; i >= 0; i--)
		{
			if (++(index[i]) < dims[i])
			{
				APPENDCHAR(my_extra->typdelim);
				break;
			}
			else
			{
				index[i] = 0;
				APPENDCHAR(']');
			}
		}
		j = i;
	} while (j != -1);

#undef APPENDSTR
#undef APPENDCHAR

	/* Assert that we calculated the string length accurately */
	Assert(overall_length == (p - retval + 1));

	pfree(values);
	pfree(needquotes);

	return retval;
}
