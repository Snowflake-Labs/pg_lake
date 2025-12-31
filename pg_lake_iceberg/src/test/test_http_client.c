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
#include "fmgr.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "pg_lake/http/http_client.h"
#include "pg_lake/util/array_utils.h"

PG_FUNCTION_INFO_V1(test_http_get);
PG_FUNCTION_INFO_V1(test_http_head);
PG_FUNCTION_INFO_V1(test_http_delete);
PG_FUNCTION_INFO_V1(test_http_post);
PG_FUNCTION_INFO_V1(test_http_put);


static Datum build_http_result(FunctionCallInfo fcinfo, const HttpResult * r);
static List *extract_headers(FunctionCallInfo fcinfo, int argno);


Datum
test_http_get(PG_FUNCTION_ARGS)
{
	const char *url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	List	   *headers = extract_headers(fcinfo, 1);

	HttpResult	r = HttpGet(url, headers);

	PG_RETURN_DATUM(build_http_result(fcinfo, &r));
}


Datum
test_http_head(PG_FUNCTION_ARGS)
{
	const char *url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	List	   *headers = extract_headers(fcinfo, 1);

	HttpResult	r = HttpHead(url, headers);

	PG_RETURN_DATUM(build_http_result(fcinfo, &r));
}


Datum
test_http_delete(PG_FUNCTION_ARGS)
{
	const char *url = text_to_cstring(PG_GETARG_TEXT_PP(0));

	List	   *headers = extract_headers(fcinfo, 1);

	HttpResult	r = HttpDelete(url, headers);

	PG_RETURN_DATUM(build_http_result(fcinfo, &r));
}


Datum
test_http_post(PG_FUNCTION_ARGS)
{
	const char *url = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *body = text_to_cstring(PG_GETARG_TEXT_PP(1));

	List	   *headers = extract_headers(fcinfo, 2);

	HttpResult	r = HttpPost(url, body, headers);

	PG_RETURN_DATUM(build_http_result(fcinfo, &r));
}

Datum
test_http_put(PG_FUNCTION_ARGS)
{
	const char *url = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *body = text_to_cstring(PG_GETARG_TEXT_PP(1));

	List	   *headers = extract_headers(fcinfo, 2);

	HttpResult	r = HttpPut(url, body, headers);

	PG_RETURN_DATUM(build_http_result(fcinfo, &r));
}


static List *
extract_headers(FunctionCallInfo fcinfo, int argno)
{
	if (PG_NARGS() > argno && !PG_ARGISNULL(argno))
	{
		ArrayType  *headerArr = PG_GETARG_ARRAYTYPE_P(argno);

		return StringArrayToList(headerArr);
	}
	else
	{
		return NIL;				/* No headers provided */
	}
}

/* Helper: build (status, body, resp_headers) composite                    */
static Datum
build_http_result(FunctionCallInfo fcinfo, const HttpResult * r)
{
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR, (errmsg("return type must be http_result")));

	if (r->status >= 400 && r->status < 600)
	{
		ereport(ERROR, (errmsg("HTTP request failed with status %ld", r->status),
						errdetail("%s", r->body)));
		Assert(r->status == 0);
	}

	if (r->errorMsg != NULL)
	{
		ereport(ERROR, (errmsg("%s", r->errorMsg)));
	}

	Datum		values[3];
	bool		nulls[3] = {false, false, false};

	values[0] = Int32GetDatum((int32) r->status);
	values[1] = CStringGetTextDatum(r->body ? r->body : "");
	values[2] = CStringGetTextDatum(r->headers ? r->headers : "");

	HeapTuple	tup = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tup);
}
