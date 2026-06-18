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

#include "catalog/pg_operator.h"
#include "pg_lake/util/operator_utils.h"
#include "nodes/print.h"
#include "utils/syscache.h"

/*
 * get_oper_name
 *	  returns the name of the operator with the given operid
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such operator.
 */
char *
get_oper_name(Oid operatorId)
{
	HeapTuple	tp;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorId));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_operator opertup = (Form_pg_operator) GETSTRUCT(tp);
		char	   *result;

		result = pstrdup(NameStr(opertup->oprname));
		ReleaseSysCache(tp);
		return result;
	}
	else
		return NULL;
}

/*
 * get_oper_namespace
 *
 *		Returns the pg_namespace OID associated with a given operator.
 */
Oid
get_oper_namespace(Oid operatorId)
{
	HeapTuple	tp;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorId));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_operator opertup = (Form_pg_operator) GETSTRUCT(tp);
		Oid			result;

		result = opertup->oprnamespace;
		ReleaseSysCache(tp);
		return result;
	}
	else
		return InvalidOid;
}
