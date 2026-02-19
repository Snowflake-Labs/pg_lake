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

#include "pg_lake/util/collation_utils.h"
#include "catalog/pg_collation.h"
#include "utils/pg_locale.h"

#if PG_VERSION_NUM >= 180000
#include "access/htup_details.h"
#include "catalog/pg_database.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#endif


/*
 * IsNonCollatableOrC returns whether the given collation is compatible
 * with pushdown to DuckDB. Only non-collatable types (InvalidOid) and
 * C type C collation are safe to push down.
 */
bool
IsNonCollatableOrC(Oid collation)
{
	if (collation == InvalidOid)
		return true;

	if (collation == C_COLLATION_OID)
		return true;

	if (collation == DEFAULT_COLLATION_OID)
	{
#if PG_VERSION_NUM >= 180000
		HeapTuple	tup;
		Form_pg_database dbform;
		bool		is_c = false;

		tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);
		dbform = (Form_pg_database) GETSTRUCT(tup);

		if (dbform->datlocprovider == COLLPROVIDER_LIBC)
		{
			Datum		d = SysCacheGetAttrNotNull(DATABASEOID, tup,
												   Anum_pg_database_datcollate);

			is_c = strcmp(TextDatumGetCString(d), "C") == 0;
		}
		else if (dbform->datlocprovider == COLLPROVIDER_BUILTIN)
		{
			Datum		d = SysCacheGetAttrNotNull(DATABASEOID, tup,
												   Anum_pg_database_datlocale);

			is_c = strcmp(TextDatumGetCString(d), "C") == 0;
		}

		ReleaseSysCache(tup);
		return is_c;
#else
		char	   *localeptr;

		if (default_locale.provider == COLLPROVIDER_ICU)
		{
			return false;
		}
		else if (default_locale.provider == COLLPROVIDER_LIBC)
		{
			localeptr = setlocale(LC_CTYPE, NULL);
			if (!localeptr)
				elog(ERROR, "invalid LC_CTYPE setting");
		}
#if PG_VERSION_NUM >= 170000
		else if (default_locale.provider == COLLPROVIDER_BUILTIN)
		{
			localeptr = default_locale.info.builtin.locale;
		}
#endif
		else
			elog(ERROR, "unexpected collation provider '%c'",
				 default_locale.provider);

		return strcmp(localeptr, "C") == 0;
#endif
	}

	return false;
}
