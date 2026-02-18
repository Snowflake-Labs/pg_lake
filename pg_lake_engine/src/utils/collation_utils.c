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
#include "utils/pg_locale.h"


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

#if PG_VERSION_NUM >= 180000
	pg_locale_t locale = pg_newlocale_from_collation(collation);
	return locale->ctype_is_c;
#else
	return lc_ctype_is_c(collation);
#endif
}
