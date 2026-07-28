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

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_representation.h"
#include "pg_lake/pgduck/type.h"

#include "parser/parse_type.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(pg_lake_same_iceberg_representation);

/*
 * pg_lake_same_iceberg_representation(old_type text, new_type text,
 *                                     unsupported_numeric_as_double bool,
 *                                     compatibility_mode text) -> bool
 *
 * Test helper that parses two Postgres type strings (e.g. 'varchar(50)',
 * 'text', 'numeric(50,2)', 'numeric(50,2)[]', 'uuid[]') and returns
 * SameIcebergStoredRepresentation for them.
 *
 * The third and fourth arguments pin the create-path settings the comparison
 * uses: unsupported_numeric_as_double (NULL -> live GUC) and compatibility_mode
 * (NULL -> auto/none).  They let tests exercise both GUC states and the
 * snowflake compatibility mapping without session-wide SETs or a real table.
 */
Datum
pg_lake_same_iceberg_representation(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	char	   *oldTypeStr = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *newTypeStr = text_to_cstring(PG_GETARG_TEXT_PP(1));

	Oid			oldTypeOid;
	int32		oldTypeMod;
	Oid			newTypeOid;
	int32		newTypeMod;

	parseTypeString(oldTypeStr, &oldTypeOid, &oldTypeMod, NULL);
	parseTypeString(newTypeStr, &newTypeOid, &newTypeMod, NULL);

	IcebergCreatePathContext createContext;

	if (PG_ARGISNULL(2))
		InitIcebergCreatePathContextFromGUC(&createContext);
	else
		InitIcebergCreatePathContext(&createContext, PG_GETARG_BOOL(2),
									 ICEBERG_COMPAT_AUTO);

	if (!PG_ARGISNULL(3))
		createContext.compatibilityMode =
			ParseIcebergCompatibilityMode(text_to_cstring(PG_GETARG_TEXT_PP(3)));

	bool		same = SameIcebergStoredRepresentation(MakePGType(oldTypeOid, oldTypeMod),
													   MakePGType(newTypeOid, newTypeMod),
													   &createContext);

	PG_RETURN_BOOL(same);
}
