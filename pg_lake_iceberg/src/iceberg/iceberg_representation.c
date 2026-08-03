/*
 * Copyright 2026 Snowflake Inc.
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

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_representation.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/rel_utils.h"


/*
 * State for the TypeHasUnsupportedNumericLeaf probe: what to look for, and
 * whether it was seen.
 */
typedef struct UnsupportedNumericProbe
{
	bool		nestedOnly;
	bool		found;
}			UnsupportedNumericProbe;

static bool UnsupportedNumericLeafProbe(Oid typeOid, int32 typeMod, int level,
										void *context, Oid *outOid, int32 *outMod);


/*
 * IcebergStoredPostgresType - see iceberg_representation.h.
 *
 * The GUC check mirrors MaybeConvertUnsupportedNumericColumnsToDouble: with the
 * GUC off no rewrite happens at any level, because CREATE rejects an unsupported
 * numeric outright instead of storing it in some other shape.
 */
PGType
IcebergStoredPostgresType(PGType type)
{
	if (!UnsupportedNumericAsDouble)
		return type;

	PGType		converted = MaybeConvertType(type, NULL);

	return OidIsValid(converted.postgresTypeOid) ? converted : type;
}


/*
 * IcebergStorageFieldForColumnType - see iceberg_representation.h.
 */
Field *
IcebergStorageFieldForColumnType(PGType declaredType, IcebergCompatibilityMode mode,
								 bool forAddColumn, int *subFieldIndex,
								 Field * *surfaceFieldOut)
{
	Field	   *surfaceField = PostgresTypeToIcebergField(declaredType, forAddColumn,
														  subFieldIndex);
	Field	   *storageField = DeepCopyField(surfaceField);

	ApplyCompatibilityStorageMapping(storageField, mode);

	if (surfaceFieldOut != NULL)
		*surfaceFieldOut = surfaceField;

	return storageField;
}


/*
 * TypeHasUnrepresentableLeaf - see iceberg_representation.h.
 */
bool
TypeHasUnrepresentableLeaf(PGType type, bool nestedOnly)
{
	UnsupportedNumericProbe probe = {.nestedOnly = nestedOnly,.found = false};
	Oid			rewrittenOid;
	int32		rewrittenMod;

	ConvertTypeTree(type.postgresTypeOid, type.postgresTypeMod, 0,
					UnsupportedNumericLeafProbe, &probe,
					&rewrittenOid, &rewrittenMod);

	return probe.found;
}


/*
 * UnsupportedNumericLeafProbe is the ConvertTypeTree leaf rule behind
 * TypeHasUnsupportedNumericLeaf.  It records what it sees and always returns
 * false, i.e. never requests a rewrite, which keeps ConvertTypeTree's composite
 * and map branches short of FindOrCreateCompositeTypeFromColumnDefs and
 * GetOrCreatePGMapType: the probe walks the same structure the real rewrite
 * would, and materializes nothing.
 */
static bool
UnsupportedNumericLeafProbe(Oid typeOid, int32 typeMod, int level, void *context,
							Oid *outOid, int32 *outMod)
{
	UnsupportedNumericProbe *probe = (UnsupportedNumericProbe *) context;

	if (IsUnsupportedNumericForIceberg(typeOid, typeMod) &&
		(!probe->nestedOnly || level > 0))
		probe->found = true;

	return false;
}
