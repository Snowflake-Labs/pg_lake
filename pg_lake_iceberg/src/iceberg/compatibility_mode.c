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

/*
 * compatibility_mode.c
 *  Per-table Iceberg "compatibility mode" STORAGE shaping.
 *
 * Some engines that read pg_lake's Iceberg tables have narrower type support
 * than Iceberg itself. The `compatibility_mode` table option opts a table into
 * a storage shape consumable by such an engine WITHOUT changing the PostgreSQL
 * column type the user declared.
 *
 * The only mode today is 'snowflake'. Snowflake cannot store a UUID inside a
 * semi-structured/structured type, so a uuid nested inside an array or
 * composite is stored physically as Iceberg `string`; a top-level uuid column
 * stays native `uuid`. The PostgreSQL column type is left as uuid in every
 * case -- conversion happens at the I/O boundary (uuid->varchar on write,
 * varchar->uuid on read) driven by the persisted storage mapping in
 * lake_table.field_id_mappings, so `\d` and DEFAULT/GENERATED clauses are
 * unaffected.
 *
 * This module supplies the option accessors and the registration-time Iceberg
 * Field-tree walker (ApplyCompatibilityStorageMapping). It deliberately does
 * NOT rewrite the PostgreSQL column type: the read/write codecs recover the
 * conversion from the persisted storage mapping, never from this option.
 *
 * The walker is type-agnostic: it applies a per-leaf POLICY
 * (CompatStorageScalarType) to every nested scalar field. All knowledge of
 * which surface types diverge from their storage type for a given mode lives
 * in that one small policy function, so a new rule (or a new mode) is a single
 * edit there rather than a new walker.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/util/table_type.h"


/*
 * GUC pg_lake_iceberg.default_compatibility_mode (defined in init.c). The
 * default a new table adopts when CREATE does not specify compatibility_mode.
 */
int			IcebergDefaultCompatibilityMode = ICEBERG_COMPAT_AUTO;


static const char *CompatStorageScalarType(const char *surfaceTypeName,
										   IcebergCompatibilityMode mode);
static void ApplyCompatibilityStorageMappingInternal(Field * field, int level,
													 IcebergCompatibilityMode mode);
static bool AnyCompositeFieldContainsMap(Oid typeOid);
static Oid	DomainBaseTypeOneLevel(Oid domainOid);


/*
 * IcebergCompatibilityModeName returns the canonical lowercase option string
 * for a mode. Used when seeding a new table's compatibility_mode option from
 * the GUC default, so the stored value is exactly what ParseIcebergCompatibilityMode
 * accepts.
 */
const char *
IcebergCompatibilityModeName(IcebergCompatibilityMode mode)
{
	switch (mode)
	{
		case ICEBERG_COMPAT_AUTO:
			return "auto";
		case ICEBERG_COMPAT_SNOWFLAKE:
			return "snowflake";
	}

	return "auto";
}


/*
 * ParseIcebergCompatibilityMode maps an option string to the enum. NULL (the
 * option being absent) and 'auto' both mean ICEBERG_COMPAT_AUTO, the default.
 * An unrecognized value is a hard error so the accepted set lives in exactly
 * one place; option validation in pg_lake_table calls straight into this.
 */
IcebergCompatibilityMode
ParseIcebergCompatibilityMode(const char *optionValue)
{
	if (optionValue == NULL)
		return ICEBERG_COMPAT_AUTO;

	if (strcmp(optionValue, "auto") == 0)
		return ICEBERG_COMPAT_AUTO;

	if (strcmp(optionValue, "snowflake") == 0)
		return ICEBERG_COMPAT_SNOWFLAKE;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid %s option: \"%s\"",
					ICEBERG_COMPATIBILITY_MODE_OPTION, optionValue),
			 errhint("Valid values are \"auto\" and \"snowflake\".")));

	return ICEBERG_COMPAT_AUTO; /* keep the compiler happy */
}


/*
 * IcebergCompatibilityModeFromCreateOptions reads the option out of a CREATE
 * statement's WITH (...) DefElem list.
 */
IcebergCompatibilityMode
IcebergCompatibilityModeFromCreateOptions(List *options)
{
	return ParseIcebergCompatibilityMode(
										 GetStringOption(options, ICEBERG_COMPATIBILITY_MODE_OPTION, false));
}


/*
 * IcebergCompatibilityModeFromRelation reads the option from an existing
 * relation's stored foreign-table options. Returns AUTO for non-iceberg
 * relations (e.g. during ALTER on an unrelated table).
 */
IcebergCompatibilityMode
IcebergCompatibilityModeFromRelation(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return ICEBERG_COMPAT_AUTO;

	ForeignTable *foreignTable = GetForeignTable(relationId);

	return ParseIcebergCompatibilityMode(
										 GetStringOption(foreignTable->options, ICEBERG_COMPATIBILITY_MODE_OPTION, false));
}


/*
 * CompatStorageScalarType is the ONE place that knows which surface scalar
 * types diverge from their storage type under a compatibility mode. Given the
 * iceberg type name of a nested scalar leaf, it returns the iceberg type name
 * it must be STORED as, or NULL when the leaf is stored as-is (the common
 * case). Adding a rule (or a mode) is an edit here, not a new walker.
 *
 * snowflake: Snowflake cannot hold a uuid inside a structured type, so a
 * nested uuid is stored as `string`.
 */
static const char *
CompatStorageScalarType(const char *surfaceTypeName, IcebergCompatibilityMode mode)
{
	if (surfaceTypeName == NULL)
		return NULL;

	switch (mode)
	{
		case ICEBERG_COMPAT_SNOWFLAKE:
			if (strcmp(surfaceTypeName, "uuid") == 0)
				return "string";
			break;

		case ICEBERG_COMPAT_AUTO:
			break;
	}

	return NULL;
}


/*
 * ApplyCompatibilityStorageMappingInternal rewrites, in place, the storage type
 * of every nested (level > 0) scalar leaf for which the mode's policy returns a
 * divergent storage type. level 0 is the column itself, so a top-level leaf is
 * left as-is. Maps are not descended: a column containing a map is rejected at
 * DDL time under a restrictive mode, so a divergent leaf can never reach here
 * through a map, and not descending keeps the read/write codecs free of
 * map-key/value conversion handling.
 */
static void
ApplyCompatibilityStorageMappingInternal(Field * field, int level,
										 IcebergCompatibilityMode mode)
{
	if (field == NULL)
		return;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				const char *storageTypeName = (level > 0)
					? CompatStorageScalarType(field->field.scalar.typeName, mode)
					: NULL;

				if (storageTypeName != NULL)
					field->field.scalar.typeName = pstrdup(storageTypeName);
				break;
			}

		case FIELD_TYPE_LIST:
			ApplyCompatibilityStorageMappingInternal(field->field.list.element,
													 level + 1, mode);
			break;

		case FIELD_TYPE_STRUCT:
			for (size_t i = 0; i < field->field.structType.nfields; i++)
				ApplyCompatibilityStorageMappingInternal(field->field.structType.fields[i].type,
														 level + 1, mode);
			break;

		case FIELD_TYPE_MAP:

			/*
			 * Unreachable: this walker only runs for non-auto modes (see
			 * ApplyCompatibilityStorageMapping), and every such mode rejects
			 * map columns at DDL time
			 * (ErrorIfColumnsUnsupportedForCompatibilityMode), so a divergent
			 * leaf can never reach here through a map. The Assert documents
			 * and enforces that invariant; the break keeps production builds
			 * safe if a future mode ever relaxes the DDL rejection.
			 */
			Assert(false);
			break;
	}
}


void
ApplyCompatibilityStorageMapping(Field * field, IcebergCompatibilityMode mode)
{
	if (mode == ICEBERG_COMPAT_AUTO)
		return;

	ApplyCompatibilityStorageMappingInternal(field, 0, mode);
}


/*
 * AnyCompositeFieldContainsMap returns true if any non-dropped attribute of the
 * composite type contains a map at any depth.
 */
static bool
AnyCompositeFieldContainsMap(Oid typeOid)
{
	TupleDesc	tupleDesc = lookup_rowtype_tupdesc(typeOid, -1);
	bool		found = false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeContainsMap(attr->atttypid))
		{
			found = true;
			break;
		}
	}

	ReleaseTupleDesc(tupleDesc);
	return found;
}


/*
 * TypeContainsMap returns true if typeOid is a pg_map or contains one at any
 * nesting depth (array element or composite field). A map's own key/value are
 * not descended: the presence of the map itself is already disqualifying.
 *
 * The map check runs BEFORE any domain resolution, and domains are resolved
 * one level at a time, because pg_map types are themselves domains over
 * arrays; getBaseType() would skip past the map OID and miss it.
 */
bool
TypeContainsMap(Oid typeOid)
{
	if (IsMapTypeOid(typeOid))
		return true;

	if (type_is_array(typeOid))
		return TypeContainsMap(get_element_type(typeOid));

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_COMPOSITE)
		return AnyCompositeFieldContainsMap(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
		return TypeContainsMap(DomainBaseTypeOneLevel(typeOid));

	return false;
}


/*
 * DomainBaseTypeOneLevel returns the immediate base type of a domain (one
 * level of unwrapping). Unlike getBaseType(), it does not chase the whole
 * domain chain, so a pg_map domain nested under a user domain is still seen as
 * a map by the callers above.
 */
static Oid
DomainBaseTypeOneLevel(Oid domainOid)
{
	HeapTuple	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(domainOid));

	if (!HeapTupleIsValid(tp))
		return domainOid;

	Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
	Oid			baseType = typtup->typbasetype;

	ReleaseSysCache(tp);

	return OidIsValid(baseType) ? baseType : domainOid;
}
