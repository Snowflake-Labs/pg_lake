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
 *  Per-table Iceberg "compatibility mode" option recognition and DDL guards.
 *
 * Some engines that read pg_lake's Iceberg tables have narrower type support
 * than Iceberg itself. The `compatibility_mode` table option opts a table into
 * a storage shape consumable by such an engine WITHOUT changing the PostgreSQL
 * column type the user declared.
 *
 * This module is the option layer: it parses/validates the option value
 * ('auto' or 'snowflake'), exposes accessors that read the mode off a CREATE
 * statement or an existing relation, and provides TypeContainsMap so the DDL
 * paths can reject types a restrictive mode cannot represent (a pg_map under
 * 'snowflake'). On its own this layer is a pure storage no-op: choosing
 * 'snowflake' is accepted and validated, but no surface->storage divergence is
 * yet recorded or applied.
 *
 * The actual storage shaping (e.g. storing a nested uuid as Iceberg `string`)
 * and its persistence in lake_table.field_id_mappings are layered on top of
 * this option by the surface/storage type-mapping change; the read/write
 * codecs then recover the conversion from the persisted mapping, never from
 * this option directly.
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
