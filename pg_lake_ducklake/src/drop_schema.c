/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * object_access_hook for DROP SCHEMA.
 *
 * The seed in pg_lake_ducklake--3.4.sql writes the user-visible PG
 * schemas into lake_ducklake.schema at install time, and
 * DucklakeRegisterTable / DucklakeRenameSchema keep that table in sync
 * for inserts and renames. DROP SCHEMA is the missing edge: a regular
 * ProcessUtility hook only sees the top-level statement, so a
 * DROP SCHEMA myapp CASCADE that pulls dependent objects with it would
 * leave the schema row in lake_ducklake.schema marked live even after
 * the PG-side schema is gone.
 *
 * The object_access_hook fires once per dropped object (including
 * cascade), so we hook there and end-snapshot the row when the dropped
 * object is a namespace.
 */
#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"

#include "pg_lake/ducklake/catalog.h"
#include "pg_lake/ducklake/spi_priv.h"

static object_access_hook_type PreviousObjectAccessHook = NULL;

static void DropSchemaAccessHook(ObjectAccessType access, Oid classId,
								 Oid objectId, int subId, void *arg);
static bool LakeDucklakeSchemaCatalogExists(void);


void
InitializeDucklakeDropSchemaHandler(void)
{
	PreviousObjectAccessHook = object_access_hook;
	object_access_hook = DropSchemaAccessHook;
}


static void
DropSchemaAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
					 int subId, void *arg)
{
	if (PreviousObjectAccessHook)
		PreviousObjectAccessHook(access, classId, objectId, subId, arg);

	if (access != OAT_DROP)
		return;

	if (classId != NamespaceRelationId)
		return;

	/*
	 * Skip when DuckDB's DDL replay is the one issuing the drop -- the
	 * inbound DuckDB transaction already updated lake_ducklake.schema and
	 * we'd just push a redundant version row.
	 */
	if (DucklakeInDDLReplay)
		return;

	/*
	 * Bail out if the extension's own catalog has already been dropped (or
	 * never existed in this database). This covers the DROP EXTENSION
	 * pg_lake_ducklake CASCADE path, where lake_ducklake itself is going away
	 * in the same command.
	 */
	if (!LakeDucklakeSchemaCatalogExists())
		return;

	DucklakeDropSchemaByOid(objectId);
}


/*
 * LakeDucklakeSchemaCatalogExists returns true if lake_ducklake.schema
 * is currently a regular table in the system catalogs. Cheap syscache
 * lookups; safe to call from the object_access_hook hot path.
 */
static bool
LakeDucklakeSchemaCatalogExists(void)
{
	Oid			nspOid;
	Oid			relOid;

	nspOid = get_namespace_oid("lake_ducklake", true);
	if (!OidIsValid(nspOid))
		return false;

	relOid = get_relname_relid("schema", nspOid);
	return OidIsValid(relOid);
}
