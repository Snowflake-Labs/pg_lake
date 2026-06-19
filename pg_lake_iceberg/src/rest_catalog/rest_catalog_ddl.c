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

/*
 * DDL guards for iceberg_catalog servers and the iceberg_catalog FDW.
 * Kept separate from rest_catalog.c (option resolution, token cache,
 * HTTP transport, REST operations) so the protection rules can be
 * read in isolation from the runtime path.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_server.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"

#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/catalog_type.h"


/*
 * ServerHasDependentRestIcebergTable returns true if the given server
 * has at least one dependent REST-backed iceberg table (read-only or
 * writable) recorded in pg_depend.  Used to block ALTER SERVER changes
 * that would silently break existing tables, since both writable and
 * read-only REST tables make REST API calls at runtime against the
 * server's rest_endpoint.
 */
static bool
ServerHasDependentRestIcebergTable(Oid serverOid)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;
	bool		found = false;

	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ForeignServerRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(serverOid));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);

		if (depForm->classid != RelationRelationId)
			continue;

		IcebergCatalogType type = GetIcebergCatalogType(depForm->objid);

		if (type == REST_CATALOG_READ_WRITE || type == REST_CATALOG_READ_ONLY)
		{
			found = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(depRel, AccessShareLock);

	return found;
}


/*
 * RejectIfBuiltinCatalogServerName errors out if `name` is one of the
 * three internal pg_lake_iceberg catalog server names.  Shared by the
 * CREATE SERVER and ALTER SERVER ... RENAME TO branches, where the
 * concern is "users must not be able to create or end up with a server
 * carrying one of these reserved names".
 */
static void
RejectIfBuiltinCatalogServerName(const char *name)
{
	if (!IsBuiltinCatalogServerName(name))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("server name \"%s\" is reserved for an internal "
					"pg_lake_iceberg catalog server", name),
			 errhint("Choose a different server name.")));
}


/*
 * RejectIfModifyingBuiltinCatalogServer errors out if `name` refers to
 * one of the three built-in pg_lake_iceberg catalog servers.  The
 * `operation` verb fills in the user-facing error ("cannot %s the
 * built-in...") and should read naturally in that template (e.g.
 * "alter", "change owner of").  Shared by the ALTER SERVER OPTIONS
 * and ALTER SERVER ... OWNER TO branches.
 */
static void
RejectIfModifyingBuiltinCatalogServer(const char *name, const char *operation)
{
	if (!IsBuiltinCatalogServerName(name))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot %s the built-in pg_lake_iceberg "
					"catalog server \"%s\"", operation, name),
			 errhint("Configure built-in catalogs via "
					 "pg_lake_iceberg GUCs, or create a "
					 "user-defined iceberg_catalog server.")));
}


/*
 * ValidateIcebergCatalogServerDDL validates DDL on iceberg_catalog
 * servers and the iceberg_catalog FDW itself, and
 * extension-membership changes that affect any of these objects.
 *
 * Two layers of protection:
 *
 * 1. Short reserved names ('postgres', 'object_store', 'rest') -- these
 *    are the user-facing catalog= values.  We block CREATE SERVER and
 *    RENAME TO these names so users can't shadow the built-in catalogs.
 *
 * 2. Built-in long server names ('pg_lake_postgres_catalog', etc.) --
 *    these are the pre-created anchors.  Outside of CREATE/ALTER
 *    EXTENSION we block CREATE/ALTER/RENAME/OWNER on them entirely so
 *    they remain pure structural anchors with all configuration in
 *    GUCs.  DROP is blocked by core via extension membership.
 *
 * Additionally:
 *  - CREATE SERVER must specify TYPE 'rest'; TYPE 'postgres' and
 *    'object_store' are reserved for the built-in servers.
 *  - Renaming any iceberg_catalog server is blocked (dependent tables
 *    record the server name as a string option in ftoptions).
 *  - For user-created REST servers, ALTER SERVER OPTIONS may not change
 *    rest_endpoint while dependent iceberg tables exist.
 *  - ALTER EXTENSION ... DROP SERVER/FOREIGN DATA WRAPPER is rejected
 *    for iceberg_catalog objects, preventing detachment of the
 *    DEPENDENCY_EXTENSION edge that shields them from standalone DROP.
 *  - ALTER FOREIGN DATA WRAPPER iceberg_catalog is rejected, preventing
 *    replacement of the validator or addition of a handler.
 *
 * The last two are defense-in-depth against superuser misuse: both
 * require superuser privilege, but a superuser detaching the
 * extension edge or swapping the validator would silently weaken the
 * protection model.
 */
bool
ValidateIcebergCatalogServerDDL(ProcessUtilityParams * processUtilityParams,
								void *arg)
{
	Node	   *parsetree = processUtilityParams->plannedStmt->utilityStmt;

	if (creating_extension)
		return false;

	if (IsA(parsetree, CreateForeignServerStmt))
	{
		CreateForeignServerStmt *stmt = (CreateForeignServerStmt *) parsetree;

		if (stmt->fdwname == NULL ||
			strcmp(stmt->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
			return false;

		if (IsCatalogOwnedByExtension(stmt->servername))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("server name \"%s\" is reserved for the extension-owned catalog",
							stmt->servername),
					 errhint("Choose a different server name.")));

		RejectIfBuiltinCatalogServerName(stmt->servername);

		if (stmt->servertype != NULL &&
			(pg_strcasecmp(stmt->servertype, POSTGRES_CATALOG_NAME) == 0 ||
			 pg_strcasecmp(stmt->servertype, OBJECT_STORE_CATALOG_NAME) == 0))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot create iceberg_catalog server with TYPE '%s'",
							stmt->servertype),
					 errhint("Use the built-in \"%s\" or \"%s\" catalogs, "
							 "or create a server of type 'rest'.",
							 POSTGRES_CATALOG_NAME, OBJECT_STORE_CATALOG_NAME)));

		if (stmt->servertype == NULL ||
			pg_strcasecmp(stmt->servertype, REST_CATALOG_NAME) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("iceberg_catalog server requires TYPE 'rest'")));
	}
	else if (IsA(parsetree, RenameStmt))
	{
		RenameStmt *stmt = (RenameStmt *) parsetree;

		if (stmt->renameType != OBJECT_FOREIGN_SERVER)
			return false;

		if (IsCatalogOwnedByExtension(stmt->newname))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("server name \"%s\" is reserved for the extension-owned catalog",
							stmt->newname),
					 errhint("Choose a different server name.")));

		RejectIfBuiltinCatalogServerName(stmt->newname);

		/*
		 * Renaming an iceberg_catalog server is blocked because dependent
		 * iceberg tables store the server name as a string option
		 * (catalog='<name>') in pg_foreign_table.ftoptions.  A rename would
		 * silently break those references.
		 */
		ForeignServer *server = GetForeignServerByName(strVal(stmt->object),
													   true);

		if (server != NULL)
		{
			ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

			if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rename iceberg_catalog server \"%s\"",
								strVal(stmt->object)),
						 errhint("Drop and recreate the server with the new name.")));
		}
	}
	else if (IsA(parsetree, AlterOwnerStmt))
	{
		AlterOwnerStmt *stmt = (AlterOwnerStmt *) parsetree;

		if (stmt->objectType != OBJECT_FOREIGN_SERVER)
			return false;

		const char *serverName = strVal(stmt->object);

		RejectIfModifyingBuiltinCatalogServer(serverName, "change owner of");
	}
	else if (IsA(parsetree, AlterForeignServerStmt))
	{
		AlterForeignServerStmt *stmt = (AlterForeignServerStmt *) parsetree;

		ForeignServer *server = GetForeignServerByName(stmt->servername, true);

		if (server == NULL)
			return false;

		ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

		if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
			return false;

		/*
		 * The three built-in servers are immutable structural anchors.
		 * Configuration for the built-in catalogs lives in GUCs; reject any
		 * option, version, or other ALTER change so we never end up with
		 * hidden server-side state that pg_dump cannot round-trip cleanly for
		 * an extension-owned object.
		 */
		RejectIfModifyingBuiltinCatalogServer(stmt->servername, "alter");

		/*
		 * Changing rest_endpoint on a user-created server with dependent
		 * iceberg tables (writable or read-only) would silently point them at
		 * a different REST catalog, breaking the metadata chain.
		 */
		ListCell   *lc;

		foreach(lc, stmt->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (pg_strcasecmp(def->defname, "rest_endpoint") == 0 &&
				ServerHasDependentRestIcebergTable(server->serverid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change \"rest_endpoint\" on server \"%s\" "
								"because it has dependent iceberg tables",
								stmt->servername),
						 errhint("Drop the dependent tables first, or create a "
								 "new server with the desired endpoint.")));
			}
		}
	}
	else if (IsA(parsetree, AlterExtensionContentsStmt))
	{
		AlterExtensionContentsStmt *stmt =
			(AlterExtensionContentsStmt *) parsetree;

		/*
		 * Block ALTER EXTENSION pg_lake_iceberg DROP SERVER / DROP FOREIGN
		 * DATA WRAPPER for iceberg_catalog objects.  Without this, a
		 * superuser could detach the DEPENDENCY_EXTENSION edge and then DROP
		 * the object freely, breaking every table that depends on it.
		 *
		 * Only the DROP direction (action < 0) is dangerous; ADD is benign
		 * (it re-attaches membership, which is the state we want).
		 */
		if (stmt->action < 0)
		{
			if (stmt->objtype == OBJECT_FOREIGN_SERVER)
			{
				const char *serverName = strVal(stmt->object);

				if (IsBuiltinCatalogServerName(serverName))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot remove the built-in catalog "
									"server \"%s\" from its extension",
									serverName),
							 errhint("The built-in catalog servers are "
									 "structural anchors; detaching them "
									 "from the extension would allow DROP "
									 "and break every dependent table.")));
			}
			else if (stmt->objtype == OBJECT_FDW)
			{
				const char *fdwName = strVal(stmt->object);

				if (strcmp(fdwName, ICEBERG_CATALOG_FDW_NAME) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot remove the \"%s\" foreign data "
									"wrapper from its extension",
									ICEBERG_CATALOG_FDW_NAME),
							 errhint("The iceberg_catalog FDW is required by "
									 "all catalog servers; detaching it would "
									 "allow DROP and break the entire catalog "
									 "infrastructure.")));
			}
		}
	}
	else if (IsA(parsetree, AlterFdwStmt))
	{
		AlterFdwStmt *stmt = (AlterFdwStmt *) parsetree;

		/*
		 * Block ALTER FOREIGN DATA WRAPPER iceberg_catalog entirely.
		 * Replacing the validator would silently disable option checking on
		 * all future CREATE/ALTER SERVER and USER MAPPING statements; adding
		 * a handler would change the FDW semantics.  Both require superuser,
		 * but the downstream damage is hard to notice and impossible to undo
		 * without recreating every dependent server.
		 */
		if (strcmp(stmt->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter the \"%s\" foreign data wrapper",
							ICEBERG_CATALOG_FDW_NAME),
					 errhint("The iceberg_catalog FDW is managed by the "
							 "pg_lake_iceberg extension and must not be "
							 "modified directly.")));
	}

	return false;
}
