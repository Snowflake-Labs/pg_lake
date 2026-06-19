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
 * DDL guards for iceberg_catalog servers, user mappings, and the
 * iceberg_catalog FDW, plus statement-text redaction for USER MAPPING
 * credentials.  Kept separate from rest_catalog.c (option resolution,
 * token cache, HTTP transport, REST operations) so the protection
 * rules can be read in isolation from the runtime path.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/catalog_type.h"


static void IcebergCatalogObjectAccessHook(ObjectAccessType access, Oid classId,
										   Oid objectId, int subId, void *arg);
static void EnsureUserMappingDropAllowed(Oid umOid);
static bool ServerHasDependentRestIcebergTable(Oid serverOid);

static object_access_hook_type PreviousObjectAccessHook = NULL;


/*
 * InitializeIcebergCatalogObjectAccessHook chains
 * IcebergCatalogObjectAccessHook into Postgres' object_access_hook.
 * Called once from _PG_init.
 */
void
InitializeIcebergCatalogObjectAccessHook(void)
{
	PreviousObjectAccessHook = object_access_hook;
	object_access_hook = IcebergCatalogObjectAccessHook;
}


/*
 * IcebergCatalogObjectAccessHook gates DROP USER MAPPING and
 * DROP SERVER ... CASCADE for user-created iceberg_catalog servers
 * with dependent iceberg tables.
 */
static void
IcebergCatalogObjectAccessHook(ObjectAccessType access, Oid classId,
							   Oid objectId, int subId, void *arg)
{
	if (PreviousObjectAccessHook)
		PreviousObjectAccessHook(access, classId, objectId, subId, arg);

	if (access != OAT_DROP)
		return;

	if (classId != UserMappingRelationId)
		return;

	if (!IsExtensionCreated(PgLakeIceberg))
		return;

	EnsureUserMappingDropAllowed(objectId);
}


/*
 * EnsureUserMappingDropAllowed errors out if the user mapping's
 * server is a user-created iceberg_catalog server with dependent
 * iceberg tables.  Called from the OAT_DROP hook on UserMappingRelationId,
 * which fires for both direct DROP USER MAPPING and cascade-driven
 * removal during DROP SERVER ... CASCADE.
 */
static void
EnsureUserMappingDropAllowed(Oid umOid)
{
	HeapTuple	tup = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(umOid));

	if (!HeapTupleIsValid(tup))
		return;

	Oid			serverOid = ((Form_pg_user_mapping) GETSTRUCT(tup))->umserver;

	ReleaseSysCache(tup);

	ForeignServer *server = GetForeignServerExtended(serverOid, FSV_MISSING_OK);

	if (server == NULL)
		return;

	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
		return;

	/* Built-in catalog servers reject CREATE USER MAPPING upfront. */
	if (IsBuiltinCatalogServerName(server->servername))
		return;

	if (!ServerHasDependentRestIcebergTable(serverOid))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot drop the credentials for server \"%s\" "
					"because it has dependent iceberg tables",
					server->servername),
			 errhint("Drop the dependent iceberg tables first.")));
}


/*
 * ServerHasForeignUserMappings returns true if any pg_user_mapping
 * row targets the given foreign server (per-role or PUBLIC).
 *
 * pg_user_mapping has no index keyed on umserver alone, so this is a
 * sequential scan with index_ok = false.  The catalog is small in
 * practice (one row per (role, server) pair).
 */
static bool
ServerHasForeignUserMappings(Oid serverOid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;
	bool		found;

	rel = table_open(UserMappingRelationId, AccessShareLock);

	ScanKeyInit(&key,
				Anum_pg_user_mapping_umserver,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(serverOid));

	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &key);

	tup = systable_getnext(scan);
	found = HeapTupleIsValid(tup);

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return found;
}


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


/* Forward declaration; defined alongside the redaction stack below. */
static bool IsRedactableUserMappingSecret(const char *name);


/*
 * ValidateIcebergCatalogServerDDL validates DDL on iceberg_catalog
 * servers and on user mappings that target them, the iceberg_catalog FDW itself,
 * and extension-membership changes that affect any of these objects.
 *
 * Two layers of protection for server DDL:
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
 *  - CREATE/ALTER USER MAPPING is rejected against the three built-in
 *    catalog servers.  The built-in pg_lake_rest_catalog reads
 *    credentials from GUCs only (ignores pg_user_mapping), while the
 *    postgres / object_store built-ins
 *    have no notion of OAuth credentials at all.  User mappings on
 *    user-created REST servers are unaffected.
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
		 * Block changes to rest_endpoint or oauth_endpoint while the server
		 * has user mappings or dependent iceberg tables.  Both options pin
		 * the URL that the OAuth grant is POSTed to; flipping them under
		 * existing mappings would redirect the mapping owner's credentials.
		 * Match covers ADD/SET/DROP via the same defname check.
		 */
		ListCell   *lc;
		bool		existence_checked = false;
		bool		has_mappings = false;
		bool		has_tables = false;

		foreach(lc, stmt->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (pg_strcasecmp(def->defname, "rest_endpoint") != 0 &&
				pg_strcasecmp(def->defname, "oauth_endpoint") != 0)
				continue;

			if (!existence_checked)
			{
				has_mappings = ServerHasForeignUserMappings(server->serverid);
				has_tables = ServerHasDependentRestIcebergTable(server->serverid);
				existence_checked = true;
			}

			if (has_mappings || has_tables)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change \"%s\" on server \"%s\" "
								"while it has user mappings or "
								"dependent iceberg tables",
								def->defname, stmt->servername),
						 errhint("Drop user mappings and dependent "
								 "iceberg tables first, then ALTER, "
								 "then recreate them.  Changing the "
								 "endpoint on a server in use would "
								 "redirect catalog credentials of every "
								 "mapping owner to the new URL.")));
		}
	}
	else if (IsA(parsetree, CreateUserMappingStmt))
	{
		CreateUserMappingStmt *stmt = (CreateUserMappingStmt *) parsetree;

		/*
		 * Name-only check: the three built-in long names are reserved for
		 * iceberg_catalog servers and can never be backed by any other FDW
		 * (the extension creates them and the CREATE SERVER guard above
		 * blocks anyone from grabbing the name).  No GetForeignServerByName
		 * lookup needed here.
		 */
		RejectIfModifyingBuiltinCatalogServer(stmt->servername,
											  "create user mapping for");
	}
	else if (IsA(parsetree, AlterUserMappingStmt))
	{
		AlterUserMappingStmt *stmt = (AlterUserMappingStmt *) parsetree;

		RejectIfModifyingBuiltinCatalogServer(stmt->servername,
											  "alter user mapping for");

		/*
		 * Block OPTIONS (DROP client_id) / (DROP client_secret) while the
		 * server has dependent iceberg tables.  SET/ADD are rotation, not
		 * removal, and remain allowed.
		 */
		ForeignServer *server = GetForeignServerByName(stmt->servername, true);

		if (server != NULL)
		{
			ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

			if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0 &&
				!IsBuiltinCatalogServerName(server->servername))
			{
				ListCell   *lc;
				bool		dependents_checked = false;
				bool		has_dependents = false;

				foreach(lc, stmt->options)
				{
					DefElem    *def = (DefElem *) lfirst(lc);

					if (def->defaction != DEFELEM_DROP)
						continue;

					if (!IsRedactableUserMappingSecret(def->defname))
						continue;

					if (!dependents_checked)
					{
						has_dependents =
							ServerHasDependentRestIcebergTable(server->serverid);
						dependents_checked = true;
					}

					if (has_dependents)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot drop the \"%s\" option "
										"from the user mapping for "
										"server \"%s\" because it has "
										"dependent iceberg tables",
										def->defname, stmt->servername),
								 errhint("Drop the dependent iceberg "
										 "tables first.")));
				}
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


/*
 * IsRedactableUserMappingSecret returns true for option names whose
 * values are secrets that must never appear in queryString-derived
 * surfaces (pg_stat_statements, log_min_duration_statement, ereport
 * error contexts).  Kept in lockstep with the descriptor table in
 * rest_catalog_options.c: any option carrying credential material
 * should be added here as well.
 */
static bool
IsRedactableUserMappingSecret(const char *name)
{
	return pg_strcasecmp(name, "client_id") == 0 ||
		pg_strcasecmp(name, "client_secret") == 0;
}


/*
 * IsKnownNonIcebergCatalogServer returns true ONLY when `serverName` refers
 * to a real foreign server whose FDW is something other than
 * iceberg_catalog.  Every other input -- a built-in iceberg long name,
 * a real iceberg_catalog server, a typo / not-yet-existent server, or
 * a NULL name -- returns false.
 *
 * Built-in long names (pg_lake_postgres_catalog, ...) are recognised
 * by name without a syscache lookup; they are reserved and only ever
 * back the extension's catalog servers.  This matters because the
 * ValidateIcebergCatalogServerDDL handler is about to ereport on those
 * names, and the redaction stack must already have scrubbed the
 * queryString by then.
 *
 * The asymmetry on missing servers is the security contract: when we
 * cannot prove the target belongs to a different FDW, we treat it as
 * "could be iceberg" so the caller scrubs the queryString anyway.
 * That closes the typo-leak: a fat-finger on the server name in a
 * CREATE USER MAPPING ... OPTIONS (client_id, client_secret) would
 * otherwise survive into pg_stat_statements / log_min_duration_statement
 * / ereport contexts on the failing core lookup.  A real, non-iceberg
 * FDW user mapping is still left alone -- we don't volunteer ourselves
 * as the redactor-of-record for somebody else's option vocabulary.
 *
 * Never raises.
 */
static bool
IsKnownNonIcebergCatalogServer(const char *serverName)
{
	if (serverName == NULL)
		return false;

	if (IsBuiltinCatalogServerName(serverName))
		return false;

	ForeignServer *server = GetForeignServerByName(serverName, /* missing_ok */ true);

	if (server == NULL)
		return false;

	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	return strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0;
}


/*
 * HasRedactableUserMappingSecret returns true if any DefElem in the
 * list carries a credential whose value must not appear in queryString
 * derived surfaces (pg_stat_statements, log_min_duration_statement,
 * ereport error contexts).  DROP options (def->arg == NULL) still
 * count: even naming the dropped credential is fine in cleartext, but
 * we are conservative and treat any reference to a credential option
 * as enough reason to scrub the whole statement.
 */
static bool
HasRedactableUserMappingSecret(List *options)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (IsRedactableUserMappingSecret(def->defname))
			return true;
	}
	return false;
}


/*
 * RedactWholeStatement overwrites the entire queryString slice that
 * corresponds to this single utility statement with a fixed marker,
 * padding with spaces so the buffer stays the same length.  Callers
 * must have already confirmed that the statement is a CREATE/ALTER
 * USER MAPPING targeting an iceberg_catalog server AND that it
 * carries at least one credential option.
 *
 * In-place mutation is essential: pg_stat_statements, ereport error
 * contexts, and log_min_duration_statement all read the queryString
 * pointer AFTER ProcessUtility returns, so overwriting the backing
 * buffer is observed by every downstream consumer.  Actual DDL
 * execution is unaffected because CREATE/ALTER USER MAPPING reads
 * option values from DefElem->arg, not from queryString.
 *
 * pstmt->stmt_location / stmt_len bound the erasure to exactly this
 * statement, so the surrounding statements in a multi-statement
 * queryString are left intact.  Conventions follow CleanQuerytext()
 * in queryjumblefuncs.c: stmt_location < 0 means "unknown" (treat as
 * 0, blank from the start of the buffer), stmt_len <= 0 means
 * "rest of string".
 */
static void
RedactWholeStatement(char *queryString, const PlannedStmt *pstmt)
{
	static const char marker[] = "<redacted: USER MAPPING with credentials>";

	if (queryString == NULL)
		return;

	int			start = pstmt->stmt_location > 0 ? pstmt->stmt_location : 0;
	int			len = pstmt->stmt_len > 0
		? pstmt->stmt_len
		: (int) strlen(queryString) - start;

	if (len <= 0)
		return;

	int			mlen = (int) sizeof(marker) - 1;

	if (mlen > len)
		mlen = len;				/* truncate marker to fit short statements */

	memcpy(queryString + start, marker, mlen);
	memset(queryString + start + mlen, ' ', len - mlen);
}


/*
 * RedactRestCatalogUserMappingSecrets is a ProcessUtility handler
 * that scrubs CREATE/ALTER USER MAPPING when it carries an iceberg
 * credential option (client_id / client_secret) by blanking the whole
 * statement text out of the queryString in place.  Returns false so
 * that downstream handlers (notably ValidateIcebergCatalogServerDDL)
 * and the rest of ProcessUtility continue to run unaffected.
 *
 * The server-name gate is conservative: we redact whenever we cannot
 * affirmatively prove the target is a real foreign server backed by a
 * different FDW.  A typo, a not-yet-existent server, or a built-in
 * iceberg long name all fall into the "could be iceberg" bucket and
 * are scrubbed defensively, since the alternative -- letting the
 * credentials reach pg_stat_statements / log_min_duration_statement /
 * ereport contexts via queryString just because the user fat-fingered
 * the server name -- is exactly the leak we exist to prevent.
 *
 * The "any credential option present" gate means a non-secret
 * ALTER USER MAPPING (e.g. SET scope '...') is left visible in
 * pg_stat_statements -- only statements that actually move credential
 * bytes through queryString get scrubbed.  When credentials are
 * present we redact the entire statement (including non-secret
 * options in the same OPTIONS list and the server name), trading a
 * little ops observability for a much smaller attack surface than a
 * hand-rolled string-literal parser.
 *
 * Registered AFTER ValidateIcebergCatalogServerDDL on purpose:
 * RegisterUtilityStatementHandler prepends to the handler list, so
 * the last registration is the head and runs first.  Redaction must
 * happen before the validator's ereport(ERROR), so that the failing
 * "cannot create user mapping for the built-in ..." path never
 * surfaces an unredacted queryString in its error context.
 */
bool
RedactRestCatalogUserMappingSecrets(ProcessUtilityParams * processUtilityParams,
									void *arg)
{
	Node	   *parsetree = processUtilityParams->plannedStmt->utilityStmt;
	const char *serverName = NULL;
	List	   *options = NIL;

	if (IsA(parsetree, CreateUserMappingStmt))
	{
		CreateUserMappingStmt *stmt = (CreateUserMappingStmt *) parsetree;

		serverName = stmt->servername;
		options = stmt->options;
	}
	else if (IsA(parsetree, AlterUserMappingStmt))
	{
		AlterUserMappingStmt *stmt = (AlterUserMappingStmt *) parsetree;

		serverName = stmt->servername;
		options = stmt->options;
	}
	else
		return false;

	if (options == NIL)
		return false;

	if (!HasRedactableUserMappingSecret(options))
		return false;

	if (IsKnownNonIcebergCatalogServer(serverName))
		return false;

	/*
	 * queryString is typed const char * to discourage incidental mutation,
	 * but the redaction contract is explicitly to scrub the backing buffer in
	 * place so every downstream reader sees the redacted form.  Cast away
	 * const here at the one call site that owns this invariant.
	 */
	RedactWholeStatement((char *) processUtilityParams->queryString,
						 processUtilityParams->plannedStmt);

	return false;
}
