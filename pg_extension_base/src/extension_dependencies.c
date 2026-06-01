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
#include "miscadmin.h"

#include "access/xact.h"
#include "commands/extension.h"
#include "pg_extension_base/extension_control_file.h"
#include "pg_extension_base/extension_dependencies.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "tcop/utility.h"
#include "catalog/pg_authid.h"
#include "utils/acl.h"
#include "utils/guc.h"


static void ExtensionDependencyInstallerHook(PlannedStmt *plannedStmt,
											 const char *queryString,
											 bool readOnlyTree,
											 ProcessUtilityContext context,
											 ParamListInfo params,
											 struct QueryEnvironment *queryEnv,
											 DestReceiver *dest,
											 QueryCompletion *completionTag);
static void HandleAlterExtensionStmt(AlterExtensionStmt *alterExtension);
static void HandleCreateExtensionStmt(CreateExtensionStmt *createExtension);
static void EnsureExtensionExists(char *extensionName);
static void EnsureExtensionIsUpdated(char *extensionName);
static bool HasOption(List *options, char *optionName);
static bool ShouldEscalateForDependency(void);


/* whether to auto-install new dependencies */
bool		EnableExtensionDependencyCreate = true;

/* whether to auto-update existing dependencies */
bool		EnableExtensionDependencyUpdate = true;

/*
 * Optional role name; if the current user is a member of this role, the
 * synthesized CREATE/ALTER EXTENSION calls used to install or update
 * dependencies will run as the bootstrap superuser. This lets pg_extension_base
 * escalate dependency work even when the surrounding session is not running
 * under a privileged escalator (e.g. snowflake_user). Empty disables the
 * behavior.
 */
char	   *DependencyEscalationRoleName = "";

/* previous hooks */
static ProcessUtility_hook_type PrevProcessUtility = NULL;


/*
 * InitializeExtensionDependencyInstaller sets the ProcessUtility hook for
 * intercepting ALTER EXTENSION commands.
 */
void
InitializeExtensionDependencyInstaller(void)
{
	PrevProcessUtility = (ProcessUtility_hook != NULL) ?
		ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = ExtensionDependencyInstallerHook;
}


/*
 * ExtensionDependencyInstallerHook is the ProcessUtility hook for handling
 * extension dependencies.
 */
static void
ExtensionDependencyInstallerHook(PlannedStmt *plannedStmt,
								 const char *queryString,
								 bool readOnlyTree,
								 ProcessUtilityContext context,
								 ParamListInfo params,
								 struct QueryEnvironment *queryEnv,
								 DestReceiver *dest,
								 QueryCompletion *completionTag)
{
	Node	   *parsetree = plannedStmt->utilityStmt;

	if (IsA(parsetree, AlterExtensionStmt))
	{
		AlterExtensionStmt *alterExtension = (AlterExtensionStmt *) parsetree;

		HandleAlterExtensionStmt(alterExtension);
	}
	else if (IsA(parsetree, CreateExtensionStmt))
	{
		CreateExtensionStmt *createExtension = (CreateExtensionStmt *) parsetree;

		HandleCreateExtensionStmt(createExtension);
	}

	PrevProcessUtility(plannedStmt, queryString, readOnlyTree, context,
					   params, queryEnv, dest, completionTag);
}


/*
 * HandleAlterExtensionStmt runs before an ALTER EXTENSION .. UPDATE statement.
 *
 */
static void
HandleAlterExtensionStmt(AlterExtensionStmt *alterExtension)
{
	if (!EnableExtensionDependencyCreate && !EnableExtensionDependencyUpdate)
		return;

	char	   *extensionName = alterExtension->extname;

	/* if the extension does not exist, let postgres figure out what to do */
	bool		missingOk = true;
	Oid			extensionId = get_extension_oid(extensionName, missingOk);

	if (extensionId == InvalidOid)
		return;

	/*
	 * We only update dependencies to latest version when updating the parent
	 * to the latest version.
	 */
	bool		hasVersion = HasOption(alterExtension->options, "new_version");

	/* make sure all dependencies exists and are at the latest version */
	List	   *dependencyList = GetExtensionDependencyList(extensionName);

	ListCell   *dependencyCell = NULL;

	foreach(dependencyCell, dependencyList)
	{
		char	   *dependencyName = lfirst(dependencyCell);

		bool		missingOk = true;
		Oid			extensionId = get_extension_oid(dependencyName, missingOk);

		if (extensionId == InvalidOid && EnableExtensionDependencyCreate)
			EnsureExtensionExists(dependencyName);
		else if (!hasVersion && EnableExtensionDependencyUpdate)
			EnsureExtensionIsUpdated(dependencyName);
	}
}


/*
 * HandleCreateExtensionStmt runs before an CREATE EXTENSION .. statement and
 * updates any dependencies before going back to ProcessUtility to create the
 * extension.
 */
static void
HandleCreateExtensionStmt(CreateExtensionStmt *createExtension)
{
	if (!EnableExtensionDependencyUpdate)
		return;

	/*
	 * We only update dependencies to latest version when creating the parent
	 * using the latest version.
	 */
	if (HasOption(createExtension->options, "new_version"))
		return;

	char	   *extensionName = createExtension->extname;
	List	   *dependencyList = GetExtensionDependencyList(extensionName);
	ListCell   *dependencyCell = NULL;

	foreach(dependencyCell, dependencyList)
	{
		char	   *dependencyName = lfirst(dependencyCell);

		bool		missingOk = true;
		Oid			extensionId = get_extension_oid(dependencyName, missingOk);

		/*
		 * If the extension exists, we update it. Otherwise, leave it to the
		 * CASCADE logic in regular ProcessUtility.
		 */
		if (extensionId != InvalidOid)
			EnsureExtensionIsUpdated(dependencyName);
	}
}


/*
 * EnsureExtensionExists creates an extension if it does not exist.
 */
static void
EnsureExtensionExists(char *extensionName)
{
	CreateExtensionStmt *createExtensionStmt = makeNode(CreateExtensionStmt);

	DefElem    *cascadeOption = makeDefElem("cascade", (Node *) makeInteger(true), -1);

	createExtensionStmt->extname = extensionName;
	createExtensionStmt->if_not_exists = true;
	createExtensionStmt->options = list_make1(cascadeOption);

	Oid			savedUserId = InvalidOid;
	int			savedSecContext = 0;
	bool		escalated = ShouldEscalateForDependency();

	if (escalated)
	{
		GetUserIdAndSecContext(&savedUserId, &savedSecContext);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   savedSecContext | SECURITY_LOCAL_USERID_CHANGE);
	}

	PG_TRY();
	{
		CreateExtension(NULL, createExtensionStmt);
		CommandCounterIncrement();
	}
	PG_FINALLY();
	{
		if (escalated)
			SetUserIdAndSecContext(savedUserId, savedSecContext);
	}
	PG_END_TRY();
}


/*
 * EnsureExtensionIsUpdated alters an extension to make sure it's updated.
 */
static void
EnsureExtensionIsUpdated(char *extensionName)
{
	int			gucNestLevel = NewGUCNestLevel();

	/*
	 * ALTER EXTENSION suppresses notices emitted by extension script. We
	 * prefer to suppress the notice emitted by ALTER EXTENSION as well.
	 *
	 * If the user changed client_min_messages to debug, we do want to show
	 * these messages. If they changed it to >= warning, then we don't need to
	 * do anything here. Hence, we only change it if is currently notice (the
	 * default).
	 */
	if (client_min_messages == NOTICE)
	{
		(void) set_config_option("client_min_messages", "warning",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	}

	AlterExtensionStmt *alterExtensionStmt = makeNode(AlterExtensionStmt);

	alterExtensionStmt->extname = extensionName;
	alterExtensionStmt->options = NIL;

	/* recursively update/create dependencies */
	check_stack_depth();
	HandleAlterExtensionStmt(alterExtensionStmt);

	Oid			savedUserId = InvalidOid;
	int			savedSecContext = 0;
	bool		escalated = ShouldEscalateForDependency();

	if (escalated)
	{
		GetUserIdAndSecContext(&savedUserId, &savedSecContext);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   savedSecContext | SECURITY_LOCAL_USERID_CHANGE);
	}

	PG_TRY();
	{
		ExecAlterExtensionStmt(NULL, alterExtensionStmt);
		CommandCounterIncrement();
	}
	PG_FINALLY();
	{
		if (escalated)
			SetUserIdAndSecContext(savedUserId, savedSecContext);
	}
	PG_END_TRY();

	AtEOXact_GUC(true, gucNestLevel);
}


/*
 * ShouldEscalateForDependency returns true when the configured escalation role
 * is non-empty and the current user is a member of it. The role is resolved
 * each call so that SET ROLE / RESET ROLE inside the session is honored. If the
 * configured role does not exist we silently treat it as "no escalation"; an
 * unresolvable role is a configuration issue and we don't want to break
 * unrelated DDL on the chance it happens to escalate dependencies.
 */
static bool
ShouldEscalateForDependency(void)
{
	if (DependencyEscalationRoleName == NULL ||
		DependencyEscalationRoleName[0] == '\0')
		return false;

	/* already running with full privileges; nothing to do */
	if (superuser())
		return false;

	Oid			roleOid = get_role_oid(DependencyEscalationRoleName, true);

	if (!OidIsValid(roleOid))
		return false;

	return is_member_of_role(GetUserId(), roleOid);
}


/*
 * HasOption returns whether an option with the given name appears
 * in the options list.
 */
static bool
HasOption(List *options, char *optionName)
{
	ListCell   *optionCell = NULL;

	foreach(optionCell, options)
	{
		DefElem    *defel = (DefElem *) lfirst(optionCell);

		if (strcmp(defel->defname, optionName) == 0)
		{
			return true;
		}
	}

	return false;
}
