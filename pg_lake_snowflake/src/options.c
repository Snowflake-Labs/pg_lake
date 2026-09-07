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
 * options.c
 *
 * The options of a Snowflake foreign server, its user mappings, its foreign
 * tables and their columns, plus the resolution of those options into the
 * SnowflakeConnection and SnowflakeTable structs the rest of the extension
 * works with.
 *
 * Identifier casing lives here as well. Snowflake folds unquoted identifiers
 * to upper case and Postgres folds them to lower case, so a name that arrived
 * through an option is used exactly as written, and a name that came from the
 * Postgres catalog is upper-cased when it is a plain lower-case name (the
 * reversible case) and used as written otherwise.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/reloptions.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"

/* SnowflakeOption describes one option and where it may appear */
typedef struct SnowflakeOption
{
	const char *optionName;
	Oid			catalogId;
	bool		superuserOnly;
}			SnowflakeOption;

static SnowflakeOption SnowflakeOptions[] =
{
	/* server options */
	{
		"account", ForeignServerRelationId, false
	},
	{
		"account_url", ForeignServerRelationId, false
	},
	{
		"database", ForeignServerRelationId, false
	},
	{
		"schema_name", ForeignServerRelationId, false
	},
	{
		"warehouse", ForeignServerRelationId, false
	},
	{
		"role", ForeignServerRelationId, false
	},
	{
		"statement_timeout", ForeignServerRelationId, false
	},
	{
		"enable_aggregate_pushdown", ForeignServerRelationId, false
	},

	/* user mapping options */
	{
		"user", UserMappingRelationId, false
	},
	{
		"authenticator", UserMappingRelationId, false
	},
	{
		"token", UserMappingRelationId, false
	},
	{
		"private_key", UserMappingRelationId, false
	},
	{
		"private_key_path", UserMappingRelationId, true
	},
	{
		"private_key_passphrase", UserMappingRelationId, false
	},

	/* foreign table options */
	{
		"database", ForeignTableRelationId, false
	},
	{
		"schema_name", ForeignTableRelationId, false
	},
	{
		"table_name", ForeignTableRelationId, false
	},
	{
		"row_estimate", ForeignTableRelationId, false
	},

	/* column options */
	{
		"column_name", AttributeRelationId, false
	},

	{
		NULL, InvalidOid, false
	}
};

PG_FUNCTION_INFO_V1(pg_lake_snowflake_validator);

static SnowflakeOption * FindSnowflakeOption(const char *optionName, Oid catalogId);
static char *KnownOptionNames(Oid catalogId);
static void ValidateAccountUrlOption(const char *accountUrl);
static char *AccountUrlFromAccount(const char *account);
static char *NormalizeAccountUrl(const char *accountUrl);
static char *AccountFromAccountUrl(const char *accountUrl);
static SnowflakeAuthMethod AuthMethodFromName(const char *authenticator);
static char *ReadPrivateKeyFile(const char *path);
static bool IsPlainLowerCaseIdentifier(const char *identifier);
static char *FindOption(List *options, const char *optionName);


/*
 * pg_lake_snowflake_validator checks the options of a Snowflake foreign
 * server, user mapping, foreign table or column.
 */
Datum
pg_lake_snowflake_validator(PG_FUNCTION_ARGS)
{
	List	   *optionList = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalogId = PG_GETARG_OID(1);
	ListCell   *optionCell = NULL;
	bool		hasAccount = false;
	bool		hasAccountUrl = false;
	bool		hasToken = false;
	bool		hasPrivateKey = false;

	foreach(optionCell, optionList)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);
		const char *optionName = option->defname;
		char	   *optionValue = defGetString(option);
		SnowflakeOption *definition = FindSnowflakeOption(optionName, catalogId);

		if (definition == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", optionName),
					 errhint("Valid options in this context are: %s",
							 KnownOptionNames(catalogId))));
		}

		if (definition->superuserOnly && !superuser())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("option \"%s\" may only be set by a superuser", optionName),
					 errdetail("Reading a private key from a file happens as the "
							   "operating system user that runs PostgreSQL."),
					 errhint("Use the \"private_key\" option to pass the key itself "
							 "instead of a path to it.")));
		}

		if (strcmp(optionName, "account") == 0)
		{
			hasAccount = true;

			if (strchr(optionValue, '.') != NULL || strchr(optionValue, '/') != NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("\"account\" must be an account identifier, not a host name"),
						 errhint("Use \"account_url\" to point at a specific host.")));
			}
		}
		else if (strcmp(optionName, "account_url") == 0)
		{
			hasAccountUrl = true;
			ValidateAccountUrlOption(optionValue);
		}
		else if (strcmp(optionName, "statement_timeout") == 0)
		{
			int			timeoutSeconds = 0;

			if (!parse_int(optionValue, &timeoutSeconds, 0, NULL) || timeoutSeconds < 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("\"statement_timeout\" must be a non-negative "
								"number of seconds, not \"%s\"", optionValue)));
			}
		}
		else if (strcmp(optionName, "enable_aggregate_pushdown") == 0)
		{
			bool		pushdownEnabled = false;

			if (!parse_bool(optionValue, &pushdownEnabled))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("\"enable_aggregate_pushdown\" must be a boolean, "
								"not \"%s\"", optionValue)));
			}
		}
		else if (strcmp(optionName, "row_estimate") == 0)
		{
			double		rowEstimate = 0;

			if (!parse_real(optionValue, &rowEstimate, 0, NULL) || rowEstimate < 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("\"row_estimate\" must be a non-negative number, "
								"not \"%s\"", optionValue)));
			}
		}
		else if (strcmp(optionName, "authenticator") == 0)
		{
			if (AuthMethodFromName(optionValue) == SNOWFLAKE_AUTH_UNSPECIFIED)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("unrecognized authenticator \"%s\"", optionValue),
						 errhint("Valid authenticators are: pat, oauth, keypair.")));
			}
		}
		else if (strcmp(optionName, "token") == 0)
		{
			hasToken = true;
		}
		else if (strcmp(optionName, "private_key") == 0 ||
				 strcmp(optionName, "private_key_path") == 0)
		{
			hasPrivateKey = true;
		}

		if (optionValue[0] == '\0')
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("option \"%s\" cannot be empty", optionName)));
		}
	}

	if (catalogId == ForeignServerRelationId && !hasAccount && !hasAccountUrl)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("a Snowflake server requires an \"account\" or an \"account_url\" option"),
				 errhint("For example: OPTIONS (account 'myorg-myaccount', "
						 "warehouse 'my_wh')")));
	}

	if (catalogId == UserMappingRelationId && !hasToken && !hasPrivateKey)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("a Snowflake user mapping requires a \"token\" or a "
						"\"private_key\" option"),
				 errhint("Use \"token\" for a programmatic access token or an OAuth "
						 "token, or \"private_key\" together with \"user\" for "
						 "key-pair authentication.")));
	}

	if (catalogId == UserMappingRelationId && hasToken && hasPrivateKey)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("a Snowflake user mapping cannot combine \"token\" with a "
						"private key")));
	}

	PG_RETURN_VOID();
}


/*
 * FindSnowflakeOption returns the definition of the given option in the given
 * catalog, or NULL when the option does not belong there.
 */
static SnowflakeOption *
FindSnowflakeOption(const char *optionName, Oid catalogId)
{
	for (const SnowflakeOption * option = SnowflakeOptions; option->optionName; option++)
	{
		if (option->catalogId == catalogId && strcmp(option->optionName, optionName) == 0)
			return option;
	}

	return NULL;
}


/*
 * KnownOptionNames lists the options that are valid in the given catalog, for
 * the hint of an invalid-option error.
 */
static char *
KnownOptionNames(Oid catalogId)
{
	StringInfo	names = makeStringInfo();

	for (const SnowflakeOption * option = SnowflakeOptions; option->optionName; option++)
	{
		if (option->catalogId != catalogId)
			continue;

		if (names->len > 0)
			appendStringInfoString(names, ", ");

		appendStringInfoString(names, option->optionName);
	}

	if (names->len == 0)
		appendStringInfoString(names, "(none)");

	return names->data;
}


/*
 * ValidateAccountUrlOption rejects an account_url that is not a bare https
 * host. A path or a query would let a server point the requests at an
 * arbitrary endpoint of that host, and plain http would send the credentials
 * in the clear, which is only allowed for tests that set allow_plain_http.
 */
static void
ValidateAccountUrlOption(const char *accountUrl)
{
	const char *host = NULL;

	if (pg_strncasecmp(accountUrl, "https://", 8) == 0)
	{
		host = accountUrl + 8;
	}
	else if (pg_strncasecmp(accountUrl, "http://", 7) == 0)
	{
		if (!SnowflakeAllowPlainHttp)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("\"account_url\" must use https"),
					 errdetail("Credentials travel in the request headers.")));
		}

		host = accountUrl + 7;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("\"account_url\" must start with https://"),
				 errdetail("The value was \"%s\".", accountUrl)));
	}

	if (host[0] == '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("\"account_url\" does not contain a host name")));
	}

	/* a trailing slash is accepted and stripped, anything else is not */
	const char *slash = strchr(host, '/');

	if (slash != NULL && slash[1] != '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("\"account_url\" must be a host name without a path"),
				 errdetail("The value was \"%s\".", accountUrl)));
	}

	if (strpbrk(host, "?#@ ") != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("\"account_url\" must be a plain host name"),
				 errdetail("The value was \"%s\".", accountUrl)));
	}
}


/*
 * AccountUrlFromAccount builds the default URL of an account identifier.
 */
static char *
AccountUrlFromAccount(const char *account)
{
	return psprintf("https://%s.snowflakecomputing.com", account);
}


/*
 * NormalizeAccountUrl strips a trailing slash so that paths can be appended.
 */
static char *
NormalizeAccountUrl(const char *accountUrl)
{
	size_t		urlLength = strlen(accountUrl);

	while (urlLength > 0 && accountUrl[urlLength - 1] == '/')
		urlLength--;

	return pnstrdup(accountUrl, urlLength);
}


/*
 * AccountFromAccountUrl derives the account identifier from a host name, which
 * is its first label in upper case. This is only a guess for a regional or
 * private-link host, so key-pair authentication asks for an explicit account
 * rather than relying on it.
 */
static char *
AccountFromAccountUrl(const char *accountUrl)
{
	const char *host = strstr(accountUrl, "://");

	if (host == NULL)
		return NULL;

	host += 3;

	const char *labelEnd = strchr(host, '.');

	if (labelEnd == NULL || labelEnd == host)
		return NULL;

	return str_toupper(host, labelEnd - host, DEFAULT_COLLATION_OID);
}


/*
 * AuthMethodFromName maps the authenticator option to its method.
 */
static SnowflakeAuthMethod
AuthMethodFromName(const char *authenticator)
{
	if (pg_strcasecmp(authenticator, "pat") == 0 ||
		pg_strcasecmp(authenticator, "programmatic_access_token") == 0)
		return SNOWFLAKE_AUTH_PAT;

	if (pg_strcasecmp(authenticator, "oauth") == 0)
		return SNOWFLAKE_AUTH_OAUTH;

	if (pg_strcasecmp(authenticator, "keypair") == 0 ||
		pg_strcasecmp(authenticator, "snowflake_jwt") == 0)
		return SNOWFLAKE_AUTH_KEYPAIR;

	return SNOWFLAKE_AUTH_UNSPECIFIED;
}


/*
 * FindOption returns the value of an option in a DefElem list, or NULL.
 */
static char *
FindOption(List *options, const char *optionName)
{
	ListCell   *optionCell = NULL;

	foreach(optionCell, options)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, optionName) == 0)
			return defGetString(option);
	}

	return NULL;
}


/*
 * GetSnowflakeConnection resolves the options of a foreign server and of the
 * user mapping that applies to the given user.
 */
SnowflakeConnection *
GetSnowflakeConnection(Oid serverId, Oid userId)
{
	ForeignServer *server = GetForeignServer(serverId);
	SnowflakeConnection *connection = palloc0(sizeof(SnowflakeConnection));

	connection->serverId = serverId;
	connection->userId = userId;
	connection->serverName = pstrdup(server->servername);
	connection->statementTimeoutSeconds = SnowflakeStatementTimeoutSeconds;
	connection->enableAggregatePushdown = SnowflakeEnableAggregatePushdown;

	char	   *account = FindOption(server->options, "account");
	char	   *accountUrl = FindOption(server->options, "account_url");

	if (accountUrl != NULL)
	{
		connection->accountUrl = NormalizeAccountUrl(accountUrl);
		connection->account = account != NULL ? pstrdup(account) :
			AccountFromAccountUrl(connection->accountUrl);
	}
	else if (account != NULL)
	{
		connection->accountUrl = AccountUrlFromAccount(account);
		connection->account = pstrdup(account);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("foreign server \"%s\" has no \"account\" or "
						"\"account_url\" option", server->servername)));
	}

	connection->database = FindOption(server->options, "database");
	connection->schemaName = FindOption(server->options, "schema_name");
	connection->warehouse = FindOption(server->options, "warehouse");
	connection->role = FindOption(server->options, "role");

	char	   *statementTimeout = FindOption(server->options, "statement_timeout");

	if (statementTimeout != NULL)
		(void) parse_int(statementTimeout, &connection->statementTimeoutSeconds, 0, NULL);

	char	   *aggregatePushdown = FindOption(server->options, "enable_aggregate_pushdown");

	if (aggregatePushdown != NULL)
		(void) parse_bool(aggregatePushdown, &connection->enableAggregatePushdown);

	/*
	 * A user mapping carries the credentials, so a missing one is the most
	 * common first-use error and deserves a usable hint.
	 */
	if (!OidIsValid(GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
									ObjectIdGetDatum(userId), ObjectIdGetDatum(serverId))) &&
		!OidIsValid(GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
									ObjectIdGetDatum(InvalidOid), ObjectIdGetDatum(serverId))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no user mapping for user \"%s\" on server \"%s\"",
						GetUserNameFromId(userId, false), server->servername),
				 errhint("CREATE USER MAPPING FOR CURRENT_USER SERVER %s "
						 "OPTIONS (token '...');", quote_identifier(server->servername))));
	}

	UserMapping *userMapping = GetUserMapping(userId, serverId);

	connection->userName = FindOption(userMapping->options, "user");
	connection->token = FindOption(userMapping->options, "token");
	connection->privateKeyPassphrase = FindOption(userMapping->options,
												  "private_key_passphrase");

	char	   *privateKey = FindOption(userMapping->options, "private_key");
	char	   *privateKeyPath = FindOption(userMapping->options, "private_key_path");

	if (privateKey != NULL)
		connection->privateKeyPem = privateKey;
	else if (privateKeyPath != NULL)
		connection->privateKeyPem = ReadPrivateKeyFile(privateKeyPath);

	char	   *authenticator = FindOption(userMapping->options, "authenticator");

	if (authenticator != NULL)
		connection->authMethod = AuthMethodFromName(authenticator);
	else if (connection->privateKeyPem != NULL)
		connection->authMethod = SNOWFLAKE_AUTH_KEYPAIR;
	else
		connection->authMethod = SNOWFLAKE_AUTH_PAT;

	if (connection->authMethod == SNOWFLAKE_AUTH_KEYPAIR)
	{
		if (connection->privateKeyPem == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("key-pair authentication on server \"%s\" needs a "
							"\"private_key\" or \"private_key_path\" option",
							server->servername)));
		}

		if (connection->userName == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("key-pair authentication on server \"%s\" needs a "
							"\"user\" option", server->servername)));
		}

		if (connection->account == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("key-pair authentication on server \"%s\" needs an "
							"\"account\" option", server->servername),
					 errdetail("The account identifier is part of the token that "
							   "is signed, and it cannot be derived from "
							   "\"account_url\" reliably.")));
		}
	}
	else if (connection->token == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("user mapping on server \"%s\" has no \"token\" option",
						server->servername)));
	}

	return connection;
}


/*
 * GetSnowflakeConnectionForRelation resolves the connection of the server that
 * a foreign table belongs to, for the current user. Query execution goes
 * through GetSnowflakeConnection directly, because a query may run with the
 * privileges of a view owner rather than the current user.
 */
SnowflakeConnection *
GetSnowflakeConnectionForRelation(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);

	return GetSnowflakeConnection(foreignTable->serverid, GetUserId());
}


/*
 * ReadPrivateKeyFile reads a PEM file from the file system of the server.
 */
static char *
ReadPrivateKeyFile(const char *path)
{
	FILE	   *keyFile = AllocateFile(path, PG_BINARY_R);

	if (keyFile == NULL)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open private key file \"%s\": %m", path)));
	}

	StringInfo	keyContents = makeStringInfo();
	char		readBuffer[4096];
	size_t		bytesRead = 0;

	while ((bytesRead = fread(readBuffer, 1, sizeof(readBuffer), keyFile)) > 0)
	{
		appendBinaryStringInfo(keyContents, readBuffer, bytesRead);

		if (keyContents->len > 1024 * 1024)
		{
			FreeFile(keyFile);
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("private key file \"%s\" is unreasonably large", path)));
		}
	}

	if (ferror(keyFile))
	{
		FreeFile(keyFile);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read private key file \"%s\": %m", path)));
	}

	FreeFile(keyFile);

	if (keyContents->len == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("private key file \"%s\" is empty", path)));
	}

	return keyContents->data;
}


/*
 * GetSnowflakeTable resolves which Snowflake table a foreign table is attached
 * to, filling in the server defaults and the Postgres relation name where the
 * table options are silent.
 */
SnowflakeTable *
GetSnowflakeTable(Oid relationId, SnowflakeConnection * connection)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	char	   *database = FindOption(foreignTable->options, "database");
	char	   *schemaName = FindOption(foreignTable->options, "schema_name");
	char	   *tableName = FindOption(foreignTable->options, "table_name");
	char	   *rowEstimate = FindOption(foreignTable->options, "row_estimate");

	/*
	 * A schema named on the table wins, then the one on the server, and only
	 * then the name of the Postgres schema the table lives in. Getting that
	 * order wrong is invisible until the two names differ.
	 */
	bool		schemaFromOption = schemaName != NULL;

	if (schemaName == NULL)
	{
		schemaName = connection->schemaName;
		schemaFromOption = true;
	}

	if (schemaName == NULL)
	{
		schemaName = get_namespace_name(get_rel_namespace(relationId));
		schemaFromOption = false;
	}

	SnowflakeTable *table = MakeSnowflakeTable(connection,
											   database, database != NULL,
											   schemaName, schemaFromOption,
											   tableName != NULL ? tableName :
											   get_rel_name(relationId),
											   tableName != NULL);

	table->relationId = relationId;

	if (rowEstimate != NULL)
		(void) parse_real(rowEstimate, &table->rowEstimate, 0, NULL);

	return table;
}


/*
 * MakeSnowflakeTable builds the table description from names that may or may not
 * have come from options, filling in the database of the server when the table
 * does not name one.
 */
SnowflakeTable *
MakeSnowflakeTable(SnowflakeConnection * connection,
				   const char *database, bool databaseFromOption,
				   const char *schemaName, bool schemaNameFromOption,
				   const char *tableName, bool tableNameFromOption)
{
	SnowflakeTable *table = palloc0(sizeof(SnowflakeTable));

	table->rowEstimate = -1;

	if (database == NULL)
	{
		database = connection->database;

		/*
		 * A database that comes from a server option was written by hand, so
		 * it is taken as spelled either way.
		 */
		databaseFromOption = true;
	}

	if (database == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("no Snowflake database for table \"%s\"", tableName),
				 errhint("Set a \"database\" option on the table or on server \"%s\".",
						 connection->serverName)));
	}

	if (schemaName == NULL)
	{
		schemaName = connection->schemaName;
		schemaNameFromOption = true;
	}

	if (schemaName == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("no Snowflake schema for table \"%s\"", tableName),
				 errhint("Set a \"schema_name\" option on the table or on server \"%s\".",
						 connection->serverName)));
	}

	table->database = pstrdup(database);
	table->databaseFromOption = databaseFromOption;
	table->schemaName = pstrdup(schemaName);
	table->schemaNameFromOption = schemaNameFromOption;
	table->tableName = pstrdup(tableName);
	table->tableNameFromOption = tableNameFromOption;

	return table;
}


/*
 * IsPlainLowerCaseIdentifier returns whether an identifier is one that
 * Snowflake would produce by folding an unquoted identifier, so that
 * upper-casing it is the exact inverse of what Postgres did to it.
 */
static bool
IsPlainLowerCaseIdentifier(const char *identifier)
{
	if (identifier[0] == '\0')
		return false;

	if (!(identifier[0] >= 'a' && identifier[0] <= 'z') && identifier[0] != '_')
		return false;

	for (const char *cursor = identifier; *cursor != '\0'; cursor++)
	{
		bool		isLower = (*cursor >= 'a' && *cursor <= 'z');
		bool		isDigit = (*cursor >= '0' && *cursor <= '9');

		if (!isLower && !isDigit && *cursor != '_' && *cursor != '$')
			return false;
	}

	return true;
}


/*
 * SnowflakeIdentifierName returns the Snowflake name an identifier refers to,
 * unquoted. See SnowflakeQuoteIdentifier for what decides the spelling.
 */
char *
SnowflakeIdentifierName(const char *identifier, bool verbatim)
{
	if (!verbatim && IsPlainLowerCaseIdentifier(identifier))
		return str_toupper(identifier, strlen(identifier), DEFAULT_COLLATION_OID);

	return pstrdup(identifier);
}


/*
 * SnowflakeQuoteIdentifier returns an identifier that Snowflake will resolve to
 * the object the caller means.
 *
 * A name that arrived through an option is quoted as written, because the user
 * spelled out the Snowflake name. A name that came from the Postgres catalog is
 * upper-cased first when it is a plain lower-case name, since that reverses the
 * folding Postgres applied; any other name is quoted as written, because
 * Postgres only has it in that spelling if it was quoted there too.
 */
char *
SnowflakeQuoteIdentifier(const char *identifier, bool verbatim)
{
	const char *name = SnowflakeIdentifierName(identifier, verbatim);
	StringInfo	quoted = makeStringInfo();

	appendStringInfoChar(quoted, '"');

	for (const char *cursor = name; *cursor != '\0'; cursor++)
	{
		if (*cursor == '"')
			appendStringInfoChar(quoted, '"');

		appendStringInfoChar(quoted, *cursor);
	}

	appendStringInfoChar(quoted, '"');

	return quoted->data;
}


/*
 * SnowflakeQualifiedTableName returns the fully qualified, quoted name of the
 * Snowflake table behind a foreign table.
 */
char *
SnowflakeQualifiedTableName(SnowflakeTable * table)
{
	return psprintf("%s.%s.%s",
					SnowflakeQuoteIdentifier(table->database, table->databaseFromOption),
					SnowflakeQuoteIdentifier(table->schemaName, table->schemaNameFromOption),
					SnowflakeQuoteIdentifier(table->tableName, table->tableNameFromOption));
}


/*
 * SnowflakeColumnName returns the quoted Snowflake name of a column of a
 * foreign table.
 */
char *
SnowflakeColumnName(Oid relationId, AttrNumber attributeNumber)
{
	List	   *columnOptions = GetForeignColumnOptions(relationId, attributeNumber);
	char	   *columnName = FindOption(columnOptions, "column_name");

	if (columnName != NULL)
		return SnowflakeQuoteIdentifier(columnName, true);

	columnName = get_attname(relationId, attributeNumber, false);

	return SnowflakeQuoteIdentifier(columnName, false);
}
