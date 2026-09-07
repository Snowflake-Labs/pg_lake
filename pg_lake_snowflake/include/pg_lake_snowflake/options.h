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
 * options.h
 * Foreign server, user mapping, foreign table and column options.
 */
#pragma once

#include "postgres.h"

#include "access/attnum.h"

typedef enum SnowflakeAuthMethod
{
	SNOWFLAKE_AUTH_UNSPECIFIED = 0,
	SNOWFLAKE_AUTH_PAT,
	SNOWFLAKE_AUTH_OAUTH,
	SNOWFLAKE_AUTH_KEYPAIR
}			SnowflakeAuthMethod;

/*
 * SnowflakeConnection is everything needed to send a statement to an account:
 * the foreign server options merged with the user mapping options that apply
 * to the current user.
 */
typedef struct SnowflakeConnection
{
	Oid			serverId;
	Oid			userId;
	char	   *serverName;

	/* scheme and host only, without a trailing slash */
	char	   *accountUrl;

	/* account identifier, needed to build a key-pair JWT */
	char	   *account;

	/* session defaults for statements sent to this server */
	char	   *database;
	char	   *schemaName;
	char	   *warehouse;
	char	   *role;

	int			statementTimeoutSeconds;
	bool		enableAggregatePushdown;

	SnowflakeAuthMethod authMethod;
	char	   *userName;
	char	   *token;
	char	   *privateKeyPem;
	char	   *privateKeyPassphrase;
}			SnowflakeConnection;

/*
 * SnowflakeTable is the Snowflake object a foreign table is attached to.
 * The *FromOption flags record whether the name came from an option, which
 * decides how it is quoted (see SnowflakeQuoteIdentifier).
 */
typedef struct SnowflakeTable
{
	Oid			relationId;
	char	   *database;
	bool		databaseFromOption;
	char	   *schemaName;
	bool		schemaNameFromOption;
	char	   *tableName;
	bool		tableNameFromOption;

	/* -1 when the row_estimate option is not set */
	double		rowEstimate;
}			SnowflakeTable;

extern SnowflakeConnection * GetSnowflakeConnection(Oid serverId, Oid userId);
extern SnowflakeConnection * GetSnowflakeConnectionForRelation(Oid relationId);
extern SnowflakeTable * GetSnowflakeTable(Oid relationId, SnowflakeConnection * connection);
extern SnowflakeTable * MakeSnowflakeTable(SnowflakeConnection * connection,
										   const char *database, bool databaseFromOption,
										   const char *schemaName, bool schemaNameFromOption,
										   const char *tableName, bool tableNameFromOption);

/* the value of an option of a foreign table, or of its server, or NULL */
extern char *SnowflakeTableOption(Oid relationId, const char *optionName);

/* whether writes to a foreign table are allowed, from the updatable option */
extern bool SnowflakeTableIsUpdatable(Oid relationId);

extern char *SnowflakeIdentifierName(const char *identifier, bool verbatim);
extern char *SnowflakeQuoteIdentifier(const char *identifier, bool verbatim);
extern char *SnowflakeQualifiedTableName(SnowflakeTable * table);
extern char *SnowflakeColumnName(Oid relationId, AttrNumber attributeNumber);
