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

#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "pg_lake/json/json_reader.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/explain.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"


/*
 * extra_info field in DuckDB explain output.
 */
typedef struct ExtraInfo
{
	const char *key;
	const char *value;
} ExtraInfo;

/*
 * Physical operator object in DuckDB explain output.
 */
struct PhysicalOperator;
typedef struct PhysicalOperator
{
	const char *name;

	struct PhysicalOperator *children;
	size_t		childrenLength;

	ExtraInfo  *extraInfo;
	size_t		extraInfoLength;
} PhysicalOperator;


static char *GetExplainJson(char *query, int numParams, const char **parameterValues);
static void ExplainPhysicalOperator(PhysicalOperator *operator, ExplainState *es);
static void ReadPhysicalOperator(JsonbContainer *json, PhysicalOperator *operator);
static void ReadExtraInfo(const char *key, JsonbValue *jsonValue, ExtraInfo *property);


/*
 * ExplainPGDuckQuery explains a query using pgduck and emits explain output
 * using the PostgreSQL API.
 */
void
ExplainPGDuckQuery(char *query, int numParams, const char **parameterValues,
				   ExplainState *es)
{
	char	   *explainOutput = GetExplainJson(query, numParams, parameterValues);
	Datum		jsonbDatum = DirectFunctionCall1(jsonb_in, PointerGetDatum(explainOutput));
	Jsonb	   *explainJsonb = DatumGetJsonbP(jsonbDatum);

	JsonbIterator *iterator = JsonbIteratorInit(&explainJsonb->root);
	JsonbIteratorToken iteratorToken;
	JsonbValue	element;

	/* "Plans": [ in JSON, <Plans> in XML */
	ExplainOpenGroup("Plans", "Plans", false, es);

	while ((iteratorToken = JsonbIteratorNext(&iterator, &element, true)) != WJB_DONE)
	{
		if (iteratorToken == WJB_ELEM)
		{
			PhysicalOperator operator;

			ReadPhysicalOperator(element.val.binary.data, &operator);

			ExplainPhysicalOperator(&operator, es);
		}
	}

	/* ] in JSON, </Plans> in XML */
	ExplainCloseGroup("Plans", "Plans", false, es);
}


/*
 * ExplainPhysicalOperator explains a DuckDB physical operator using PostgreSQL
 * functions.
 */
static void
ExplainPhysicalOperator(PhysicalOperator *operator, ExplainState *es)
{
	/* { in JSON, <Physical-Operator> in XML */
	ExplainOpenGroup("Physical Operator", NULL, true, es);

	/* in text format the operator name is a heading, in JSON/XML a property */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		ExplainOpenGroup(operator->name, operator->name, true, es);

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "->  %s\n", operator->name);
		es->indent += 3;
	}
	else
	{
		ExplainPropertyText("Node Type", operator->name, es);
	}

	/* list all the properties received from DuckDB */
	for (int infoIndex = 0; infoIndex < operator->extraInfoLength; infoIndex++)
	{
		ExtraInfo  *extraInfo = &operator->extraInfo[infoIndex];

		if (extraInfo->value != NULL)
			ExplainPropertyText(extraInfo->key, extraInfo->value, es);
	}

	/* list all the subplans */
	if (operator->childrenLength > 0)
	{
		/* "Plans": [ in JSON, <Plans> in XML */
		ExplainOpenGroup("Plans", "Plans", false, es);

		for (int childIndex = 0; childIndex < operator->childrenLength; childIndex++)
		{
			PhysicalOperator *child = &operator->children[childIndex];

			ExplainPhysicalOperator(child, es);
		}

		/* ] in JSON, </Plans> in XML */
		ExplainCloseGroup("Plans", "Plans", false, es);
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		es->indent -= 3;

		ExplainCloseGroup(operator->name, operator->name, true, es);
	}

	/* } in JSON, </Physical-Operator> in XML */
	ExplainCloseGroup("Physical Operator", NULL, true, es);
}


/*
 * ReadPhysicalOperator parses a DuckDB physical operator from explain output.
 */
static void
ReadPhysicalOperator(JsonbContainer *json, PhysicalOperator *operator)
{
	memset(operator, '\0', sizeof(PhysicalOperator));

	size_t		length = 0;

	JsonExtractStringField(json, "name", FIELD_REQUIRED, &operator->name, &length);
	JsonExtractObjectArrayField(json, "children", FIELD_OPTIONAL, (JsonParseFunction) ReadPhysicalOperator,
								sizeof(PhysicalOperator),
								(void **) &operator->children, &operator->childrenLength);
	JsonExtractMapField(json, "extra_info", FIELD_OPTIONAL,
						(JsonParseMapEntryFunction) ReadExtraInfo,
						sizeof(ExtraInfo),
						(void **) &operator->extraInfo, &operator->extraInfoLength);
}


/*
 * ReadExtraInfo reads the extra_info properties.
 */
static void
ReadExtraInfo(const char *key, JsonbValue *jsonValue, ExtraInfo *property)
{
	memset(property, '\0', sizeof(ExtraInfo));

	if (jsonValue->type != jbvString)
	{
		/* we currently only handle string values */
		return;
	}

	/* JsonExtractMapField does palloc */
	property->key = key;
	property->value = pnstrdup(jsonValue->val.string.val, jsonValue->val.string.len);
}


/*
 * GetExplainJson gets the explain output for a query as JSON.
 */
static char *
GetExplainJson(char *query, int numParams, const char **parameterValues)
{
	char	   *explainCommand = psprintf("explain (format 'json') %s", query);

	char	   *value = NULL;

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();

	SendQueryWithParams(pgDuckConn, explainCommand, numParams, parameterValues);

	PGresult   *result = WaitForResult(pgDuckConn);

	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		int			rowCount = PQntuples(result);
		int			columnCount = PQnfields(result);

		if (columnCount < 2)
			ereport(ERROR, (errmsg("unexpected column count %d", columnCount)));

		if (rowCount < 1)
			ereport(ERROR, (errmsg("unexpected row count %d", rowCount)));

		value = pstrdup(PQgetvalue(result, 0, 1));

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);

	return value;
}
