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
 *  keywords.c
 *
 *  Knowledge about duckdb parser keywords
 */

#include "postgres.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include <string.h>

#include "pg_lake/pgduck/keywords.h"

/* Keyword categories --- from common/keywords.h */

#define UNRESERVED_KEYWORD		0
#define COL_NAME_KEYWORD		1
#define TYPE_FUNC_NAME_KEYWORD	2
#define RESERVED_KEYWORD		3

/* simple table lookup */
typedef struct DuckDBKeyword
{
	char	   *keywordName;
	int			keywordCategory;
} DuckDBKeyword;

/* we only care about the name and the category here */
#define PG_KEYWORD(name,value,cat,is_bare_label) {(name), (cat)},

/*
 * As of this writing, DuckDB uses the Postgres keyword list for its own parser
 * keywords.  This is convenient for us, as we don't need to parse the output of
 * duckdb_keywords() or maintain a table separately, instead we can just include
 * the list ourselves.
 */

static const DuckDBKeyword duckDBKeywordList[] =
{
#include "parser/kwlist.h"
};


#define NUM_KEYWORDS (sizeof(duckDBKeywordList)/sizeof(DuckDBKeyword))

/* Simple function to compare input string against name in table */
static int
compareKeywords(const void *userKey, const void *keywordCandidate)
{
	return strcasecmp((const char *) userKey,
					  ((const DuckDBKeyword *) keywordCandidate)->keywordName);
}

bool
IsDuckDBReservedWord(char *candidateWord)
{
	DuckDBKeyword *foundKeyword = bsearch(candidateWord,
										  duckDBKeywordList,
										  NUM_KEYWORDS,
										  sizeof(DuckDBKeyword),
										  compareKeywords);

	if (!foundKeyword)
		return false;

	/* We found our keyword, but we don't care if it's non-reserved. */

	return foundKeyword->keywordCategory != UNRESERVED_KEYWORD;
}
