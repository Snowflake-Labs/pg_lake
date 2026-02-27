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

#ifndef PGDUCK_PARSE_STRUCT_H
#define PGDUCK_PARSE_STRUCT_H

#include "nodes/pg_list.h"

/* Recursively create a Postgres type based on a STRUCT string */
Oid			GetOrCreatePGStructType(const char *name);

/* types for our STRUCT parsing */
typedef struct CompositeType
{
	char	   *typeName;		/* name of the type itself; if isFinalized,
								 * this is complete and can be used, if not
								 * then we are still constructing it. */
	Oid			typeOid;		/* if 0, this type has not yet been created,
								 * otherwise the oid */
	List	   *cols;			/* a list of columns */
	bool		isArray;		/* the top-level type can be an array */
	bool		isFinalized;	/* if true, then we know all component parts
								 * exist */
}			CompositeType;

typedef struct CompositeCol
{
	char	   *colName;		/* the column name */
	Oid			colType;		/* our oid type, or 0 if not yet allocated  */
	char	   *colTypeName;	/* string version of the type */
	CompositeType *subStruct;	/* for a non-composite type, NULL; for
								 * composite, pointer to that type's struct */
	bool		isArray;		/* whether we are an array type or not */
}			CompositeCol;


/* Composite Type functions - convert to/from postgres composite types and
 * STRUCT strings */
extern PGDLLEXPORT CompositeType * GetCompositeTypeForPGType(Oid postgresType);
extern PGDLLEXPORT char *GetDuckDBStructDefinitionForCompositeType(CompositeType * type);
extern PGDLLEXPORT char *GetDuckDBStructDefinitionForIceberg(CompositeType * type);
extern PGDLLEXPORT char *GetDuckDBStructDefinitionForPGType(Oid postgresType);

/* simple string to parse tree */
extern PGDLLEXPORT CompositeType * ParseStructString(char *parse);

#endif
