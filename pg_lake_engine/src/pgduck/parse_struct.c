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
 *  parse_struct.c
 *
 * Routines to handle parsing of STRUCT data structures and mapping to Postgres
 * composite types.
 */

#include "postgres.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include "pg_lake/pgduck/keywords.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/parse_struct.h"
#include "pg_lake/pgduck/type.h"
#include "pg_extension_base/spi_helpers.h"

#include "access/table.h"
#include "access/xlogutils.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "common/string.h"
#include "commands/typecmds.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/* We need a sentinel value that will never be used for a valid oid, similar to
 * InvalidOid, but one which we can flag on for other purposes.  We are using
 * RECORDOID because it likely should not be part of a concrete tuple type, but
 * still indicates the purpose here.  Note that the specific value is
 * unimportant, just that we can identify it from other types. */

#define TRANSIENTSTRUCTOID RECORDOID

typedef struct StructParserState
{
	const char *sourceString;
	char	   *position;
	List	   *uncreatedTypes;
}			StructParserState;


/* pulling from type.c without general include */
extern Oid	GetPGTypeForDuckDBTypeNameBuiltin(const char *name, int *typeMod, bool arrayOid);

/* prototypes */
static Oid	FindOrCreatePGCompositeType(CompositeType * type);
static Oid	CreatePGCompositeType(CompositeType * type);
static void CreateUncreatedTypes(List *uncreatedTypes);
static Oid	GetRelatedTypeOid(Oid inputType, bool getArray);
static void EnsureColumnTypesArePopulated(CompositeType * myType);

/* parser-related returns */
/* not exposed, but importable via extern */
CompositeType *ParseStructType(StructParserState * parse);
static CompositeCol * ParseStructColumn(StructParserState * parse);
static bool ParseSkipToNextField(StructParserState * parse);
static char *ParseSkipCasePrefix(const char *string, const char *prefix);

/* exposed for map parsing as well */
char	   *ParseDuckDBFieldType(char **sourceStringPtr, bool *isArray);

/* helper functions for parsing */
static char *ParseDuckDBFieldName(char **sourceStringPtr);
static void AssignBaseStructTypeName(CompositeType * type);
static void FinalizeCompositeTypeName(CompositeType * type);

/* simple wrapper for easily parsing a single type */
CompositeType *
ParseStructString(char *name)
{
	if (!IsStructType(name))
		return NULL;

	StructParserState parse = {
		.sourceString = name,
		.uncreatedTypes = NIL,
	};

	return ParseStructType(&parse);
}

/*
 * GetOrCreatePGStructType takes a DuckDB struct type name and decomposes it,
 * until we have all known types, and then creates the underlying pieces as
 * necessary.
 */

Oid
GetOrCreatePGStructType(const char *name)
{
	Assert(IsStructType(name));

	/*
	 * Let's go ahead and parse this bad boy...
	 */

	StructParserState parse = {
		.sourceString = pstrdup(name),
		.uncreatedTypes = NIL,
	};

	CompositeType *newType = ParseStructType(&parse);

	if (!newType)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_engine: couldn't parse STRUCT type: %s",
							   name)));

	/*
	 * Great, we got our type parsed, let's actually create the underlying
	 * database types...
	 */

	if (parse.uncreatedTypes != NIL)
	{
		/*
		 * This should always be true, since our new type needs to be
		 * created...
		 */
		CreateUncreatedTypes(parse.uncreatedTypes);

		/*
		 * If for some reason CreateUncreatedTypes() did not work,
		 * newType.typeOid will still be InvalidOid, so this works for success
		 * and failure cases both.
		 */

		return newType->typeOid;
	}
	else
	{
		elog(WARNING, "pg_lake_engine: should always need to create a new type");
		return InvalidOid;
	}
}

/*
 * This routine is the main entrypoint to create a parse tree for composite
 * types.  If we are able to successfully parse our type, we return a
 * CompositeType tree which contains all of the information we need to know
 * about this type.  If there is a parse error of sorts, we return NULL here.
 *
 * Also on success, we set uncreatedTypes with a list of composite types that
 * need to be created in the database, in reverse dependency order.  This means
 * that we will be able to process these sequentially, creating the types in
 * order in one pass.
 */
CompositeType *
ParseStructType(StructParserState * parse)
{
	/* we should only be running this if we are a struct type */
	Assert(IsStructType(parse->sourceString));

	/* we know we're a struct type, so skip our prefix for parsing */
	parse->position = (char *) ParseSkipCasePrefix(parse->sourceString, "STRUCT");

	CompositeType *myType = palloc0(sizeof(CompositeType));

	/*
	 * If we are well-formed input, we should have balanced parens, so the
	 * NULL byte check is just to avoid bad input/runaway data.
	 */

	while (*parse->position && *parse->position != ')')
	{
		CompositeCol *col = ParseStructColumn(parse);

		if (!col)
			return NULL;

		myType->cols = lappend(myType->cols, col);

		/*
		 * ParseSkipToNextField() skips whitespace, a comma, and any more
		 * whitespace, or a closing paren.
		 */
		ParseSkipToNextField(parse);
	}

	if (*parse->position != ')')
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_engine: STRUCT ended before closing paren: %s",
							   parse->position)));

	/* are we an array? */
	myType->isArray =
		parse->position[1] == '[' && parse->position[2] == ']';

	/*
	 * Now that we got this close let's assign a base name; this is not
	 * finalized until isFinalized is true.  (If you don't care about the
	 * final name in the database matching this value, you can certainly
	 * inspect it, such as DESCRIBE on a replica), but there are no guarantees
	 * about it fitting inside NAMEDATALEN and it will not match the eventual
	 * name in the database.
	 */

	AssignBaseStructTypeName(myType);

	/*
	 * We successfully created our CompositeType struct, let's add this type
	 * to the list of uncreated types to later be created.
	 */
	parse->uncreatedTypes = lappend(parse->uncreatedTypes, myType);
	return myType;
}


/*
 * Skip the opening STRUCT, optional whitespace, and an opening paren.  Returns
 * NULL if we don't match it.
 */
static char *
ParseSkipCasePrefix(const char *string, const char *prefix)
{
	/* require our matching prefix */
	if (strncasecmp(prefix, string, strlen(prefix)) != 0)
		return NULL;

	/* skip past the prefix */
	string += strlen(prefix);

	/* skip whitespace */
	while (isspace(*string))
		string++;

	/* require next opening paren */
	if (*string != '(')
		return NULL;

	/* advance past the paren */
	return (char *) string + 1;
}

/*
 * Parse a single column definition consisting of a field name and a type (which
 * itself could be a struct, which will also recursively be parsed).
 */
static CompositeCol *
ParseStructColumn(StructParserState * parse)
{
	CompositeCol *myCol = palloc0(sizeof(CompositeCol));

	/* parse a field name using DuckDB's quoting rules */
	char	   *fieldName = ParseDuckDBFieldName(&parse->position);

	if (!fieldName)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_engine: couldn't parse STRUCT field name: %s",
							   parse->position)));

	/* skip whitespace */
	while (*parse->position && isspace(*parse->position))
		parse->position++;

	/*
	 * Parse the field type, which itself could be a struct; this returns the
	 * raw string which in turn is later processed.  The isArray boolean is
	 * set if the field type had an array qualifier, but the string itself
	 * does not include the array modifier. (It is skipped by the parser,
	 * however.)
	 */

	bool		isArray;
	char	   *fieldType = ParseDuckDBFieldType(&parse->position, &isArray);

	if (!fieldType)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_engine: couldn't parse STRUCT field type: %s",
							   parse->position)));

	/*
	 * If we are not a supported builting type, this should be InvalidOid,
	 * which will later be assigned in the createUncreatedTypes() routine
	 */
	int			typeMod;

	myCol->colType = GetPGTypeForDuckDBTypeNameBuiltin(fieldType, &typeMod, isArray);
	myCol->colTypeName = fieldType;
	myCol->isArray = isArray;
	myCol->colName = fieldName;

	/*
	 * If our discovered type is a nested struct, then we need to do a parse
	 * of /that/ type, and aggregating the results from that one into our
	 * state (mainly the uncreatedTypes list).
	 */
	if (IsStructType(fieldType))
	{
		/* create a sub-parser just for this field type */
		StructParserState subparse = {
			.sourceString = fieldType,
			.uncreatedTypes = parse->uncreatedTypes,
		};

		CompositeType *myType = ParseStructType(&subparse);

		if (!myType)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("pg_lake_engine: couldn't parse sub-STRUCT: %s",
								   fieldType)));

		parse->uncreatedTypes = subparse.uncreatedTypes;
		myCol->subStruct = myType;

		/*
		 * Since we are later pulling our Oid from the sub-STRUCT, we cannot
		 * have it set here if it was set earlier.
		 */

		myCol->colType = InvalidOid;
	}
	else if (IsMapType(fieldType))
	{
		/*
		 * XXX - This will not work on replicas to create a map type if the
		 * underlying type does not exist already in the catalogs...; anything
		 * to do here?  This is also an immediate creation, compared to the
		 * ParseStruct approach which delays creation until the end of the
		 * overall nested parse strategy.
		 */
		myCol->colType = GetOrCreatePGMapType(fieldType);
	}
	return myCol;
}

/*
 * This routine serves to create all of our uncreated types in the database in
 * one go.  Since they are returned in dependency order, we just need to create
 * the type, stash the OID in our loop, then the dependency lookup will catch
 * the newly-created OID when the dependent type itself is processed.
 */

static void
CreateUncreatedTypes(List *uncreatedTypes)
{
	if (uncreatedTypes == NIL)
		return;

	ListCell   *lc;

	foreach(lc, uncreatedTypes)
	{
		CompositeType *myType = (CompositeType *) lfirst(lc);

		/*
		 * At this point in the process, all dependent column types should be
		 * ready, so let's complete the process of naming to our final form.
		 */
		EnsureColumnTypesArePopulated(myType);
		FinalizeCompositeTypeName(myType);

		Oid			myOid = FindOrCreatePGCompositeType(myType);

		if (myOid == InvalidOid)
		{
			/*
			 * Error message; other types won't be able to be created either,
			 * so probably doesn't make sense to continue processing at this
			 * point.
			 */

			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("pg_lake_engine: couldn't create one or more dependent composite types")));
		}
		else
			myType->typeOid = GetRelatedTypeOid(myOid, myType->isArray);
	}
}


/*
 * This parser helper will parse a field name using quoting rules, advance the
 * input pointer and return a pointer to the valid name.  If there is an error,
 * NULL is returned.
 */

char *
ParseDuckDBFieldName(char **sourceString)
{
	char	   *parseInput = *sourceString;

	/*
	 * Fastpath for unquoted names.  Note that we assume there is no internal
	 * quoting or backslashes in unquoted identifiers.
	 */

	if (*parseInput != '"')
	{
		/*
		 * Technically this would match leading numbers, which should be
		 * quoted, but close enough, since this data should be coming in
		 * well-formed.
		 */
		while (*parseInput && (isalnum(*parseInput) || *parseInput == '_'))
			parseInput++;

		/* no matching chars found */
		if (parseInput == *sourceString)
			return NULL;

		/* return the piece of the string we found */
		char	   *ret = pnstrdup(*sourceString, parseInput - *sourceString);

		*sourceString = parseInput;
		return ret;
	}

	/*
	 * The rest is quoted name handling. We need to consider quoting and
	 * escaping.  First we allocate an output buffer that is as long as a
	 * possible string, including terminator.  Fortunately, since we know we
	 * are removing 2 chars (at least), we can just use the location of the
	 * first unescaped quote char, we can use this as the output buffer size.
	 */

	char	   *lastQuote = parseInput;

	while (true)
	{
		/* Start our search after the last known quote char */
		lastQuote = strchr(lastQuote + 1, '"');

		/* Bummer, didn't find one before end of string */
		if (!lastQuote)
			return NULL;

		/*
		 * Count the number of preceding backslashes; no body, just iteration
		 * in this loop
		 */
		int			i;

		for (i = 0; *(lastQuote - (i + 1)) == '\\'; i++)
			;

		/*
		 * If we had an even number of consecutive backslashes, then we're
		 * done, this quote isn't escaped.
		 */
		if (!(i % 2))
			break;

		/* otherwise we slog on to the next one */
	}

	/*
	 * We want a buffer that will hold our unquoted string plus a terminating
	 * NUL.  Since we know we have 2 quote characters that will be removed, we
	 * can just use (lastQuote - parseInput) and since escaping only _removes_
	 * characters from the total unescaped length, this will be as large as we
	 * need.  (We don't care about the 1+ wasted bytes here, this is
	 * ephemeral.
	 */

	char	   *quotedBuf = palloc0(lastQuote - parseInput);

	/* start writing at the beginning of this buffer */
	char	   *quotedBufWritePos = quotedBuf;

	/* parseInput is currently the opening quote, so let's skip past that */
	for (parseInput++; parseInput < lastQuote; parseInput++)
	{
		char		bufWriteChar = *parseInput;

		/*
		 * We just need to check if our character is an escape character; if
		 * so, we perform a few transforms.  If we _must_, we can expand this
		 * into other escapes (octal, unicode, etc); just do the common ones
		 * here.
		 */

		if (bufWriteChar == '\\')
		{
			parseInput++;
			switch (*parseInput)
			{
				case 't':
					bufWriteChar = '\t';
					break;
				case 'n':
					bufWriteChar = '\n';
					break;
				case 'r':
					bufWriteChar = '\r';
					break;
				default:
					/* just copy the next char in the default case */
					/* this handles escaped quotes and escaped escapes */
					bufWriteChar = *parseInput;
			}
		}

		/* now that we have the char to write in our buffer, let's write it. */
		*quotedBufWritePos = bufWriteChar;
		quotedBufWritePos++;
	}

	/* advance our parser pointer past the last quote */
	*sourceString = lastQuote + 1;

	return quotedBuf;
}


/*
 * This parser helper will parse a field type, set the advance the input pointer
 * and return a pointer to palloc'd extracted piece.  If there is an error, NULL is
 * returned.  On success we advance the input pointer.
 *
 * The rules for parsing a field type are simple:
 *
 * - check if we are looking at a struct type
 *   - if yes, then start counting open quotes until the balanced close quote
 *   - if no, then return the largest sequence of isalpha
 *
 * Note that this does not account for other unhandled complex types, such as
 * MAP or UNION which would also have additional data following them related to
 * those complex types.  In this case, the type would be identified by this
 * parser as a simple type of "MAP" or whatnot, which it turn would be returned
 * to the caller.  When this type is resolved further, we get the unhandled
 * InvalidOid in those cases, which end up creating an error further down the
 * path, so the underlying composite type will not end up getting created
 * anyway.
 *
 * This does result in the behavior we want (aborting the creation of this
 * composite type), but the handling could presumably be cleaned up a bit,
 * particularly if we end up doing some sort of transformations for other types
 * in the future.  (In particular, MAP could likely be handled as a jsonb type.
 * It is unclear what semantics for UNION map cleanly into Postgres, so this is
 * likely to remain unsupported.)
 *
 * Most of the type-handling is straightforward to parse, however the
 * DECIMAL/NUMERIC type includes a precision/scale type qualifier that we need
 * to account for.
 */

char *
ParseDuckDBFieldType(char **sourceString, bool *isArray)
{
	char	   *parseInput = *sourceString;
	int			quoteCount = 1; /* if used, we have opening quote already */

	if (IsStructType(parseInput) || IsMapType(parseInput))
	{
		bool		isStruct = IsStructType(parseInput);

		parseInput = ParseSkipCasePrefix(parseInput, isStruct ? "STRUCT" : "MAP");

		if (!parseInput)
			return NULL;

		while (*parseInput && quoteCount)
		{
			if (*parseInput == '(')
				quoteCount++;
			if (*parseInput == ')')
				quoteCount--;
			parseInput++;
		}
	}
	else
	{
		/*
		 * To account for types with spaces in the names, we skip past words
		 * with spaces
		 */
		while (isalnum(*parseInput) || isspace(*parseInput))
			parseInput++;

		/*
		 * The only remaining scalar types that are not just \w+ are
		 * NUMERIC/DECIMAL. We could try to verify that we are currently
		 * parsing one of those types, but since this is returned by a driver
		 * not user-provided data, let's just allow "(\d+,\s*\d+)" to be
		 * appended to any type and assume the driver got it right.
		 */

		if (*parseInput == '(')
		{
			parseInput++;

			/* skip whitespace */
			while (*parseInput && isspace(*parseInput))
				parseInput++;

			/* verify int */
			if (!isdigit(*parseInput))
				return NULL;

			/* skip digits */
			while (*parseInput && isdigit(*parseInput))
				parseInput++;

			/* skip whitespace */
			while (*parseInput && isspace(*parseInput))
				parseInput++;

			/* verify/skip comma */
			if (*parseInput != ',')
				return NULL;
			else
				parseInput++;

			/* skip whitespace */
			while (*parseInput && isspace(*parseInput))
				parseInput++;

			/* verify int */
			if (!isdigit(*parseInput))
				return NULL;

			/* skip digits */
			while (*parseInput && isdigit(*parseInput))
				parseInput++;

			/* skip whitespace */
			while (*parseInput && isspace(*parseInput))
				parseInput++;

			/* verify/skip closing paren */
			if (*parseInput != ')')
				return NULL;
			else
				parseInput++;
		}

	}
	if (parseInput == *sourceString)
		return NULL;

	char	   *ret = pnstrdup(*sourceString, parseInput - *sourceString);

	/*
	 * We must parse out as well whether this field type is a list or an
	 * array. TODO: do we need to detect/parse dimensions here as well?
	 */

	if (parseInput[0] == '[' && parseInput[1] == ']')
	{
		*isArray = true;
		parseInput += 2;
	}
	else
		*isArray = false;

	*sourceString = parseInput;
	return ret;
}


/*
 * This function advances the parser to skip across whitespace until the next
 * comma or closing paren.  Returns true if we found the end of the field, or
 * false if there was an error.
 */
static bool
ParseSkipToNextField(StructParserState * parse)
{
	/* skip leading whitespace */
	while (isspace(*parse->position))
		parse->position++;

	/* unexpected ending? */
	if (!*parse->position)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_engine: unexpected '\\0' in STRUCT parsing")));

	/* are we at a closing paren? */
	if (*parse->position == ')')
		return true;

	if (*parse->position != ',')
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_engine: expecting comma in STRUCT parsing: %s",
							   parse->position)));

	/* skip our known comma */
	parse->position++;

	/* skip trailing whitespace */
	while (*parse->position && isspace(*parse->position))
		parse->position++;

	return true;
}


/*
 * This function will attempt to locate an existing composite type in the
 * database which has the same structure as the desired type.  If any unknown
 * column types are found in our composite, then we don't even bother looking up
 * the type since we know at least one type will need to be created.
 *
 * If we are in recovery, we will not be able to create a new concrete postgres
 * type for the composite type, however, we can still fake it out a bit for the
 * purposes of previewing the overall structure.  We can also look for an
 * existing composite type that matches our structs.
 *
 * If we cannot find a matching composite type (as a primary) then we just run
 * the create routine which in the worst case could result in duplicate types
 * between relations.  We are forgoing ownership of the created types at this
 * point, so might need to add some sort of periodic analysis to remove
 * created/imported types that have no consumers in any table structure.
 *
 * If the CompositeType is marked as an array, we return the array OID for the
 * type instead of the elem composite type.
 */
static Oid
FindOrCreatePGCompositeType(CompositeType * type)
{
	elog(DEBUG1, "Looking up existing composite postgres type for: %s", GetDuckDBStructDefinitionForCompositeType(type));

	/*
	 * Structured as a do block so we can bail to our followup routine at any
	 * point
	 */
	do
	{
		StringInfo	oidArray = makeStringInfo();
		StringInfo	nameArray = makeStringInfo();
		ListCell   *lc;

		foreach(lc, type->cols)
		{
			CompositeCol *col = (CompositeCol *) lfirst(lc);

			Assert(col->colType);

			/* if we are the first time through, skip this */
			if (oidArray->len)
			{
				appendStringInfoChar(oidArray, ',');
				appendStringInfoChar(nameArray, ',');
			}

			appendStringInfo(oidArray, "%d", col->colType);
			appendStringInfo(nameArray, "%s", col->colName);
		}

		char	   *findTypeQuery = psprintf(
											 "SELECT oid, typname, typarray FROM pg_type JOIN "
											 "(SELECT attrelid,"
											 "    array_agg(atttypid ORDER BY attnum) AS types,"
											 "    array_agg(attname ORDER BY attnum) AS names "
											 "    FROM pg_attribute "
											 "    WHERE attnum > 0 AND NOT attisdropped "
											 "    GROUP BY attrelid) atts "
											 "ON pg_type.typrelid = atts.attrelid AND "
											 "    atts.names = '{%s}'::name[] and atts.types='{%s}'::oid[] "
											 "WHERE typnamespace :: regnamespace in ('pg_catalog','"
											 STRUCT_TYPES_SCHEMA "') LIMIT 1",
											 nameArray->data,
											 oidArray->data);

		elog(DEBUG1, "checking for matching type oid via query: %s", findTypeQuery);

		Oid			foundOid = InvalidOid;
		Oid			foundArrayOid = InvalidOid;
		char	   *foundName = NULL;

		SPI_START();

		int			ret = SPI_execute(findTypeQuery, false, 1);

		if (ret == SPI_OK_SELECT && SPI_processed >= 1)
		{
			HeapTuple	tuple = SPI_tuptable->vals[0];
			TupleDesc	tupleDesc = SPI_tuptable->tupdesc;
			bool		isNull;

			/* pull field 1, the Oid of the underlying type */
			Datum		datum = heap_getattr(tuple, 1, tupleDesc, &isNull);

			if (!isNull)
				foundOid = DatumGetObjectId(datum);

			/* pull field 2, the name of the underlying type */
			datum = heap_getattr(tuple, 2, tupleDesc, &isNull);

			if (!isNull)
			{
				foundName = NameStr(*DatumGetName(datum));
				elog(DEBUG1, "found compatible type: %s", foundName);
			}

			/* pull field 3, the array Oid of the underlying type */
			datum = heap_getattr(tuple, 3, tupleDesc, &isNull);

			if (!isNull)
				foundArrayOid = DatumGetObjectId(datum);
		}

		SPI_END();

		if (foundOid != InvalidOid)
			return type->isArray ? foundArrayOid : foundOid;

	} while (false);

	/*
	 * If we didn't pass any of the above, we hit our fallback.
	 */

	if (RecoveryInProgress())
		return TRANSIENTSTRUCTOID;

	Oid			newTypeOid = CreatePGCompositeType(type);

	/*
	 * since the composite type id that is returned is a scalar, we may need
	 * to return the array oid type instead depending on expected context
	 */

	return GetRelatedTypeOid(newTypeOid, type->isArray);
}


/*
 * This function will create a composite postgres type for the given column name
 * and type oids.  We presume that none of the oids are InvalidOid and that all
 * of the columns match.
 */
static Oid
CreatePGCompositeType(CompositeType * type)
{
	RangeVar   *typevar;
	List	   *coldeflist = NIL;
	ListCell   *lc;

	/*
	 * We need to build out a parse tree for DefineType, which means we need
	 * to map the oids that we have back to their typnames so we can properly
	 * create the composite type.
	 */

	foreach(lc, type->cols)
	{
		CompositeCol *compositeColumn = lfirst(lc);

		/*
		 * This piece is the only tricky bit here; if our Oid is unset, we set
		 * it based on our subStruct, which previously calculated and stored
		 * the Oid in its *own* struct.
		 */

		Assert(compositeColumn && compositeColumn->colType);

		HeapTuple	tup = SearchSysCache1(TYPEOID, compositeColumn->colType);

		if (!HeapTupleIsValid(tup))
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("pg_lake_engine: cache lookup failed for type %u",
								   compositeColumn->colType)));

		Form_pg_type pg_type_tuple = (Form_pg_type) GETSTRUCT(tup);

		/* create new node to represent our column */
		ColumnDef  *newColNode = makeNode(ColumnDef);

		newColNode->colname = pstrdup(compositeColumn->colName);
		newColNode->typeName =
			makeTypeNameFromOid(pg_type_tuple->oid, -1);
		newColNode->is_local = true;

		coldeflist = lappend(coldeflist, newColNode);

		ReleaseSysCache(tup);
	}

	typevar = makeRangeVar(STRUCT_TYPES_SCHEMA, type->typeName, -1);

	ObjectAddress newCompositeType = DefineCompositeType(typevar, coldeflist);

	/*
	 * Since this is in a transaction, we want to be able to see/use the
	 * results here in order to make other types, so increment the command
	 * counter.
	 */

	CommandCounterIncrement();

	/*
	 * If this failed then the objectId field is InvalidObject, so no special
	 * handling required here.
	 */

	return newCompositeType.objectId;
}

/*
 * This creates a new struct name based on the passed-in CompositeType and sets
 * the appropriate typeName field.  If all column types are known, it also
 * finalizes the name.
 */
static void
AssignBaseStructTypeName(CompositeType * type)
{
	/* get column names */

	StringInfo	string = makeStringInfo();
	ListCell   *lc;
	bool		allColTypesKnown = true;

	foreach(lc, type->cols)
	{
		CompositeCol *col = (CompositeCol *) lfirst(lc);

		appendStringInfoString(string, col->colName);
		appendStringInfoChar(string, '_');

		if (col->colType == InvalidOid)
			allColTypesKnown = false;
	}

	type->typeName = string->data;

	if (allColTypesKnown)
		FinalizeCompositeTypeName(type);
}

/*
 * This performs all of the necessary cleanup steps to ensure that a new struct
 * name is unique and will fit inside the database.  It uses the base name and
 * the column types to construct a CRC32C hash and include this portion in the
 * generated name.  It also changes spaces to underscores in the output.
 */
static void
FinalizeCompositeTypeName(CompositeType * type)
{
	if (type->isFinalized)
		return;

	Assert(type->typeName);
	StringInfo	string = makeStringInfo();
	pg_crc32c	typeCrc;

	INIT_CRC32C(typeCrc);
	COMP_CRC32C(typeCrc, type->typeName, strlen(type->typeName));

	/* also add the types into our hash */

	ListCell   *lc;

	foreach(lc, type->cols)
	{
		CompositeCol *col = (CompositeCol *) lfirst(lc);

		Assert(col->colType != InvalidOid);
		COMP_CRC32C(typeCrc, &col->colType, sizeof(col->colType));
	}

	FIN_CRC32C(typeCrc);

	/* done with our hash identification, let's finesse this name */

	appendStringInfoString(string, type->typeName);

	/*
	 * Append the hash to the end of our string; if too long, we'll just copy
	 * the last hash piece over the part that doesn't work and terminate.
	 */

	appendStringInfo(string, "%8x", typeCrc);

	/*
	 * In order to make things a little easier, we replace any spaces with
	 * underscores, making it so we don't need to quote as many fields. Note
	 * that since we took the CRC of the actual field names, not the
	 * underscored version, we will be able to distinguish between `"foo
	 * column" INTEGER` and `foo_column INTEGER` so the whole readable
	 * non-hash version of the display is basically for the convenience of the
	 * user.
	 */

	for (int i = 0; i < string->len; i++)
		if (string->data[i] == ' ')
			string->data[i] = '_';

	if (string->len > NAMEDATALEN)
	{
		/*
		 * Okay, our string is too long, so we need to find a place to
		 * properly truncate to.  We assume the data is encoded in UTF-8; we
		 * need to find a character boundary to split things at or we'll get
		 * invalid UTF-8 code points.
		 */

		/*
		 * In our best case we have the 9 chars for underscore, hash plus 1
		 * for terminator.
		 */

		char	   *copyDest = string->data + NAMEDATALEN - 11;

		/*
		 * Now we have to back up for partial hi-bit chars. In UTF-8 encoding,
		 * partial characters all have the high bit pattern b10XXXXXX, so walk
		 * backwards to skip.  In validly-encoded UTF-8, we will be pointing
		 * at the initial character boundary at this point.  In
		 * invalidly-encoded UTF-8, this is as good a place to stop as any, as
		 * it's likely some other 8-bit encoding without the UTF-8 properties,
		 * so partial bytes will not be an issue for truncated character data.
		 */

		while (((*copyDest) & 0xC0) == 0x80)
			copyDest--;

		/*
		 * For valid UTF-8, we would remove no more than 3 partial bytes since
		 * 4 is the largest number of bytes that can be encoded.  This
		 * assertion should be safe anyway with the larger subtraction here,
		 * just a sanity-check that we didn't underrun on invalid data.
		 */

		Assert(copyDest > string->data + NAMEDATALEN - 15);

		/*
		 * At this point, copyDest is aligned to a character that we can
		 * overwrite without consequence. And we will.  We use memmove()
		 * instead of memcpy() for overlap-safe copy here.
		 */

		memmove(copyDest, string->data + string->len - 9, 9);

		/* Add terminator after our 9 chars */
		copyDest[9] = '\0';
	}

	type->isFinalized = true;
	type->typeName = string->data;
}

/*
 * Wrapper for decomposing a postgres type and returning the corresponding
 * STRUCT string.
 */
char *
GetDuckDBStructDefinitionForPGType(Oid postgresType)
{
	CompositeType *type = GetCompositeTypeForPGType(postgresType);

	return GetDuckDBStructDefinitionForCompositeType(type);
}

/*
 * This function turns a CompositeType into a STRUCT type string.  It is
 * basically the inverse of the ParseStructType() routine.
 */
char *
GetDuckDBStructDefinitionForCompositeType(CompositeType * type)
{
	ListCell   *lc;
	StringInfo	string = makeStringInfo();
	bool		processedOne = false;

	appendStringInfo(string, "STRUCT(");

	/* append our columns */
	foreach(lc, type->cols)
	{
		CompositeCol *col = (CompositeCol *) lfirst(lc);

		if (processedOne)
			appendStringInfoChar(string, ',');

		appendStringInfoString(string, QuoteDuckDBFieldName(col->colName));
		appendStringInfoChar(string, ' ');

		if (col->subStruct != NULL)
		{
			/* add a child composite type */
			appendStringInfoString(string,
								   GetDuckDBStructDefinitionForCompositeType(col->subStruct));
		}
		else
		{
			/*
			 * Now add our simple type string -- array handling added later,
			 * so use underlying element type.
			 */

			PGType		baseColumnType = MakePGTypeOid(GetRelatedTypeOid(col->colType, false));
			DuckDBType	duckDBType = GetDuckDBTypeForPGType(baseColumnType);

			if (duckDBType)
			{
				const char *duckDBName = GetFullDuckDBTypeNameForPGType(baseColumnType);

				if (duckDBName)
					appendStringInfoString(string, duckDBName);
				else
					ereport(ERROR, (errmsg("unresolved duckdb type name for type: %d", duckDBType),
									errcode(ERRCODE_INTERNAL_ERROR)));
			}
			else
				ereport(ERROR, (errmsg("composite types with a \"%s\" field cannot be exported to data lake",
									   format_type_be(col->colType)),
								errcode(ERRCODE_INDETERMINATE_DATATYPE)));
		}
		if (col->isArray)
			appendStringInfoString(string, "[]");

		/* set our sentinel */
		processedOne = true;
	}

	/* close the opening struct */
	appendStringInfoChar(string, ')');

	if (type->isArray)
		appendStringInfo(string, "[]");

	return string->data;
}


/*
 * This helper just quotes our input string to be used in a duckdb field name.
 */
const char *
QuoteDuckDBFieldName(char *fieldName)
{
	char	   *position = fieldName;

	/*
	 * If we are a reserved word or the field name starts with a digit, then
	 * we *must* quote, so advance position to end of the string.
	 */
	if (IsDuckDBReservedWord(fieldName) || isdigit(*position))
	{
		position = strchr(position, '\0');
	}
	else
	{
		/*
		 * Check if we even need quoting -- this does not include keyword
		 * consideration, so maybe we should unconditionally quote.
		 */
		while (*position && (isalnum(*position) || *position == '_'))
			position++;

		/*
		 * If we reached the end of string, we can just return the original
		 * input.
		 */
		if (!*position)
			return fieldName;
	}

	/*
	 * The current quoting can only add at most one additional character per
	 * input character, so we get a safe-sized buffer by doubling the input
	 * fieldName length and adding space for two quotes and a terminator.
	 */
	char	   *quotedBuf = palloc0(2 * strlen(fieldName) + 3);

	/* reset our position */
	position = quotedBuf;

	/* start quote */
	*position++ = '"';

	/* reuse fieldName to walk the source string */
	while (*fieldName)
	{
		char		writeChar = *fieldName;

		switch (writeChar)
		{
			case '"':
				*position++ = '\\';
				*position++ = '"';
				break;
			case '\\':
				*position++ = '\\';
				*position++ = '\\';
				break;
			case '\r':
				*position++ = '\\';
				*position++ = 'r';
				break;
			case '\n':
				*position++ = '\\';
				*position++ = 'n';
				break;
			case '\t':
				*position++ = '\\';
				*position++ = 't';
				break;
			default:
				*position++ = writeChar;
		}
		fieldName++;
	}

	/* end quote */
	*position++ = '"';

	/* quotedBuf is now our quoted string */
	return quotedBuf;
}


/*
 * Get a CompositeType* object from a PostgreSQL type id.  This is guaranteed to
 * have populated all of the relevant fields in the object, walking the defined
 * type to pull out the various relevant pieces.
 */

CompositeType *
GetCompositeTypeForPGType(Oid postgresType)
{
	TypeCacheEntry *typEntry = lookup_type_cache(postgresType, TYPECACHE_TUPDESC);

	/*
	 * If for some reason we aren't able to find the type in the typecache
	 * it's an error.
	 */

	if (!typEntry)
		return NULL;

	TupleDesc	tupDesc = typEntry->tupDesc;

	if (!tupDesc)
		return NULL;

	/*
	 * We need fields not currently in typecache (typname and typarray), so we
	 * also need to lookup the full pg_type row.
	 */
	HeapTuple	tup = SearchSysCache1(TYPEOID, postgresType);

	if (!HeapTupleIsValid(tup))
		return NULL;

	/* load our actual struct for the underlying row */
	Form_pg_type rowTuple = (Form_pg_type) GETSTRUCT(tup);

	/* go ahead and allocate an object we will use */
	CompositeType *myType = palloc0(sizeof(CompositeType));

	myType->typeOid = postgresType;
	myType->typeName = pstrdup(NameStr(rowTuple->typname));
	/* if typarray = 0, postgresType represents a top-level array */
	myType->isArray = rowTuple->typarray == 0;

	/*
	 * Now we need to iterate through the composite tupleDesc from syscache to
	 * process the columns.
	 */

	for (int i = 0; i < tupDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, i);

		/*
		 * If the column has been dropped, atttypid is 0, just skip without
		 * error.
		 */

		if (!attr->atttypid)
			continue;

		CompositeCol *col = palloc0(sizeof(CompositeCol));

		col->colName = pstrdup(NameStr(attr->attname));

		/*
		 * Our internal representation wants to track the base type of any
		 * array type and to carry a flag instead atttypid directly.
		 */

		Oid			rawTypid = attr->atttypid;

		col->colType = GetRelatedTypeOid(rawTypid, false);
		col->isArray = rawTypid != col->colType;

		/*
		 * We need more info about this type than just the name, so already
		 * need the syscache entry here; look up instead of just checking
		 * something like attndims > 0.
		 */

		HeapTuple	colTup = SearchSysCache1(TYPEOID, col->colType);

		if (!HeapTupleIsValid(colTup))
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("pg_lake_engine: unknown type when constructing CompositeCol")));

		Form_pg_type colTuple = (Form_pg_type) GETSTRUCT(colTup);

		/* get the name of the type itself */
		col->colTypeName = pstrdup(NameStr(colTuple->typname));

		/* finally, let's handle things for our dependent types */
		if (colTuple->typtype == TYPTYPE_COMPOSITE)
		{
			/* recursive entry here */
			col->subStruct = GetCompositeTypeForPGType(col->colType);

			if (!col->subStruct)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("pg_lake_engine: unexpected NULL struct returned for type oid %u", col->colType)));
		}
		ReleaseSysCache(colTup);

		/* our column is constructed, let's add to our column list */
		myType->cols = lappend(myType->cols, col);
	}

	/* cleanup our held tuple */
	ReleaseSysCache(tup);

	return myType;
}


/*
 * If getArray is true, return the typoid of the corresponding input type's
 * array, or the input type itself if it is already an array.
 *
 * If getArray is false, return the element typoid if the input type is an array
 * type, otherwise return the input type.
 */
static Oid
GetRelatedTypeOid(Oid inputType, bool getArray)
{
	HeapTuple	typTup = SearchSysCache1(TYPEOID, inputType);

	if (!HeapTupleIsValid(typTup))
		return InvalidOid;

	Form_pg_type typeTuple = (Form_pg_type) GETSTRUCT(typTup);

	Oid			returnOid;

	if (getArray)
		returnOid = typeTuple->typarray == 0 ? inputType : typeTuple->typarray;
	else
		returnOid = typeTuple->typarray == 0 ? typeTuple->typelem : inputType;

	ReleaseSysCache(typTup);

	return returnOid;
}


/*
 * This routine populates any unpopulated column type fields from the subStruct.
 */
static void
EnsureColumnTypesArePopulated(CompositeType * myType)
{
	ListCell   *lc;

	foreach(lc, myType->cols)
	{
		CompositeCol *col = (CompositeCol *) lfirst(lc);

		/* unknown type, we have to create */
		if (!col->colType)
		{
			if (col->subStruct && col->subStruct->typeOid)
				col->colType = GetRelatedTypeOid(col->subStruct->typeOid, col->isArray);
			else
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("pg_lake_engine: couldn't find sub-STRUCT link")));
		}
	}
}
