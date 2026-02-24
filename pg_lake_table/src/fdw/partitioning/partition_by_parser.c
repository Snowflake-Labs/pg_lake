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
#include "access/table.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/string_utils.h"

#include <inttypes.h>


/* parser helpers */
static bool IsEmptyPartitionBy(const char *partitionBy);
static void SkipWhitespace(const char **ptr);
static int	ParseInteger(const char **ptr);
static char *ParseIdentifier(const char **ptr);
static IcebergPartitionTransformType LookupTransformType(const char *str);
static ParsedIcebergPartitionTransform * ParseOneTransform(const char **ptr);
static const char *NormalizeTransformColumnName(const char *colName);

/* analyzer helpers */
static void EnsureTransformSourceColumnExists(ParsedIcebergPartitionTransform * parsedTransform, Oid relationId);
static void EnsureTransformSourceColumnScalar(ParsedIcebergPartitionTransform * parsedTransform, DataFileSchemaField * sourceField);
static void EnsureNoDuplicateTransforms(List *transforms);
static void EnsureValidTypeForTransform(IcebergPartitionTransformType transformType, Oid typeOid);
static void EnsureValidTypeForIdentityTransform(Oid typeOid);
static void EnsureValidTypeForYearTransform(Oid typeOid);
static void EnsureValidTypeForMonthTransform(Oid typeOid);
static void EnsureValidTypeForDayTransform(Oid typeOid);
static void EnsureValidTypeForHourTransform(Oid typeOid);
static void EnsureValidTypeForTruncateTransform(Oid typeOid);
static void EnsureValidTypeForBucketTransform(Oid typeOid);
static bool IsDateOrTimestampType(Oid typeOid);
static bool IsTimeOrTimestampType(Oid typeOid);
static const char *GenerateTransformName(IcebergPartitionTransform * transform);
static const char *GeneratePartitionFieldName(IcebergPartitionTransform * transform, Oid relationId);
static int32_t AdjustTypmodForTruncateTransformIfNeeded(IcebergPartitionTransform * transform);


/*
 * Parse the partition_by string (ex: "truncate(\"my, col\", 3), bucket(col2, 10), year(col3)")
 * and return a list of IcebergPartitionTransform.
 */
List *
ParseIcebergTablePartitionBy(Oid relationId)
{
	const char *partitionBy = GetIcebergTablePartitionByOption(relationId);

	if (partitionBy == NULL)
		return NIL;

	if (IsEmptyPartitionBy(partitionBy))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("partition_by option cannot be empty")));

	Assert(partitionBy != NULL);

	List	   *parsedTransforms = NIL;

	bool		seenComma = false;

	const char *p = partitionBy;

	while (true)
	{
		SkipWhitespace(&p);

		if (*p == '\0')
		{
			if (seenComma)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("expected transform name or column name after comma")));
			}
			else
			{
				break;			/* end of string => done */
			}
		}

		/* Parse one transform */
		ParsedIcebergPartitionTransform *parsedTransform = ParseOneTransform(&p);

		parsedTransforms = lappend(parsedTransforms, parsedTransform);

		/* after one transform, expect either comma or end of string */
		SkipWhitespace(&p);

		seenComma = false;

		if (*p == ',')
		{
			p++;				/* skip comma */
			seenComma = true;
			continue;
		}
		else if (*p == '\0')
		{
			/* done */
			break;
		}
		else
		{
			/* unexpected character => syntax error */
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unexpected character \"%c\" in partition_by string", *p)));
		}
	}

	return parsedTransforms;
}


/*
 * IsIcebergTableWithDefaultPartitionSpec returns true if the given iceberg table
 * has a default partition specification (i.e., no explicit partitioning).
 */
bool
IsIcebergTableWithDefaultPartitionSpec(Oid relationId)
{
	const char *partitionBy = GetIcebergTablePartitionByOption(relationId);

	return partitionBy == NULL;
}


/*
 * Parse one transform from the current position of the string.
 * (ex: "year(col)", "bucket(mycol, 10)", or just "colName")
 */
static ParsedIcebergPartitionTransform *
ParseOneTransform(const char **ptr)
{
	ParsedIcebergPartitionTransform *parsedTransform = palloc0(sizeof(ParsedIcebergPartitionTransform));

	/* Step 1: parse a token => either transform name or column name. */
	char	   *token = ParseIdentifier(ptr);

	if (!token)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid token while parsing transform")));

	SkipWhitespace(ptr);

	/*
	 * If next char is '(', then 'token' is presumably the transform name.
	 * Otherwise, 'token' is the column name => IDENTITY transform.
	 */
	if (**ptr == '(')
	{
		/* So 'token' is transform name (year, month, etc.) */
		parsedTransform->type = LookupTransformType(token);

		(*ptr)++;				/* skip '(' */
		SkipWhitespace(ptr);

		if (parsedTransform->type == PARTITION_TRANSFORM_BUCKET ||
			parsedTransform->type == PARTITION_TRANSFORM_TRUNCATE)
		{
			int			param = ParseInteger(ptr);

			SkipWhitespace(ptr);

			/* Expect comma before identifier. */
			if (**ptr == ',')
			{
				(*ptr)++;

				if (parsedTransform->type == PARTITION_TRANSFORM_BUCKET)
					parsedTransform->bucketCount = (size_t) param;
				else
					parsedTransform->truncateLen = (size_t) param;
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("expected comma after column name for \"%s\" transform",
								(parsedTransform->type == PARTITION_TRANSFORM_BUCKET) ? "bucket" : "truncate")));
			}
		}

		SkipWhitespace(ptr);

		/* Next parse the column name inside parentheses. */
		char	   *colName = ParseIdentifier(ptr);

		parsedTransform->columnName = NormalizeTransformColumnName(colName);

		SkipWhitespace(ptr);
		if (**ptr == ')')
		{
			(*ptr)++;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("missing closing parenthesis in partition transform")));
		}
	}
	else
	{
		/* The token is the column name; interpret as IDENTITY transform. */
		parsedTransform->type = PARTITION_TRANSFORM_IDENTITY;
		parsedTransform->columnName = NormalizeTransformColumnName(token);
	}

	return parsedTransform;
}


static bool
IsEmptyPartitionBy(const char *partitionBy)
{
	if (partitionBy == NULL)
		return true;

	/* Skip leading whitespace */
	const char *p = partitionBy;

	SkipWhitespace(&p);

	return *p == '\0';
}


/*
 * Skip whitespace in the string.
 */
static void
SkipWhitespace(const char **ptr)
{
	while ((**ptr) && isspace((unsigned char) **ptr))
		(*ptr)++;
}


/*
 * Parse an integer from the current position, consuming digits.
 * Raise an error if we don't see any digit.
 */
static int
ParseInteger(const char **ptr)
{
	int32_t		val = 0;
	bool		hasDigit = false;

	SkipWhitespace(ptr);

	while (isdigit((unsigned char) **ptr))
	{
		val = val * 10 + ((**ptr) - '0');
		(*ptr)++;
		hasDigit = true;

		if (val <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bucket or truncate size must be between 1 and %" PRId32, INT32_MAX)));
	}

	if (!hasDigit)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("expected integer parameter for partition transform")));

	return val;
}


/*
 * Parse a possibly quoted identifier.
 * If quoted with double quotes ("), read until matching quote.
 * If unquoted, read until whitespace, comma, or parenthesis.
 * Return a freshly palloc'ed string or raise an error if invalid.
 */
static char *
ParseIdentifier(const char **ptr)
{
	SkipWhitespace(ptr);

	if (**ptr == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unexpected end of string while parsing identifier")));

	/* If quoted */
	if (**ptr == '"')
	{
		char		quote = **ptr;
		const char *start;
		size_t		len;

		start = *ptr;

		(*ptr)++;				/* skip initial quote */

		/*
		 * skip until final quote by skipping escaped quotes (double quotes
		 * are escaped quote, e.g. """"
		 */
		bool		seenFinalQuote = false;

		while ((**ptr) && !seenFinalQuote)
		{
			if (**ptr == quote)
			{
				if (*(*ptr + 1) && *(*ptr + 1) == quote)
				{
					/* skip escaped quote */
					(*ptr) += 2;
				}
				else
				{
					seenFinalQuote = true;
				}
			}
			else
			{
				(*ptr)++;
			}
		}

		if (**ptr != quote)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("missing closing quote \"%c\"", quote)));

		(*ptr)++;				/* skip closing quote */

		len = *ptr - start;

		/* Allocate and copy */
		char	   *ident = (char *) palloc(len + 1);

		memcpy(ident, start, len);
		ident[len] = '\0';

		SkipWhitespace(ptr);

		return ident;
	}
	else
	{
		/* Unquoted case */
		const char *start = *ptr;
		size_t		len = 0;

		while (**ptr &&
			   !isspace((unsigned char) **ptr) &&
			   **ptr != '(' &&
			   **ptr != ')' &&
			   **ptr != ',')
		{
			(*ptr)++;
			len++;
		}

		if (len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expected identifier while parsing partition_by")));

		char	   *ident = (char *) palloc(len + 1);

		memcpy(ident, start, len);
		ident[len] = '\0';

		SkipWhitespace(ptr);
		return ident;
	}
}


/*
 * Convert string to a known partition transform type.
 * If not recognized, return IDENTITY.
 */
static IcebergPartitionTransformType
LookupTransformType(const char *str)
{
	if (pg_strcasecmp(str, "year") == 0)
		return PARTITION_TRANSFORM_YEAR;
	else if (pg_strcasecmp(str, "month") == 0)
		return PARTITION_TRANSFORM_MONTH;
	else if (pg_strcasecmp(str, "day") == 0)
		return PARTITION_TRANSFORM_DAY;
	else if (pg_strcasecmp(str, "hour") == 0)
		return PARTITION_TRANSFORM_HOUR;
	else if (pg_strcasecmp(str, "bucket") == 0)
		return PARTITION_TRANSFORM_BUCKET;
	else if (pg_strcasecmp(str, "truncate") == 0)
		return PARTITION_TRANSFORM_TRUNCATE;
	else if (pg_strcasecmp(str, "void") == 0)
		return PARTITION_TRANSFORM_VOID;

	/* Not recognized */
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unknown partition transform \"%s\"", str)));
}


/*
 * NormalizeTransformColumnName normalizes the column name for the transform.
 * It removes any quotes, if quoted, from column name without changing case.
 * It lowers the case of the column name if not quoted.
 */
static const char *
NormalizeTransformColumnName(const char *colName)
{
	if (strlen(colName) > 1 && colName[0] == '"' && colName[strlen(colName) - 1] == '"')
	{
		/* Remove starting and final quotes */
		char	   *normalized = pstrdup(colName + 1);

		normalized[strlen(normalized) - 1] = '\0';

		/* unescape double quotes */
		for (int i = 0; i < strlen(normalized); i++)
		{
			if (normalized[i] == '"' && normalized[i + 1] == '"')
			{
				memmove(normalized + i, normalized + i + 1, strlen(normalized) - i);
			}
		}

		return normalized;
	}
	else
	{
		/* Lower case */
		char	   *normalized = pstrdup(colName);

		for (int i = 0; normalized[i]; i++)
			normalized[i] = tolower(normalized[i]);
		return normalized;
	}
}


/*
 * AnalyzeIcebergTablePartitionBy analyzes the already parsed partition_by
 * of given iceberg table, if any. Returns a list of IcebergPartitionTransform.
 *
 * It checks that each referenced column exists, is scalar, has an appropriate type for the transform,
 * and also checks for duplicate transforms on the same source id in the partition spec.
 */
List *
AnalyzeIcebergTablePartitionBy(Oid relationId, List *parsedTransforms)
{
	int			largestPartitionFieldId = GetLargestPartitionFieldId(relationId);

	List	   *analyzedTransforms = NIL;

	/* analyze */
	ListCell   *parsedTransformCell = NULL;

	foreach(parsedTransformCell, parsedTransforms)
	{
		ParsedIcebergPartitionTransform *parsedTransform = lfirst(parsedTransformCell);

		/*
		 * 1) Look up the column in the relation's TupleDesc. We do a
		 * case-sensitive or case-insensitive match depending on your FDW's
		 * requirements. Here we do an exact match.
		 */
		EnsureTransformSourceColumnExists(parsedTransform, relationId);

		IcebergPartitionTransform *analyzedTransform = palloc0(sizeof(IcebergPartitionTransform));

		analyzedTransform->parsedTransform = *parsedTransform;

		/* set column no */
		analyzedTransform->attnum = get_attnum(relationId, parsedTransform->columnName);

		Oid			collation = InvalidOid;

		get_atttypetypmodcoll(relationId, analyzedTransform->attnum,
							  &analyzedTransform->pgType.postgresTypeOid,
							  &analyzedTransform->pgType.postgresTypeMod,
							  &collation);

		/* set column type */
		if (!(collation == InvalidOid || collation == DEFAULT_COLLATION_OID ||
			  collation == C_COLLATION_OID))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Columns with collation are not supported in partition_by: \"%s\"",
							parsedTransform->columnName)));
		}

		/* set result column type */
		analyzedTransform->resultPgType = GetTransformResultPGType(analyzedTransform);
		/* set source field id */
		DataFileSchemaField *sourceField = GetRegisteredFieldForAttribute(relationId, analyzedTransform->attnum);

		/* 2) Check scalar column */
		EnsureTransformSourceColumnScalar(parsedTransform, sourceField);

		analyzedTransform->sourceField = sourceField;

		/* set transform name */
		analyzedTransform->specField = palloc0(sizeof(IcebergPartitionSpecField));
		analyzedTransform->specField->transform = GenerateTransformName(analyzedTransform);
		analyzedTransform->specField->transform_length = strlen(analyzedTransform->specField->transform);

		/* set partition field name */
		analyzedTransform->specField->name = GeneratePartitionFieldName(analyzedTransform, relationId);
		analyzedTransform->specField->name_length = strlen(analyzedTransform->specField->name);

		/* set partition field id */
		analyzedTransform->specField->field_id = ++largestPartitionFieldId;
		/* set source field id */
		analyzedTransform->specField->source_id = sourceField->id;
		analyzedTransform->specField->source_ids_length = 1;
		analyzedTransform->specField->source_ids = palloc0(sizeof(int) * analyzedTransform->specField->source_ids_length);
		analyzedTransform->specField->source_ids[0] = analyzedTransform->specField->source_id;

		/* 3) Check column type compatibility. */
		EnsureValidTypeForTransform(analyzedTransform->parsedTransform.type, analyzedTransform->pgType.postgresTypeOid);

		analyzedTransforms = lappend(analyzedTransforms, analyzedTransform);
	}

	/*
	 * 4) Check for duplicate transforms on the same source id.
	 */
	EnsureNoDuplicateTransforms(analyzedTransforms);

	return analyzedTransforms;
}


static bool
IsDateOrTimestampType(Oid typeOid)
{
	return (typeOid == DATEOID ||
			typeOid == TIMESTAMPOID ||
			typeOid == TIMESTAMPTZOID);
}


static bool
IsTimeOrTimestampType(Oid typeOid)
{
	return (typeOid == TIMEOID ||
			typeOid == TIMESTAMPOID ||
			typeOid == TIMESTAMPTZOID);
}


const char *
GetIcebergTablePartitionByOption(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	DefElem    *partitionByOption = GetOption(options, "partition_by");

	if (partitionByOption == NULL)
		return NULL;

	return defGetString(partitionByOption);
}


/*
 * GenerateTransformName generates the transform name for the given transform.
 */
static const char *
GenerateTransformName(IcebergPartitionTransform * transform)
{
	switch (transform->parsedTransform.type)
	{
		case PARTITION_TRANSFORM_IDENTITY:
			return "identity";
		case PARTITION_TRANSFORM_BUCKET:
			return psprintf("bucket[%zu]", transform->parsedTransform.bucketCount);
		case PARTITION_TRANSFORM_TRUNCATE:
			return psprintf("truncate[%zu]", transform->parsedTransform.truncateLen);
		case PARTITION_TRANSFORM_YEAR:
			return "year";
		case PARTITION_TRANSFORM_MONTH:
			return "month";
		case PARTITION_TRANSFORM_DAY:
			return "day";
		case PARTITION_TRANSFORM_HOUR:
			return "hour";
		case PARTITION_TRANSFORM_VOID:
			return "void";
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unknown partition transform type %d", transform->parsedTransform.type)));
	}
}


/*
 * GeneratePartitionFieldName generates the partition field name for given transform.
 */
static const char *
GeneratePartitionFieldName(IcebergPartitionTransform * transform, Oid relationId)
{
	switch (transform->parsedTransform.type)
	{
		case PARTITION_TRANSFORM_IDENTITY:
			return transform->parsedTransform.columnName;
		case PARTITION_TRANSFORM_BUCKET:
			return psprintf("%s_bucket_%zu", transform->parsedTransform.columnName, transform->parsedTransform.bucketCount);
		case PARTITION_TRANSFORM_TRUNCATE:
			return psprintf("%s_trunc_%zu", transform->parsedTransform.columnName, transform->parsedTransform.truncateLen);
		case PARTITION_TRANSFORM_YEAR:
			return psprintf("%s_year", transform->parsedTransform.columnName);
		case PARTITION_TRANSFORM_MONTH:
			return psprintf("%s_month", transform->parsedTransform.columnName);
		case PARTITION_TRANSFORM_DAY:
			return psprintf("%s_day", transform->parsedTransform.columnName);
		case PARTITION_TRANSFORM_HOUR:
			return psprintf("%s_hour", transform->parsedTransform.columnName);
		case PARTITION_TRANSFORM_VOID:
			return psprintf("%s_void", transform->parsedTransform.columnName);
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unknown partition transform type %d", transform->parsedTransform.type)));
	}
}


/*
 * EnsureTransformSourceColumnExists checks that the source column for the
 * given transform exists in the relation. If it doesn't, it raises an error.
 */
static void
EnsureTransformSourceColumnExists(ParsedIcebergPartitionTransform * parsedTransform, Oid relationId)
{
	Relation	rel;
	TupleDesc	desc;
	bool		foundCol = false;

	/* Open the relation to examine its attributes. */
	rel = table_open(relationId, AccessShareLock);
	desc = RelationGetDescr(rel);

	/*
	 * Look up the column in the relation's TupleDesc. We do a case-sensitive
	 * or case-insensitive match depending on your FDW's requirements. Here we
	 * do an exact match.
	 */
	for (int attnum = 0; attnum < desc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, attnum);

		if (attr->attisdropped)
			continue;			/* skip dropped columns */

		if (strcmp(NameStr(attr->attname), parsedTransform->columnName) == 0)
		{
			foundCol = true;
			break;
		}
	}

	if (!foundCol)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist in relation \"%s\"",
						parsedTransform->columnName,
						get_rel_name(relationId))));
	}

	table_close(rel, AccessShareLock);
}


/*
  * GetTransformResultPGType returns the result type of the transform.
  */
PGType
GetTransformResultPGType(IcebergPartitionTransform * transform)
{
	Oid			resultType = InvalidOid;
	int32_t		resultTypMod = -1;

	switch (transform->parsedTransform.type)
	{
		case PARTITION_TRANSFORM_IDENTITY:
			resultType = transform->pgType.postgresTypeOid;
			resultTypMod = transform->pgType.postgresTypeMod;
			break;
		case PARTITION_TRANSFORM_TRUNCATE:
			{
				if (transform->pgType.postgresTypeOid == INT2OID)
				{
					/* to not overflow during truncation */
					resultType = INT4OID;
				}
				else
				{
					resultType = transform->pgType.postgresTypeOid;
				}

				resultTypMod = AdjustTypmodForTruncateTransformIfNeeded(transform);

				break;
			}
		case PARTITION_TRANSFORM_BUCKET:
		case PARTITION_TRANSFORM_YEAR:
		case PARTITION_TRANSFORM_MONTH:
		case PARTITION_TRANSFORM_DAY:
		case PARTITION_TRANSFORM_HOUR:
			resultType = INT4OID;
			resultTypMod = -1;
			break;
		case PARTITION_TRANSFORM_VOID:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("void partition transform is not supported")));
			break;				/* not needed, but compiler complaining */
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unknown partition transform type %d", transform->parsedTransform.type)));
			break;
	}

	return MakePGType(resultType, resultTypMod);
}


/*
 * AdjustTypmodForTruncateTransformIfNeeded adjusts the typmod for the truncate transform
 * if the transform applies to a source column for which we need to adjust result typmod.
 *
 * It currently only applies to VARCHAR(n) and BPCHAR(n) types.
 */
static int32_t
AdjustTypmodForTruncateTransformIfNeeded(IcebergPartitionTransform * transform)
{
	PGType		pgType = transform->pgType;

	int32_t		resultTypMod = -1;

	/*
	 * Adjust varchar(N) and bpchar(N) to varchar(truncateLen) and
	 * bpchar(truncateLen) only if truncateLen is lower than the current
	 * length. Otherwise, we keep the original typmod.
	 *
	 * - varchar(N): any longer (> N) string will be truncated to N.
	 *
	 * - bpchar(N): any smaller (< N) string will be space-padded to N.
	 */
	if ((pgType.postgresTypeOid == VARCHAROID || pgType.postgresTypeOid == BPCHAROID) &&
		pgType.postgresTypeMod != -1 && GetAnyCharLengthFrom(pgType.postgresTypeMod) > transform->parsedTransform.truncateLen)
	{
		resultTypMod = AdjustAnyCharTypmod(transform->pgType.postgresTypeMod,
										   transform->parsedTransform.truncateLen);
	}
	else
	{
		resultTypMod = transform->pgType.postgresTypeMod;
	}

	return resultTypMod;
}


/*
 * EnsureTransformSourceColumnScalar checks that the source column for the
 * given transform is a scalar type. If it isn't, it raises an error.
 */
static void
EnsureTransformSourceColumnScalar(ParsedIcebergPartitionTransform * parsedTransform, DataFileSchemaField * sourceField)
{
	if (sourceField->type->type != FIELD_TYPE_SCALAR)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("partition transform column \"%s\" must be a scalar type",
						parsedTransform->columnName)));
}


/*
 * EnsureNoDuplicateTransforms ensures that there are no duplicate transforms
 * in the list of transforms.
 */
static void
EnsureNoDuplicateTransforms(List *transforms)
{
	for (int i = 0; i < list_length(transforms); i++)
	{
		IcebergPartitionTransform *transform = list_nth(transforms, i);

		for (int j = i + 1; j < list_length(transforms); j++)
		{
			IcebergPartitionTransform *other = list_nth(transforms, j);

			if (transform->parsedTransform.type == other->parsedTransform.type &&
				transform->sourceField->id == other->sourceField->id)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("\"%s\" transform on column \"%s\" appears multiple times in partition spec",
								transform->specField->transform, transform->parsedTransform.columnName)));
		}
	}
}


static void
EnsureValidTypeForTransform(IcebergPartitionTransformType transformType, Oid typeOid)
{
	switch (transformType)
	{
		case PARTITION_TRANSFORM_YEAR:
			EnsureValidTypeForYearTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_MONTH:
			EnsureValidTypeForMonthTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_DAY:
			EnsureValidTypeForDayTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_HOUR:
			EnsureValidTypeForHourTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_BUCKET:
			EnsureValidTypeForBucketTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_TRUNCATE:
			EnsureValidTypeForTruncateTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_IDENTITY:
			EnsureValidTypeForIdentityTransform(typeOid);
			break;
		case PARTITION_TRANSFORM_VOID:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("void partition transform is not supported")));
		default:
			break;
	}
}


static void
EnsureValidTypeForIdentityTransform(Oid typeOid)
{
	/*
	 * Identity transform typically can apply to any Iceberg base type except
	 * for geometry, geography, and variant. But these types (v3) are not
	 * supported in pg_lake_iceberg tables.
	 */
	if (!IsDateOrTimestampType(typeOid) && !IsTimeOrTimestampType(typeOid) &&
		typeOid != INT2OID && typeOid != INT4OID && typeOid != INT8OID && typeOid != BOOLOID &&
		typeOid != FLOAT4OID && typeOid != FLOAT8OID && typeOid != NUMERICOID &&
		typeOid != TEXTOID && typeOid != VARCHAROID && typeOid != BPCHAROID &&
		typeOid != BYTEAOID && typeOid != UUIDOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("identity transform requires a date, timestamp(tz), time(tz), "
						"int2, int4, int8, bool, float4, float8, numeric, text, bytea, varchar or "
						"uuid column, but type is %s",
						format_type_be(typeOid))));
}


static void
EnsureValidTypeForYearTransform(Oid typeOid)
{
	if (!IsDateOrTimestampType(typeOid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("year transform requires a date or timestamp(tz) column, but type is %s",
						format_type_be(typeOid))));
}


static void
EnsureValidTypeForMonthTransform(Oid typeOid)
{
	if (!IsDateOrTimestampType(typeOid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("month transform requires a date or timestamp(tz) column, but type is %s",
						format_type_be(typeOid))));
}


static void
EnsureValidTypeForDayTransform(Oid typeOid)
{
	if (!IsDateOrTimestampType(typeOid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("day transform requires a date or timestamp(tz) column, but type is %s",
						format_type_be(typeOid))));
}


static void
EnsureValidTypeForHourTransform(Oid typeOid)
{
	if (!IsTimeOrTimestampType(typeOid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("hour transform requires a time(tz) or timestamp(tz) column, but type is %s",
						format_type_be(typeOid))));
}


static void
EnsureValidTypeForTruncateTransform(Oid typeOid)
{
	/*
	 * normally truncate transform works on NUMERICOID type. But we disallow
	 * it due to numeric overflows during truncation. See spark issue
	 * https://github.com/apache/iceberg/issues/12915
	 */

	if (typeOid != INT2OID && typeOid != INT4OID && typeOid != INT8OID &&
		typeOid != TEXTOID && typeOid != VARCHAROID &&
		typeOid != BPCHAROID && typeOid != BYTEAOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("truncate transform requires a int2, int4, int8, text, varchar or "
						"bytea column, but type is %s",
						format_type_be(typeOid))));
}


static void
EnsureValidTypeForBucketTransform(Oid typeOid)
{
	if (!IsDateOrTimestampType(typeOid) && !IsTimeOrTimestampType(typeOid) &&
		typeOid != INT2OID && typeOid != INT4OID && typeOid != INT8OID &&
		typeOid != NUMERICOID && typeOid != TEXTOID && typeOid != VARCHAROID &&
		typeOid != BPCHAROID && typeOid != BYTEAOID && typeOid != UUIDOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("bucket transform requires a int2, int4, int8, numeric, text, varchar, bytea or"
						"uuid column, but type is %s",
						format_type_be(typeOid))));
}
