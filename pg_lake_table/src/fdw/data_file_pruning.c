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
* data_file_pruning.c
*   PruneDataFiles - Prune data files based on the filters in the query
*   execution and statistics of the data files.
*/
#include "postgres.h"

#include "catalog/pg_am_d.h"
#include "catalog/pg_index.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "commands/defrem.h"
#include "common/int.h"
#include "common/hashfn.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/data_file_pruning.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/temporal_utils.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/iceberg/hash_utils.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/s3_reader_utils.h"

bool		EnableDataFilePruning = true;
bool		EnablePartitionPruning = true;

typedef struct ColumnToFieldIdMapping
{
	/* key */
	AttrNumber	attrNo;

	/* value */
	int			fieldId;
	Var		   *column;

	/*
	 * We cache the bool operator (col >= null and col <= null) and (col =
	 * null) constraints for the column to avoid creating them multiple times.
	 * This has a significant performance overhead, so be careful when
	 * changing this. Note that null is a placeholder for the min/max value,
	 * which we will replace with the actual min/max value when creating the
	 * constraints.
	 */
	BoolExpr   *columnBoundInclusiveUpper;
	BoolExpr   *columnBoundExclusiveUpper;
	OpExpr	   *equalityOperatorExpression;
	NullTest   *isNullExpression;

	/*
	 * This is for bucket partitioning, see
	 * BucketPartitionFieldBoundConstraint on how this is used.
	 */
	OpExpr	   *syntheticColEqualityOperator;

	bool		constByVal;
	int16		typLen;
}			ColumnToFieldIdMapping;

static HTAB *CreateFieldIdMappingHash(void);
static void AddFieldIdsUsedInQuery(HTAB *fieldIdsToUseInBounds, Oid relationId,
								   PgLakeTableProperties tableProperties,
								   List *columnsUsedInFilters);
static List *GetExternalIcebergFieldsForAttributes(Oid relationId, List *attrNos);
static List *GetColumnBoundConstraints(Oid relationId, HTAB *fieldIdCache, List *columnStats,
									   List *partitionTransforms, Partition * partition);
static List *GetColumnBoundConstraintsFromColumnStats(Oid relationId, List *columnStats,
													  ColumnToFieldIdMapping * entry);
static List *GetColumnBoundConstraintsFromPartitions(Oid relationId, ColumnToFieldIdMapping * entry,
													 List *partitionTransforms, Partition * partition);
static void StripAllImplicitCoercionsInList(List *baseRestrictInfoList);
static Node *StripAllImplicitCoercions(Node *expr);
static Node *StripAllImplicitCoercionsMutator(Node *node, void *context);
static List *ExtractClausesFromBaseRestrictInfos(List *baseRestrictInfoList);
static BoolExpr *BuildConstraintsWithNullConst(Var *variable, int lessThanStrategyNumber,
											   int greaterThanStrategyNumber);
static List *ColumnsUsedInRestrictions(Oid relationId, List *baseRestrictInfoList);
static BoolExpr *CreateConstraintWithBounds(BoolExpr *node, bool constByVal, int16 typLen, Datum minValue, Datum maxValue);
static OpExpr *MakeOpExpression(Var *variable, int16 strategyNumber);
static NullTest *MakeIsNullExpression(Var *variable);
static Oid	GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static HTAB *TryCreateBatchFilterHash(List *baseRestrictInfoList, Var *filenameCol);

/* partition pruning functions */
static Expr *PartitionFieldBoundConstraint(PartitionField * partitionField,
										   IcebergPartitionTransform * partitionTransform,
										   ColumnToFieldIdMapping * entry);
static BoolExpr *TruncatePartitionFieldBoundConstraint(PartitionField * partitionField,
													   IcebergPartitionTransform * partitionTransform,
													   ColumnToFieldIdMapping * entry);

static Expr *IdentityPartitionFieldBoundConstraint(PartitionField * partitionField,
												   IcebergPartitionTransform * partitionTransform,
												   ColumnToFieldIdMapping * entry);
static BoolExpr *YearPartitionFieldBoundConstraint(PartitionField * partitionField,
												   IcebergPartitionTransform * partitionTransform,
												   ColumnToFieldIdMapping * entry);
static BoolExpr *MonthPartitionFieldBoundConstraint(PartitionField * partitionField,
													IcebergPartitionTransform * partitionTransform,
													ColumnToFieldIdMapping * entry);
static BoolExpr *DayPartitionFieldBoundConstraint(PartitionField * partitionField,
												  IcebergPartitionTransform * partitionTransform,
												  ColumnToFieldIdMapping * entry);
static BoolExpr *HourPartitionFieldBoundConstraint(PartitionField * partitionField,
												   IcebergPartitionTransform * partitionTransform,
												   ColumnToFieldIdMapping * entry);
static OpExpr *BucketPartitionFieldBoundConstraint(PartitionField * partitionField,
												   IcebergPartitionTransform * partitionTransform,
												   ColumnToFieldIdMapping * entry);

static OpExpr *CreateConstraintWithEquality(OpExpr *eqOp, bool constByVal, int16 typLen,
											Datum equalityDatum);
static char *TruncateUpperBoundForText(char *upperBound, size_t truncateLen);
static bytea *TruncateUpperBoundForBytea(bytea *data, int truncateLen);
static List *ExtendClausesForBucketPartitioning(Partition * partition,
												List *partitionTransforms,
												HTAB *fieldIdMapping,
												List *baseRestrictInfoList);
static List *ExtendBaseRestrictInfoForBucketPartition(List *baseRestrictInfoList,
													  IcebergPartitionTransform * partitionTransform,
													  OpExpr *syntheticColEqualityOperator);
static OpExpr *GetSyntheticBucketForPartitionField(PartitionField * partitionField,
												   OpExpr *syntheticColEqualityOperator);
static Const *ConstantEqualityOperatorExpressionOnColumn(Expr *clause, AttrNumber attrNo);
static bool EqualityOperator(Oid opno);
static Var *MakeSyntheticInt4Column(AttrNumber attrNo);
static Const *TransformConstToTargetType(Const *constantClause, Oid targetTypeId,
										 int32_t targetTypMod);

/*
* PruneDataFiles prunes the data files based on the filters in the query
* execution and statistics of the data files.
* baseRestrictInfoList is the list of RestrictInfo nodes that represent the
* filters in the query execution for the given relationId.
*/
List *
PruneDataFiles(Oid relationId, List *dataFiles, List *baseRestrictInfoList, PruneType pruneType)
{
	List	   *retainedDataFiles = NIL;
	List	   *columnsUsedInFilters = ColumnsUsedInRestrictions(relationId, baseRestrictInfoList);
	List	   *partitionTransforms = AllPartitionTransformList(relationId);
	PgLakeTableProperties tableProperties = GetPgLakeTableProperties(relationId);
	PgLakeTableType tableType = tableProperties.tableType;
	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	if ((!EnableDataFilePruning && !EnablePartitionPruning) ||
		!IsInternalOrExternalIcebergTable(tableProperties))
	{
		/*
		 * User disabled or no columns used in filters, so we cannot prune any
		 * files. We also do not prune files for non-iceberg tables as there
		 * are no column stats for those.
		 *
		 * For full match we say nothing matches,
		 */
		return pruneType == FULL_MATCH ? NIL : dataFiles;
	}

	/* special case for full match: no clauses means all match */
	if (pruneType == FULL_MATCH && baseRestrictInfoList == NIL)
		return dataFiles;

	/*
	 * If there are no filters on columns, there is no meaningful pruning we
	 * can do. We check this after the special case of no clauses + full
	 * match, since that also has no column filters but we still want pruning.
	 */
	if (columnsUsedInFilters == NIL)
		return pruneType == FULL_MATCH ? NIL : dataFiles;

	/*
	 * predicate_refuted_by() expects the baseRestrictInfoList to have no
	 * implicit coercions, so we strip.
	 */
	StripAllImplicitCoercionsInList(baseRestrictInfoList);

	/*
	 * predicated_implied_by requires a simple clause list.
	 */
	List	   *clauses = ExtractClausesFromBaseRestrictInfos(baseRestrictInfoList);

	/*
	 * Create a hash table to store the mapping of attrNo->fieldId, column for
	 * the columns used in the filters. We do this here to avoid doing this
	 * for each data file.
	 */
	HTAB	   *fieldIdsUsedInQuery = CreateFieldIdMappingHash();

	AddFieldIdsUsedInQuery(fieldIdsUsedInQuery, relationId, tableProperties, columnsUsedInFilters);

	int			dataFileCount = list_length(dataFiles);

	for (int dataFileIndex = 0; dataFileIndex < dataFileCount; ++dataFileIndex)
	{
		List	   *columnBoundConstraints = NIL;
		List	   *columnStats = NIL;
		Partition  *partition = NULL;

		if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE &&
			(icebergCatalogType == POSTGRES_CATALOG || icebergCatalogType == REST_CATALOG_READ_WRITE ||
			 icebergCatalogType == OBJECT_STORE_READ_WRITE))
		{
			TableDataFile *tableDataFile = (TableDataFile *) list_nth(dataFiles, dataFileIndex);

			Assert(tableDataFile->content == CONTENT_DATA);
			columnStats = tableDataFile->stats.columnStats;
			partition = tableDataFile->partition;
		}
		else if ((tableType == PG_LAKE_TABLE_TYPE && tableProperties.format == DATA_FORMAT_ICEBERG) ||
				 (tableType == PG_LAKE_ICEBERG_TABLE_TYPE &&
				  (icebergCatalogType == REST_CATALOG_READ_ONLY ||
				   icebergCatalogType == OBJECT_STORE_READ_ONLY)))
		{
			DataFile   *dataFile = (DataFile *) list_nth(dataFiles, dataFileIndex);

			List	   *leafFields = GetLeafFieldsForTable(relationId);
			char	   *dataFilePath = (char *) ((DataFile *) dataFile)->file_path;

			columnStats = GetRemoteParquetColumnStats(dataFilePath, leafFields);
		}
		else
		{
			/* never expect to get here, still better than assert */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Unsupported table type for data file pruning")));
		}

		/* calculate bound constraints */
		columnBoundConstraints =
			GetColumnBoundConstraints(relationId, fieldIdsUsedInQuery, columnStats,
									  partitionTransforms, partition);

		bool		includeDataFile = false;

		if (pruneType == PARTIAL_MATCH)
		{
			/* for bucket partitioning, we add some extra synthetic filters */
			List	   *extendedClauses =
				ExtendClausesForBucketPartitioning(partition, partitionTransforms,
												   fieldIdsUsedInQuery,
												   clauses);

			/* include if column constraints are not refuted by WHERE filter */
			includeDataFile = !predicate_refuted_by(columnBoundConstraints, extendedClauses, false);
		}
		else
		{
			/* include if column constraints are implied by WHERE filter */
			includeDataFile = predicate_implied_by(clauses, columnBoundConstraints, false);
		}

		if (includeDataFile)
			retainedDataFiles = lappend(retainedDataFiles, list_nth(dataFiles, dataFileIndex));
	}

	return retainedDataFiles;
}


/*
* CreateFieldIdMappingHash creates a hash table to store the mapping of
* fieldIds to the corresponding pgAttNum, pgType, and aims to check if
* the column is used in the filters.
*/
static HTAB *
CreateFieldIdMappingHash(void)
{
	/* create the HTAB */
	HASHCTL		hashCtl;

	hashCtl.keysize = sizeof(AttrNumber);
	hashCtl.entrysize = sizeof(ColumnToFieldIdMapping);
	hashCtl.hcxt = CurrentMemoryContext;
	hashCtl.hash = tag_hash;

	uint32		hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB	   *fieldIdsToUseInBounds = hash_create("ColumnToFieldIdMapping", 32, &hashCtl, hashFlags);

	return fieldIdsToUseInBounds;
}


/*
* AddFieldIdsUsedInQuery adds the fieldIds for the columns used in the filters
* to the fieldIdsToUseInBounds hash table.
*/
static void
AddFieldIdsUsedInQuery(HTAB *fieldIdsUsedInQuery, Oid relationId, PgLakeTableProperties tableProperties,
					   List *columnsUsedInFilters)
{
	PgLakeTableType tableType = tableProperties.tableType;
	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	List	   *attrNos = NIL;
	ListCell   *columnCell = NULL;

	foreach(columnCell, columnsUsedInFilters)
	{
		Var		   *column = lfirst(columnCell);
		AttrNumber	pgAttNum = column->varattno;

		attrNos = lappend_int(attrNos, pgAttNum);
	}

	/* fetch the field mappings for all columns in a single catalog lookup */
	List	   *fields = NIL;

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE &&
		(icebergCatalogType == POSTGRES_CATALOG || icebergCatalogType == REST_CATALOG_READ_WRITE ||
		 icebergCatalogType == OBJECT_STORE_READ_WRITE))
	{
		fields = GetRegisteredFieldForAttributes(relationId, attrNos);

		/*
		 * it is guaranteed that GetRegisteredFieldForAttributes() returns the
		 * same number of elements
		 */
		Assert(list_length(fields) == list_length(columnsUsedInFilters));
	}
	else if ((tableType == PG_LAKE_TABLE_TYPE && tableProperties.format == DATA_FORMAT_ICEBERG) ||
			 (tableType == PG_LAKE_ICEBERG_TABLE_TYPE && (icebergCatalogType == REST_CATALOG_READ_ONLY ||
														  icebergCatalogType == OBJECT_STORE_READ_ONLY)))
	{
		fields = GetExternalIcebergFieldsForAttributes(relationId, columnsUsedInFilters);

		/*
		 * We did our best to find the field for each column used in the
		 * filters, but we could not find the field for at least one of the
		 * columns. So, we skip data file pruning for this relation.
		 */
		if (list_length(fields) != list_length(columnsUsedInFilters))
		{
			ereport(DEBUG2,
					(errmsg("Skipping data file pruning for relation %s as we "
							"could not find the field for one of the columns "
							"used in the filters", GetQualifiedRelationName(relationId))));

			return;
		}
	}
	else
	{
		/* never expect to get here, still better than assert */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Unsupported table type for data file pruning")));
	}


	for (int columnIndex = 0; columnIndex < list_length(columnsUsedInFilters); columnIndex++)
	{
		Var		   *column = list_nth(columnsUsedInFilters, columnIndex);
		AttrNumber	pgAttNum = column->varattno;

		PGType		pgType = GetAttributePGType(relationId, pgAttNum);

		if (type_is_array(pgType.postgresTypeOid) ||
			IsMapTypeOid(pgType.postgresTypeOid) ||
			get_typtype(pgType.postgresTypeOid) == TYPTYPE_COMPOSITE ||
			IsGeometryTypeId(pgType.postgresTypeOid))
		{
			/*
			 * We currently do not support pruning on array, map, and
			 * composite types.
			 */
			continue;
		}

		bool		isFound = false;
		ColumnToFieldIdMapping *entry =
			hash_search(fieldIdsUsedInQuery, &pgAttNum, HASH_ENTER, &isFound);

		if (isFound)
		{
			/* attribute is already in the hash table. */
			continue;
		}

		DataFileSchemaField *field = list_nth(fields, columnIndex);
#ifdef USE_ASSERT_CHECKING
		if (EnableHeavyAsserts && tableType == PG_LAKE_ICEBERG_TABLE_TYPE &&
			!(GetIcebergCatalogType(relationId) == REST_CATALOG_READ_ONLY ||
			  GetIcebergCatalogType(relationId) == OBJECT_STORE_READ_ONLY))
		{
			/*
			 * It is guaranteed that fields and columnsUsedInFilters are in
			 * the same order.
			 */
			DataFileSchemaField *fieldAssert = GetRegisteredFieldForAttribute(relationId, pgAttNum);

			Assert(field->id == fieldAssert->id);
		}
#endif

		entry->fieldId = field->id;
		entry->column = column;

		Oid			collation = column->varcollid;

		/*
		 * We currently do not support data file pruning for columns with
		 * collations.
		 */
		if (collation == InvalidOid || collation == DEFAULT_COLLATION_OID ||
			collation == C_COLLATION_OID)
		{
			entry->columnBoundInclusiveUpper =
				BuildConstraintsWithNullConst(column, BTLessEqualStrategyNumber, BTGreaterEqualStrategyNumber);
			entry->equalityOperatorExpression = MakeOpExpression(column, BTEqualStrategyNumber);
			entry->isNullExpression = MakeIsNullExpression(column);
			entry->columnBoundExclusiveUpper =
				BuildConstraintsWithNullConst(column, BTLessStrategyNumber, BTGreaterEqualStrategyNumber);

			int			syntheticColId = MaxAttrNumber - pgAttNum;

			/*
			 * For bucket partitioning, we create a synthetic column that
			 * represents the bucket partition bound.
			 *
			 * Our convention is to use the maximum attribute number
			 * (MaxAttrNumber) minus the attribute number of the column. This
			 * way, we can ensure that each column/partition field has a
			 * unique synthetic column id, and we can use it to create the
			 * synthetic filters for the bucket partitioning.
			 */
			entry->syntheticColEqualityOperator = syntheticColId > 0 ?
				MakeOpExpression(MakeSyntheticInt4Column(syntheticColId), BTEqualStrategyNumber) :
				NULL;

			get_typlenbyval(column->vartype, &entry->typLen, &entry->constByVal);
		}
		else
		{
			entry->columnBoundInclusiveUpper = NULL;
			entry->equalityOperatorExpression = NULL;
			entry->isNullExpression = NULL;
			entry->columnBoundExclusiveUpper = NULL;
			entry->syntheticColEqualityOperator = NULL;
			entry->constByVal = false;
			entry->typLen = -1;
		}
	}
}


/*
* GetColumnBoundConstraints returns the constraints for the columns used in the
* query. The constraints are based on the column statistics.
*/
static List *
GetColumnBoundConstraints(Oid relationId, HTAB *fieldIdMapping, List *columnStats,
						  List *partitionTransforms, Partition * partition)
{
	List	   *constraintList = NIL;

	/* iterate on fieldIdMapping hashMap */
	HASH_SEQ_STATUS status;
	ColumnToFieldIdMapping *entry = NULL;

	/*
	 * For each column used in the query, find the corresponding column stats
	 * and create a constraint for the column. We typically expect a small
	 * number of columns used in the query, so iterating over the stats per
	 * column is not a major performance concern. If this ever becomes a
	 * bottleneck, we can consider optimizing this by introducing a hash table
	 * to store the column stats.
	 */
	hash_seq_init(&status, fieldIdMapping);
	while ((entry = hash_seq_search(&status)) != NULL)
	{

		if (EnableDataFilePruning)
		{
			List	   *columnStatsBoundConstraints =
				GetColumnBoundConstraintsFromColumnStats(relationId,
														 columnStats,
														 entry);

			constraintList = list_concat(constraintList, columnStatsBoundConstraints);
		}

		if (EnablePartitionPruning)
		{
			List	   *partitionBoundConstraints =
				GetColumnBoundConstraintsFromPartitions(relationId,
														entry,
														partitionTransforms,
														partition);

			constraintList = list_concat(constraintList, partitionBoundConstraints);
		}


	}

	return constraintList;
}


/*
* GetColumnBoundConstraintsFromColumnStats returns the constraints for the
* columns used in the query. The constraints are based on the column
* min/max statistics stored in iceberg metadata.
*/
static List *
GetColumnBoundConstraintsFromColumnStats(Oid relationId, List *columnStats,
										 ColumnToFieldIdMapping * entry)
{
	List	   *constraintList = NIL;
	ListCell   *columnStatCell = NULL;

	foreach(columnStatCell, columnStats)
	{
		DataFileColumnStats *columnStat = lfirst(columnStatCell);

		int			statFieldId = columnStat->leafField.fieldId;

		if (statFieldId != entry->fieldId)
		{
			/*
			 * This is not the stat we are looking for, so we skip it.
			 */
			continue;
		}
		else if (columnStat->lowerBoundText == NULL || columnStat->upperBoundText == NULL)
		{
			/*
			 * We do not have stats for the column, so we cannot prune the
			 * file based on the column's bounds.
			 */
			continue;
		}

		PGType		pgType = GetAttributePGType(relationId, entry->attrNo);

		char	   *lowerBoundText = columnStat->lowerBoundText;
		char	   *upperBoundText = columnStat->upperBoundText;

		Expr	   *columnConstraint = NULL;

		/* check for min == max through string comparison */
		if (strcmp(lowerBoundText, upperBoundText) == 0)
		{
			Datum		valueDatum = ColumnBoundDatum(lowerBoundText, pgType);

			/*
			 * For predicate_implied_by (full scan pruning) we find it helpful
			 * to define an equality constraint for the column when the min
			 * and max are equal, to enable pruning when there are equality
			 * constraints in WHERE.
			 */
			OpExpr	   *columnBoundTemplate = copyObject(entry->equalityOperatorExpression);

			columnConstraint = (Expr *) CreateConstraintWithEquality(columnBoundTemplate,
																	 entry->constByVal,
																	 entry->typLen,
																	 valueDatum);
		}
		else
		{
			Datum		lowerBoundDatum = ColumnBoundDatum(lowerBoundText, pgType);
			Datum		upperBoundDatum = ColumnBoundDatum(upperBoundText, pgType);

			BoolExpr   *columnBoundTemplate = copyObject(entry->columnBoundInclusiveUpper);

			columnConstraint = (Expr *) CreateConstraintWithBounds(columnBoundTemplate,
																   entry->constByVal,
																   entry->typLen,
																   lowerBoundDatum,
																   upperBoundDatum);
		}

		if (columnConstraint == NULL)
		{
			/*
			 * We could not build the constraint for the column, so we cannot
			 * prune the file based on the column's bounds. The type is
			 * missing operators to create the constraints.
			 */
			continue;
		}

		constraintList = lappend(constraintList, columnConstraint);
	}

	return constraintList;
}


/*
* GetColumnBoundConstraintsFromPartitions returns the constraints for the
* columns used in the query. The constraints are based on the partition
* statistics stored in iceberg metadata.
*/
static List *
GetColumnBoundConstraintsFromPartitions(Oid relationId, ColumnToFieldIdMapping * entry,
										List *partitionTransforms, Partition * partition)
{
	List	   *constraintList = NIL;
	int			partitionFieldCount = partition ? partition->fields_length : 0;

	for (int partitionFieldIndex = 0; partitionFieldIndex < partitionFieldCount; partitionFieldIndex++)
	{
		PartitionField *partitionField = &partition->fields[partitionFieldIndex];

		IcebergPartitionTransform *partitionTransform =
			FindPartitionTransformById(partitionTransforms, partitionField->field_id);

		/* unexpected but better than crash */
		if (partitionTransform == NULL)
			elog(ERROR, "Could not find partition transform for field %d", partitionField->field_id);

		/* skip if the partition field is not used in the query */
		if (partitionTransform->sourceField->id != entry->fieldId)
		{
			continue;
		}

		Expr	   *boundsConstraint =
			PartitionFieldBoundConstraint(partitionField, partitionTransform, entry);

		if (boundsConstraint != NULL)
		{
			constraintList = lappend(constraintList, boundsConstraint);
		}
	}
	return constraintList;
}


/*
* For each partition field, we create a constraint based on the
* partition transform.
*/
static Expr *
PartitionFieldBoundConstraint(PartitionField * partitionField, IcebergPartitionTransform * partitionTransform,
							  ColumnToFieldIdMapping * entry)
{
	IcebergPartitionTransformType type = partitionTransform->type;

	if (type != PARTITION_TRANSFORM_IDENTITY &&
		partitionField->value == NULL)
	{
		/* we only support null value pruning for identity partitions */
		return NULL;
	}

	switch (type)
	{
		case PARTITION_TRANSFORM_IDENTITY:
			return IdentityPartitionFieldBoundConstraint(partitionField, partitionTransform, entry);
		case PARTITION_TRANSFORM_TRUNCATE:
			return (Expr *) TruncatePartitionFieldBoundConstraint(partitionField, partitionTransform, entry);
		case PARTITION_TRANSFORM_YEAR:
			return (Expr *) YearPartitionFieldBoundConstraint(partitionField, partitionTransform, entry);
		case PARTITION_TRANSFORM_MONTH:
			return (Expr *) MonthPartitionFieldBoundConstraint(partitionField, partitionTransform, entry);
		case PARTITION_TRANSFORM_DAY:
			return (Expr *) DayPartitionFieldBoundConstraint(partitionField, partitionTransform, entry);
		case PARTITION_TRANSFORM_HOUR:
			return (Expr *) HourPartitionFieldBoundConstraint(partitionField, partitionTransform, entry);
		case PARTITION_TRANSFORM_BUCKET:
			return (Expr *) BucketPartitionFieldBoundConstraint(partitionField, partitionTransform, entry);

		default:
			/* unexpected but better than crash */
			elog(DEBUG1, "Partition transform type %d not supported", type);
			break;
	}

	return NULL;
}


/*
* IdentityPartitionFieldBoundConstraint creates a constraint for the
* partition field based on the identity transform. The constraint is
* created as:
*   column = partitionField->value
*/
static Expr *
IdentityPartitionFieldBoundConstraint(PartitionField * partitionField,
									  IcebergPartitionTransform * partitionTransform,
									  ColumnToFieldIdMapping * entry)
{
	/* non-null values */
	if (partitionField->value != NULL)
	{
		bool		isNull = false;
		Datum		partitionDatum =
			PartitionValueToDatum(partitionTransform->type, partitionField->value, partitionField->value_length,
								  partitionTransform->resultPgType, &isNull);

		OpExpr	   *columnBoundEquality = copyObject(entry->equalityOperatorExpression);

		return (Expr *) CreateConstraintWithEquality(columnBoundEquality, entry->constByVal, entry->typLen,
													 partitionDatum);
	}
	else
	{
		return (Expr *) copyObject(entry->isNullExpression);
	}
}


/*
* TruncatePartitionFieldBoundConstraint
*/
static BoolExpr *
TruncatePartitionFieldBoundConstraint(PartitionField * partitionField,
									  IcebergPartitionTransform * partitionTransform,
									  ColumnToFieldIdMapping * entry)
{
	BoolExpr   *columnBoundExclusiveUpper = copyObject(entry->columnBoundExclusiveUpper);
	PGType		pgType = partitionTransform->pgType;

	if (pgType.postgresTypeOid == INT4OID || pgType.postgresTypeOid == INT2OID)
	{
		int32		partitionValue = *(int32_t *) partitionField->value;
		int			truncateLen = partitionTransform->truncateLen;

		int32		upperBound;

		if (pg_add_s32_overflow(partitionValue, truncateLen, &upperBound))
		{
			upperBound = INT32_MAX;
		}

		Datum		lowerBoundDatum = Int32GetDatum(partitionValue);
		Datum		upperBoundDatum = Int32GetDatum(upperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == INT8OID)
	{
		int64		partitionValue = *(int64_t *) partitionField->value;
		int			truncateLen = partitionTransform->truncateLen;

		int64		upperBound;

		if (pg_add_s64_overflow(partitionValue, truncateLen, &upperBound))
		{
			upperBound = INT64_MAX;
		}

		Datum		lowerBoundDatum = Int64GetDatum(partitionValue);
		Datum		upperBoundDatum = Int64GetDatum(upperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TEXTOID ||
			 pgType.postgresTypeOid == VARCHAROID ||
			 pgType.postgresTypeOid == BPCHAROID)
	{
		char	   *partitionValue = pstrdup((char *) partitionField->value);

		if (strlen(partitionValue) != pg_mbstrlen(partitionValue))
		{
			/* non-ascii char, so we currently don't prune */
			return NULL;
		}

		int			truncateLen = partitionTransform->truncateLen;
		char	   *truncatedUpperBound = TruncateUpperBoundForText(pstrdup(partitionValue), truncateLen);

		if (truncatedUpperBound == NULL)
		{
			/* the last byte is the max ascii char, so we can't prune */
			return NULL;
		}

		Datum		lowerBoundDatum = CStringGetTextDatum(partitionValue);
		Datum		upperBoundDatum = CStringGetTextDatum(truncatedUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == BYTEAOID)
	{
		bytea	   *partitionValue = palloc0(partitionField->value_length + VARHDRSZ);

		SET_VARSIZE(partitionValue, partitionField->value_length + VARHDRSZ);
		memcpy(VARDATA_ANY(partitionValue), partitionField->value, partitionField->value_length);

		bytea	   *partitionValueCopy = (bytea *) pg_detoast_datum_copy((struct varlena *) partitionValue);
		int			truncateLen = partitionTransform->truncateLen;

		/* increment the last byte of the upper bound, which does not overflow */
		partitionValueCopy = TruncateUpperBoundForBytea(partitionValueCopy, truncateLen);
		if (partitionValueCopy == NULL)
		{
			/* the last byte is the max ascii char, so we can't prune */
			return NULL;
		}
		Datum		lowerBoundDatum = PointerGetDatum(partitionValue);
		Datum		upperBoundDatum = PointerGetDatum(partitionValueCopy);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else
	{
		elog(DEBUG1, "Truncate partition transform pruning is not yet supported for type %s",
			 format_type_be(pgType.postgresTypeOid));
	}

	return NULL;
}



/*
* YearPartitionFieldBoundConstraint
*/
static BoolExpr *
YearPartitionFieldBoundConstraint(PartitionField * partitionField,
								  IcebergPartitionTransform * partitionTransform,
								  ColumnToFieldIdMapping * entry)
{
	BoolExpr   *columnBoundExclusiveUpper = copyObject(entry->columnBoundExclusiveUpper);
	PGType		pgType = partitionTransform->pgType;

	/* we know that year transform stores the partition value as int */
	int32		yearsSinceEpoch = *(int32_t *) partitionField->value;

	if (pgType.postgresTypeOid == DATEOID)
	{
		DateADT		partitionLowerBound = YearsFromEpochToDate(yearsSinceEpoch);
		DateADT		partitionUpperBound = YearsFromEpochToDate(yearsSinceEpoch + 1);

		Datum		lowerBoundDatum = DateADTGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = DateADTGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		TimestampTz partitionLowerBound = YearsFromEpochToTimestamp(yearsSinceEpoch);
		TimestampTz partitionUpperBound = YearsFromEpochToTimestamp(yearsSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampTzGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampTzGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz partitionLowerBound = (TimestampTz) YearsFromEpochToTimestamp(yearsSinceEpoch);
		TimestampTz partitionUpperBound = (TimestampTz) YearsFromEpochToTimestamp(yearsSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampTzGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampTzGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}

	else
	{
		elog(DEBUG1, "Year partition transform pruning is not yet supported for type %s",
			 format_type_be(pgType.postgresTypeOid));
	}


	return NULL;
}


/*
* MonthPartitionFieldBoundConstraint
*/
static BoolExpr *
MonthPartitionFieldBoundConstraint(PartitionField * partitionField,
								   IcebergPartitionTransform * partitionTransform,
								   ColumnToFieldIdMapping * entry)
{
	BoolExpr   *columnBoundExclusiveUpper = copyObject(entry->columnBoundExclusiveUpper);
	PGType		pgType = partitionTransform->pgType;

	/* we know that month transform stores the partition value as int */
	int32		monthsSinceEpoch = *(int32_t *) partitionField->value;

	if (pgType.postgresTypeOid == DATEOID)
	{
		DateADT		partitionLowerBound = MonthsFromEpochToDate(monthsSinceEpoch);
		DateADT		partitionUpperBound = MonthsFromEpochToDate(monthsSinceEpoch + 1);

		Datum		lowerBoundDatum = DateADTGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = DateADTGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	partitionLowerBound = MonthsFromUnixEpochToTimestamp(monthsSinceEpoch);
		Timestamp	partitionUpperBound = MonthsFromUnixEpochToTimestamp(monthsSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz partitionLowerBound = (TimestampTz) MonthsFromUnixEpochToTimestamp(monthsSinceEpoch);
		TimestampTz partitionUpperBound = (TimestampTz) MonthsFromUnixEpochToTimestamp(monthsSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampTzGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampTzGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else
	{
		elog(DEBUG1, "Month partition transform pruning is not yet supported for type %s",
			 format_type_be(pgType.postgresTypeOid));
	}


	return NULL;
}


/*
* DayPartitionFieldBoundConstraint
*/
static BoolExpr *
DayPartitionFieldBoundConstraint(PartitionField * partitionField,
								 IcebergPartitionTransform * partitionTransform,
								 ColumnToFieldIdMapping * entry)
{
	BoolExpr   *columnBoundExclusiveUpper = copyObject(entry->columnBoundExclusiveUpper);
	PGType		pgType = partitionTransform->pgType;

	/* we know that day transform stores the partition value as int */
	int32		daysSinceEpoch = *(int32_t *) partitionField->value;

	if (pgType.postgresTypeOid == DATEOID)
	{
		DateADT		partitionLowerBound = DaysFromEpochToDate(daysSinceEpoch);
		DateADT		partitionUpperBound = DaysFromEpochToDate(daysSinceEpoch + 1);

		Datum		lowerBoundDatum = DateADTGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = DateADTGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	partitionLowerBound = DaysFromUnixEpochToTimestamp(daysSinceEpoch);
		Timestamp	partitionUpperBound = DaysFromUnixEpochToTimestamp(daysSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz partitionLowerBound = (TimestampTz) DaysFromUnixEpochToTimestamp(daysSinceEpoch);
		TimestampTz partitionUpperBound = (TimestampTz) DaysFromUnixEpochToTimestamp(daysSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampTzGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampTzGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else
	{
		elog(DEBUG1, "Day partition transform pruning is not yet supported for type %s",
			 format_type_be(pgType.postgresTypeOid));
	}

	return NULL;
}


/*
* HourPartitionFieldBoundConstraint
*/
static BoolExpr *
HourPartitionFieldBoundConstraint(PartitionField * partitionField,
								  IcebergPartitionTransform * partitionTransform,
								  ColumnToFieldIdMapping * entry)
{
	BoolExpr   *columnBoundExclusiveUpper = copyObject(entry->columnBoundExclusiveUpper);
	PGType		pgType = partitionTransform->pgType;

	if (pgType.postgresTypeOid == TIMEOID)
	{
		int32		hoursSinceMidnight = *(int32_t *) partitionField->value;

		TimeADT		partitionLowerBound = HoursFromUnixEpochToTime(hoursSinceMidnight);
		TimeADT		partitionUpperBound = HoursFromUnixEpochToTime(hoursSinceMidnight + 1);

		Datum		lowerBoundDatum = TimeADTGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimeADTGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		int32		hoursSinceEpoch = *(int32_t *) partitionField->value;;

		Timestamp	partitionLowerBound = HoursFromUnixEpochToTimestamp(hoursSinceEpoch);
		Timestamp	partitionUpperBound = HoursFromUnixEpochToTimestamp(hoursSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		int32		hoursSinceEpoch = *(int32_t *) partitionField->value;;

		TimestampTz partitionLowerBound = (TimestampTz) HoursFromUnixEpochToTimestamp(hoursSinceEpoch);
		TimestampTz partitionUpperBound = (TimestampTz) HoursFromUnixEpochToTimestamp(hoursSinceEpoch + 1);

		Datum		lowerBoundDatum = TimestampTzGetDatum(partitionLowerBound);
		Datum		upperBoundDatum = TimestampTzGetDatum(partitionUpperBound);

		return CreateConstraintWithBounds(columnBoundExclusiveUpper, entry->constByVal, entry->typLen,
										  lowerBoundDatum, upperBoundDatum);
	}
	else
	{
		elog(DEBUG1, "Hour partition transform pruning is not yet supported for type %s",
			 format_type_be(pgType.postgresTypeOid));
	}

	return NULL;
}


/*
* BucketPartitionFieldBoundConstraint
*/
static OpExpr *
BucketPartitionFieldBoundConstraint(PartitionField * partitionField,
									IcebergPartitionTransform * partitionTransform,
									ColumnToFieldIdMapping * entry)
{
	if (entry->syntheticColEqualityOperator == NULL)
	{
		/*
		 * Only happens if we cannot allocate a synthetic partition attribute
		 * as the table has more than MaxAttrNumber - attrNo.
		 */
		return NULL;
	}

	OpExpr	   *syntheticColEqualityOperator = copyObject(entry->syntheticColEqualityOperator);

	/*
	 * Our algorithm for bucket transform is as follows:
	 *
	 * (1)Iterate on the baseRestrictInfoList, look for equality operators in
	 * the form of, "partitionFieldColumn = const". When found, create the
	 * synthetic eq. operator expression with the synthetic column:
	 * syntheticInt4Column = murmur_hash3(const) % bucketLen. Step (1) is done
	 * in ExtendClausesForBucketPartitioning(). Add these synthetic
	 * restrictions to the baseRestrictInfoList.
	 *
	 * (2)Add the synthetic partition bound constraints to the
	 * columnBoundConstraints. The synthetic constraints are of the form:
	 * syntheticInt4Column = partitionValue. This is done in right here in
	 * BucketPartitionFieldBoundConstraint().
	 *
	 * (3) Let predicate_refuted_by() check if these constraints are refuted
	 * by the extended base restrict infos. If not, we retain the data file.
	 */
	return GetSyntheticBucketForPartitionField(partitionField, syntheticColEqualityOperator);
}


/*
 * TruncateUpperBoundForText truncates the given upper bound text to
 * the given length. The last byte of the upper bound is incremented
 * by 1, which does not overflow. If the last byte is the max ascii
 * char, we return null.
 */
static char *
TruncateUpperBoundForText(char *inputText, size_t truncateLen)
{
	int			inputTextLen = strlen(inputText);

	if (inputTextLen < truncateLen)
	{
		truncateLen = inputTextLen;
	}

	/*
	 * increment the last byte of the upper bound, which does not overflow. If
	 * not found, return null.
	 */
	for (int i = truncateLen - 1; i >= 0; i--)
	{
		/*
		 * Check if overflows max ascii char.
		 *
		 * Todo: We currently do not support non-ascii chars for pruning. If
		 * we want to support this, we need to check the encoding of the
		 * inputText and increment the last byte accordingly.
		 */
		if (inputText[i] != INT8_MAX)
		{
			inputText[i]++;

			return inputText;
		}
	}

	return NULL;
}


/*
* TruncateUpperBoundForBytea truncates the given upper bound bytea to
* the given length. The last byte of the upper bound is incremented
* by 1, which does not overflow. If the last byte is the max bytea
* char, we return null.
*/
static bytea *
TruncateUpperBoundForBytea(bytea *data, int truncateLen)
{
	int			dataSize = VARSIZE_ANY_EXHDR(data);

	if (dataSize < truncateLen)
	{
		truncateLen = dataSize;
	}

	/*
	 * increment the last byte of the upper bound, which does not overflow. If
	 * not found, return null.
	 */
	for (int i = truncateLen - 1; i >= 0; i--)
	{
		/* check if overflows max byte */
		if ((unsigned char) VARDATA_ANY(data)[i] != UINT8_MAX)
		{
			VARDATA_ANY(data)[i]++;
			return data;
		}
	}

	return NULL;
}



/*
* StripAllImplicitCoercionsInList strips all implicit coercions from the given
* baseRestrictInfoList. It modifies the baseRestrictInfoList in place.
*/
static void
StripAllImplicitCoercionsInList(List *baseRestrictInfoList)
{
	ListCell   *baseRestrictInfoCell = NULL;

	foreach(baseRestrictInfoCell, baseRestrictInfoList)
	{
		RestrictInfo *restrictInfo = lfirst(baseRestrictInfoCell);
		Expr	   *restrictionClause = restrictInfo->clause;

		restrictInfo->clause = (Expr *) StripAllImplicitCoercions((Node *) restrictionClause);
	}
}


/*
 * StripAllImplicitCoercions
 *
 * Entry point. Takes an expression tree (Node *) and returns a new tree with
 * all implicit coercions removed.
 */
static Node *
StripAllImplicitCoercions(Node *expr)
{
	if (expr == NULL)
		return NULL;

	return expression_tree_mutator(expr,
								   StripAllImplicitCoercionsMutator,
								   NULL);
}

/*
 * StripAllImplicitCoercionsMutator
 *
 * StripAllImplicitCoercionsMutator is a wrapper around strip_implicit_coercions()
 * as strip_implicit_coercions() only works on the top-level node. Instead, we want to
 * traverse the entire expression tree and strip all implicit coercions.
 */
static Node *
StripAllImplicitCoercionsMutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		case T_RelabelType:
		case T_FuncExpr:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ConvertRowtypeExpr:
		case T_CoerceToDomain:
			{
				return strip_implicit_coercions(node);
			}

		default:
			{
				/* for the rest, traverse the expression tree */

				return expression_tree_mutator(node,
											   StripAllImplicitCoercionsMutator,
											   context);
				break;
			}
	}
}


/*
 * ExtractClausesFromBaseRestrictInfos extracts all the clauses from the
 * base restrict info list, since predicate_xxx_by functions only really
 * operate on the clauses.
 */
static List *
ExtractClausesFromBaseRestrictInfos(List *baseRestrictInfoList)
{
	List	   *clauses = NIL;

	foreach_ptr(RestrictInfo, restrictInfo, baseRestrictInfoList)
	{
		clauses = lappend(clauses, restrictInfo->clause);
	}

	return clauses;
}


/*
* BuildConstraintsWithNullConst builds constraints for the given variable with
* a null constant. The constraints are built as:
*   variable >= NULL AND variable <= NULL
*/
static BoolExpr *
BuildConstraintsWithNullConst(Var *variable, int lessThanStrategyNumber,
							  int greaterThanStrategyNumber)
{
	OpExpr	   *lessThanExpr = MakeOpExpression(variable, lessThanStrategyNumber);
	OpExpr	   *greaterThanExpr = MakeOpExpression(variable, greaterThanStrategyNumber);

	if (lessThanExpr == NULL || greaterThanExpr == NULL)
	{
		/* missing operators */
		return NULL;
	}

	return (BoolExpr *) make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);
}


/*
 * GetOperatorByType returns the operator oid for the given type, access method,
 * and strategy number.
 * Returns InvalidOid if the operator is not found.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid			operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	if (operatorClassId == InvalidOid)
		return InvalidOid;

	Oid			operatorFamily = get_opclass_family(operatorClassId);

	if (operatorFamily == InvalidOid)
		return InvalidOid;

	Oid			operatorClassInputType = get_opclass_input_type(operatorClassId);

	if (operatorClassInputType == InvalidOid)
		return InvalidOid;

	return get_opfamily_member(operatorFamily, operatorClassInputType,
							   operatorClassInputType, strategyNumber);
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number.
 *
 * Returns NULL if the operator is not found.
 */
static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid			typeId = variable->vartype;
	Oid			typeModId = variable->vartypmod;
	Oid			collationId = variable->varcollid;

	Oid			accessMethodId = BTREE_AM_OID;

	/* Load the operator from system catalogs */
	Oid			operatorId =
		GetOperatorByType(typeId, accessMethodId, strategyNumber);

	if (operatorId == InvalidOid)
		return NULL;

	Const	   *constantValue = makeNullConst(typeId, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	OpExpr	   *expression = (OpExpr *) make_opclause(operatorId,
													  InvalidOid,
													  false,
													  (Expr *) variable,
													  (Expr *) constantValue,
													  InvalidOid, collationId);

	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * MakeIsNullExpression
 *      Build an expression tree that is equivalent to
 *          <column> IS NULL
 *
 * The caller passes the Var that represents the column.
 */
static NullTest *
MakeIsNullExpression(Var *variable)
{
	/* Build an empty NullTest node. */
	NullTest   *isnull = makeNode(NullTest);

	/*
	 * The Var itself is the argument we test.  Use a copy so callers can
	 * reuse the original Var elsewhere without side effects.
	 */
	isnull->arg = (Expr *) copyObject(variable);

	/* Tell the executor we want the “IS NULL” variant of NullTest. */
	isnull->nulltesttype = IS_NULL;

	isnull->argisrow = false;
	isnull->location = -1;

	return isnull;
}



/*
* CreateConstraintWithBounds creates a constraint with the given bounds for the
* given variable. In the end, we have a constraint that looks like:
*   variable >= minValue AND variable <= maxValue
*/
static BoolExpr *
CreateConstraintWithBounds(BoolExpr *columnBoundTemplate, bool constByVal, int16 typLen, Datum minValue, Datum maxValue)
{
	if (columnBoundTemplate == NULL)
	{
		return NULL;
	}

	BoolExpr   *andExpr = (BoolExpr *) columnBoundTemplate;
	Node	   *lessThanEqualExpr = (Node *) linitial(andExpr->args);
	Node	   *greaterThanEqualExpr = (Node *) lsecond(andExpr->args);

	Node	   *minNode = get_rightop((Expr *) greaterThanEqualExpr);
	Node	   *maxNode = get_rightop((Expr *) lessThanEqualExpr);

	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	Const	   *minConstant = (Const *) minNode;
	Const	   *maxConstant = (Const *) maxNode;

	minConstant->constvalue = datumCopy(minValue, constByVal, typLen);
	maxConstant->constvalue = datumCopy(maxValue, constByVal, typLen);

	minConstant->constisnull = false;
	maxConstant->constisnull = false;

	minConstant->constbyval = constByVal;
	maxConstant->constbyval = constByVal;

	return columnBoundTemplate;
}


/*
* CreateConstraintWithEquality creates a constraint with the given
* equality operator for the given variable. In the end, we have a constraint
* that looks like:
*   variable = equalityDatum
*/
static OpExpr *
CreateConstraintWithEquality(OpExpr *eqOp, bool constByVal, int16 typLen, Datum equalityDatum)
{
	if (eqOp == NULL)
	{
		return NULL;
	}

	Node	   *eqNode = get_rightop((Expr *) eqOp);

	Assert(IsA(eqNode, Const));

	Const	   *eqConstant = (Const *) eqNode;

	eqConstant->constvalue = datumCopy(equalityDatum, constByVal, typLen);

	eqConstant->constisnull = false;

	eqConstant->constbyval = constByVal;

	return eqOp;
}


/*
* ColumnsUsedInRestrictions returns the list of columns used in the restrictions.
*/
static List *
ColumnsUsedInRestrictions(Oid relationId, List *baseRestrictInfoList)
{
	List	   *varListInFilters = NIL;
	ListCell   *restrictCell = NULL;

	foreach(restrictCell, baseRestrictInfoList)
	{
		RestrictInfo *restrictInfo = lfirst(restrictCell);
		Node	   *qual = (Node *) restrictInfo->clause;

		List	   *varForQual = pull_var_clause(qual, PVC_INCLUDE_PLACEHOLDERS);

		ListCell   *varCell = NULL;

		foreach(varCell, varForQual)
		{
			Var		   *var = lfirst(varCell);

			varListInFilters = list_append_unique(varListInFilters, var);
		}
	}

	return varListInFilters;
}


/*
 * GetFilenameFilterColumn returns a Var corresponding to a _filename column
 * if it appears in the filters in baseRestrictInfoList.
 */
Var *
GetFilenameFilterColumn(Oid relationId, List *baseRestrictInfoList)
{
	AttrNumber	filenameAttno = get_attnum(relationId, "_filename");

	if (filenameAttno == InvalidAttrNumber)
		return NULL;

	List	   *columns = ColumnsUsedInRestrictions(relationId, baseRestrictInfoList);
	ListCell   *columnCell = NULL;

	foreach(columnCell, columns)
	{
		Var		   *var = (Var *) lfirst(columnCell);

		if (var->varattno == filenameAttno)
			return var;
	}

	return NULL;
}


/*
 * PruneByFilename returns the list of paths that are not refuted by
 * _filename filters in the baseRestrictInfoList.
 */
List *
PruneByFilename(List *paths, Oid relationId, List *baseRestrictInfoList)
{
	List	   *retainedDataFiles = NIL;

	Var		   *filenameCol = GetFilenameFilterColumn(relationId, baseRestrictInfoList);

	if (filenameCol == NULL)
		return paths;

	/*
	 * MakeOpExpression is expensive, so create a template and adjust it in
	 * the loop
	 */
	OpExpr	   *filenameExpr = MakeOpExpression(filenameCol, BTEqualStrategyNumber);
	List	   *filenameConstraintList = list_make1(filenameExpr);

	/*
	 * predicate_refuted_by() expects the baseRestrictInfoList to have no
	 * implicit coercions, so we strip.
	 */
	StripAllImplicitCoercionsInList(baseRestrictInfoList);

	/*
	 * predicate_refuted_by cannot resolve _filename = any(...) filters with
	 * more than 100 elements so for simple cases we use a separate hash
	 * table.
	 *
	 * Even for small batches, we apply this optimization because the list of
	 * paths can be very large so using HTAB might be faster than
	 * predicate_refuted_by.
	 */
	HTAB	   *batchFilterHash = TryCreateBatchFilterHash(baseRestrictInfoList, filenameCol);

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, paths)
	{
		char	   *path = lfirst(dataFileCell);
		Const	   *filenameConst = (Const *) get_rightop((Expr *) filenameExpr);

		filenameConst->constvalue = CStringGetTextDatum(path);
		filenameConst->constisnull = false;
		filenameConst->constbyval = false;

		if (batchFilterHash != NULL)
		{
			/* we have a _filename = any(...) condition */
			bool		isFound = false;

			hash_search(batchFilterHash, path, HASH_ENTER, &isFound);

			if (!isFound)
				/* not in any(..), so prune */
				continue;

			/* otherwise, let predicate_refuted_by decide */
		}

		if (!predicate_refuted_by(filenameConstraintList, baseRestrictInfoList, false))
			retainedDataFiles = lappend(retainedDataFiles, path);
	}

	return retainedDataFiles;
}


/*
 * TryCreateBatchFilterHash tries to extract an HTAB containing an
 * entry for each file in a _filename = any(..) filter.
 */
static HTAB *
TryCreateBatchFilterHash(List *baseRestrictInfoList, Var *filenameCol)
{
	ListCell   *restrictCell = NULL;

	foreach(restrictCell, baseRestrictInfoList)
	{
		RestrictInfo *restrictInfo = lfirst(restrictCell);

		if (!IsA(restrictInfo->clause, ScalarArrayOpExpr))
			continue;

		ScalarArrayOpExpr *opExpr = (ScalarArrayOpExpr *) restrictInfo->clause;

		/* only handle _filename = ANY */
		if (!opExpr->useOr)
			continue;

		/* only handle text = text operator */
		if (opExpr->opno != TextEqualOperator)
			continue;

		Node	   *leftArg = linitial(opExpr->args);
		Node	   *rightArg = lsecond(opExpr->args);

		/* _filename = any(...) can only appear from left to right */
		if (!(equal(leftArg, filenameCol) && IsA(rightArg, Const)))
			continue;

		Const	   *arrayConst = (Const *) rightArg;

		/* _filename = any(NULL) prunes all the files */
		if (arrayConst->constisnull)
			return NULL;

		ArrayType  *filenameArray = DatumGetArrayTypeP(arrayConst->constvalue);
		Datum	   *filenames = NULL;
		bool	   *filenameIsNull = NULL;
		int			filenameCount = 0;

		deconstruct_array_builtin(filenameArray, TEXTOID,
								  &filenames, &filenameIsNull, &filenameCount);

		int			hashFlags = HASH_ELEM | HASH_STRINGS | HASH_CONTEXT;
		HASHCTL		hashCtl;

		memset(&hashCtl, 0, sizeof(hashCtl));
		hashCtl.keysize = MAX_S3_PATH_LENGTH;
		hashCtl.entrysize = MAX_S3_PATH_LENGTH;
		hashCtl.hcxt = CurrentMemoryContext;

		HTAB	   *batchFilterHash =
			hash_create("batch filter hash", filenameCount, &hashCtl, hashFlags);

		for (int filenameIndex = 0; filenameIndex < filenameCount; filenameIndex++)
		{
			/* _filename = any(ARRAY[NULL]) does not match any files */
			if (filenameIsNull[filenameIndex])
				continue;

			char	   *path = TextDatumGetCString(filenames[filenameIndex]);
			bool		isFound = false;

			hash_search(batchFilterHash, path, HASH_ENTER, &isFound);
		}

		return batchFilterHash;
	}

	return NULL;
}


/*
* GetExternalIcebergFieldsForAttributes returns the fields for the given
* relationId and columnsUsedInFilters. Note that we do our best effort by
* checking the column names in the PostgreSQL tables and the iceberg
* metadata, which is not a solution that covers all cases.
*/
static List *
GetExternalIcebergFieldsForAttributes(Oid relationId, List *columnsUsedInFilters)
{
	List	   *fields = NIL;

	DataFileSchema *schema = GetDataFileSchemaForTable(relationId);

	ListCell   *columnCell = NULL;

	foreach(columnCell, columnsUsedInFilters)
	{
		Var		   *column = lfirst(columnCell);
		AttrNumber	pgAttNum = column->varattno;
		char	   *columnName = get_attname(relationId, pgAttNum, false);

		for (int i = 0; i < schema->nfields; i++)
		{
			DataFileSchemaField *field = &schema->fields[i];
			Field	   *fieldType = field->type;
			PGType		pgType = IcebergFieldToPostgresType(fieldType);

			if (strcasecmp(field->name, columnName) == 0 &&
				pgType.postgresTypeOid == column->vartype)
			{
				fields = lappend(fields, field);
				break;
			}

		}
	}

	return fields;
}


/*
* GetSyntheticBucketForPartitionField returns the synthetic bucket partition
* filter for the given partition field. The synthetic bucket partition filter is
* created based on the partition fields and the equality operator expression.
* The equality operator is created as:
*		syntheticColumn = partitionFieldValue
*
* Remember that the syntheticColumn is a synthetic int32 column created given
* bucket partition transform always returns int32.
*/
static OpExpr *
GetSyntheticBucketForPartitionField(PartitionField * partitionField,
									OpExpr *syntheticColEqualityOperator)
{
	/*
	 * Bucket partition transform stores the partition value as int32.
	 */
	int32_t		partitionBucketValue = *(int32 *) partitionField->value;

	Datum		partitionBucketDatum = Int32GetDatum(partitionBucketValue);

	return CreateConstraintWithEquality(syntheticColEqualityOperator,
										true	/* constByVal is true for
										  * int32 */ ,
										4 /* typLen is 4 for int32 */ ,
										partitionBucketDatum);
}

/*
* ExtendClausesForBucketPartitioning extends the clauses
* with synthetic restrict info for the bucket partitioning. The synthetic
* restrict info is created based on the filters on the partition columns.
* The synthetic restrict info is created as:
*   syntheticColumn = bucketValue
* for filters in the clauses that contain
*   partition_col = const
* where partition_col is the column that is used for bucket partition transforms.
*/
static List *
ExtendClausesForBucketPartitioning(Partition * partition, List *partitionTransforms,
								   HTAB *fieldIdMapping, List *clauses)
{
	List	   *syntheticClauseList = NIL;

	int			partitionFieldCount = partition ? partition->fields_length : 0;

	for (int partitionFieldIndex = 0; partitionFieldIndex < partitionFieldCount; partitionFieldIndex++)
	{
		PartitionField *partitionField = &partition->fields[partitionFieldIndex];

		IcebergPartitionTransform *partitionTransform =
			FindPartitionTransformById(partitionTransforms, partitionField->field_id);

		if (partitionTransform->type != PARTITION_TRANSFORM_BUCKET)
		{
			/* only extend restrict info for bucket transform */
			continue;
		}

		AttrNumber	columnAttrNo = partitionTransform->attnum;
		bool		isFound = false;
		ColumnToFieldIdMapping *mappingEntry =
			hash_search(fieldIdMapping, &columnAttrNo, HASH_FIND, &isFound);

		if (!isFound || mappingEntry->syntheticColEqualityOperator == NULL)
			continue;

		List	   *syntheticRestrictInfoForBucket =
			ExtendBaseRestrictInfoForBucketPartition(clauses, partitionTransform,
													 mappingEntry->syntheticColEqualityOperator);

		syntheticClauseList =
			list_concat(syntheticClauseList, syntheticRestrictInfoForBucket);
	}

	/* never modify clauses */
	return list_concat(list_copy(clauses), syntheticClauseList);
}


/*
* ExtendBaseRestrictInfoForBucketPartition is a helper function that extends the
* baseRestrictInfoList with synthetic restrict info for the bucket partition transform.
*/
static List *
ExtendBaseRestrictInfoForBucketPartition(List *clauses,
										 IcebergPartitionTransform * partitionTransform,
										 OpExpr *syntheticColEqualityOperator)
{
	AttrNumber	columnAttrNo = partitionTransform->attnum;

	List	   *syntheticClauseList = NIL;

	ListCell   *clauseCell = NULL;

	foreach(clauseCell, clauses)
	{
		Expr	   *clause = lfirst(clauseCell);

		Const	   *constantClause =
			ConstantEqualityOperatorExpressionOnColumn(clause, columnAttrNo);

		if (constantClause == NULL)
		{
			/*
			 * We only add synthetic restrict info when restrictions contain
			 * partition_col = const
			 */
			continue;
		}

		Oid			targetTypeId = partitionTransform->pgType.postgresTypeOid;
		int32_t		targetTypMod = partitionTransform->pgType.postgresTypeMod;

		/*
		 * TransformConstToTargetType materializes the constant to the target
		 * type and typmod.
		 *
		 * This is needed for two reasons: The first is that the types might
		 * not match, for example, the partition column is of type numeric(10,
		 * 2) but the user provided an int with an explicit cast.
		 *
		 * Second, we need to ensure that the constant is transformed to the
		 * target typmod, For example, the user might have provided a "1.1"
		 * numeric const, but the partition bound is created with typmod so
		 * that it is "1.1000000000000000" numeric const.
		 */
		Const	   *finalConst =
			constantClause->consttypmod != targetTypMod || constantClause->consttype != targetTypeId ?
			TransformConstToTargetType(constantClause, targetTypeId, targetTypMod) :
			copyObject(constantClause);

		if (finalConst == NULL)
		{
			/*
			 * If the constant cannot be transformed to the target type, we
			 * cannot create the synthetic restrict info.
			 */
			continue;
		}

		/*
		 * Finally, apply the bucket transform to the constant value. The
		 * result is guaranteed to be int32 as the bucket partition transform
		 * always stores the partition value as int32.
		 */
		bool		isNull = finalConst->constisnull;
		size_t		bucketSize = 0;
		int32_t    *constantBucketValue =
			(int32_t *) ApplyBucketTransformToColumn(partitionTransform, finalConst->constvalue, isNull,
													 &bucketSize);


		/*
		 * Create the synthetic equality operator expression for the bucket
		 * partition transform. The synthetic equality operator expression is
		 * created as: syntheticColumn = bucketValue
		 */
		Datum		bucketValueDatum = Int32GetDatum(*constantBucketValue);
		OpExpr	   *eqOpr =
			CreateConstraintWithEquality(copyObject(syntheticColEqualityOperator),
										 true	/* constByVal is true for
										   * int32 */ ,
										 4 /* typLen is 4 for int32 */ ,
										 bucketValueDatum);

		syntheticClauseList = lappend(syntheticClauseList, eqOpr);
	}

	return syntheticClauseList;
}


/*
 * ConstantEqualityOperatorExpressionOnColumn checks that given expression is
 * an equality operator such that one side is a Var the other side is a Const.
 * If found, it returns the Const, otherwise returns NULL.
 */
static Const *
ConstantEqualityOperatorExpressionOnColumn(Expr *clause, AttrNumber attrNo)
{
	OpExpr	   *opExpr = NULL;
	Node	   *leftOperand = NULL;
	Node	   *rightOperand = NULL;

	if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
	{
		opExpr = (OpExpr *) clause;
		leftOperand = get_leftop(clause);
		rightOperand = get_rightop(clause);
	}
	else
	{
		return NULL;
	}

	if (!EqualityOperator(opExpr->opno))
	{
		return NULL;
	}

	Const	   *constantClause = NULL;
	Var		   *varClause = NULL;

	if (IsA(rightOperand, Const) && IsA(leftOperand, Var))
	{
		constantClause = (Const *) rightOperand;
		varClause = (Var *) leftOperand;
	}
	else if (IsA(leftOperand, Const) && IsA(rightOperand, Var))
	{
		constantClause = (Const *) leftOperand;
		varClause = (Var *) rightOperand;
	}
	else
	{
		return NULL;
	}

	if (constantClause->constisnull)
	{
		return NULL;
	}

	if (varClause->varattno != attrNo)
	{
		return NULL;
	}

	return constantClause;
}

#ifdef USE_ASSERT_CHECKING
#if PG_VERSION_NUM >= 180000
#include "catalog/pg_am.h"
static bool
IsBtreeOperatorFamily(Oid opfamilyId)
{
	HeapTuple	tup;
	bool		is_btree = false;

	if (!OidIsValid(opfamilyId))
		return false;

	tup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyId));
	if (!HeapTupleIsValid(tup))
		return false;

	Form_pg_opfamily opf = (Form_pg_opfamily) GETSTRUCT(tup);

	is_btree = (opf->opfmethod == BTREE_AM_OID);
	ReleaseSysCache(tup);

	return is_btree;
}
#endif
#endif							/* USE_ASSERT_CHECKING */

/*
 * EqualityOperator checks if the given operator is an equality operator.
 */
static bool
EqualityOperator(Oid opno)
#if PG_VERSION_NUM >= 180000
{
	List	   *opList = get_op_index_interpretation(opno);
	ListCell   *opCell = NULL;

	foreach(opCell, opList)
	{
		OpIndexInterpretation *opIndex = (OpIndexInterpretation *)
			lfirst(opCell);

		/*
		 * we are sure it is btree operator family. MakeOpExpression already
		 * requires btree index for pruning.
		 */
#ifdef USE_ASSERT_CHECKING
		Assert(IsBtreeOperatorFamily(opIndex->opfamily_id));
#endif

		if (opIndex->cmptype == COMPARE_EQ)
			return true;
	}

	return false;
}
#else
{
	List	   *opList = get_op_btree_interpretation(opno);
	ListCell   *opCell = NULL;

	foreach(opCell, opList)
	{
		OpBtreeInterpretation *opBtree = (OpBtreeInterpretation *)
			lfirst(opCell);

		if (opBtree->strategy == BTEqualStrategyNumber)
			return true;
	}

	return false;
}
#endif

/*
* MakeSyntheticInt4Column creates a synthetic int4 column for the
* synthetic bucket partition filters. The synthetic column is used to
* create the equality operator for the bucket partition transform.
*/
static Var *
MakeSyntheticInt4Column(AttrNumber attrNo)
{
	Index		tableId = 0;
	AttrNumber	columnAttributeNumber = attrNo;
	Oid			columnType = INT4OID;
	int32		columnTypeMod = -1;
	Oid			columnCollationOid = InvalidOid;
	Index		columnLevelSup = 0;

	Var		   *int4Column = makeVar(tableId, columnAttributeNumber, columnType,
									 columnTypeMod, columnCollationOid, columnLevelSup);

	return int4Column;
}

/*
* TransformConstToTargetType transforms the given constantClause to the target type
* targetTypeId with the given targetTypMod. If the transformation is not
* possible, it returns NULL.
*/
static Const *
TransformConstToTargetType(Const *constantClause, Oid targetTypeId,
						   int32_t targetTypMod)
{
	Node	   *transformedValue = coerce_to_target_type(NULL, (Node *) constantClause,
														 constantClause->consttype,
														 targetTypeId,
														 targetTypMod,
														 COERCION_ASSIGNMENT,
														 COERCE_IMPLICIT_CAST, -1);

	/* if NULL, no implicit coercion is possible between the types */
	if (transformedValue == NULL)
	{
		return NULL;
	}

	/* evaluate coercion if still needed */
	if (!IsA(transformedValue, Const))
	{
		transformedValue = (Node *) expression_planner((Expr *) transformedValue);
	}

	/* no immutable coercion matched */
	if (!IsA(transformedValue, Const))
	{
		return NULL;
	}

	return (Const *) transformedValue;
}
