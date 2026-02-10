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

#pragma once

#include "pg_lake/data_file/data_files.h"
#include "nodes/primnodes.h"

RangeVar   *RowIdSequenceGetRangeVar(Oid relationId);
void		CreateRelationRowIdSequence(Oid relationId);
void		DropRowIdSequenceForRelation(Oid relationId);
Oid			FindRelationRowIdSequence(Oid relationId);
RowIdRangeMapping *CreateRowIdRangeForNewFile(Oid relationId, int64 rowCount,
											  int64 reservedRowIdStart);
int64		StartReservingRowIdRange(Oid relationId);
void		FinishReservingRowIdRange(Oid relationId, int64 rowIdOffset,
									  int64 rowCount);

/* helper function for materializing row IDs */
extern PGDLLEXPORT char *AddRowIdMaterializationToReadQuery(char *readQuery, Oid relationId, List *fileList);

/* function for scanning materialized row IDs */
List	   *GetRowIdRangesFromFile(const char *path);

/* function for enabling row_ids on an existing table */
void		EnableRowIdsOnTable(Oid relationId);

/* function to analyze lake_table.row_id_mappings */
void		AnalyzeRowIdMappings(void);

#ifdef USE_ASSERT_CHECKING
int64		GetTotalRowIdRangeRowCount(List *rowIdRanges);
#endif
