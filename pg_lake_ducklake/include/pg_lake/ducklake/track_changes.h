/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Per-transaction tracking of DuckLake metadata changes. Originally
 * pg_lake_table/include/pg_lake/transaction/track_ducklake_changes.h;
 * moved here so the implementation can live alongside the catalog
 * functions it consumes.
 */
#pragma once

#include "pg_lake/data_file/data_files.h"

extern PGDLLEXPORT void TrackDucklakeTableDataFileAddition(Oid relationId, TableDataFile * dataFile);
extern PGDLLEXPORT void TrackDucklakeTableDataFileRemoval(Oid relationId, int64 dataFileId);
extern PGDLLEXPORT void TrackDucklakeMetadataChangesInTx(Oid relationId, List *metadataOperationTypes);
