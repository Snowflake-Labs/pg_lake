/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "pg_lake/data_file/data_files.h"

extern void TrackDucklakeTableDataFileAddition(Oid relationId, TableDataFile *dataFile);
extern void TrackDucklakeTableDataFileRemoval(Oid relationId, int64 dataFileId);
extern void TrackDucklakeMetadataChangesInTx(Oid relationId, List *metadataOperationTypes);
