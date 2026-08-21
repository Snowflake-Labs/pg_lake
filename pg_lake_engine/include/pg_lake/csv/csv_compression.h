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

#ifndef PG_LAKE_CSV_COMPRESSION_H
#define PG_LAKE_CSV_COMPRESSION_H

#include <stdio.h>

#include "pg_lake/copy/copy_format.h"

/*
 * Bounds of pg_lake_engine.temp_file_compression_level: the union of what the
 * codecs accept, since one setting serves both.  Each codec clamps the level
 * into its own range, so gzip stops at 9 and only zstd sees the rest.
 */
#define PG_LAKE_MIN_TEMP_FILE_COMPRESSION_LEVEL (-22)
#define PG_LAKE_MAX_TEMP_FILE_COMPRESSION_LEVEL 22

/* pg_lake_engine.temp_file_compression setting */
extern PGDLLEXPORT int TempFileCompression;

/* pg_lake_engine.temp_file_compression_level setting */
extern PGDLLEXPORT int TempFileCompressionLevel;

/* a compressor wrapped around a stdio stream */
typedef struct CSVCompressor CSVCompressor;

extern PGDLLEXPORT CopyDataCompression InternalCSVCompression(void);
extern PGDLLEXPORT void CSVCompressionLevelRange(CopyDataCompression method,
												 int *minLevel, int *maxLevel);
extern PGDLLEXPORT CSVCompressor * CSVCompressorCreate(FILE *file,
													   CopyDataCompression method,
													   int level);
extern PGDLLEXPORT void CSVCompressorWrite(CSVCompressor * compressor,
										   const void *data, size_t size);
extern PGDLLEXPORT void CSVCompressorFinish(CSVCompressor * compressor);

#endif
