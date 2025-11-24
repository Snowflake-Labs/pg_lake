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

#ifndef PG_LAKE_COPY_FORMAT_H
#define PG_LAKE_COPY_FORMAT_H

#include "commands/copy.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/table_type.h"

#define S3_URL_PREFIX "s3://"
#define GCS_URL_PREFIX "gs://"
#define AZURE_BLOB_URL_PREFIX "az://"
#define AZURE_DLS_URL_PREFIX "abfss://"
#define HTTP_URL_PREFIX "http://"
#define HTTPS_URL_PREFIX "https://"
#define HUGGING_FACE_URL_PREFIX "hf://"


/* possible values of the COPY .. WITH (format ..) option */
typedef enum CopyDataFormat
{
	DATA_FORMAT_INVALID,
	DATA_FORMAT_CSV,
	DATA_FORMAT_JSON,
	DATA_FORMAT_PARQUET,
	DATA_FORMAT_ICEBERG,
	DATA_FORMAT_DELTA,
	DATA_FORMAT_GDAL,
	DATA_FORMAT_LOG
}			CopyDataFormat;

/* possible values of the COPY .. WITH (compression ..) option */
typedef enum CopyDataCompression
{
	DATA_COMPRESSION_INVALID,
	DATA_COMPRESSION_NONE,
	DATA_COMPRESSION_GZIP,
	DATA_COMPRESSION_ZSTD,
	DATA_COMPRESSION_SNAPPY,
	DATA_COMPRESSION_ZIP
}			CopyDataCompression;

/*
 * PgLakeTableProperties describes the main properties of an Iceberg and
 * pg_lake_table tables.
 */
typedef struct PgLakeTableProperties
{
	PgLakeTableType tableType;
	CopyDataFormat format;
	CopyDataCompression compression;
	List	   *options;
}			PgLakeTableProperties;


extern PGDLLEXPORT const char *CopyDataFormatToName(CopyDataFormat format);
extern PGDLLEXPORT const char *CopyDataCompressionToName(CopyDataCompression compression);

extern PGDLLEXPORT CopyDataFormat NameToCopyDataFormat(char *formatName);
extern PGDLLEXPORT CopyDataFormat URLToCopyDataFormat(char *url);
extern PGDLLEXPORT CopyDataFormat OptionsToCopyDataFormat(List *copyOptions);

extern PGDLLEXPORT CopyDataCompression NameToCopyDataCompression(char *compressionName);
extern PGDLLEXPORT CopyDataCompression URLToCopyDataCompression(char *url);
extern PGDLLEXPORT CopyDataCompression OptionsToCopyDataCompression(List *copyOptions);
extern PGDLLEXPORT const char *FormatToFileExtension(CopyDataFormat format,
													 CopyDataCompression compression);

extern PGDLLEXPORT bool IsSupportedURL(const char *path);

extern PGDLLEXPORT void FindDataFormatAndCompression(PgLakeTableType tableType,
													 char *path, List *copyOptions,
													 CopyDataFormat * format,
													 CopyDataCompression * compression);

#endif
