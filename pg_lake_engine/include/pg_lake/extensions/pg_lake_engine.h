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

#include "pg_extension_base/extension_ids.h"

#define PG_LAKE_INTERNAL_NSP "__lake__internal__nsp__"
#define PG_LAKE_ENGINE_NSP "lake_engine"

#define PG_LAKE_READ_TABLE "__lake_read_table"
#define IN_PROGRESS_FILES_TABLE "in_progress_files"

/*
 * Storage endpoints a user-supplied Azure URL may name in its host, as host
 * suffixes.  Covers the public, US Government and China clouds, for both the
 * Data Lake Storage (dfs) and blob endpoints.
 */
#define DEFAULT_ALLOWED_AZURE_HOST_SUFFIXES \
	".dfs.core.windows.net,.blob.core.windows.net," \
	".dfs.core.usgovcloudapi.net,.blob.core.usgovcloudapi.net," \
	".dfs.core.chinacloudapi.cn,.blob.core.chinacloudapi.cn"


/*
 * How a PostgreSQL jsonb value is encoded when we write it to a data file.
 *
 * JSONB_STORAGE_STRING writes the canonical jsonb text, which every Parquet
 * and Iceberg reader understands.  JSONB_STORAGE_VARIANT writes the parsed
 * binary variant encoding, which supports extraction and shredding but is
 * only understood by readers that implement the variant type.
 *
 * Only jsonb participates.  json is defined to preserve its input text
 * verbatim -- whitespace, key order and duplicate keys -- none of which a
 * parsed variant can represent, so json is always stored as a string.
 */
typedef enum JsonbStorage
{
	JSONB_STORAGE_STRING,
	JSONB_STORAGE_VARIANT,
}			JsonbStorage;

/* Canonical lowercase name ("string"/"variant") for a storage encoding. */
extern PGDLLEXPORT const char *JsonbStorageName(JsonbStorage storage);

/* Maps an option string ("string"/"variant"); errors on anything else. */
extern PGDLLEXPORT JsonbStorage ParseJsonbStorage(const char *optionValue);

extern PGDLLEXPORT bool EnableHeavyAsserts;
extern PGDLLEXPORT char *PgLakeStageLocation;
extern PGDLLEXPORT char *PgLakeAllowedAzureHostSuffixes;

/*
 * GUC pg_lake_engine.jsonb_storage (declared int, as Postgres requires for
 * enum GUCs).  Decides the encoding for a jsonb value written without a
 * per-column decision to follow: a new iceberg column whose table carries no
 * jsonb_storage option, and COPY TO a Parquet file, which has no table at
 * all.  Writes into an existing iceberg column follow that column's persisted
 * storage type instead, and reads always follow the file.
 */
extern PGDLLEXPORT int DefaultJsonbStorage;

/* cached extension IDs for pg_lake_engine */
extern PGDLLEXPORT CachedExtensionIds * PgLakeEngine;

void		InitializePgLakeEngineIdCache(void);

extern PGDLLEXPORT Oid ReadTableFunctionId(void);
extern PGDLLEXPORT Oid InProgressTableId(void);
extern PGDLLEXPORT Oid InProgressTablePkeyId(void);
