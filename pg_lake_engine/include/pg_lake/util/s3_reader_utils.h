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

/* Maximum lengths based on AWS S3 limitations */
#define MAX_S3_BUCKET_NAME_LENGTH 63	/* Maximum length for an S3 bucket
										 * name */
#define MAX_S3_KEY_NAME_LENGTH 1024 /* Maximum length for an S3 object key
									 * (key name) */
#define MAX_S3_URI_PREFIX_LENGTH 5	/* Length of "s3://" */
#define MAX_S3_PATH_SEPARATOR_LENGTH 1	/* Length of '/' separator */
#define MAX_S3_PATH_LENGTH ( \
    MAX_S3_URI_PREFIX_LENGTH + \
    MAX_S3_BUCKET_NAME_LENGTH + \
    MAX_S3_PATH_SEPARATOR_LENGTH + \
    MAX_S3_KEY_NAME_LENGTH + \
    2 /* null terminator and one extra space */ \
)

/* Bypass the pgduck file cache; matches NO_CACHE_PREFIX on the DuckDB side. */
#define NO_CACHE_URL_PREFIX "nocache"

extern PGDLLEXPORT char *GetTextFromURI(const char *textFileUri);
extern PGDLLEXPORT char *GetJsonFromURINoCache(const char *jsonFileUri);
extern PGDLLEXPORT char *GetBlobFromURI(const char *blobFileUri, size_t *contentLength);
