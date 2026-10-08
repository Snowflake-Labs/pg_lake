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

#ifndef PGDUCK_BINARY_TRANSMIT_H
#define PGDUCK_BINARY_TRANSMIT_H

#include <duckdb.h>
#include "c.h"
#include "lib/stringinfo.h"

/*
 * BinaryWriterKind describes how a DuckDB result column is serialized into
 * the binary send format of a PostgreSQL target type for TRANSMIT BINARY.
 */
typedef enum BinaryWriterKind
{
	BINARY_WRITER_NONE = 0,
	BINARY_WRITER_BOOL,
	BINARY_WRITER_INT,
	BINARY_WRITER_FLOAT4,
	BINARY_WRITER_FLOAT8,
	BINARY_WRITER_NUMERIC,
	BINARY_WRITER_TEXT,
	BINARY_WRITER_JSONB,
	BINARY_WRITER_BYTEA,
	BINARY_WRITER_DATE,
	BINARY_WRITER_TIMESTAMP,
	BINARY_WRITER_TIME,
	BINARY_WRITER_UUID
}			BinaryWriterKind;

typedef struct BinaryColumnWriter
{
	BinaryWriterKind kind;

	/* DuckDB type of the values (for DECIMAL: the internal storage type) */
	duckdb_type sourceType;

	/* size in bytes of the PostgreSQL integer type, for BINARY_WRITER_INT */
	int			targetSize;

	/* scale of the source value, for BINARY_WRITER_NUMERIC */
	int			scale;
}			BinaryColumnWriter;

extern bool binary_transmit_choose_writer(duckdb_logical_type logicalType,
										  Oid targetTypeId,
										  BinaryColumnWriter * writer);
extern void binary_transmit_append_header(StringInfo buf);
extern void binary_transmit_append_trailer(StringInfo buf);
extern void binary_transmit_append_value(StringInfo buf,
										 BinaryColumnWriter * writer,
										 void *data, idx_t row);

#endif
