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

#ifndef SPI_UTILITIES_H
#define SPI_UTILITIES_H

#include "miscadmin.h"

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/pg_lsn.h"

/* SPI macros for setting parameters */
#define DATUMIZE_TEXTOID(Value) CStringGetTextDatum(Value)
#define DATUMIZE_TEXTARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_CHAROID(Value) CharGetDatum(Value)
#define DATUMIZE_BOOLOID(Value) BoolGetDatum(Value)
#define DATUMIZE_OIDOID(Value) ObjectIdGetDatum(Value)
#define DATUMIZE_INT2OID(Value) Int16GetDatum(Value)
#define DATUMIZE_INT2ARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_INT4OID(Value) Int32GetDatum(Value)
#define DATUMIZE_INT4ARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_INT8OID(Value) Int64GetDatum(Value)
#define DATUMIZE_INT8ARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_FLOAT4OID(Value) Float4GetDatum(Value)
#define DATUMIZE_FLOAT4ARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_FLOAT8OID(Value) Float8GetDatum(Value)
#define DATUMIZE_FLOAT8ARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_TIMESTAMPTZOID(Value) TimestampTzGetDatum(Value)
#define DATUMIZE_JSONOID(Value) CStringGetTextDatum(Value)
#define DATUMIZE_JSONBOID(Value) JsonbPGetDatum(Value)
#define DATUMIZE_BYTEAOID(Value) PointerGetDatum(Value)
#define DATUMIZE_BYTEAARRAYOID(Value) PointerGetDatum(Value)
#define DATUMIZE_LSNOID(Value) LSNGetDatum(Value)
#define DATUMIZE_NAMEOID(Value) NameGetDatum(Value)
#define DATUMIZE(TypeId,Value) DATUMIZE_##TypeId(Value)

#define DECLARE_SPI_ARGS(Count) \
	const int spiArgCount = Count; \
	Oid spiArgTypes[Count]; \
	Datum spiArgValues[Count]; \
	char spiArgNulls[Count]; \
	memset(spiArgNulls, ' ', Count);

#define SPI_ARG_DATUM(Index,TypeId,Datum) \
	{ \
	spiArgTypes[Index-1] = TypeId; \
	spiArgValues[Index-1] = Datum; \
	spiArgNulls[Index-1] = ' '; \
	}

#define SPI_ARG_NULL(Index,TypeId) \
	{ \
	spiArgTypes[Index-1] = TypeId; \
	spiArgNulls[Index-1] = 'n'; \
	}

#define SPI_ARG_VALUE(Index,TypeId,Value,IsNull) \
	{ \
	if (IsNull) \
	{ \
		SPI_ARG_NULL(Index,TypeId); \
	} \
	else \
	{ \
		SPI_ARG_DATUM(Index,TypeId,DATUMIZE_##TypeId(Value)); \
	} \
	}

#define SPI_EXECUTE(Query,ReadOnly) \
	{ \
	int spiStatus = SPI_execute_with_args(Query, spiArgCount, spiArgTypes, \
										  spiArgValues, spiArgNulls, ReadOnly, 0); \
	if (spiStatus < 0) \
	{ \
		ereport(ERROR, (errmsg("failed to execute SPI query"))); \
	} \
	}

/* SPI macros for reading results */
#define DEDATUMIZE_TEXTOID(Value) TextDatumGetCString(Value)
#define DEDATUMIZE_TEXTARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_CHAROID(Value) DatumGetChar(Value)
#define DEDATUMIZE_BOOLOID(Value) DatumGetBool(Value)
#define DEDATUMIZE_OIDOID(Value) DatumGetObjectId(Value)
#define DEDATUMIZE_INT2OID(Value) DatumGetInt16(Value)
#define DEDATUMIZE_INT2ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_INT4OID(Value) DatumGetInt32(Value)
#define DEDATUMIZE_INT4ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_INT8OID(Value) DatumGetInt64(Value)
#define DEDATUMIZE_INT8ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_FLOAT4OID(Value) DatumGetFloat4(Value)
#define DEDATUMIZE_FLOAT4ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_FLOAT8OID(Value) DatumGetFloat8(Value)
#define DEDATUMIZE_FLOAT8ARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_TIMESTAMPTZOID(Value) DatumGetTimestampTz(Value)
#define DEDATUMIZE_JSONOID(Value) TextDatumGetCString(Value)
#define DEDATUMIZE_JSONBOID(Value) DatumGetJsonbP(Value)
#define DEDATUMIZE_BYTEAOID(Value) DatumGetByteaP(Value)
#define DEDATUMIZE_BYTEAARRAYOID(Value) DatumGetArrayTypeP(Value)
#define DEDATUMIZE_LSNOID(Value) DatumGetLSN(Value)
#define DEDATUMIZE_NAMEOID(Value) DatumGetName(Value)
#define DEDATUMIZE(TypeId,Value) DATUMIZE_##TypeId(Value)

#define GET_SPI_DATUM(RowIndex,ColumnNumber,IsNull) \
	SPI_getbinval(SPI_tuptable->vals[RowIndex], SPI_tuptable->tupdesc, ColumnNumber, IsNull)

/* TODO: avoid dedatumize when isNull is true? */
#define GET_SPI_VALUE(TypeId,RowIndex,ColumnNumber,IsNull) \
	DEDATUMIZE_##TypeId(GET_SPI_DATUM(RowIndex,ColumnNumber,IsNull))

#define SPI_START_VARS() \
	Oid			_savedUserId = InvalidOid; \
	int			_savedSecurityContext = 0;

/*
 * We do not want auto_explain to concern itself with our SPI queries,
 * since it might lead to failure, and unnecessary spilling of internals
 * into logs.
 */
#define DISABLE_QUERY_TRACKING() \
	int			_spiGUCNestLevel = NewGUCNestLevel(); \
	(void) set_config_option("auto_explain.log_min_duration", "-1", PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);\
	(void) set_config_option("pgaudit.log", "none", PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);\
	(void) set_config_option("pg_stat_statements.track", "none", PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);


#define RESET_QUERY_TRACKING() \
	AtEOXact_GUC(true, _spiGUCNestLevel);

#define SPI_START() \
	SPI_START_VARS() \
	DISABLE_QUERY_TRACKING() \
	SPI_connect();

/*
 * SPI_START_EXTENSION_OWNER runs SPI queries as the extension owner.
 *
 * This requires the caller to have ExtensionOwnerId() available
 * (from pg_extension_base/extension_ids.h).
 *
 * Usage: SPI_START_EXTENSION_OWNER(PgLakeTable)
 */
#define SPI_START_EXTENSION_OWNER(Extension) \
	SPI_START_VARS() \
	GetUserIdAndSecContext(&_savedUserId, &_savedSecurityContext); \
	SetUserIdAndSecContext(ExtensionOwnerId(Extension), SECURITY_LOCAL_USERID_CHANGE); \
	DISABLE_QUERY_TRACKING() \
	SPI_connect();

#define SPI_END() \
	if (_savedUserId != InvalidOid) \
		SetUserIdAndSecContext(_savedUserId, _savedSecurityContext); \
	RESET_QUERY_TRACKING(); \
	SPI_finish();

#endif
