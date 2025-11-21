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

#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "httpfs.hpp"
#include "s3fs.hpp"
#include "hffs.hpp"
#include "azure_blob_filesystem.hpp"
#include "azure_dfs_filesystem.hpp"
#include "azure_extension.hpp"
#include "postgres_scanner_extension.hpp"

#include "duckdb_pglake_extension.hpp"
#include "pg_lake/fs/pg_lake_s3fs.hpp"
#include "pg_lake/fs/file_cache_manager.hpp"
#include "pg_lake/fs/functions.hpp"
#include "pg_lake/fs/caching_file_system.hpp"
#include "pg_lake/fs/region_aware_s3fs.hpp"
#include "pg_lake/query_listener.hpp"
#include "pg_lake/utility_functions.hpp"

/* whether to show DEBUG logs */
bool PgLakePgcompatIsOutputVerbose = false;

/* not declared in azure_extension.hpp */
extern "C" void azure_init(duckdb::DatabaseInstance &db);

namespace duckdb {

inline void ToDateScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<double, date_t>(
	    name_vector, result, args.size(),
	    [&](double daysSinceEpoch) {
			return date_t((int32_t) daysSinceEpoch);
        });
}

inline void ThrowInternalError(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str = duckdb::StringValue::Get(args.GetValue(0,0));
	throw InternalException(str);
}


inline void AcoshPG(DataChunk &args, ExpressionState &state, Vector &result)
{
	auto &input_vector = args.data[0];

	UnaryExecutor::Execute<double, double>(
		input_vector, result, args.size(),
		[&](double value) {
			if (value < 1.0) {
				throw InvalidInputException("input is out of range");
			}
			return std::acosh(value);
		});
}


inline void AtanhPG(DataChunk &args, ExpressionState &state, Vector &result)
{
	auto &input_vector = args.data[0];

	UnaryExecutor::Execute<double, double>(
		input_vector, result, args.size(),
		[&](double value) {
			if (value < -1.0 || value > 1.0) {
				throw InvalidInputException("input is out of range");
			}
			return std::atanh(value);
		});
}


/*
* Postgres and DuckDB have different behavior for the SUBSTRING function when
* the length or offset is negative. This function implements the Postgres
* behavior for the SUBSTRING function.
*/
inline void SubstringPG(DataChunk &args, ExpressionState &state, Vector &result)
{
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];

	if (args.ColumnCount() == 3)
	{
		auto &length_vector = args.data[2];

		TernaryExecutor::Execute<string_t, long, int64_t, string_t>(
			input_vector, offset_vector, length_vector, result, args.size(),
			[&](string_t input_string, int64_t offset, int64_t length)
		{
			int64_t adjustedStartOffset = offset;
			int64_t adjustedLength = length;

			if (length < 0)
			{
				/* similar to Postgres, do not allow negative lengths */
				throw InvalidInputException("negative substring length not allowed");
			}

			/*
			* Similar to Postgres, adjust the start offset to 0 if it is less than 0.
			* Then, also adjust the length to the sum of the offset and the length.
			* See the detailed discussion:
			* https://www.postgresql.org/message-id/72911.1709703729%40sss.pgh.pa.us
			*/
			if (offset < 0)
			{
				adjustedStartOffset = 0;

				int sum = offset + length;
				adjustedLength = sum;
			}

			return SubstringUnicode(result, input_string, adjustedStartOffset, adjustedLength);
		});
	} else
	{
		BinaryExecutor::Execute<string_t, int64_t, string_t>(
		input_vector, offset_vector, result, args.size(),
		[&](string_t input_string, int64_t offset)
		{

			/*
			* Similar to Postgres, adjust the start offset to 1 if it is less than 1.
			* See https://github.com/postgres/postgres/blob/86a2d2a321215797abd1c67d9f2c52510423a97a/src/backend/utils/adt/varlena.c#L898C2-L898C17
			*/
			int64_t adjustedStartOffset = MaxValue<int64_t>(1, offset);

			return SubstringUnicode(result, input_string, adjustedStartOffset, NumericLimits<uint32_t>::Maximum());
		});
	}
}


/*
 * NthSuffixScalarFun implements the nth_suffix function which adds the
 * appropriate suffix to a number. We use this in the to_char implementation.
 */
static void
NthSuffixScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &values = args.data[0];

    UnaryExecutor::Execute<int32_t, string_t>(
	    values, result, args.size(),
	    [&](int32_t value) {
			if (value >= 11 && value <= 13)
				return StringVector::AddString(result, "th");
			if (value % 10 == 1)
				return StringVector::AddString(result, "st");
			if (value % 10 == 2)
				return StringVector::AddString(result, "nd");
			if (value % 10 == 3)
				return StringVector::AddString(result, "rd");
			else
				return StringVector::AddString(result, "th");
        });
}


/*
* Get any type and return NULL for the same type.
*/
static void NullifyAnyType(DataChunk &args, ExpressionState &state, Vector &result)
{
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

/*
 * Call ST_AsHexWKB on a geometry blob.
 */
Value
PgLakeGeometryToHexWKB(shared_ptr<DatabaseInstance> db, string_t input)
{
	static scalar_function_t st_ashexwkb = nullptr;

	try
	{
		if (st_ashexwkb == nullptr)
		{
			/* retrieve the st_ashexwkb function pointer once */
			ScalarFunctionCatalogEntry& catalogEntry = ExtensionUtil::GetFunction(*db, "st_ashexwkb");
			ScalarFunctionSet scalarFunctionSet = catalogEntry.functions;
			ScalarFunction scalarFunction = scalarFunctionSet.GetFunctionByOffset(0);
			st_ashexwkb = scalarFunction.function;
		}

		/* create the input value */
		Value inputValue = Value::BLOB_RAW(input.GetString());

		/* create a chunk with 1 vector with 1 element */
		DataChunk inputChunk;
		inputChunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BLOB}, 1);
		inputChunk.SetCardinality(1);
		inputChunk.data[0].SetValue(0, inputValue);

		/* create a minimal, fake expression state */
		unique_ptr<Expression> expr =
			make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		ExpressionExecutorState execState;
		unique_ptr<ExpressionState> exprState =
			ExpressionExecutor::InitializeState(*expr, execState);

		/* create result vector */
		Vector resultVector(LogicalType::VARCHAR, 1);

		/* call st_ashexwkb */
		st_ashexwkb(inputChunk, *exprState, resultVector);

		/* get the result value */
		return resultVector.GetValue(0);
	}
	catch (const std::exception& ex)
	{
		/*
		 * Catch the exception to prevent a crash. Not so much else we can do.
		 * We return a nonsense text that will likely trigger a parse error.
		 */
		ErrorData error(ex);

		PGDUCK_SERVER_DEBUG("Geometry conversion failed: %s", error.Message().c_str());

		return Value("<conversion error>");
	}
}



static void LoadInternal(DatabaseInstance &instance) {
    /* Register functions */
    auto to_date_function = ScalarFunction("to_date", {LogicalType::DOUBLE}, LogicalType::DATE, ToDateScalarFun);
    ExtensionUtil::RegisterFunction(instance, to_date_function);

    auto acosh_function = ScalarFunction("acosh_pg", {LogicalType::DOUBLE}, LogicalType::DOUBLE, AcoshPG);
    ExtensionUtil::RegisterFunction(instance, acosh_function);

	auto atanh_function = ScalarFunction("atanh_pg", {LogicalType::DOUBLE}, LogicalType::DOUBLE, AtanhPG);
	ExtensionUtil::RegisterFunction(instance, atanh_function);

	auto nullify_any_type = ScalarFunction("nullify_any_type", {LogicalType::ANY}, LogicalType::SQLNULL, NullifyAnyType);
	ExtensionUtil::RegisterFunction(instance, nullify_any_type);

	auto throw_internal_error = ScalarFunction("pg_lake_throw_internal_error", {LogicalType::VARCHAR}, LogicalType::SQLNULL, ThrowInternalError);
	ExtensionUtil::RegisterFunction(instance, throw_internal_error);

    auto lake_nth_suffix = ScalarFunction("lake_nth_suffix", {LogicalType::INTEGER}, LogicalType::VARCHAR, NthSuffixScalarFun);
    ExtensionUtil::RegisterFunction(instance, lake_nth_suffix);

	ScalarFunctionSet substr("substring_pg");
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR, SubstringPG));
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::VARCHAR, SubstringPG));
	ExtensionUtil::RegisterFunction(instance, substr);

	PgLakeUtilityFunctions::RegisterFunctions(instance);
	PgLakeFileSystemFunctions::RegisterFunctions(instance);

	/* Replace the S3 and HTTP file system with wrappers */
	auto &fs = instance.GetFileSystem();

	fs.UnregisterSubSystem("S3FileSystem");
	fs.RegisterSubSystem(
		make_uniq<CachingFileSystem>(
			make_uniq<RegionAwareS3FileSystem>(BufferManager::GetBufferManager(instance))
		)
	);

	fs.UnregisterSubSystem("AzureBlobStorageFileSystem");
	fs.RegisterSubSystem(
		make_uniq<CachingFileSystem>(
			make_uniq<AzureBlobStorageFileSystem>(BufferManager::GetBufferManager(instance))
		)
	);

	fs.UnregisterSubSystem("AzureDfsStorageFileSystem");
	fs.RegisterSubSystem(
		make_uniq<CachingFileSystem>(
			make_uniq<AzureDfsStorageFileSystem>(BufferManager::GetBufferManager(instance))
		)
	);

	fs.UnregisterSubSystem("HTTPFileSystem");
	fs.RegisterSubSystem(
		make_uniq<CachingFileSystem>(
			make_uniq<HTTPFileSystem>()
		)
	);

	fs.UnregisterSubSystem("HuggingFaceFileSystem");
	fs.RegisterSubSystem(
		make_uniq<CachingFileSystem>(
			make_uniq<HuggingFaceFileSystem>()
		)
	);

}

void DuckdbPglakeExtension::Load(DuckDB &db) {
	azure_init(*db.instance);
	postgres_scanner_init(*db.instance);
	LoadInternal(*db.instance);
}
std::string DuckdbPglakeExtension::Name() {
	return "duckdb_pglake";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckdb_pglake_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DuckdbPglakeExtension>();
}

DUCKDB_EXTENSION_API const char *duckdb_pglake_version() {
	return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_decimal_to_string(duckdb_decimal input) {
	duckdb::hugeint_t value;
	value.upper = input.value.upper;
	value.lower = input.value.lower;
	return strdup(duckdb::Decimal::ToString(value, input.width, input.scale).c_str());
}


DUCKDB_EXTENSION_API const char * duckdb_pglake_uuid_to_string(duckdb_hugeint input) {
	duckdb::hugeint_t value;
	value.upper = input.upper;
	value.lower = input.lower;
	return strdup(duckdb::UUID::ToString(value).c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_hugeint_to_string(duckdb_hugeint input) {
	duckdb::hugeint_t value;
	value.upper = input.upper;
	value.lower = input.lower;
	return strdup(duckdb::Hugeint::ToString(value).c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_uhugeint_to_string(duckdb_uhugeint input) {
	duckdb::uhugeint_t value;
	value.upper = input.upper;
	value.lower = input.lower;
	return strdup(duckdb::Uhugeint::ToString(value).c_str());
}


DUCKDB_EXTENSION_API const char * duckdb_pglake_timestamp_to_string(duckdb_timestamp input) {
	return strdup(duckdb::Timestamp::ToString(duckdb::timestamp_t(input.micros)).c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_timestamp_ns_to_string(duckdb_timestamp input) {
	duckdb::Value v = duckdb::Value::TIMESTAMPNS(duckdb::timestamp_ns_t(input.micros));
	return strdup(v.ToString().c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_timestamp_ms_to_string(duckdb_timestamp input) {
	duckdb::Value v = duckdb::Value::TIMESTAMPMS(duckdb::timestamp_ms_t(input.micros));
	return strdup(v.ToString().c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_timestamp_sec_to_string(duckdb_timestamp input) {
	duckdb::Value v = duckdb::Value::TIMESTAMPSEC(duckdb::timestamp_sec_t(input.micros));
	return strdup(v.ToString().c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_timestamp_tz_to_string(duckdb_timestamp input) {
	duckdb::Value v = duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(input.micros));
	return strdup(v.ToString().c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_interval_to_string(duckdb_interval input) {
    duckdb::interval_t interval;
    interval.months = input.months;
    interval.days = input.days;
    interval.micros = input.micros;
	return strdup(duckdb::Interval::ToString(interval).c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_time_to_string(duckdb_time input) {
	return strdup(duckdb::Time::ToString(duckdb::dtime_t(input.micros)).c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_time_tz_to_string(duckdb_time_tz input) {
	duckdb::Value v = duckdb::Value::TIMETZ(duckdb::dtime_tz_t(input.bits));
	return strdup(v.ToString().c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_date_to_string(duckdb_date input) {
	return strdup(duckdb::Date::ToString(duckdb::date_t(input.days)).c_str());
}

DUCKDB_EXTENSION_API const char * duckdb_pglake_geometry_to_string(duckdb_database database, duckdb_string_t input) {
	duckdb::DatabaseWrapper *wrapper = reinterpret_cast<duckdb::DatabaseWrapper *>(database);
	duckdb::shared_ptr<duckdb::DatabaseInstance> db = wrapper->database->instance;

	duckdb::string_t data = *(duckdb::string_t *)(&input);
	duckdb::Value hexWKB = PgLakeGeometryToHexWKB(db, data);

	return strdup(hexWKB.ToString().c_str());
}

DUCKDB_EXTENSION_API void duckdb_pglake_set_output_verbose(bool verbose) {
	PgLakePgcompatIsOutputVerbose = verbose;
}

DUCKDB_EXTENSION_API void duckdb_pglake_init_connection(duckdb_connection connection, int64_t connectionId) {
	duckdb::Connection *conn = reinterpret_cast<duckdb::Connection *>(connection);

	shared_ptr<PgLakeQueryListener> queryListener =
		conn->context->registered_state->GetOrCreate<duckdb::PgLakeQueryListener>("pg_lake_query_listener");

	queryListener->connectionId = connectionId;
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
