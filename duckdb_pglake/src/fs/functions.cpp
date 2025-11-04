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

#include "duckdb.hpp"

#include "pg_lake/fs/file_cache_manager.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/fs/functions.hpp"
#include "pg_lake/fs/region_aware_s3fs.hpp"

#include "azure_blob_filesystem.hpp"
#include "azure_dfs_filesystem.hpp"

namespace duckdb {

/*
 * Name of the region in which the server resides.
 */
const string PG_LAKE_REGION_SETTING = "pg_lake_region";


/*
 * CacheFileFunctionData defines the custom state for pg_lake_cache_file.
 */
struct CopyFileFunctionData : public TableFunctionData
{
	/* Function arguments */
	string sourceFilePath;
	string destinationFilePath;

	/* Function state */
	bool finished = false;
};

/*
 * CopyFileBind implements the bind phase for pg_lake_copy_file.
 */
static unique_ptr<FunctionData>
CopyFileBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names)
{
	/* Get the arguments */
	auto functionData = make_uniq<CopyFileFunctionData>();
	functionData->sourceFilePath = input.inputs[0].ToString();
	functionData->destinationFilePath = input.inputs[1].ToString();

	/* Set the return type */
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("bytes_written");

	return std::move(functionData);
}

/*
 * CopyFileExec implements the execution for pg_lake_copy_file.
 */
static void CopyFileExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
{
	auto &data = (CopyFileFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}

	int64_t total_bytes_written =
		FileUtils::CopyFile(context, data.sourceFilePath, data.destinationFilePath);

	// Set return values for all modified params
	output.SetValue(0,0, Value(total_bytes_written));
	output.SetCardinality(1);

	data.finished = true;
}

/*
 * CacheFileFunctionData defines the custom state for pg_lake_cache_file.
 */
struct CacheFileFunctionData : public TableFunctionData
{
	/* Function arguments */
	string url;
	bool force = false;

	/* Function state */
	bool finished = false;
};

/*
 * CacheFileBind implements the bind phase for pg_lake_cache_file.
 */
static unique_ptr<FunctionData>
CacheFileBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names)
{
	/* Get the arguments */
	auto functionData = make_uniq<CacheFileFunctionData>();
	functionData->url = input.inputs[0].ToString();

	if (input.inputs.size() >= 2)
		functionData->force = input.inputs[1].GetValue<bool>();

	/* Set the return type */
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("bytes_written");

	return std::move(functionData);
}

/*
 * CacheFileExecute implements the execution for pg_lake_cache_file.
 */
static void CacheFileExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
{
	auto &functionData = (CacheFileFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;

	/* always wait for the lock when user facing function called */
	bool waitForLock = true;

	/* Do the work */
	shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(context);
	int64_t bytes_written = cacheManager->CacheFile(context, functionData.url, functionData.force, waitForLock);

	/* Set return values */
	output.SetValue(0,0, Value(bytes_written));
	output.SetCardinality(1);

	functionData.finished = true;
}

/*
 * Implementation of the pg_lake_cache_file_local_path(text) scalar function.
 *
 * The function synchronously caches the file and returns the local path.
 */
static void
CacheFileLocalPathScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &fileNameVector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
		fileNameVector, result, args.size(),
		[&](string_t url) {
			ClientContext &context = state.GetContext();
			FileOpener *opener = context.client_data->file_opener.get();
			shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(context);
			bool force = false;
			bool waitForLock = true;

			string cacheDir;

			if (!cacheManager->TryGetCacheDir(opener, cacheDir))
				/* caching is not configured */
				throw InvalidInputException("caching is currently disabled");

			/* where the file will be cached */
			string localPath;

			if (!cacheManager->TryGetCacheFilePath(cacheDir, url.GetString(), localPath))
				throw InvalidInputException("URL cannot be cached: " + url.GetString());

			cacheManager->CacheFile(context, url.GetString(), force, waitForLock);

			return StringVector::AddString(result, localPath);
		}
	);
}


/*
 * UncacheFileFunctionData defines the custom state for pg_lake_uncache_file.
 */
struct UncacheFileFunctionData : public TableFunctionData
{
	/* Function arguments */
	string url;

	/* Function state */
	bool finished = false;
};

/*
 * UncacheFileBind implements the bind phase for pg_lake_uncache_file.
 */
static unique_ptr<FunctionData>
UncacheFileBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names)
{
	/* Get the arguments */
	auto functionData = make_uniq<UncacheFileFunctionData>();
	functionData->url = input.inputs[0].ToString();

	/* Set the return type */
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("removed");

	return std::move(functionData);
}


/*
 * UncacheFileExecute implements the execution for pg_lake_uncache_file.
 */
static void
UncacheFileExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
{
	auto &functionData = (UncacheFileFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;

	/* always wait for the lock when user facing function called */
	bool waitForLock = true;

	/* Do the work */
	shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(context);
	FileCacheManager::CacheRemoveStatus status =
		cacheManager->RemoveCacheFile(context, functionData.url, waitForLock);

	/* Set return values */
	output.SetValue(0,0, Value(status == FileCacheManager::CacheRemoveStatus::FILE_EXISTS));
	output.SetCardinality(1);

	functionData.finished = true;
}


/*
 * ManageCacheFunctionData defines the custom state for pg_lake_manage_cache
 */
struct ManageCacheFunctionData : public TableFunctionData
{
	/* Function arguments */
	int64_t max_cache_size;

	/* Function state */
	vector<CacheAction> actions;
	int actionOffset;
	bool finished = false;
};


/*
 * ManageCacheBind implements the bind phase for pg_lake_manage_cache.
 */
static unique_ptr<FunctionData> ManageCacheBind(ClientContext &context,
											    TableFunctionBindInput &input,
											    vector<LogicalType> &return_types,
											    vector<string> &names) {

	/* Get the arguments */
	auto functionData = make_uniq<ManageCacheFunctionData>();
	functionData->max_cache_size = input.inputs[0].GetValue<int64_t>();

	/* Set the return type */
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::BIGINT);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("url");
	names.emplace_back("file_size");
	names.emplace_back("action");

	return std::move(functionData);
}


/*
 * ManageCacheExecute implements the execution for pg_lake_manage_cache.
 */
static void ManageCacheExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &functionData = (ManageCacheFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;

	/* Do the work */
	if (functionData.actionOffset == 0)
	{
		shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(context);
		functionData.actions = cacheManager->ManageCache(context, functionData.max_cache_size);
	}

	/* Set return values */
	idx_t rowInChunk = 0;
	while (functionData.actionOffset < functionData.actions.size() && rowInChunk < STANDARD_VECTOR_SIZE) {
		CacheAction &action = functionData.actions[functionData.actionOffset];
		output.SetValue(0, rowInChunk, Value(action.url));
		output.SetValue(1, rowInChunk, Value(action.fileSize));

		switch (action.action)
		{
			case ADDED:
				output.SetValue(2, rowInChunk, Value("added"));
				break;
			case ADD_FAILED:
				output.SetValue(2, rowInChunk, Value("add failed"));
				break;
			case REMOVED:
				output.SetValue(2, rowInChunk, Value("removed"));
				break;
			case SKIPPED_TOO_OLD:
				output.SetValue(2, rowInChunk, Value("skipped (newer files will be cached)"));
				break;
			case SKIPPED_TOO_LARGE:
				output.SetValue(2, rowInChunk, Value("skipped (larger than max cache size)"));
				break;
			case SKIPPED_CONCURRENT_MODIFY:
				output.SetValue(2, rowInChunk, Value("skipped (cache file was modified concurrently)"));
				break;
		}

		rowInChunk++;
		functionData.actionOffset++;
	}

	output.SetCardinality(rowInChunk);

	if (functionData.actionOffset == functionData.actions.size())
		functionData.finished = true;
}


/*
 * ListCacheFunctionData defines the custom state for pg_lake_list_cache
 */
struct ListCacheFunctionData : public TableFunctionData
{
	/* Function state */
	vector<CacheItem> files;
	int fileOffset;
	bool finished = false;
};


/*
 * ListCacheBind implements the bind phase for pg_lake_list_cache.
 */
static unique_ptr<FunctionData> ListCacheBind(ClientContext &context,
											    TableFunctionBindInput &input,
											    vector<LogicalType> &return_types,
											    vector<string> &names) {

	/* Get the arguments */
	auto functionData = make_uniq<ListCacheFunctionData>();

	/* Set the return type */
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::BIGINT);
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("url");
	names.emplace_back("file_size");
	names.emplace_back("last_access_time");

	return std::move(functionData);
}


/*
 * ListCacheExecute implements the execution for pg_lake_list_cache.
 */
static void ListCacheExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &functionData = (ListCacheFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;

	/* Do the work */
	if (functionData.fileOffset == 0)
	{
		shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(context);
		functionData.files = cacheManager->ListCache(context);
	}

	/* Set return values */
	idx_t rowInChunk = 0;
	while (functionData.fileOffset < functionData.files.size() && rowInChunk < STANDARD_VECTOR_SIZE) {
		CacheItem &file = functionData.files[functionData.fileOffset];
		timestamp_t lastAccessTime = Timestamp::FromEpochSeconds(file.lastAccessTime);

		output.SetValue(0, rowInChunk, Value(file.url));
		output.SetValue(1, rowInChunk, Value(file.fileSize));
		output.SetValue(2, rowInChunk, Value::TIMESTAMP(lastAccessTime));

		rowInChunk++;
		functionData.fileOffset++;
	}

	output.SetCardinality(rowInChunk);

	if (functionData.fileOffset == functionData.files.size())
		functionData.finished = true;
}


/*
 * ListFilexFunctionData defines the custom state for pg_lake_list_files
 */
struct ListFilesFunctionData : public TableFunctionData
{
	/* Function argument */
	string globPattern;

	/* Function state */
	vector<OpenFileInfo> files;
	bool hasDetails = false;

	int fileOffset;
	bool finished = false;
};


/*
 * ListFilesBind implements the bind phase for pg_lake_list_files.
 */
static unique_ptr<FunctionData> ListFilesBind(ClientContext &context,
											  TableFunctionBindInput &input,
											  vector<LogicalType> &return_types,
											  vector<string> &names) {

	/* Get the arguments */
	auto functionData = make_uniq<ListFilesFunctionData>();
	functionData->globPattern = input.inputs[0].ToString();

	/* Set the return type */
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::BIGINT);
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("url");
	names.emplace_back("file_size");
	names.emplace_back("last_modified_time");
	names.emplace_back("etag");

	return std::move(functionData);
}


/*
 * ListFilesExecute implements the execution for pg_lake_list_files.
 */
static void ListFilesExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &functionData = (ListFilesFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;

	/* Do the work */
	if (functionData.fileOffset == 0)
	{
		FileOpener *opener = context.client_data->file_opener.get();
		string &globPattern = functionData.globPattern;

		DatabaseInstance &db = DatabaseInstance::GetDatabase(context);
		BufferManager &bufferManager = BufferManager::GetBufferManager(db);

		/*
		 * It seems a bit simpler to just instantiate file systems
		 * than to implement singleton infrastructure just for this purpose.
		 */
		RegionAwareS3FileSystem s3fs(bufferManager);
		AzureBlobStorageFileSystem abfs(bufferManager);
		AzureDfsStorageFileSystem adfs(bufferManager);

		if (s3fs.CanHandleFile(globPattern))
		{
			/* S3 URL, get details from the S3 file system */
			functionData.files = s3fs.List(functionData.globPattern, false, opener);
			functionData.hasDetails = true;
		}
		else if (abfs.CanHandleFile(globPattern))
		{
			functionData.files = abfs.Glob(functionData.globPattern, opener);
			functionData.hasDetails = true;
		}
		else if (adfs.CanHandleFile(globPattern))
		{
			functionData.files = adfs.Glob(functionData.globPattern, opener);
			functionData.hasDetails = true;
		}
		else
		{
			/* other URL (e.g. HuggingFace), only include names for now */
			FileSystem &fs = FileSystem::GetFileSystem(context);

			functionData.files = fs.Glob(globPattern);
			functionData.hasDetails = false;
		}
	}

	/* Set return values */
	idx_t rowInChunk = 0;
	while (functionData.fileOffset < functionData.files.size() && rowInChunk < STANDARD_VECTOR_SIZE) {
		OpenFileInfo &file = functionData.files[functionData.fileOffset];

		output.SetValue(0, rowInChunk, Value(file.path));

		if (functionData.hasDetails && file.extended_info)
		{
			auto entry1 = file.extended_info->options.find("file_size");
			if (entry1 != file.extended_info->options.end()) {
				output.SetValue(1, rowInChunk, entry1->second);
			}
			auto entry2 = file.extended_info->options.find("last_modified");
			if (entry2 != file.extended_info->options.end()) {
				output.SetValue(2, rowInChunk, entry2->second);
			}
			auto entry3 = file.extended_info->options.find("etag");
			if (entry3 != file.extended_info->options.end()) {
				output.SetValue(3, rowInChunk, entry3->second);
			}
		}
		else
		{
			output.SetValue(1, rowInChunk, Value(nullptr));
			output.SetValue(2, rowInChunk, Value(nullptr));
			output.SetValue(3, rowInChunk, Value(nullptr));
		}

		rowInChunk++;
		functionData.fileOffset++;
	}

	output.SetCardinality(rowInChunk);

	if (functionData.fileOffset == functionData.files.size())
		functionData.finished = true;
}


/*
 * Implementation of the pg_lake_file_size scalar function.
 */
static void
FileSizeScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &fileNameVector = args.data[0];

	UnaryExecutor::Execute<string_t, int64_t>(
		fileNameVector, result, args.size(),
		[&](string_t fileName) {
			FileSystem &fs = FileSystem::GetFileSystem(state.GetContext());
			unique_ptr<FileHandle> fileHandle = fs.OpenFile(fileName.GetString(), FileFlags::FILE_FLAGS_READ);
			return fs.GetFileSize(*fileHandle);
		}
	);
}

/*
 * Implementation of the pg_lake_remove_file scalar function.
 */
static void
RemoveFileScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &fileNameVector = args.data[0];

	UnaryExecutor::Execute<string_t, bool>(
		fileNameVector, result, args.size(),
		[&](string_t fileName) {
			FileSystem &fs = FileSystem::GetFileSystem(state.GetContext());
			fs.RemoveFile(fileName.GetString());

			/*
			 * Ideally we would return whether the file existed before removal.
			 * For now we always return true, since RemoveFile does not have a return
			 * value.
			 */
			return true;
		}
	);
}


/*
 * AddS3ExpressRegionEndpointScalarFun is a wrapper around AddS3ExpressRegionEndpoint
 * for testing purposes.
 */
static void
AddS3ExpressRegionEndpointScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(),
	    [&](string_t url) {
			string urlWithEndpoint;

			if (!AddS3ExpressRegionEndpoint(url.GetString(), urlWithEndpoint))
				throw InvalidInputException("not an S3 express URL");

			return StringVector::AddString(result, urlWithEndpoint);
        });
}


/*
 * Implementation of the pg_lake_file_exists scalar function.
 */
static void
FileExistsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &fileNameVector = args.data[0];

	UnaryExecutor::Execute<string_t, bool>(
		fileNameVector, result, args.size(),
		[&](string_t fileName) {
			FileSystem &fs = FileSystem::GetFileSystem(state.GetContext());
			return fs.FileExists(fileName.GetString());
		}
	);
}


/*
 * Implementation of the pg_lake_get_bucket_region scalar function.
 */
static void
GetBucketRegionScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
{
	auto &urlVector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
		urlVector, result, args.size(),
		[&](string_t urlStr) {
			ClientContext &context = state.GetContext();
			FileOpener *opener = context.client_data->file_opener.get();
			DatabaseInstance &db = DatabaseInstance::GetDatabase(state.GetContext());
			RegionAwareS3FileSystem s3fs(BufferManager::GetBufferManager(db));
			string url = urlStr.GetString();

			/* non-S3 URLs do not have a region */
			if (!s3fs.CanHandleFile(url))
				return StringVector::AddString(result, "");

			/* non-S3 URLs do not have a region */
			return StringVector::AddString(result, s3fs.GetBucketRegion(url, opener));
		}
	);
}


/*
 * Implementation of the pg_lake_get_managed_storage_region scalar function.
 */
static void
GetManagedStorageRegionScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
{
	ClientContext &context = state.GetContext();

	Value setting;

	if (context.TryGetCurrentSetting(PG_LAKE_REGION_SETTING, setting))
	{
		/* return the value of the setting */
		result.Reference(setting);
	}
	else
	{
		/* return an empty string if not set */
		Value emptyResult("");
		result.Reference(emptyResult);
	}
}


/*
 * RegisterFunctions registers the pg_lake file system SQL functions.
 */
void
PgLakeFileSystemFunctions::RegisterFunctions(ExtensionLoader &loader)
{
	/* pg_lake_cache_file function definition */
	{
		TableFunctionSet pg_lake_cache_file("pg_lake_cache_file");

		/* pg_lake_cache_file(url varchar) */
		pg_lake_cache_file.AddFunction(
			TableFunction({LogicalTypeId::VARCHAR},
						  CacheFileExec, CacheFileBind));

		/* pg_lake_cache_file(url varchar, force bool) */
		pg_lake_cache_file.AddFunction(
			TableFunction({LogicalTypeId::VARCHAR, LogicalTypeId::BOOLEAN},
						  CacheFileExec, CacheFileBind));

	    loader.RegisterFunction(pg_lake_cache_file);
	}

	/* pg_lake_cache_file_local_path(text) function definition */
	{
		ScalarFunction pg_lake_cache_file_local_path =
			ScalarFunction("pg_lake_cache_file_local_path",
						   {LogicalType::VARCHAR},
						   LogicalType::VARCHAR,
						   CacheFileLocalPathScalarFun);
	    loader.RegisterFunction(pg_lake_cache_file_local_path);
	}


	/* pg_lake_uncache_file function definition */
	{
		TableFunctionSet pg_lake_uncache_file("pg_lake_uncache_file");

		/* pg_lake_uncache_file(url varchar) */
	    pg_lake_uncache_file.AddFunction(
			TableFunction({LogicalTypeId::VARCHAR},
						  UncacheFileExec, UncacheFileBind));

	    loader.RegisterFunction(pg_lake_uncache_file);
	}

	/* pg_lake_manage_cache function definition */
	{
		TableFunctionSet pg_lake_manage_cache("pg_lake_manage_cache");

		/* pg_lake_manage_cache(max_cache_size bigint) */
		pg_lake_manage_cache.AddFunction(
			TableFunction({LogicalTypeId::BIGINT},
						  ManageCacheExec, ManageCacheBind));

	    loader.RegisterFunction(pg_lake_manage_cache);
	}

	/* pg_lake_list_cache function definition */
	{
		TableFunctionSet pg_lake_list_cache("pg_lake_list_cache");

		/* pg_lake_list_cache() */
		pg_lake_list_cache.AddFunction(
			TableFunction({},
						  ListCacheExec, ListCacheBind));

	    loader.RegisterFunction(pg_lake_list_cache);
	}

	/* pg_lake_list_files function definition */
	{
		TableFunctionSet pg_lake_list_files("pg_lake_list_files");

		/* pg_lake_list_files() */
		pg_lake_list_files.AddFunction(
			TableFunction({LogicalType::VARCHAR},
						  ListFilesExec, ListFilesBind));

	    loader.RegisterFunction(pg_lake_list_files);
	}


	/* pg_lake_file_size function definition */
	{
		ScalarFunction pg_lake_file_size =
			ScalarFunction("pg_lake_file_size",
						   {LogicalType::VARCHAR},
						   LogicalType::BIGINT,
						   FileSizeScalarFun);

        loader.RegisterFunction(pg_lake_file_size);
	}

	/* pg_lake_remove_file function definition */
	{
		ScalarFunction pg_lake_remove_file =
			ScalarFunction("pg_lake_remove_file",
						   {LogicalType::VARCHAR},
						   LogicalType::BOOLEAN,
						   RemoveFileScalarFun);

		loader.RegisterFunction(pg_lake_remove_file);
	}

	/* pg_lake_test_add_s3_express(text) function definition */
	{
		ScalarFunction pg_lake_test_add_s3_express =
			ScalarFunction("pg_lake_test_add_s3_express",
						   {LogicalType::VARCHAR},
						   LogicalType::VARCHAR,
						   AddS3ExpressRegionEndpointScalarFun);
	    loader.RegisterFunction(pg_lake_test_add_s3_express);
	}

	/* pg_lake_copy_file function definition */
	{
		TableFunctionSet pg_lake_copy_file("pg_lake_copy_file");

		/* pg_lake_copy_file(sourceFilePath varchar, destinationFilePath varchar) */
		pg_lake_copy_file.AddFunction(
			TableFunction({LogicalTypeId::VARCHAR, LogicalTypeId::VARCHAR},
						  CopyFileExec, CopyFileBind));

	    loader.RegisterFunction(pg_lake_copy_file);
	}

	/* pg_lake_file_exists function definition */
	{
		ScalarFunction pg_lake_file_exists =
			ScalarFunction("pg_lake_file_exists",
						   {LogicalType::VARCHAR},
						   LogicalType::BOOLEAN,
						   FileExistsScalarFun);

		loader.RegisterFunction(pg_lake_file_exists);
	}

	/* pg_lake_get_bucket_region function definition */
	{
		ScalarFunction pg_lake_get_bucket_region =
			ScalarFunction("pg_lake_get_bucket_region",
						   {LogicalType::VARCHAR},
						   LogicalType::VARCHAR,
						   GetBucketRegionScalarFun);

		loader.RegisterFunction(pg_lake_get_bucket_region);
	}

	/* pg_lake_get_managed_storage_region function definition */
	{
		ScalarFunction pg_lake_get_managed_storage_region =
			ScalarFunction("pg_lake_get_managed_storage_region",
						   {},
						   LogicalType::VARCHAR,
						   GetManagedStorageRegionScalarFun);

		loader.RegisterFunction(pg_lake_get_managed_storage_region);
	}

	/* Add settings */
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(CACHE_DIR_SETTING, "PgLake Cache Directory", LogicalType::VARCHAR);
	config.AddExtensionOption(CACHE_ON_WRITE_MAX_SIZE, "PgLake cache-on-write max size", LogicalType::BIGINT);
	config.AddExtensionOption(PG_LAKE_REGION_SETTING, "The region of the server", LogicalType::VARCHAR);
	config.AddExtensionOption(MANAGED_STORAGE_BUCKET_SETTING, "PgLake managed storage bucket location", LogicalType::VARCHAR);
	config.AddExtensionOption(MANAGED_STORAGE_KEY_ID_SETTING, "PgLake managed storage customer key ID", LogicalType::VARCHAR);
}

} // namespace duckdb
