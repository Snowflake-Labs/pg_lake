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

/*
 * Functions for parsing/deparsing COPY data formats.
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/copy/copy_format.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

/* mapping of format name to enum */
typedef struct CopyDataFormatName
{
	char	   *name;
	CopyDataFormat format;
}			CopyDataFormatName;

static CopyDataFormatName DataFormatNames[] =
{
	{
		"csv", DATA_FORMAT_CSV
	},
	{
		"json", DATA_FORMAT_JSON
	},
	{
		"parquet", DATA_FORMAT_PARQUET
	},
	{
		"iceberg", DATA_FORMAT_ICEBERG
	},
#if PG_LAKE_DELTA_SUPPORT == 1
	{
		"delta", DATA_FORMAT_DELTA
	},
#endif
	{
		"gdal", DATA_FORMAT_GDAL
	},
	{
		"log", DATA_FORMAT_LOG
	},
	{
		NULL, DATA_FORMAT_INVALID
	},
};

/* mapping of compression name to enum */
typedef struct CopyDataCompressionName
{
	char	   *name;
	CopyDataCompression compression;
}			CopyDataCompressionName;

static CopyDataCompressionName DataCompressionNames[] =
{
	{
		"none", DATA_COMPRESSION_NONE
	},
	{
		"gzip", DATA_COMPRESSION_GZIP
	},
	{
		"zstd", DATA_COMPRESSION_ZSTD
	},
	{
		"snappy", DATA_COMPRESSION_SNAPPY
	},
	{
		"zip", DATA_COMPRESSION_ZIP
	},
	{
		NULL, DATA_COMPRESSION_INVALID
	},
};

/* mapping of file extension to format and compression */
typedef struct CopyDataFormatExtension
{
	char	   *extension;

	CopyDataFormat format;
	CopyDataCompression compression;
}			CopyDataFormatExtension;

static CopyDataFormatExtension DataFormatExtensions[] =
{
	{
		".csv", DATA_FORMAT_CSV, DATA_COMPRESSION_NONE
	},
	{
		".csv.gz", DATA_FORMAT_CSV, DATA_COMPRESSION_GZIP
	},
	{
		".csv.zst", DATA_FORMAT_CSV, DATA_COMPRESSION_ZSTD
	},
	/* before .json, to take priority in URLToCopyDataFormat */
	{
		".metadata.json", DATA_FORMAT_ICEBERG, DATA_COMPRESSION_NONE
	},
	{
		".json", DATA_FORMAT_JSON, DATA_COMPRESSION_NONE
	},
	{
		".json.gz", DATA_FORMAT_JSON, DATA_COMPRESSION_GZIP
	},
	{
		".json.zst", DATA_FORMAT_JSON, DATA_COMPRESSION_ZSTD
	},
	{
		".parquet", DATA_FORMAT_PARQUET, DATA_COMPRESSION_INVALID
	},
	{
		".geojson", DATA_FORMAT_GDAL, DATA_COMPRESSION_NONE
	},
	{
		".geojson.gz", DATA_FORMAT_GDAL, DATA_COMPRESSION_GZIP
	},
	{
		".gpkg", DATA_FORMAT_GDAL, DATA_COMPRESSION_NONE
	},
	{
		".gpkg.gz", DATA_FORMAT_GDAL, DATA_COMPRESSION_GZIP
	},
	{
		".fgb", DATA_FORMAT_GDAL, DATA_COMPRESSION_NONE
	},
	{
		".kml", DATA_FORMAT_GDAL, DATA_COMPRESSION_NONE
	},
	{
		".kmz", DATA_FORMAT_GDAL, DATA_COMPRESSION_ZIP
	},
	{
		".zip", DATA_FORMAT_GDAL, DATA_COMPRESSION_ZIP
	},
	{
		".dxf", DATA_FORMAT_GDAL, DATA_COMPRESSION_NONE
	},
	{
		".gml", DATA_FORMAT_GDAL, DATA_COMPRESSION_NONE
	},
	{
		NULL, DATA_FORMAT_INVALID, DATA_COMPRESSION_INVALID
	},
};

static char *GetBaseURL(char *url);

/*
 * CopyDataFormatToName returns the name of a copy data format.
 *
 * We could be clever here and use the enum as an index, but the
 * arrays are small and looping is a bit more robust to future
 * changes.
 */
const char *
CopyDataFormatToName(CopyDataFormat format)
{
	const char *name = NULL;

	for (int fmtIndex = 0; DataFormatNames[fmtIndex].name != NULL; fmtIndex++)
	{
		if (DataFormatNames[fmtIndex].format == format)
		{
			name = DataFormatNames[fmtIndex].name;
			break;
		}
	}

	return name;
}


/*
 * CopyDataCompressionToName returns the name of a copy data compression.
 *
 * We could be clever here and use the enum as an index, but the
 * arrays are small and looping is a bit more robust to future
 * changes.
 */
const char *
CopyDataCompressionToName(CopyDataCompression compression)
{
	const char *name = NULL;

	for (int cmpIndex = 0; DataCompressionNames[cmpIndex].name != NULL; cmpIndex++)
	{
		if (DataCompressionNames[cmpIndex].compression == compression)
		{
			name = DataCompressionNames[cmpIndex].name;
			break;
		}
	}

	return name;
}


/*
 * NameToCopyDataFormat finds a CopyDataFormat by name.
 */
CopyDataFormat
NameToCopyDataFormat(char *formatName)
{
	CopyDataFormat format = DATA_FORMAT_INVALID;

	for (int fmtIndex = 0; DataFormatNames[fmtIndex].name != NULL; fmtIndex++)
	{
		if (strcasecmp(formatName, DataFormatNames[fmtIndex].name) == 0)
		{
			format = DataFormatNames[fmtIndex].format;
			break;
		}
	}

	return format;
}


/*
 * NameToCopyDataCompression finds a CopyDataCompression by name.
 */
CopyDataCompression
NameToCopyDataCompression(char *compressionName)
{
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;

	for (int cmpIndex = 0; DataCompressionNames[cmpIndex].name != NULL; cmpIndex++)
	{
		if (strcasecmp(compressionName, DataCompressionNames[cmpIndex].name) == 0)
		{
			compression = DataCompressionNames[cmpIndex].compression;
			break;
		}
	}

	return compression;
}


/*
 * URLToCopyDataFormat finds a CopyDataFormat based on the URL suffix.
 */
CopyDataFormat
URLToCopyDataFormat(char *url)
{
	CopyDataFormat format = DATA_FORMAT_INVALID;
	char	   *baseURL = GetBaseURL(url);

	for (int extIndex = 0; DataFormatExtensions[extIndex].extension != NULL; extIndex++)
	{
		if (pg_str_endswith(baseURL, DataFormatExtensions[extIndex].extension))
		{
			format = DataFormatExtensions[extIndex].format;
			break;
		}
	}

	return format;
}


/*
 * URLToCopyDataCompression finds a CopyDataCompression based on the URL suffix.
 */
CopyDataCompression
URLToCopyDataCompression(char *url)
{
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;
	char	   *baseURL = GetBaseURL(url);

	for (int extIndex = 0; DataFormatExtensions[extIndex].extension != NULL; extIndex++)
	{
		if (pg_str_endswith(baseURL, DataFormatExtensions[extIndex].extension))
		{
			compression = DataFormatExtensions[extIndex].compression;
			break;
		}
	}

	return compression;
}


/*
 * GetBaseURL returns the part of the URL before the ? or the
 * whole URL if no ? can be found.
 */
char *
GetBaseURL(char *url)
{
	char	   *questionMarkPointer = strchr(url, '?');

	if (questionMarkPointer == NULL)
	{
		return url;
	}

	/* allocate a new string containing only the part before ? */
	int			baseLength = questionMarkPointer - url;
	char	   *baseURL = palloc(baseLength + 1);

	strncpy(baseURL, url, baseLength);
	baseURL[baseLength] = '\0';

	return baseURL;
}


/*
 * OptionsToCopyDataFormat returns the CopyDataFormat specified
 * by a format option in the options list, if any, and if recognized.
 */
CopyDataFormat
OptionsToCopyDataFormat(List *copyOptions)
{
	CopyDataFormat format = DATA_FORMAT_INVALID;

	ListCell   *optionCell = NULL;

	foreach(optionCell, copyOptions)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, "format") == 0)
		{
			char	   *formatName = defGetString(option);

			format = NameToCopyDataFormat(formatName);
			break;
		}
	}

	return format;
}


/*
 * OptionsToCopyDataCompression returns the CopyDataCompression specified
 * by a compression option in the options list, if any, and if recognized.
 */
CopyDataCompression
OptionsToCopyDataCompression(List *copyOptions)
{
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;

	ListCell   *optionCell = NULL;

	foreach(optionCell, copyOptions)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, "compression") == 0)
		{
			char	   *compressionName = defGetString(option);

			compression = NameToCopyDataCompression(compressionName);
			break;
		}
	}

	return compression;
}


/*
 * URLToCopyDataFormat finds a CopyDataFormat based on the URL suffix.
 */
const char *
FormatToFileExtension(CopyDataFormat format, CopyDataCompression compression)
{
	/* Parquet files use a single extension */
	if (format == DATA_FORMAT_PARQUET)
		return ".parquet";

	/* CSV/JSON extension depends on compression */
	for (int extIndex = 0; DataFormatExtensions[extIndex].extension != NULL; extIndex++)
	{
		if (DataFormatExtensions[extIndex].format == format &&
			DataFormatExtensions[extIndex].compression == compression)
		{
			return DataFormatExtensions[extIndex].extension;
		}
	}

	return NULL;
}


/*
 * IsSupportedURL returns whether the given path is prefixed with
 * one of the supported URL protocols.
 */
bool
IsSupportedURL(const char *path)
{
	if (path == NULL)
	{
		return false;
	}

	/*
	 * We added (public) saved query prefix here ahead of more proper HTTPS
	 * support as a small easter egg and because it seems nice for the overall
	 * user experience.
	 */

	return strncmp(path, S3_URL_PREFIX, strlen(S3_URL_PREFIX)) == 0 ||
		strncmp(path, GCS_URL_PREFIX, strlen(GCS_URL_PREFIX)) == 0 ||
		strncmp(path, AZURE_BLOB_URL_PREFIX, strlen(AZURE_BLOB_URL_PREFIX)) == 0 ||
		strncmp(path, AZURE_DLS_URL_PREFIX, strlen(AZURE_DLS_URL_PREFIX)) == 0 ||
		strncmp(path, HTTP_URL_PREFIX, strlen(HTTP_URL_PREFIX)) == 0 ||
		strncmp(path, HTTPS_URL_PREFIX, strlen(HTTPS_URL_PREFIX)) == 0 ||
		strncmp(path, HUGGING_FACE_URL_PREFIX, strlen(HUGGING_FACE_URL_PREFIX)) == 0;
}


/*
 * FindDataFormatAndCompression tries to determine the format and compression
 * from the URL and copy options, or throws an error if they cannot be
 * determined.
 */
void
FindDataFormatAndCompression(PgLakeTableType tableType,
							 char *path, List *copyOptions,
							 CopyDataFormat * format,
							 CopyDataCompression * compression)
{
	*format = DATA_FORMAT_INVALID;
	*compression = DATA_COMPRESSION_INVALID;

	/*
	 * First, guess the format and compression from the path.
	 *
	 * We treat the path as a URL, which mainly means that if it contains a ?
	 * symbol we check the suffix before the ? symbol.
	 */
	if (path != NULL)
	{
		*format = URLToCopyDataFormat(path);
		*compression = URLToCopyDataCompression(path);
	}

	/* override the format/compression if explicitly specified */
	ListCell   *optionCell = NULL;

	foreach(optionCell, copyOptions)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, "format") == 0)
		{
			char	   *formatName = defGetString(option);

			*format = NameToCopyDataFormat(formatName);
			if (*format == DATA_FORMAT_INVALID)
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("pg_lake_copy: format \"%s\" not recognized",
									   formatName)));
			}
		}
		else if (strcmp(option->defname, "compression") == 0)
		{
			char	   *compressionName = defGetString(option);

			*compression = NameToCopyDataCompression(compressionName);
			if (*compression == DATA_COMPRESSION_INVALID)
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("pg_lake_copy: compression \"%s\" not recognized",
									   compressionName)));
			}
		}
	}

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		*format = DATA_FORMAT_PARQUET;
		*compression = DATA_COMPRESSION_SNAPPY;
	}

	if (*format == DATA_FORMAT_INVALID)
	{
		/* no explicit format, and extension not recognized */
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_copy: unrecognized file format")));
	}

	if (*compression == DATA_COMPRESSION_INVALID)
	{
		if (*format == DATA_FORMAT_PARQUET)
		{
			/*
			 * For Parquet we use snappy if not specified because it appears
			 * to be the most common compression algorithm for Parquet. This
			 * value is not used for reads, since the metadata of Parquet
			 * files specifies which compression algorithm is used.
			 */
			*compression = DATA_COMPRESSION_SNAPPY;
		}
		else
		{
			/*
			 * If we do not recognize the extension, do have an explicit
			 * format, but do not have an explicit compression, then default
			 * to 'none'
			 */
			*compression = DATA_COMPRESSION_NONE;
		}
	}

	if (*format == DATA_FORMAT_GDAL)
	{
		if (*compression != DATA_COMPRESSION_NONE &&
			*compression != DATA_COMPRESSION_GZIP &&
			*compression != DATA_COMPRESSION_ZIP)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("%s compression is not supported for GDAL",
								   CopyDataCompressionToName(*compression))));

		}
	}
}
