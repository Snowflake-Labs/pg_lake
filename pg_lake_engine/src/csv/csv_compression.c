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
 * csv_compression.c
 *
 * Compression for the temporary CSV files that carry rows between PostgreSQL
 * and the query engine.  The CSV writer pushes each finished row through a
 * CSVCompressor instead of writing it to stdio directly, and the read_csv()
 * call that reads the file back gets a matching compression= argument.
 *
 * The exchange CSV is much larger than the Parquet it becomes -- values are
 * text, and bytea costs four bytes per input byte -- so a bulk load can need
 * several times the table's size in temporary space.  Compressing it trades
 * CPU for that space and for the write and read bandwidth underneath it.
 *
 * The codec has to be one both ends understand, which leaves gzip and zstd:
 * those are the only two DuckDB's CSV reader can unwrap, and the only two of
 * PostgreSQL's own compression methods it shares.  lz4 does not qualify, even
 * though DuckDB decompresses it for Parquet column pages -- that is the raw
 * block format, and there is no file system there to unwrap an lz4 frame off a
 * CSV.  zstd is the faster of the two survivors at any given ratio, and its
 * negative levels are faster still, so it is what to reach for when the point
 * is throughput rather than space.
 */
#include <zlib.h>
#include <zstd.h>

#include "postgres.h"

#include "pg_lake/csv/csv_compression.h"
#include "utils/memutils.h"

/* pg_lake_engine.temp_file_compression setting */
int			TempFileCompression = DATA_COMPRESSION_NONE;

/* pg_lake_engine.temp_file_compression_level setting */
int			TempFileCompressionLevel = 1;

/*
 * Staging buffer for compressor output.  Only needs to be large enough to keep
 * the write count down; both codecs are called repeatedly until they run out of
 * output.  It also clears ZSTD_CStreamOutSize(), one zstd block plus framing,
 * which is what lets the frame usually close in a single pass.
 */
#define COMPRESS_BUFFER_SIZE (256 * 1024)

/*
 * windowBits 15 (the maximum, and zlib's default) plus 16 to ask for a gzip
 * container rather than a raw zlib one, which is what DuckDB's read_csv()
 * expects from compression = 'gzip'.
 */
#define GZIP_WINDOW_BITS (15 + 16)

/* zlib's own default, which trades memory for compression ratio */
#define GZIP_MEM_LEVEL 8

struct CSVCompressor
{
	CopyDataCompression method;
	FILE	   *file;

	/* gzip state, plus the context holding all of zlib's allocations */
	z_stream	stream;
	MemoryContext context;

	/* zstd state, released by ReleaseZstdStream() on the error path */
	ZSTD_CStream *zstd;

	unsigned char buffer[COMPRESS_BUFFER_SIZE];
};

static void GzipCompressorCreate(CSVCompressor * compressor, int level);
static void GzipCompressorWrite(CSVCompressor * compressor, const void *data,
								size_t size);
static void GzipCompressorFinish(CSVCompressor * compressor);
static void ZstdCompressorCreate(CSVCompressor * compressor, int level);
static void ZstdCompressorWrite(CSVCompressor * compressor, const void *data,
								size_t size);
static void ZstdCompressorFinish(CSVCompressor * compressor);
static void ReleaseZstdStream(void *arg);
static void *CompressorAlloc(void *opaque, unsigned int items, unsigned int size);
static void CompressorFree(void *opaque, void *address);
static void WriteCompressorBuffer(CSVCompressor * compressor, size_t produced);


/*
 * InternalCSVCompression returns the compression method to apply to temporary
 * CSV files.  Both the writer and the read_csv() clause that reads the file
 * back call this, and a temporary CSV never outlives the statement that wrote
 * it, so the two always agree.
 */
CopyDataCompression
InternalCSVCompression(void)
{
	return (CopyDataCompression) TempFileCompression;
}


/*
 * CSVCompressionLevelRange reports the levels a codec accepts.
 */
void
CSVCompressionLevelRange(CopyDataCompression method, int *minLevel, int *maxLevel)
{
	switch (method)
	{
		case DATA_COMPRESSION_GZIP:
			*minLevel = Z_BEST_SPEED;
			*maxLevel = Z_BEST_COMPRESSION;
			break;

		case DATA_COMPRESSION_ZSTD:

			/*
			 * Below zero is zstd's "fast" mode, which gives up ratio for a
			 * good deal of speed.  ZSTD_minCLevel() names a floor far below
			 * anything useful on a file that lives for one statement, so the
			 * setting's own bound stands in for it.
			 */
			*minLevel = PG_LAKE_MIN_TEMP_FILE_COMPRESSION_LEVEL;
			*maxLevel = Min(ZSTD_maxCLevel(),
							PG_LAKE_MAX_TEMP_FILE_COMPRESSION_LEVEL);
			break;

		default:
			*minLevel = 0;
			*maxLevel = 0;
			break;
	}
}


/*
 * CSVCompressorCreate starts a compressed stream on top of an already-open
 * file.  The caller keeps ownership of the file and must not write to it
 * directly while the compressor is alive.
 */
CSVCompressor *
CSVCompressorCreate(FILE *file, CopyDataCompression method, int level)
{
	int			minLevel;
	int			maxLevel;

	if (method != DATA_COMPRESSION_GZIP && method != DATA_COMPRESSION_ZSTD)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported temporary file compression \"%s\"",
						CopyDataCompressionToName(method))));

	CSVCompressor *compressor = palloc0(sizeof(CSVCompressor));

	compressor->method = method;
	compressor->file = file;

	/*
	 * One setting covers both codecs, and they disagree about how far a level
	 * goes, so a level meant for the other one is clamped rather than
	 * rejected: all it can change is how the codec spends CPU, and refusing
	 * to write the CSV over it would be a poor trade.
	 */
	CSVCompressionLevelRange(method, &minLevel, &maxLevel);
	level = Min(Max(level, minLevel), maxLevel);

	if (method == DATA_COMPRESSION_GZIP)
		GzipCompressorCreate(compressor, level);
	else
		ZstdCompressorCreate(compressor, level);

	return compressor;
}


/*
 * CSVCompressorWrite compresses a buffer into the underlying file.
 */
void
CSVCompressorWrite(CSVCompressor * compressor, const void *data, size_t size)
{
	if (size == 0)
	{
		/* deflate() reports Z_BUF_ERROR when there is nothing to do */
		return;
	}

	if (compressor->method == DATA_COMPRESSION_GZIP)
		GzipCompressorWrite(compressor, data, size);
	else
		ZstdCompressorWrite(compressor, data, size);
}


/*
 * CSVCompressorFinish drains the codec's buffers and writes whatever closes
 * out its container.  It must be called before the underlying file is closed,
 * otherwise the file is a truncated stream that no reader will accept.
 */
void
CSVCompressorFinish(CSVCompressor * compressor)
{
	if (compressor->method == DATA_COMPRESSION_GZIP)
		GzipCompressorFinish(compressor);
	else
		ZstdCompressorFinish(compressor);
}


/*
 * GzipCompressorCreate starts a gzip stream.
 */
static void
GzipCompressorCreate(CSVCompressor * compressor, int level)
{
	/*
	 * Route zlib's allocations into a context of our own so that an error
	 * anywhere in the COPY -- which skips deflateEnd() -- still releases the
	 * deflate state when the surrounding context goes away.
	 */
	compressor->context = AllocSetContextCreate(CurrentMemoryContext,
												"pg_lake CSV compressor",
												ALLOCSET_SMALL_SIZES);

	compressor->stream.zalloc = CompressorAlloc;
	compressor->stream.zfree = CompressorFree;
	compressor->stream.opaque = compressor->context;

	int			returnCode = deflateInit2(&compressor->stream, level, Z_DEFLATED,
										  GZIP_WINDOW_BITS, GZIP_MEM_LEVEL,
										  Z_DEFAULT_STRATEGY);

	if (returnCode != Z_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not initialize gzip compression: %s",
						compressor->stream.msg ? compressor->stream.msg : "unknown error")));
}


/*
 * GzipCompressorWrite deflates a buffer into the underlying file.
 */
static void
GzipCompressorWrite(CSVCompressor * compressor, const void *data, size_t size)
{
	z_stream   *stream = &compressor->stream;

	stream->next_in = (Bytef *) data;
	stream->avail_in = size;

	/*
	 * Keep calling deflate() as long as it fills the buffer completely, which
	 * is the only way to know it has more to give.  A single row wider than
	 * the buffer goes around more than once.
	 */
	do
	{
		stream->next_out = compressor->buffer;
		stream->avail_out = sizeof(compressor->buffer);

		int			returnCode = deflate(stream, Z_NO_FLUSH);

		if (returnCode != Z_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not compress COPY file: %s",
							stream->msg ? stream->msg : "unknown error")));

		WriteCompressorBuffer(compressor,
							  sizeof(compressor->buffer) - stream->avail_out);
	} while (stream->avail_out == 0);

	Assert(stream->avail_in == 0);
}


/*
 * GzipCompressorFinish drains zlib's internal buffers and writes the gzip
 * trailer.
 */
static void
GzipCompressorFinish(CSVCompressor * compressor)
{
	z_stream   *stream = &compressor->stream;
	int			returnCode;

	stream->next_in = NULL;
	stream->avail_in = 0;

	do
	{
		stream->next_out = compressor->buffer;
		stream->avail_out = sizeof(compressor->buffer);

		returnCode = deflate(stream, Z_FINISH);

		if (returnCode != Z_OK && returnCode != Z_STREAM_END)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not finish compressing COPY file: %s",
							stream->msg ? stream->msg : "unknown error")));

		WriteCompressorBuffer(compressor,
							  sizeof(compressor->buffer) - stream->avail_out);
	} while (returnCode != Z_STREAM_END);

	/* deflateEnd frees through the context, so it has to go first */
	deflateEnd(stream);
	MemoryContextDelete(compressor->context);
	compressor->context = NULL;
}


/*
 * ZstdCompressorCreate starts a zstd stream.
 */
static void
ZstdCompressorCreate(CSVCompressor * compressor, int level)
{
	compressor->zstd = ZSTD_createCStream();

	if (compressor->zstd == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not initialize zstd compression")));

	/*
	 * zstd's stable API takes no allocator, so the compression state -- tens
	 * of megabytes at the higher levels -- sits outside any memory context,
	 * and an error in the middle of the COPY, which skips
	 * CSVCompressorFinish(), would strand it.  A reset callback on the
	 * current context frees it on the way out instead; the callback and the
	 * normal path keep out of each other's way through compressor->zstd.
	 */
	MemoryContextCallback *callback = palloc0(sizeof(MemoryContextCallback));

	callback->func = ReleaseZstdStream;
	callback->arg = compressor;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, callback);

	size_t		returnCode = ZSTD_initCStream(compressor->zstd, level);

	if (ZSTD_isError(returnCode))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not initialize zstd compression: %s",
						ZSTD_getErrorName(returnCode))));
}


/*
 * ZstdCompressorWrite compresses a buffer into the underlying file.
 */
static void
ZstdCompressorWrite(CSVCompressor * compressor, const void *data, size_t size)
{
	ZSTD_inBuffer input = {.src = data,.size = size,.pos = 0};

	/*
	 * Unlike deflate(), zstd reports how much of the input it consumed, so
	 * the loop can run until the input is drained rather than until the
	 * output buffer comes back full.
	 */
	while (input.pos < input.size)
	{
		ZSTD_outBuffer output = {
			.dst = compressor->buffer,
			.size = sizeof(compressor->buffer),
			.pos = 0
		};

		size_t		returnCode = ZSTD_compressStream2(compressor->zstd, &output,
													  &input, ZSTD_e_continue);

		if (ZSTD_isError(returnCode))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not compress COPY file: %s",
							ZSTD_getErrorName(returnCode))));

		WriteCompressorBuffer(compressor, output.pos);
	}
}


/*
 * ZstdCompressorFinish flushes zstd's block buffer and closes the frame.
 */
static void
ZstdCompressorFinish(CSVCompressor * compressor)
{
	ZSTD_inBuffer input = {.src = NULL,.size = 0,.pos = 0};
	size_t		remaining;

	do
	{
		ZSTD_outBuffer output = {
			.dst = compressor->buffer,
			.size = sizeof(compressor->buffer),
			.pos = 0
		};

		/* on success the return value is what is left of the frame */
		remaining = ZSTD_compressStream2(compressor->zstd, &output, &input,
										 ZSTD_e_end);

		if (ZSTD_isError(remaining))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not finish compressing COPY file: %s",
							ZSTD_getErrorName(remaining))));

		WriteCompressorBuffer(compressor, output.pos);
	} while (remaining > 0);

	ZSTD_freeCStream(compressor->zstd);
	compressor->zstd = NULL;
}


/*
 * ReleaseZstdStream frees a zstd stream that CSVCompressorFinish() never got
 * to, which is the case whenever the COPY raised an error.
 */
static void
ReleaseZstdStream(void *arg)
{
	CSVCompressor *compressor = (CSVCompressor *) arg;

	if (compressor->zstd != NULL)
	{
		ZSTD_freeCStream(compressor->zstd);
		compressor->zstd = NULL;
	}
}


/*
 * CompressorAlloc and CompressorFree let zlib allocate out of a PostgreSQL
 * memory context.
 */
static void *
CompressorAlloc(void *opaque, unsigned int items, unsigned int size)
{
	return MemoryContextAlloc((MemoryContext) opaque, (Size) items * size);
}


static void
CompressorFree(void *opaque, void *address)
{
	pfree(address);
}


/*
 * WriteCompressorBuffer writes whatever the codec just produced.
 */
static void
WriteCompressorBuffer(CSVCompressor * compressor, size_t produced)
{
	if (produced == 0)
		return;

	if (fwrite(compressor->buffer, produced, 1, compressor->file) != 1 ||
		ferror(compressor->file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to COPY file: %m")));
}
