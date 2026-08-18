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
 */
#include <zlib.h>

#include "postgres.h"

#include "pg_lake/csv/csv_compression.h"
#include "utils/memutils.h"

/* pg_lake_engine.temp_file_compression setting */
int			TempFileCompression = DATA_COMPRESSION_NONE;

/* pg_lake_engine.temp_file_compression_level setting */
int			TempFileCompressionLevel = 1;

/*
 * Staging buffer for deflate() output.  Only needs to be large enough to keep
 * the write count down; deflate is called repeatedly until it stops filling
 * the buffer.
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
	FILE	   *file;
	z_stream	stream;

	/* holds all of zlib's allocations, so an error can't leak them */
	MemoryContext context;

	unsigned char buffer[COMPRESS_BUFFER_SIZE];
};

static void *CompressorAlloc(void *opaque, unsigned int items, unsigned int size);
static void CompressorFree(void *opaque, void *address);
static void FlushCompressorBuffer(CSVCompressor * compressor);


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
 * CSVCompressorCreate starts a compressed stream on top of an already-open
 * file.  The caller keeps ownership of the file and must not write to it
 * directly while the compressor is alive.
 */
CSVCompressor *
CSVCompressorCreate(FILE *file, CopyDataCompression method, int level)
{
	if (method != DATA_COMPRESSION_GZIP)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported temporary file compression \"%s\"",
						CopyDataCompressionToName(method))));

	CSVCompressor *compressor = palloc0(sizeof(CSVCompressor));

	compressor->file = file;

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

	return compressor;
}


/*
 * CSVCompressorWrite compresses a buffer into the underlying file.
 */
void
CSVCompressorWrite(CSVCompressor * compressor, const void *data, size_t size)
{
	z_stream   *stream = &compressor->stream;

	if (size == 0)
	{
		/* deflate() reports Z_BUF_ERROR when there is nothing to do */
		return;
	}

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

		FlushCompressorBuffer(compressor);
	} while (stream->avail_out == 0);

	Assert(stream->avail_in == 0);
}


/*
 * CSVCompressorFinish drains zlib's internal buffers and writes the gzip
 * trailer.  It must be called before the underlying file is closed, otherwise
 * the file is a truncated gzip stream that no reader will accept.
 */
void
CSVCompressorFinish(CSVCompressor * compressor)
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

		FlushCompressorBuffer(compressor);
	} while (returnCode != Z_STREAM_END);

	/* deflateEnd frees through the context, so it has to go first */
	deflateEnd(stream);
	MemoryContextDelete(compressor->context);
	compressor->context = NULL;
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
 * FlushCompressorBuffer writes whatever deflate() just produced.
 */
static void
FlushCompressorBuffer(CSVCompressor * compressor)
{
	size_t		produced = sizeof(compressor->buffer) - compressor->stream.avail_out;

	if (produced == 0)
		return;

	if (fwrite(compressor->buffer, produced, 1, compressor->file) != 1 ||
		ferror(compressor->file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to COPY file: %m")));
}
