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
 * csv_writer.c
 *
 * Derived from copyto.c in PostgreSQL because the CopyToStateData is private.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include <sys/stat.h>

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_type_d.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/util/numeric.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "port/pg_bswap.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"


/*
 * Represents the different dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to file */
	/* other options removed, since we only supported COPY_FILE */
} CopyDest;

/*
 * This struct contains all the state variables used throughout a COPY TO
 * operation.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is true
 * when we have to do it the hard way.
 */
typedef struct CopyToStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO */

	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDOUT */
	bool		is_program;
	copy_data_dest_cb data_dest_cb; /* function for writing data */

	CopyFormatOptions opts;
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */
	uint64		bytes_processed;	/* number of bytes processed so far */

	int			maxLineSize;
	bool		quoteEmptyLines;
	CopyDataFormat targetFormat;

	/* whether the CSV writer should survive multiple transactions */
	bool		sessionLifetime;
} CopyToStateData;

/* DestReceiver for COPY (query) TO */
typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	CopyToState cstate;			/* CopyToStateData for the command */
	uint64		processed;		/* # of tuples processed */
} DR_copy;

/* NOTE: there's a copy of this in copyfromparse.c */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/* non-export function prototypes */
static void EndCopy(CopyToState cstate);
static void CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot);
static void CopyAttributeOutText(CopyToState cstate, const char *string);
static void CopyAttributeOutCSV(CopyToState cstate, const char *string,
								bool use_quote, bool single_attr);

/* Low-level communications functions */
static void CopySendData(CopyToState cstate, const void *databuf, int datasize);
static void CopySendString(CopyToState cstate, const char *str);
static void CopySendChar(CopyToState cstate, char c);
static void CopySendEndOfRow(CopyToState cstate);
static void CopySendInt32(CopyToState cstate, int32 val);
static void CopySendInt16(CopyToState cstate, int16 val);

static List *TupleDescColumnNameList(TupleDesc tupleDescriptor);
static List *CopyGetAttnumsFixed(TupleDesc tupDesc, List *attnamelist);
static bool ShouldUseDuckSerialization(CopyDataFormat targetFormat, PGType postgresType);
static void ErrorIfCopyToExceedsUnboundedNumericMaxAllowedDigits(const char *numericStr);
static void ErrorIfSpecialNumeric(const char *input_str);


/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(CopyToState cstate, const void *databuf, int datasize)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, databuf, datasize);
}

static void
CopySendString(CopyToState cstate, const char *str)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

static void
CopySendChar(CopyToState cstate, char c)
{
	appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

static void
CopySendEndOfRow(CopyToState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	if (cstate->quoteEmptyLines && fe_msgbuf->len == 0 &&
		list_length(cstate->attnumlist) != 0)
	{
		/*
		 * DuckDB prefers empty single column lines to be quoted (probably a
		 * bug)
		 */
		appendStringInfoString(cstate->fe_msgbuf, "\"\"");
	}

	if (fe_msgbuf->len > cstate->maxLineSize)
	{
		cstate->maxLineSize = fe_msgbuf->len;
	}

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			if (!cstate->opts.binary)
			{
				/* Default line termination depends on platform */
#ifndef WIN32
				CopySendChar(cstate, '\n');
#else
				CopySendString(cstate, "\r\n");
#endif
			}

			if (fwrite(fe_msgbuf->data, fe_msgbuf->len, 1,
					   cstate->copy_file) != 1 ||
				ferror(cstate->copy_file))
			{
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY file: %m")));
			}
			break;
		default:
			/* we only use COPY to emit files */
			Assert(false);
			break;
	}

	/* Update the progress */
	cstate->bytes_processed += fe_msgbuf->len;

	resetStringInfo(fe_msgbuf);
}

/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static inline void
CopySendInt32(CopyToState cstate, int32 val)
{
	uint32		buf;

	buf = pg_hton32((uint32) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static inline void
CopySendInt16(CopyToState cstate, int16 val)
{
	uint16		buf;

	buf = pg_hton16((uint16) val);
	CopySendData(cstate, &buf, sizeof(buf));
}


/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void
EndCopy(CopyToState cstate)
{
	if (cstate->opts.binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);
		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	if (cstate->filename != NULL)
	{
		if (cstate->sessionLifetime)
		{
			fclose(cstate->copy_file);
		}
		else
		{
			if (FreeFile(cstate->copy_file))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not close file \"%s\": %m",
								cstate->filename)));
		}
	}

	MemoryContextDelete(cstate->rowcontext);
	MemoryContextDelete(cstate->copycontext);
}

/*
 * CreateCopyToState creates the CopyToState.
 *
 * It is derived from BeginCopyTo and DoCopy.
 */
static CopyToState
CreateCopyToState(char *filename, List *copyOptions)
{
	/*
	 * Prevent write to relative path ... too easy to shoot oneself in the
	 * foot by overwriting a database file ...
	 */
	if (!is_absolute_path(filename))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("relative path not allowed for COPY to file")));

	/* Allocate workspace and zero all fields */
	CopyToState cstate = (CopyToStateData *) palloc0(sizeof(CopyToStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	MemoryContext oldContext = MemoryContextSwitchTo(cstate->copycontext);

	/* Extract options from the statement node tree */
	ParseState *pstate = NULL;

	ProcessCopyOptions(pstate, &cstate->opts, false /* is_from */ ,
					   copyOptions);

	cstate->opts.null_print_client = cstate->opts.null_print;	/* default */

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->opts.file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();
	else
		cstate->file_encoding = cstate->opts.file_encoding;

	/*
	 * Set up encoding conversion info.  Even if the file and server encodings
	 * are the same, we must apply pg_any_to_server() to validate data in
	 * multibyte encodings.
	 */
	cstate->need_transcoding =
		(cstate->file_encoding != GetDatabaseEncoding() ||
		 pg_database_encoding_max_length() > 1);
	/* See Multibyte encoding comment above */
	cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);

	/* We use fe_msgbuf as a per-row buffer */
	cstate->fe_msgbuf = makeStringInfo();

	cstate->copy_dest = COPY_FILE;	/* default */
	cstate->filename = pstrdup(filename);

	cstate->maxLineSize = 0;
	cstate->bytes_processed = 0;

	MemoryContextSwitchTo(oldContext);

	return cstate;
}

/*
 * Setup CopyToState to write to the given file.
 */
static void
StartCopyTo(CopyToState cstate, TupleDesc tupDesc)
{
	ListCell   *cur;

	MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* create the file */
	mode_t		oumask = umask(S_IWGRP | S_IWOTH);

	PG_TRY();
	{
		if (cstate->sessionLifetime)
			cstate->copy_file = fopen(cstate->filename, "w");
		else
			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
	}
	PG_FINALLY();
	{
		umask(oumask);
	}
	PG_END_TRY();

	if (cstate->copy_file == NULL)
	{
		/* copy errno because ereport subfunctions might change it */
		int			save_errno = errno;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for writing: %m",
						cstate->filename),
				 (save_errno == ENOENT || save_errno == EACCES) ?
				 errhint("COPY TO instructs the PostgreSQL server process to write a file. "
						 "You may want a client-side facility such as psql's \\copy.") : 0));
	}

	struct stat st;

	if (fstat(fileno(cstate->copy_file), &st))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						cstate->filename)));

	if (S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a directory", cstate->filename)));


	/* get the attribute names from the tuple descriptor */
	List	   *attnamelist = TupleDescColumnNameList(tupDesc);

	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnumsFixed(tupDesc, attnamelist);

	int			num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_QUOTE name list to per-column flags, check validity */
	cstate->opts.force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_quote_all)
	{
		int			i;

		for (i = 0; i < num_phys_attrs; i++)
			cstate->opts.force_quote_flags[i] = true;
	}
	else if (cstate->opts.force_quote)
	{
		List	   *attnums;

		attnums = CopyGetAttnumsFixed(tupDesc, cstate->opts.force_quote);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_QUOTE column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_quote_flags[attnum - 1] = true;
		}
	}

	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		if (cstate->opts.binary)
			getTypeBinaryOutputInfo(attr->atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr->atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_SIZES);

	if (cstate->opts.binary)
	{
		/* Generate header for a binary copy */
		int32		tmp;

		/* Signature */
		CopySendData(cstate, BinarySignature, 11);
		/* Flags field */
		tmp = 0;
		CopySendInt32(cstate, tmp);
		/* No header extension */
		tmp = 0;
		CopySendInt32(cstate, tmp);
	}
	else
	{
		/*
		 * For non-binary copy, we need to convert null_print to file
		 * encoding, because it will be sent directly with CopySendString.
		 */
		if (cstate->need_transcoding)
			cstate->opts.null_print_client = pg_server_to_any(cstate->opts.null_print,
															  cstate->opts.null_print_len,
															  cstate->file_encoding);

		/* if a header has been requested send the line */
		if (cstate->opts.header_line)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->opts.delim[0]);
				hdr_delim = true;

				colname = NameStr(TupleDescAttr(tupDesc, attnum - 1)->attname);

				if (cstate->opts.csv_mode)
					CopyAttributeOutCSV(cstate, colname, false,
										list_length(cstate->attnumlist) == 1);
				else
					CopyAttributeOutText(cstate, colname);
			}

			CopySendEndOfRow(cstate);
		}
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * TupleDescColumnNameList returns a list of column names for a TupleDesc.
 */
static List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	int			columnCount = tupleDescriptor->natts;
	List	   *columnNameList = NIL;

	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDescriptor, columnIndex);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);

		columnNameList = lappend(columnNameList, makeString(columnName));
	}

	return columnNameList;
}


/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 *
 * Copied from copy.c, but fixed a generated columns bug.
 */
static List *
CopyGetAttnumsFixed(TupleDesc tupDesc, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			if (TupleDescAttr(tupDesc, i)->attgenerated)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					/*
					 * note: we removed the generated column check from here,
					 * which PostgreSQL incorrectly applies when writing CSV.
					 */
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" does not exist",
								name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

/*
 * ErrorIfCopyToExceedsUnboundedNumericMaxAllowedDigits ensures the digits for
 * both precision and scale of the numeric, in string, does not exceed the max
 * allowed digits during COPY TO.
 */
static void
ErrorIfCopyToExceedsUnboundedNumericMaxAllowedDigits(const char *numericStr)
{
	int			totalIntegralDigits = 0;
	int			totalDecimalDigits = 0;
	bool		foundDotSeparator = false;

	for (int i = 0; numericStr[i] != '\0'; i++)
	{
		if (numericStr[i] == '.')
		{
			foundDotSeparator = true;
			continue;
		}

		if (!isdigit(numericStr[i]))
			continue;

		if (foundDotSeparator)
			totalDecimalDigits++;
		else
			totalIntegralDigits++;

		if (totalIntegralDigits > (UnboundedNumericDefaultPrecision - UnboundedNumericDefaultScale))
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("unbounded numeric type exceeds max allowed digits %d "
								   "before decimal point",
								   UnboundedNumericDefaultPrecision - UnboundedNumericDefaultScale),
							errhint("Consider specifying precision and scale for numeric types, "
									"i.e. \"numeric(P,S)\" instead of \"numeric\".")));
		}

		if (totalDecimalDigits > UnboundedNumericDefaultScale)
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("unbounded numeric type exceeds max allowed digits %d "
								   "after decimal point", UnboundedNumericDefaultScale),
							errhint("Consider specifying precision and scale for numeric types, "
									"i.e. \"numeric(P,S)\" instead of \"numeric\".")));
		}
	}
}


/*
* ErrorIfSpecialNumeric ensures that the input string does not contain
* special numeric values like NaN, Inf, -Inf.
*/
static void
ErrorIfSpecialNumeric(const char *input_str)
{

	char	   *num = (char *) input_str;

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	if (pg_strncasecmp(num, "NaN", 3) == 0 ||
		pg_strncasecmp(num, "Infinity", 8) == 0 ||
		pg_strncasecmp(num, "+Infinity", 9) == 0 ||
		pg_strncasecmp(num, "-Infinity", 9) == 0 ||
		pg_strncasecmp(num, "inf", 3) == 0 ||
		pg_strncasecmp(num, "+inf", 4) == 0 ||
		pg_strncasecmp(num, "-inf", 4) == 0)
	{
		ereport(ERROR, errmsg("Special numeric values like NaN, Inf, -Inf are not allowed for numeric type"),
				errhint("Use float type instead."));
	}
}


/*
 * Emit one row
 */
static void
CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot)
{
	bool		need_delim = false;
	FmgrInfo   *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	char	   *string;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	if (cstate->opts.binary)
	{
		/* Binary per-tuple header */
		CopySendInt16(cstate, list_length(cstate->attnumlist));
	}

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = slot->tts_values[attnum - 1];
		bool		isnull = slot->tts_isnull[attnum - 1];

		if (!cstate->opts.binary)
		{
			if (need_delim)
				CopySendChar(cstate, cstate->opts.delim[0]);
			need_delim = true;
		}

		if (isnull)
		{
			if (!cstate->opts.binary)
				CopySendString(cstate, cstate->opts.null_print_client);
			else
				CopySendInt32(cstate, -1);
		}
		else
		{
			if (!cstate->opts.binary)
			{
				/*
				 * Lookup the underlying tuple's attribute so we can pass in
				 * the typid
				 */
				Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1);

				if (ShouldUseDuckSerialization(cstate->targetFormat, MakePGType(attr->atttypid, attr->atttypmod)))
				{
					/*
					 * Since we are at the top-level when emitting an
					 * attribute in CopyOneRowTo(), we are not inside a
					 * composite type.
					 */

					string = PGDuckSerialize(&out_functions[attnum - 1],
											 attr->atttypid,
											 value,
											 cstate->targetFormat);
				}
				else
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);

				if (attr->atttypid == NUMERICOID)
				{
					if (IsUnboundedNumeric(NUMERICOID, attr->atttypmod))
						ErrorIfCopyToExceedsUnboundedNumericMaxAllowedDigits(string);

					/* do not allow Nan, Inf etc. */
					ErrorIfSpecialNumeric(string);
				}


				if (cstate->opts.csv_mode)
					CopyAttributeOutCSV(cstate, string,
										cstate->opts.force_quote_flags[attnum - 1],
										list_length(cstate->attnumlist) == 1);
				else
					CopyAttributeOutText(cstate, string);
			}
			else
			{
				bytea	   *outputbytes;

				outputbytes = SendFunctionCall(&out_functions[attnum - 1],
											   value);
				CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(cstate, VARDATA(outputbytes),
							 VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
	}

	CopySendEndOfRow(cstate);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

static void
CopyAttributeOutText(CopyToState cstate, const char *string)
{
	const char *ptr;
	const char *start;
	char		c;
	char		delimc = cstate->opts.delim[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				start = ptr++;	/* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				start = ptr++;	/* we include char in next run */
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(CopyToState cstate, const char *string,
					bool use_quote, bool single_attr)
{
	const char *ptr;
	const char *start;
	char		c;
	char		delimc = cstate->opts.delim[0];
	char		quotec = cstate->opts.quote[0];
	char		escapec = cstate->opts.escape[0];

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->opts.null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			const char *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		CopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr;	/* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
		DUMPSOFAR();

		CopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		CopySendString(cstate, ptr);
	}
}

/*
 * copy_dest_startup --- executor startup
 */
static void
copy_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyToState cstate = myState->cstate;

	StartCopyTo(cstate, typeinfo);
}

/*
 * copy_dest_receive --- receive one tuple
 */
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyToState cstate = myState->cstate;

	/* Send the data */
	CopyOneRowTo(cstate, slot);

	return true;
}

/*
 * copy_dest_shutdown --- executor end
 */
static void
copy_dest_shutdown(DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyToState cstate = myState->cstate;

	EndCopy(cstate);
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void
copy_dest_destroy(DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;

	pfree(myState->cstate);
	pfree(self);
}

/*
 * CreateCSVDestReceiver creates a DestReceiver that writes to a CSV file.
 *
 * The targetFormat parameter is the final output format of the intended
 * DestReceiver.  This is important because the final serialization format of
 * some types (arrays, bytea, records) will depend on what the target format is;
 * Parquet supports composite types in DuckDB format differently than CSV, say.
 *
 * Since all targetFormat types currently use CSV as an exchange format, the
 * targetFormat should be considered only for the /final/ format, not the CSV
 * format of the intermediate file.
 */
DestReceiver *
CreateCSVDestReceiver(char *filename, List *copyOptions,
					  CopyDataFormat targetFormat)
{
	DR_copy    *self = (DR_copy *) palloc0(sizeof(DR_copy));

	self->pub.receiveSlot = copy_dest_receive;
	self->pub.rStartup = copy_dest_startup;
	self->pub.rShutdown = copy_dest_shutdown;
	self->pub.rDestroy = copy_dest_destroy;
	self->pub.mydest = DestCopyOut;

	self->processed = 0;
	self->cstate = CreateCopyToState(filename, copyOptions);

	/*
	 * We currently always quote empty lines because the DuckDB CSV parser
	 * prefers it. If we start using the DestReceiver for writing
	 * PG-compatible CSV, we may need to make this changeable.
	 */
	self->cstate->quoteEmptyLines = true;
	self->cstate->targetFormat = targetFormat;

	return (DestReceiver *) self;
}


/*
 * CreateCSVDestReceiverExtended adds additional arguments to
 * CreateCSVDestReceiver for specialized scenarios.
 */
DestReceiver *
CreateCSVDestReceiverExtended(char *filename, List *copyOptions,
							  CopyDataFormat targetFormat,
							  bool sessionLifetime)
{
	DR_copy    *self =
		(DR_copy *) CreateCSVDestReceiver(filename, copyOptions, targetFormat);

	self->cstate->sessionLifetime = sessionLifetime;

	return (DestReceiver *) self;
}


/*
 * GetCSVDestReceiverMaxLineSize returns the maximum line size observed
 * by the CSV writer.
 */
int
GetCSVDestReceiverMaxLineSize(DestReceiver *dest)
{
	DR_copy    *myState = (DR_copy *) dest;
	CopyToState cstate = myState->cstate;

	return cstate->maxLineSize;
}



/*
 * GetCSVDestReceiverFileSize returns the number of bytes processed
 * by the CSV writer so far.
 */
uint64
GetCSVDestReceiverFileSize(DestReceiver *dest)
{
	DR_copy    *myState = (DR_copy *) dest;
	CopyToState cstate = myState->cstate;

	return cstate->bytes_processed;
}


/*
 * ShouldUseDuckSerialization determines whether we should adjust the
 * serialization format to match DuckDB for a given column, such that
 * we can convert it to a corresponding DuckDB engine type when converting
 * the CSV to the target format.
 *
 * In some cases (e.g. writing CSV, writing blobs to JSON)), we prefer to
 * preserve the PostgreSQL serialization format, and therefore return false.
 *
 * NOTE: This function should stay in sync with ChooseDuckDBEngineTypeForWrite
 * where we decide how to parse the values emitted by the CSV writer.
 */
bool
ShouldUseDuckSerialization(CopyDataFormat targetFormat, PGType postgresType)
{
	Oid			typeId = postgresType.postgresTypeOid;

	if (IsGeometryType(postgresType))
	{
		/*
		 * We prefer to always pass geometry in WKT format, since DuckDB has
		 * some limitations around parsing Hex WKB (e.g. read_csv does not
		 * accept GEOMETRY type in that case).
		 */
		return true;
	}

	switch (targetFormat)
	{
			/*
			 * For JSON, we can convert nested types to JSON, but we prefer to
			 * preserve BYTEA in PostgreSQL serialization format.
			 */
		case DATA_FORMAT_JSON:
			return typeId != BYTEAOID;

			/*
			 * For Parquet, we convert all relevant types to DuckDB
			 * serialization format, since they can then be converted to the
			 * corresponding Parquet data types.
			 */
		case DATA_FORMAT_ICEBERG:
		case DATA_FORMAT_PARQUET:
			return true;

			/*
			 * For CSV, we prefer to write PostgreSQL serialization format.
			 */
		case DATA_FORMAT_CSV:
		default:
			return false;
	}
}
