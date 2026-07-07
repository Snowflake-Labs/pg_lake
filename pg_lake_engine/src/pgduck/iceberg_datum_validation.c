/*
 * Copyright 2026 Snowflake Inc.
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
 * C-level Iceberg write-time datum validation.
 *
 * Validates individual Datum values against Iceberg constraints on
 * the PostgreSQL side (non-pushdown path).  Called from
 * IcebergErrorOrClampSlotInPlace during FDW inserts and from partition
 * transform code (to keep partition keys consistent with clamped data).
 *
 * Handles:
 *   - Temporal boundaries (date/timestamp/timestamptz): values outside
 *     the Iceberg range are clamped or rejected.
 *   - Bounded numeric NaN: clamped to NULL or rejected.
 *   - Multidimensional arrays: clamped to NULL or rejected, since
 *     PostgreSQL's single array type (e.g. int[]) maps to a flat
 *     LIST(T) in DuckDB/Iceberg.
 *
 * Validation recurses through arrays, composites, maps, and domains.
 *
 * Temporal boundaries:
 *   - Date: proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 *   - Timestamp/TimestampTZ: 0001-01-01 .. 9999-12-31 23:59:59.999999.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/detoast.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "datatype/timestamp.h"
#include "mb/pg_wchar.h"
#include "pg_lake/pgduck/iceberg_datum_validation.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/temporal_utils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"


static Datum ErrorOrClampTemporal(Datum value, Oid typeOid, int year,
								  IcebergOutOfRangePolicy policy);
static Datum IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid,
											  IcebergOutOfRangePolicy policy);
static Datum IcebergErrorOrClampNumericDatum(Datum value, int32 typmod,
											 IcebergOutOfRangePolicy policy,
											 bool *isNull);
static Datum IcebergErrorOrClampMultiDimArrayDatum(ArrayType *array,
												   IcebergOutOfRangePolicy policy,
												   bool *isNull);
static Datum IcebergErrorOrClampNestedDatum(Datum value, Oid typeOid,
											int32 typmod,
											IcebergOutOfRangePolicy policy,
											bool *isNull, bool *modified);
static Datum IcebergSizeClampStringScalar(Datum value, Oid typeOid,
										  IcebergOutOfRangePolicy policy,
										  const char *columnName,
										  bool *isNull);
static Datum IcebergSizeClampBinaryScalar(Datum value,
										  IcebergOutOfRangePolicy policy,
										  const char *columnName);
static Datum IcebergSizeClampNestedDatum(Datum value, Oid typeOid,
										 int32 typmod,
										 IcebergOutOfRangePolicy policy,
										 const char *columnName,
										 bool *isNull, bool *modified);
static void RaiseSizeOverflow(const char *columnName, const char *typeLabel,
							  int64 size, int64 limit, const char *limitLabel);
static bool TypeContainsJsonbLeaf(Oid typeOid);
static int64 ContainerByteSizeViaOutputFunc(Datum value, Oid typeOid);


/*
 * TypeContainsJsonbLeaf returns true if `typeOid` contains a jsonb or
 * json leaf at any nesting depth (top-level, inside an array element,
 * inside a composite field, inside a domain over either, inside a map
 * value).  Used to decide whether the container aggregate-size check
 * needs the JSON-text measurement (slow but correct) instead of the
 * varlena binary size (fast but under-counts jsonb).
 *
 * Memoized in a process-local hashtable keyed by typeOid; results stay
 * stable for the life of a type and are flushed on TYPEOID/RELOID
 * syscache invalidation, so DDL on a composite type re-walks its
 * fields on the next call.
 */
typedef struct
{
	Oid			typeOid;
	bool		containsJsonbLeaf;
}			TypeContainsJsonbLeafEntry;

static HTAB *TypeContainsJsonbLeafCache = NULL;

static void
TypeContainsJsonbLeafCacheInval(Datum arg, int cacheid, uint32 hashvalue)
{
	if (TypeContainsJsonbLeafCache != NULL)
	{
		hash_destroy(TypeContainsJsonbLeafCache);
		TypeContainsJsonbLeafCache = NULL;
	}
}

static bool
TypeContainsJsonbLeafUncached(Oid typeOid)
{
	if (typeOid == JSONBOID || typeOid == JSONOID)
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return TypeContainsJsonbLeaf(elemType);

	if (IsMapTypeOid(typeOid))
	{
		/*
		 * Map is a domain over array of (key, value) composite.  The base
		 * type is the array; recursing into its element walks key+value.
		 */
		Oid			baseType = getBaseType(typeOid);

		return TypeContainsJsonbLeaf(baseType);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
		return TypeContainsJsonbLeaf(getBaseType(typeOid));

	if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;
			if (TypeContainsJsonbLeaf(attr->atttypid))
			{
				ReleaseTupleDesc(tupdesc);
				return true;
			}
		}
		ReleaseTupleDesc(tupdesc);
	}

	return false;
}

static bool
TypeContainsJsonbLeaf(Oid typeOid)
{
	bool		found;
	TypeContainsJsonbLeafEntry *entry;

	if (TypeContainsJsonbLeafCache == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(TypeContainsJsonbLeafEntry);
		ctl.hcxt = CacheMemoryContext;

		TypeContainsJsonbLeafCache =
			hash_create("Iceberg TypeContainsJsonbLeaf cache", 64, &ctl,
						HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		CacheRegisterSyscacheCallback(TYPEOID,
									  TypeContainsJsonbLeafCacheInval,
									  (Datum) 0);
		CacheRegisterSyscacheCallback(RELOID,
									  TypeContainsJsonbLeafCacheInval,
									  (Datum) 0);
	}

	entry = hash_search(TypeContainsJsonbLeafCache, &typeOid,
						HASH_ENTER, &found);

	if (!found)
		entry->containsJsonbLeaf = TypeContainsJsonbLeafUncached(typeOid);

	return entry->containsJsonbLeaf;
}


/*
 * ContainerByteSizeViaOutputFunc invokes the type's output function and
 * returns the resulting text length.  For containers with jsonb leaves
 * this renders the jsonb leaves through jsonb_out (their canonical JSON
 * text form), which is what the downstream consumer ultimately sees in
 * an OBJECT/ARRAY/VARIANT column — and which the binary varlena size
 * can under-count by 1.5-2x for jsonb-heavy values.
 *
 * This is the slower path; callers should reserve it for types whose
 * leaves include jsonb/json (TypeContainsJsonbLeaf), and stick with
 * toast_raw_datum_size otherwise.
 */
static int64
ContainerByteSizeViaOutputFunc(Datum value, Oid typeOid)
{
	Oid			typoutput;
	bool		typIsVarlena;

	getTypeOutputInfo(typeOid, &typoutput, &typIsVarlena);

	char	   *txt = OidOutputFunctionCall(typoutput, value);
	int64		len = (int64) strlen(txt);

	pfree(txt);
	return len;
}


/*
 * RaiseSizeOverflow ereports a uniform "value too long" error for size-clamp
 * violations under ICEBERG_OOR_ERROR.  columnName may be NULL/empty when the
 * caller doesn't have column context; the message degrades gracefully.
 */
static void
RaiseSizeOverflow(const char *columnName, const char *typeLabel,
				  int64 size, int64 limit, const char *limitLabel)
{
	if (columnName != NULL && columnName[0] != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("value of column \"%s\" (%s, %lld bytes) exceeds %s (%lld)",
						columnName, typeLabel, (long long) size,
						limitLabel, (long long) limit),
				 errhint("Set out_of_range_values = 'clamp' on the table to "
						 "truncate oversize values instead of erroring.")));
	else
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("%s value of %lld bytes exceeds %s (%lld)",
						typeLabel, (long long) size, limitLabel,
						(long long) limit),
				 errhint("Set out_of_range_values = 'clamp' on the table to "
						 "truncate oversize values instead of erroring.")));
}


/*
 * SizeClampVarlenaFastPath returns true if `value`'s on-disk varlena size is
 * already at or below the cap that applies to `typeOid`, in which case no
 * leaf clamp and no aggregate clamp can fire and the recursion can be
 * skipped.  The cap is picked per type, in the type's own measurement
 * currency:
 *
 *   text/varchar/bpchar/json:  varlena content bytes equals the
 *                              consumer-visible byte length, so the STRING
 *                              cap is a hard bound.
 *   bytea:                     same, against the BINARY cap.
 *   array/composite/map:       bounded by the BINARY cap (the smallest
 *                              per-leaf cap).  A container under the
 *                              aggregate NESTED cap can still hold a single
 *                              string/binary leaf over its own per-leaf cap
 *                              (the slow path recurses to clamp it), so the
 *                              container only fast-paths out when its whole
 *                              varlena is small enough that no leaf can
 *                              exceed any cap -- i.e. at or below the
 *                              smallest per-leaf cap, since a leaf never
 *                              exceeds the container that holds it.
 *
 * Excludes any type that contains a jsonb/json leaf -- top-level jsonb has
 * its own type-aware fast path in IcebergSizeClampStringScalar, and jsonb
 * inside a container needs the slow path's text-form aggregate measurement
 * to catch JSON-form overflow that the binary varlena size would
 * under-count.
 *
 * Domains are unwrapped to the base type so a domain over text is bounded
 * by the STRING cap, not the (larger) NESTED cap.
 *
 * Caller must ensure typeOid is a varlena type (typlen == -1) before
 * dereferencing the Datum as a varlena pointer.
 */
static bool
SizeClampVarlenaFastPath(Datum value, Oid typeOid)
{
	if (TypeContainsJsonbLeaf(typeOid))
		return false;
	if (get_typlen(typeOid) != -1)
		return false;

	Oid			baseType = getBaseType(typeOid);
	int64		totalSize = (int64) toast_raw_datum_size(value) - VARHDRSZ;

	if (baseType == TEXTOID || baseType == VARCHAROID ||
		baseType == BPCHAROID || baseType == JSONOID)
		return totalSize <= ICEBERG_SNOWFLAKE_MAX_STRING_BYTES;

	if (baseType == BYTEAOID)
		return totalSize <= ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES;

	/*
	 * Other scalar leaves Iceberg stores as string/binary (hstore, citext,
	 * geometry, ...) are bounded by the per-leaf STRING/BINARY cap, not the
	 * looser NESTED cap the container fallback below uses.
	 */
	{
		bool		isBinary = false;

		if (IcebergScalarStorageIsStringOrBinary(baseType, &isBinary))
			return totalSize <= (isBinary ? ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES :
								 ICEBERG_SNOWFLAKE_MAX_STRING_BYTES);
	}

	/*
	 * Container (array/composite/map): a value at or below the smallest
	 * per-leaf cap (BINARY) cannot contain any leaf that exceeds its own cap,
	 * and is trivially under the aggregate NESTED cap, so it fast-paths out.
	 * Larger containers take the slow path, which applies the aggregate check
	 * and recurses to per-leaf-clamp inner strings/bytea.
	 */
	return totalSize <= ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES;
}


/*
 * ErrorOrClampTemporal handles an out-of-range temporal value.
 *
 * In error mode: raises an error.
 * In clamp mode: returns the nearest boundary value.
 */
static Datum
ErrorOrClampTemporal(Datum value, Oid typeOid, int year,
					 IcebergOutOfRangePolicy policy)
{
	Assert(IsTemporalType(typeOid));

	if (policy == ICEBERG_OOR_ERROR)
	{
		const char *errMsg;

		switch (typeOid)
		{
			case DATEOID:
				errMsg = "date out of range";
				break;
			case TIMESTAMPOID:
				errMsg = "timestamp out of range";
				break;
			case TIMESTAMPTZOID:
				errMsg = "timestamptz out of range";
				break;
			default:
				elog(ERROR, "unexpected temporal type OID: %u", typeOid);
		}

		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("%s", errMsg)));
	}

	/*
	 * Clamp mode: determine if value is below or above range.
	 *
	 * For infinity values, NOBEGIN = -infinity -> clamp to min, NOEND =
	 * +infinity -> clamp to max.
	 *
	 * For finite values, use the extracted year to decide direction.
	 */
	bool		clampToMin;

	if (typeOid == DATEOID)
	{
		DateADT		d = DatumGetDateADT(value);

		if (DATE_NOT_FINITE(d))
			clampToMin = DATE_IS_NOBEGIN(d);
		else
			clampToMin = (year < TEMPORAL_DATE_MIN_YEAR);

		if (clampToMin)
			return DateADTGetDatum(MakeDateFromYMD(TEMPORAL_DATE_MIN_YEAR, 1, 1));
		else
			return DateADTGetDatum(MakeDateFromYMD(TEMPORAL_MAX_YEAR, 12, 31));
	}
	else if (typeOid == TIMESTAMPOID)
	{
		Timestamp	ts = DatumGetTimestamp(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			clampToMin = TIMESTAMP_IS_NOBEGIN(ts);
		else
			clampToMin = (year < TEMPORAL_TIMESTAMP_MIN_YEAR);

		if (clampToMin)
			return TimestampGetDatum(
									 MakeTimestampUsec(TEMPORAL_TIMESTAMP_MIN_YEAR, 1, 1, 0, 0, 0, 0));
		else
			return TimestampGetDatum(
									 MakeTimestampUsec(TEMPORAL_MAX_YEAR, 12, 31, 23, 59, 59, 999999));
	}
	else
	{
		Assert(typeOid == TIMESTAMPTZOID);

		/*
		 * TIMESTAMPTZOID: clamp to UTC boundaries.  Iceberg stores
		 * timestamptz as UTC microseconds, so the boundaries are defined in
		 * UTC regardless of the session timezone.
		 */
		TimestampTz ts = DatumGetTimestampTz(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			clampToMin = TIMESTAMP_IS_NOBEGIN(ts);
		else
			clampToMin = (year < TEMPORAL_TIMESTAMP_MIN_YEAR);

		if (clampToMin)
			return TimestampTzGetDatum(
									   MakeTimestampUsec(TEMPORAL_TIMESTAMP_MIN_YEAR, 1, 1, 0, 0, 0, 0));
		else
			return TimestampTzGetDatum(
									   MakeTimestampUsec(TEMPORAL_MAX_YEAR, 12, 31, 23, 59, 59, 999999));
	}
}


/*
 * IcebergErrorOrClampTemporalDatum validates a date, timestamp, or
 * timestamptz Datum against Iceberg temporal boundaries.
 *
 * For timestamptz, GetYearFromTimestamp extracts the UTC year (since
 * TimestampTz is stored as UTC microseconds internally), so the range
 * check and clamping are timezone-independent.
 */
static Datum
IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid,
								 IcebergOutOfRangePolicy policy)
{
	Assert(IsTemporalType(typeOid));

	if (typeOid == DATEOID)
	{
		DateADT		d = DatumGetDateADT(value);

		if (DATE_NOT_FINITE(d))
			return ErrorOrClampTemporal(value, typeOid, 0, policy);

		int			year = GetYearFromDate(d);

		if (year < TEMPORAL_DATE_MIN_YEAR || year > TEMPORAL_MAX_YEAR)
			return ErrorOrClampTemporal(value, typeOid, year, policy);
	}
	else
	{
		Assert(typeOid == TIMESTAMPTZOID || typeOid == TIMESTAMPOID);

		Timestamp	ts = (typeOid == TIMESTAMPTZOID) ?
			DatumGetTimestampTz(value) :
			DatumGetTimestamp(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			return ErrorOrClampTemporal(value, typeOid, 0, policy);

		int			year = GetYearFromTimestamp(ts);

		if (year < TEMPORAL_TIMESTAMP_MIN_YEAR || year > TEMPORAL_MAX_YEAR)
			return ErrorOrClampTemporal(value, typeOid, year, policy);
	}

	return value;
}


/*
 * IcebergErrorOrClampNumericDatum validates a bounded numeric Datum for NaN.
 *
 * Unbounded numerics (and those with precision/scale beyond Iceberg limits)
 * are mapped to float8, which supports NaN natively, so they are returned
 * unchanged.  Only bounded numerics that map to Iceberg decimal are checked.
 *
 * In clamp mode: sets *isNull to true and returns 0 (caller writes NULL).
 * In error mode: raises an error.
 * For non-NaN values the datum is returned unchanged.
 */
static Datum
IcebergErrorOrClampNumericDatum(Datum value, int32 typmod,
								IcebergOutOfRangePolicy policy,
								bool *isNull)
{
	if (IsUnsupportedNumericForIceberg(NUMERICOID, typmod))
		return value;

	if (!numeric_is_nan(DatumGetNumeric(value)))
		return value;

	if (policy == ICEBERG_OOR_CLAMP)
	{
		*isNull = true;
		return (Datum) 0;
	}

	Assert(policy == ICEBERG_OOR_ERROR);
	ereport(ERROR,
			errmsg("NaN is not supported for Iceberg decimal"),
			errhint("Use float type instead."));
}


/*
 * IcebergErrorOrClampMultiDimArrayDatum handles a multidimensional
 * PostgreSQL array: raises an error, or nullifies it via *isNull.
 *
 * PostgreSQL does not distinguish int[] from int[][] at the type level,
 * so a column declared as int[] (Iceberg list(integer)) can hold arrays
 * with ndim > 1 at runtime.  Since the PG array type maps to a flat
 * LIST(T) in DuckDB/Iceberg, multidimensional values cannot be stored
 * faithfully.  Rather than
 * silently restructuring the data by flattening, we treat them like
 * other unsupported values (NaN) and clamp to NULL.
 */
static Datum
IcebergErrorOrClampMultiDimArrayDatum(ArrayType *array,
									  IcebergOutOfRangePolicy policy,
									  bool *isNull)
{
	Assert(ARR_NDIM(array) > 1);

	if (policy == ICEBERG_OOR_ERROR)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("multidimensional arrays are not supported "
						"in Iceberg tables"),
				 errhint("Use out_of_range_values = 'clamp' to "
						 "automatically replace multidimensional "
						 "arrays with NULL.")));
	}

	*isNull = true;
	return (Datum) 0;
}


/*
 * IcebergErrorOrClampNestedDatum recursively validates a Datum for
 * Iceberg write constraints, deconstructing and reconstructing arrays,
 * composites, maps (domain over array of composites), and domains.
 *
 * *isNull is set to true only when a scalar numeric NaN is clamped at
 * this recursion level.  For containers, NaN-clamped elements are
 * absorbed as NULL within the reconstructed container.
 *
 * *modified is set to true when the returned Datum differs from the
 * input, allowing callers to skip reconstruction when nothing changed.
 */
static Datum
IcebergErrorOrClampNestedDatum(Datum value, Oid typeOid, int32 typmod,
							   IcebergOutOfRangePolicy policy,
							   bool *isNull, bool *modified)
{
	*modified = false;

	if (IsTemporalType(typeOid))
	{
		Datum		result = IcebergErrorOrClampTemporalDatum(value, typeOid,
															  policy);

		*modified = (result != value);
		return result;
	}

	if (typeOid == NUMERICOID)
	{
		Datum		result = IcebergErrorOrClampNumericDatum(value, typmod,
															 policy, isNull);

		*modified = *isNull;
		return result;
	}

	/*
	 * array types: nullify multidim (→ NULL), validate elements,
	 * reconstruct
	 */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		ArrayType  *array = DatumGetArrayTypeP(value);
		bool		needsMultidimClamp = ARR_NDIM(array) > 1;
		bool		needsElementValidation =
			TypeNeedsIcebergValidation(elemType, typmod, false);

		if (!needsMultidimClamp && !needsElementValidation)
			return value;

		if (needsMultidimClamp)
		{
			*modified = true;
			return IcebergErrorOrClampMultiDimArrayDatum(array, policy,
														 isNull);
		}

		if (!needsElementValidation)
			return value;

		int16		elmlen;
		bool		elmbyval;
		char		elmalign;

		get_typlenbyvalalign(elemType, &elmlen, &elmbyval, &elmalign);

		Datum	   *elems;
		bool	   *nulls;
		int			nelems;

		deconstruct_array(array, elemType, elmlen, elmbyval, elmalign,
						  &elems, &nulls, &nelems);

		bool		anyModified = false;

		for (int i = 0; i < nelems; i++)
		{
			if (nulls[i])
				continue;

			bool		elemIsNull = false;
			bool		elemModified = false;
			Datum		clamped = IcebergErrorOrClampNestedDatum(elems[i],
																 elemType,
																 typmod,
																 policy,
																 &elemIsNull,
																 &elemModified);

			if (elemModified || elemIsNull)
			{
				elems[i] = clamped;
				nulls[i] = elemIsNull;
				anyModified = true;
			}
		}

		if (!anyModified)
			return value;

		ArrayType  *result = construct_md_array(elems, nulls,
												ARR_NDIM(array),
												ARR_DIMS(array),
												ARR_LBOUND(array),
												elemType, elmlen,
												elmbyval, elmalign);

		*modified = true;
		return PointerGetDatum(result);
	}

	/*
	 * Domain types (including maps): unwrap to base type and recurse. Maps
	 * are domains over arrays of composites, so unwrapping naturally leads to
	 * the array -> composite recursion above.
	 */
	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
	{
		int32		baseTypmod = typmod;
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &baseTypmod);

		return IcebergErrorOrClampNestedDatum(value, baseType, baseTypmod,
											  policy, isNull, modified);
	}

	/* composite types: deform tuple, validate fields, reform */
	if (typtype == TYPTYPE_COMPOSITE)
	{
		HeapTupleHeader tup = DatumGetHeapTupleHeader(value);
		Oid			tupType = HeapTupleHeaderGetTypeId(tup);
		int32		tupTypmod = HeapTupleHeaderGetTypMod(tup);
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
		int			natts = tupdesc->natts;

		HeapTupleData tmptup;

		tmptup.t_len = HeapTupleHeaderGetDatumLength(tup);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = tup;

		Datum	   *values = (Datum *) palloc(natts * sizeof(Datum));
		bool	   *attrNulls = (bool *) palloc(natts * sizeof(bool));

		heap_deform_tuple(&tmptup, tupdesc, values, attrNulls);

		bool		anyModified = false;

		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped || attrNulls[i])
				continue;

			if (!TypeNeedsIcebergValidation(attr->atttypid, attr->atttypmod,
											false))
				continue;

			bool		attrIsNull = false;
			bool		attrModified = false;
			Datum		clamped = IcebergErrorOrClampNestedDatum(values[i],
																 attr->atttypid,
																 attr->atttypmod,
																 policy,
																 &attrIsNull,
																 &attrModified);

			if (attrModified || attrIsNull)
			{
				values[i] = clamped;
				attrNulls[i] = attrIsNull;
				anyModified = true;
			}
		}

		if (!anyModified)
		{
			pfree(values);
			pfree(attrNulls);
			ReleaseTupleDesc(tupdesc);
			return value;
		}

		HeapTuple	newTuple = heap_form_tuple(tupdesc, values, attrNulls);

		pfree(values);
		pfree(attrNulls);
		ReleaseTupleDesc(tupdesc);
		*modified = true;
		return HeapTupleGetDatum(newTuple);
	}

	return value;
}


/*
 * IcebergErrorOrClampDatum validates a Datum for Iceberg write constraints.
 *
 * Recursively handles scalar types (date/timestamp/timestamptz/numeric)
 * as well as nested containers (arrays, composites, maps, domains).
 * For types that need no validation the value is returned unchanged.
 *
 * typmod is used to distinguish bounded numerics (Iceberg decimal) from
 * unbounded ones (mapped to float8).  Only bounded numerics have NaN
 * clamped; unbounded numerics pass through unchanged.
 *
 * *isNull is set to true when a top-level unsupported value is clamped:
 * numeric NaN or a multidimensional array.  The caller should write NULL
 * instead of the original value.  Unsupported values nested inside
 * containers are absorbed as NULL within the reconstructed container.
 */
Datum
IcebergErrorOrClampDatum(Datum value, Oid typeOid, int32 typmod,
						 IcebergOutOfRangePolicy policy, bool *isNull)
{
	*isNull = false;

	bool		modified = false;

	return IcebergErrorOrClampNestedDatum(value, typeOid, typmod, policy,
										  isNull, &modified);
}


/*
 * IcebergSizeClampStringScalar handles a single text/varchar/bpchar/jsonb/json
 * Datum: text-y values are truncated at a UTF-8 character boundary to fit
 * ICEBERG_SNOWFLAKE_MAX_STRING_BYTES; jsonb/json values over the limit are
 * NULLed via *isNull, since truncation would corrupt them.  Sizes are read
 * from the varlena/TOAST header (toast_raw_datum_size); the value is detoasted
 * only in clamp mode, when an oversize value must actually be clipped.
 */
static Datum
IcebergSizeClampStringScalar(Datum value, Oid typeOid,
							 IcebergOutOfRangePolicy policy,
							 const char *columnName,
							 bool *isNull)
{
	const int32 maxBytes = ICEBERG_SNOWFLAKE_MAX_STRING_BYTES;

	if (typeOid == JSONBOID)
	{
		int64		binSize = (int64) toast_raw_datum_size(value) - VARHDRSZ;

		/*
		 * jsonb_out expands by at most ~6x (a control byte becomes the 6-char
		 * \uXXXX escape), so binSize <= maxBytes/6 cannot overflow once
		 * serialized -- skip the jsonb_out walk for the common small case.
		 */
		if (binSize * 6 <= (int64) maxBytes)
			return value;

		char	   *cstr = DatumGetCString(DirectFunctionCall1(jsonb_out, value));
		int32		textLen = strlen(cstr);

		pfree(cstr);

		if (textLen <= maxBytes)
			return value;

		if (policy == ICEBERG_OOR_ERROR)
			RaiseSizeOverflow(columnName, "jsonb", textLen, maxBytes,
							  "Snowflake STRING column limit");

		*isNull = true;
		return (Datum) 0;
	}

	int32		sourceValueLength = (int32) ((int64) toast_raw_datum_size(value) - VARHDRSZ);

	if (sourceValueLength <= maxBytes)
		return value;

	if (typeOid == JSONOID)
	{
		if (policy == ICEBERG_OOR_ERROR)
			RaiseSizeOverflow(columnName, "json", sourceValueLength, maxBytes,
							  "Snowflake STRING column limit");

		*isNull = true;
		return (Datum) 0;
	}

	Assert(typeOid == TEXTOID || typeOid == VARCHAROID ||
		   typeOid == BPCHAROID);

	if (policy == ICEBERG_OOR_ERROR)
	{
		const char *typeLabel = (typeOid == TEXTOID) ? "text" :
			(typeOid == VARCHAROID) ? "varchar" : "bpchar";

		RaiseSizeOverflow(columnName, typeLabel, sourceValueLength, maxBytes,
						  "Snowflake STRING column limit");
	}

	/* Clamp mode: fetch the bytes to clip at a UTF-8 character boundary. */
	struct varlena *sourceValueDetoasted = (struct varlena *) PG_DETOAST_DATUM_PACKED(value);
	const char *sourceValueData = VARDATA_ANY(sourceValueDetoasted);
	int32		trimmedLen = pg_mbcliplen(sourceValueData,
										  VARSIZE_ANY_EXHDR(sourceValueDetoasted),
										  maxBytes);

	struct varlena *result = (struct varlena *) palloc(VARHDRSZ + trimmedLen);

	SET_VARSIZE(result, VARHDRSZ + trimmedLen);
	memcpy(VARDATA(result), sourceValueData, trimmedLen);

	if (sourceValueDetoasted != (struct varlena *) DatumGetPointer(value))
		pfree(sourceValueDetoasted);

	return PointerGetDatum(result);
}


/*
 * IcebergSizeClampBinaryScalar byte-truncates a bytea Datum to
 * ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES when needed.  Returns the original
 * Datum unchanged when no truncation is required.
 */
static Datum
IcebergSizeClampBinaryScalar(Datum value,
							 IcebergOutOfRangePolicy policy,
							 const char *columnName)
{
	const int32 maxBytes = ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES;

	int32		sourceValueLength = (int32) ((int64) toast_raw_datum_size(value) - VARHDRSZ);

	if (sourceValueLength <= maxBytes)
		return value;

	if (policy == ICEBERG_OOR_ERROR)
		RaiseSizeOverflow(columnName, "bytea", sourceValueLength, maxBytes,
						  "Snowflake BINARY column limit");

	/* Clamp mode: fetch the bytes to copy the leading maxBytes. */
	struct varlena *sourceValueDetoasted = (struct varlena *) PG_DETOAST_DATUM_PACKED(value);
	struct varlena *result = (struct varlena *) palloc(VARHDRSZ + maxBytes);

	SET_VARSIZE(result, VARHDRSZ + maxBytes);
	memcpy(VARDATA(result), VARDATA_ANY(sourceValueDetoasted), maxBytes);

	if (sourceValueDetoasted != (struct varlena *) DatumGetPointer(value))
		pfree(sourceValueDetoasted);

	return PointerGetDatum(result);
}


/*
 * IcebergSizeClampNestedDatum recursively size-clamps a Datum, deconstructing
 * and reconstructing arrays, composites, maps (domain over array of
 * composites), and domains.  The recursion shape mirrors
 * IcebergErrorOrClampNestedDatum.
 *
 * In addition to per-leaf clamping, an aggregate-size check NULLs the entire
 * array or composite when its varlena content size exceeds
 * ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES (the limit on the serialized form
 * that downstream consumers receive when an array/struct lands in an
 * OBJECT/ARRAY/VARIANT column; distinct from the per-leaf STRING limit since
 * downstream caps usually differ).  The varlena size is a cheap proxy for the
 * JSON serialization length the consumer ultimately sees and avoids paying
 * for an extra serialization pass.
 *
 * *isNull is set to true when the value is replaced by NULL: a leaf
 * jsonb/json over the limit, or a container whose aggregate exceeds the
 * limit.  Inside containers, NULLed children are absorbed as NULL within
 * the reconstructed container.
 *
 * *modified is set to true when the returned Datum differs from the input,
 * allowing callers to skip reconstruction when nothing changed.
 */
static Datum
IcebergSizeClampNestedDatum(Datum value, Oid typeOid, int32 typmod,
							IcebergOutOfRangePolicy policy,
							const char *columnName,
							bool *isNull, bool *modified)
{
	*modified = false;

	if (typeOid == TEXTOID || typeOid == VARCHAROID ||
		typeOid == BPCHAROID || typeOid == JSONBOID ||
		typeOid == JSONOID)
	{
		Datum		result = IcebergSizeClampStringScalar(value, typeOid,
														  policy, columnName,
														  isNull);

		*modified = (result != value) || *isNull;
		return result;
	}

	if (typeOid == BYTEAOID)
	{
		Datum		result = IcebergSizeClampBinaryScalar(value, policy,
														  columnName);

		*modified = (result != value);
		return result;
	}

	/*
	 * Container types (array / composite / map / domain over either) enforce
	 * two independent Snowflake limits:
	 *
	 * 1. an aggregate cap on the whole serialized container against
	 * ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES (the OBJECT/ARRAY/VARIANT
	 * column ceiling), and 2. the per-leaf STRING / BINARY cap on every
	 * string/binary leaf inside it.  Snowflake maps a pg array/composite to a
	 * *typed* ARRAY(VARCHAR) / OBJECT(... VARCHAR ...), and each VARCHAR leaf
	 * carries the same 16 MiB physical value limit a top-level string does --
	 * a single oversize leaf breaks materialization into a native table even
	 * when the container as a whole is far under the aggregate cap.
	 *
	 * So we check the aggregate first (NULL/error the whole container when it
	 * exceeds the nested cap), then recurse into the elements/fields to clamp
	 * each leaf, mirroring IcebergErrorOrClampNestedDatum.  Because a leaf
	 * can never be larger than the container that holds it, a container that
	 * passed the aggregate cap can only shrink under per-leaf clamping, so
	 * the two checks compose without re-measuring.
	 */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		/*
		 * Default to the cheap varlena measurement.  When the container has
		 * jsonb/json leaves, switch to the type's output function so jsonb
		 * leaves are sized by their JSON-text form (jsonb_out) rather than
		 * their compact binary varlena form, which the consumer never sees.
		 */
		int64		aggregateSize;

		if (TypeContainsJsonbLeaf(typeOid))
			aggregateSize = ContainerByteSizeViaOutputFunc(value, typeOid);
		else
			aggregateSize = (int64) toast_raw_datum_size(value) - VARHDRSZ;

		if (aggregateSize > ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES)
		{
			if (policy == ICEBERG_OOR_ERROR)
				RaiseSizeOverflow(columnName, "array", aggregateSize,
								  ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES,
								  "Snowflake OBJECT/ARRAY/VARIANT column limit");

			*isNull = true;
			*modified = true;
			return (Datum) 0;
		}

		/* No clampable leaf type: nothing to recurse into. */
		if (!TypeNeedsIcebergSizeClamping(elemType))
			return value;

		ArrayType  *array = DatumGetArrayTypeP(value);
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;

		get_typlenbyvalalign(elemType, &elmlen, &elmbyval, &elmalign);

		Datum	   *elems;
		bool	   *nulls;
		int			nelems;

		deconstruct_array(array, elemType, elmlen, elmbyval, elmalign,
						  &elems, &nulls, &nelems);

		bool		anyModified = false;

		for (int i = 0; i < nelems; i++)
		{
			if (nulls[i])
				continue;

			bool		elemIsNull = false;
			bool		elemModified = false;

			/* an array's typmod applies to its element type */
			Datum		clamped = IcebergSizeClampNestedDatum(elems[i], elemType,
															  typmod, policy,
															  columnName,
															  &elemIsNull,
															  &elemModified);

			if (elemModified || elemIsNull)
			{
				elems[i] = clamped;
				nulls[i] = elemIsNull;
				anyModified = true;
			}
		}

		if (!anyModified)
			return value;

		ArrayType  *result = construct_md_array(elems, nulls,
												ARR_NDIM(array),
												ARR_DIMS(array),
												ARR_LBOUND(array),
												elemType, elmlen,
												elmbyval, elmalign);

		*modified = true;
		return PointerGetDatum(result);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
	{
		int32		baseTypmod = typmod;
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &baseTypmod);

		return IcebergSizeClampNestedDatum(value, baseType, baseTypmod,
										   policy, columnName,
										   isNull, modified);
	}

	if (typtype == TYPTYPE_COMPOSITE)
	{
		int64		aggregateSize;

		if (TypeContainsJsonbLeaf(typeOid))
			aggregateSize = ContainerByteSizeViaOutputFunc(value, typeOid);
		else
			aggregateSize = (int64) toast_raw_datum_size(value) - VARHDRSZ;

		if (aggregateSize > ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES)
		{
			if (policy == ICEBERG_OOR_ERROR)
				RaiseSizeOverflow(columnName, "composite", aggregateSize,
								  ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES,
								  "Snowflake OBJECT/ARRAY/VARIANT column limit");

			*isNull = true;
			*modified = true;
			return (Datum) 0;
		}

		HeapTupleHeader tup = DatumGetHeapTupleHeader(value);
		Oid			tupType = HeapTupleHeaderGetTypeId(tup);
		int32		tupTypmod = HeapTupleHeaderGetTypMod(tup);
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
		int			natts = tupdesc->natts;

		HeapTupleData tmptup;

		tmptup.t_len = HeapTupleHeaderGetDatumLength(tup);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = tup;

		Datum	   *values = (Datum *) palloc(natts * sizeof(Datum));
		bool	   *attrNulls = (bool *) palloc(natts * sizeof(bool));

		heap_deform_tuple(&tmptup, tupdesc, values, attrNulls);

		bool		anyModified = false;

		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute fattr = TupleDescAttr(tupdesc, i);

			if (fattr->attisdropped || attrNulls[i])
				continue;

			if (!TypeNeedsIcebergSizeClamping(fattr->atttypid))
				continue;

			bool		attrIsNull = false;
			bool		attrModified = false;
			Datum		clamped = IcebergSizeClampNestedDatum(values[i],
															  fattr->atttypid,
															  fattr->atttypmod,
															  policy,
															  columnName,
															  &attrIsNull,
															  &attrModified);

			if (attrModified || attrIsNull)
			{
				values[i] = clamped;
				attrNulls[i] = attrIsNull;
				anyModified = true;
			}
		}

		if (!anyModified)
		{
			pfree(values);
			pfree(attrNulls);
			ReleaseTupleDesc(tupdesc);
			return value;
		}

		HeapTuple	newTuple = heap_form_tuple(tupdesc, values, attrNulls);

		pfree(values);
		pfree(attrNulls);
		ReleaseTupleDesc(tupdesc);
		*modified = true;
		return HeapTupleGetDatum(newTuple);
	}

	/*
	 * Any other scalar leaf Iceberg stores as string/binary (hstore, citext,
	 * geometry, ...).  We can't safely truncate the serialized form, so NULL
	 * it (CLAMP) or raise (ERROR) when its varlena content exceeds the
	 * applicable per-leaf cap.  Fixed-length types can never reach the cap,
	 * so only varlena values are measured.
	 */
	{
		bool		isBinary = false;

		if (get_typlen(typeOid) == -1 &&
			IcebergScalarStorageIsStringOrBinary(typeOid, &isBinary))
		{
			int64		size = (int64) toast_raw_datum_size(value) - VARHDRSZ;
			int64		cap = isBinary ? ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES :
				ICEBERG_SNOWFLAKE_MAX_STRING_BYTES;

			if (size > cap)
			{
				if (policy == ICEBERG_OOR_ERROR)
					RaiseSizeOverflow(columnName, isBinary ? "binary" : "string",
									  size, cap,
									  isBinary ? "Snowflake BINARY column limit" :
									  "Snowflake STRING column limit");

				*isNull = true;
				*modified = true;
				return (Datum) 0;
			}
		}
	}

	return value;
}


/*
 * IcebergSizeClampDatum truncates or NULLs a Datum so that string, binary,
 * and nested-type values fit Snowflake's per-column byte caps.  Callers
 * gate on the table's compatibility_mode before calling; this function
 * applies the limits unconditionally otherwise.
 *
 * See header comment for full semantics.
 */
Datum
IcebergSizeClampDatum(Datum value, Oid typeOid, int32 typmod,
					  IcebergOutOfRangePolicy policy,
					  const char *columnName, bool *isNull)
{
	*isNull = false;

	if (policy == ICEBERG_OOR_NONE)
		return value;

	bool		modified = false;

	/*
	 * Fast path for varlena values (text/varchar/bpchar/bytea/json/array/
	 * struct/map): if the entire on-disk varlena fits comfortably under every
	 * active limit, no leaf clamp and no aggregate clamp can fire. Skips the
	 * recursive deconstruct/deform that the slow path would do — the
	 * typical case where a column's type is clampable but the row's value
	 * happens to be small.  Sound under both CLAMP and ERROR because no limit
	 * can be exceeded when the entire varlena fits comfortably.
	 */
	if (SizeClampVarlenaFastPath(value, typeOid))
		return value;

	return IcebergSizeClampNestedDatum(value, typeOid, typmod, policy,
									   columnName, isNull, &modified);
}
