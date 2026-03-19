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
 * Validates individual Datum values against Iceberg representable ranges
 * on the PostgreSQL side (non-pushdown path).  Called from
 * IcebergErrorOrClampSlotInPlace during FDW inserts and from partition transform
 * code (to keep partition keys consistent with clamped data).
 *
 * Handles both temporal boundaries (date/timestamp/timestamptz) and
 * numeric NaN (clamped to NULL or rejected).
 *
 * Temporal boundaries:
 *   - Date: proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 *   - Timestamp/TimestampTZ: 0001-01-01 .. 9999-12-31 23:59:59.999999.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "datatype/timestamp.h"
#include "pg_lake/pgduck/iceberg_datum_validation.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/util/temporal_utils.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"


static Datum ErrorOrClampTemporal(Datum value, Oid typeOid, int year,
								  IcebergOutOfRangePolicy policy);
static Datum IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid,
											  IcebergOutOfRangePolicy policy);
static Datum IcebergErrorOrClampNumericDatum(Datum value,
											 IcebergOutOfRangePolicy policy,
											 bool *isNull);
static Datum IcebergErrorOrClampNestedDatum(Datum value, Oid typeOid,
											int32 typmod,
											IcebergOutOfRangePolicy policy,
											bool *isNull, bool *modified);


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
 * IcebergErrorOrClampNumericDatum validates a numeric Datum for NaN.
 *
 * In clamp mode: sets *isNull to true and returns 0 (caller writes NULL).
 * In error mode: raises an error.
 * For non-NaN values the datum is returned unchanged.
 */
static Datum
IcebergErrorOrClampNumericDatum(Datum value, IcebergOutOfRangePolicy policy,
								bool *isNull)
{
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
 * TypeContainsValidatable recursively checks whether a type contains
 * any component that needs Iceberg write validation (temporal types or
 * numeric), including inside arrays, composites, maps, and domains.
 */
bool
TypeContainsValidatable(Oid typeOid)
{
	if (IsTemporalType(typeOid) || typeOid == NUMERICOID)
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return TypeContainsValidatable(elemType);

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);

		return TypeContainsValidatable(keyType.postgresTypeOid) ||
			TypeContainsValidatable(valType.postgresTypeOid);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
		return TypeContainsValidatable(getBaseType(typeOid));

	if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		found = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (TypeContainsValidatable(attr->atttypid))
			{
				found = true;
				break;
			}
		}

		ReleaseTupleDesc(tupdesc);
		return found;
	}

	return false;
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
		Datum		result = IcebergErrorOrClampNumericDatum(value, policy,
															 isNull);

		*modified = *isNull;
		return result;
	}

	/* array types: deconstruct, validate elements, reconstruct */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		if (!TypeContainsValidatable(elemType))
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
			Datum		clamped = IcebergErrorOrClampNestedDatum(elems[i],
																 elemType, -1,
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

			if (!TypeContainsValidatable(attr->atttypid))
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
 * *isNull is set to true only when a top-level numeric NaN is clamped
 * (the caller should write NULL instead of the original value).
 * NaN values nested inside containers are absorbed as NULL within
 * the reconstructed container.
 */
Datum
IcebergErrorOrClampDatum(Datum value, Oid typeOid,
						 IcebergOutOfRangePolicy policy, bool *isNull)
{
	*isNull = false;

	bool		modified = false;

	return IcebergErrorOrClampNestedDatum(value, typeOid, -1, policy,
										  isNull, &modified);
}
