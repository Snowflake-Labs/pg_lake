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

#pragma once
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type_d.h"
#include "commands/copy.h"
#include "nodes/miscnodes.h"
#include "utils/numeric.h"

/*
 * PG19 added a firstNonCachedOffsetAttr cache to TupleDesc that must be
 * populated via TupleDescFinalize() before BlessTupleDesc() or
 * heap_form_tuple() are called on a manually-constructed descriptor; PG19's
 * BlessTupleDesc Asserts on a missing finalize. Older releases don't expose
 * the function, so define a no-op shim there and let callers always finalize.
 */
#if PG_VERSION_NUM < 190000
static inline void
TupleDescFinalize(TupleDesc tupdesc)
{
}
#endif

/*
 * PG19 added PG_SIG_IGN (a cast of SIG_IGN to pqsigfunc) for use with
 * pqsignal(); older releases pass SIG_IGN directly. Define the name on
 * <=18 so callers can use PG_SIG_IGN unconditionally.
 */
#if PG_VERSION_NUM < 190000
#define PG_SIG_IGN SIG_IGN
#endif

#if PG_VERSION_NUM >= 190000

/*
 * PG19 reshaped CopyFormatOptions: the bool fields ".binary" and ".csv_mode"
 * were replaced by a single CopyFormat enum field ".format". Provide
 * accessor macros so callers keep the older names.
 */
#define CopyOptsIsBinary(opts) ((opts).format == COPY_FORMAT_BINARY)
#define CopyOptsIsCsvMode(opts) ((opts).format == COPY_FORMAT_CSV)

/*
 * PG19 renamed numeric_int4_opt_error -> numeric_int4_safe and switched the
 * out-of-band error signal from a bool* to an ErrorSaveContext* (the standard
 * "soft error" API). Wrap with the older name + bool* signature.
 */
static inline int32
numeric_int4_opt_error(Numeric num, bool *have_error)
{
	ErrorSaveContext escontext;
	int32		result;

	memset(&escontext, 0, sizeof(escontext));
	escontext.type = T_ErrorSaveContext;
	result = numeric_int4_safe(num, (Node *) &escontext);

	*have_error = escontext.error_occurred;
	return result;
}

/*
 * Likewise for the binary numeric arithmetic helpers, which were also
 * renamed from foo_opt_error(Numeric, Numeric, bool *) to
 * foo_safe(Numeric, Numeric, Node *escontext) in PG19. Our callers always
 * pass NULL for the error argument, so just forward to the new name.
 */
#define numeric_add_opt_error(num1, num2, _ignored) numeric_add_safe(num1, num2, NULL)
#define numeric_sub_opt_error(num1, num2, _ignored) numeric_sub_safe(num1, num2, NULL)
#define numeric_mul_opt_error(num1, num2, _ignored) numeric_mul_safe(num1, num2, NULL)
#define numeric_div_opt_error(num1, num2, _ignored) numeric_div_safe(num1, num2, NULL)
#define numeric_mod_opt_error(num1, num2, _ignored) numeric_mod_safe(num1, num2, NULL)

#else

#define CopyOptsIsBinary(opts) ((opts).binary)
#define CopyOptsIsCsvMode(opts) ((opts).csv_mode)

#endif

#if PG_VERSION_NUM < 170000

#define foreach_ptr(type, var, lst) foreach_internal(type, *, var, lst, lfirst)
#define foreach_int(var, lst)	foreach_internal(int, , var, lst, lfirst_int)
#define foreach_oid(var, lst)	foreach_internal(Oid, , var, lst, lfirst_oid)
#define foreach_xid(var, lst)	foreach_internal(TransactionId, , var, lst, lfirst_xid)

/*
 * The internal implementation of the above macros.  Do not use directly.
 *
 * This macro actually generates two loops in order to declare two variables of
 * different types.  The outer loop only iterates once, so we expect optimizing
 * compilers will unroll it, thereby optimizing it away.
 */
#define foreach_internal(type, pointer, var, lst, func) \
	for (type pointer var = 0, pointer var##__outerloop = (type pointer) 1; \
		 var##__outerloop; \
		 var##__outerloop = 0) \
		for (ForEachState var##__state = {(lst), 0}; \
			 (var##__state.l != NIL && \
			  var##__state.i < var##__state.l->length && \
			 (var = func(&var##__state.l->elements[var##__state.i]), true)); \
			 var##__state.i++)

#endif
