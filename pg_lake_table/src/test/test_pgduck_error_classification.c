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

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include "pg_lake/pgduck/client.h"

PG_FUNCTION_INFO_V1(test_classify_pgduck_error_message);

/*
 * test_classify_pgduck_error_message is a test-only SQL wrapper around
 * ClassifyPGDuckErrorMessage, for pytest coverage of pg_lake_engine's
 * pgduck error classification. Not declared in any production SQL file.
 */
Datum
test_classify_pgduck_error_message(PG_FUNCTION_ARGS)
{
	const char *message = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *errorClass = ClassifyPGDuckErrorMessage(message);

	PG_RETURN_TEXT_P(cstring_to_text(errorClass));
}
