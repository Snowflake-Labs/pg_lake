"""Smoke + round-trip coverage for the Iceberg v3 sample-table corpus.

The corpus is committed under
``test_common/sample/iceberg/sample_tables_v3/<db>/<table>/{data,metadata}/``
and is produced by
``test_common/sample/iceberg/scripts/generate_v3_fixtures.py``.

What this file proves at the *current* stage of the v3 effort:

1. The corpus is non-empty and reachable from the test harness (so no
   downstream stage is starved for inputs).
2. The corpus can be uploaded to moto S3 via the
   :func:`spark_generated_iceberg_v3_test` fixture without errors.
3. ``lake_iceberg.reserialize_iceberg_table_metadata`` rejects every fixture
   today (because ``read_table_metadata.c`` errors on
   ``format-version != 2``). This is captured as
   ``xfail(strict=True)`` so that the moment a later commit accepts v3
   reads, this test starts *unexpectedly passing* and CI yells until the
   marker is removed — which is exactly the forcing function we want.

Once the v3 reader is wired (Stage 4 of the v3 effort, see
``docs/iceberg-v3/PLAN.md``), drop the ``xfail`` and add asserts on the
v3-specific fields (``format-version``, ``next-row-id``, snapshot
``first-row-id``/``added-rows``).
"""

import os

import pytest
from utils_pytest import *

# Module-level collection so pytest can use ids in --collect-only output.
V3_FIXTURES = collect_v3_latest_metadata_files()
V3_FIXTURE_IDS = [label for label, _ in V3_FIXTURES]


def test_v3_corpus_present():
    assert V3_FIXTURES, (
        "no v3 sample tables found under "
        "test_common/sample/iceberg/sample_tables_v3/. "
        "Regenerate via scripts/generate_v3_fixtures.py."
    )
    for label, path in V3_FIXTURES:
        assert os.path.isfile(
            path
        ), f"v3 fixture {label} has no committed latest metadata.json at {path}"


@pytest.mark.parametrize("label,filepath", V3_FIXTURES, ids=V3_FIXTURE_IDS)
@pytest.mark.xfail(
    strict=True,
    reason=(
        "pg_lake's reader rejects format-version != 2 today; this becomes a "
        "passing assertion once Stage 4 of the v3 effort accepts v3 reads. "
        "When that lands, remove this xfail marker."
    ),
)
def test_v3_metadata_reserialize_roundtrip(
    create_reserialize_helper_functions, label, filepath, superuser_conn
):
    command = (
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{filepath}')::json"
    )
    result = run_query(command, superuser_conn)
    returned_json = result[0][0]
    assert_valid_json(returned_json)
    original_json = read_json(filepath)
    assert_jsons_equivalent(original_json, returned_json)
