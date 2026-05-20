"""Smoke + round-trip coverage for the Iceberg v3 sample-table corpus.

The corpus is committed under
``test_common/sample/iceberg/sample_tables_v3/<db>/<table>/{data,metadata}/``
and is produced by
``test_common/sample/iceberg/scripts/generate_v3_fixtures.py``.

Stage 7 of the v3 effort accepts v3 metadata reads structurally: feature-
level incompatibilities (deletion vectors, v3-only types, encryption) raise
specific errors at their own choke points; everything else reads cleanly
even before row-lineage support lands.

This file proves that:

1. The corpus is non-empty and reachable from the test harness.
2. ``reserialize_iceberg_table_metadata`` reads each non-DV v3 fixture and
   round-trips the high-level identity fields (``format-version``,
   ``table-uuid``, ``location``). It does **not** yet assert byte-equality
   of the whole metadata.json -- row-lineage fields (``next-row-id``,
   snapshot ``first-row-id`` / ``added-rows``) are intentionally dropped
   on the write side until Stages 11-12 land. When those land, this test
   gets tightened to ``assert_jsons_equivalent``.
3. ``v3_pos_dv`` is the deletion-vector fixture. The metadata.json itself
   has no DV references (those live in the manifest), so it still
   reserializes cleanly. The DV choke point is covered separately in
   ``test_v3_read_errors.py``.
"""

import json
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
def test_v3_metadata_reserialize_preserves_format_version(
    create_reserialize_helper_functions, label, filepath, superuser_conn
):
    """v3 metadata.json reads successfully and the writer keeps format-version=3.

    We only assert the small set of fields that the v2-shaped writer is
    guaranteed to preserve today. Stages 11-12 add row lineage to the
    writer, at which point this test gets tightened.
    """
    command = (
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{filepath}')::json"
    )
    result = run_query(command, superuser_conn)
    # psycopg2's json adapter already deserializes ::json results into a dict.
    returned = result[0][0]
    original = read_json(filepath)

    assert (
        returned["format-version"] == 3
    ), f"{label}: writer downgraded format-version to {returned['format-version']}"

    for stable_key in ("table-uuid", "location", "current-schema-id"):
        assert returned[stable_key] == original[stable_key], (
            f"{label}: writer changed {stable_key!r}: "
            f"{original[stable_key]!r} -> {returned[stable_key]!r}"
        )
