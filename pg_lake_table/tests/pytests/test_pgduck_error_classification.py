"""Unit coverage for pg_lake_engine's ClassifyPGDuckErrorMessage (client.c).

Exercises the classifier via a test-only SQL wrapper
(test_classify_pgduck_error_message, src/test/test_pgduck_error_classification.c)
using literal error strings confirmed from production incidents, so the
mapping is pinned against real-world DuckDB/pgduck_server wording rather
than guessed text.
"""

import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def classify_fn(superuser_conn):
    run_command(
        """
        CREATE SCHEMA IF NOT EXISTS test_error_classification;

        CREATE FUNCTION test_error_classification.classify_pgduck_error_message(msg TEXT)
        RETURNS TEXT
        LANGUAGE C VOLATILE
        AS 'pg_lake_table', 'test_classify_pgduck_error_message';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP SCHEMA IF EXISTS test_error_classification CASCADE", superuser_conn
    )
    superuser_conn.commit()


@pytest.mark.parametrize(
    "message,expected_class",
    [
        # Real production strings, confirmed via incident logs. Only the
        # "<Category> Error: " prefix (or, for lost_connection, the full
        # literal) is what the classifier matches on -- the detail after
        # it is never inspected, so these use representative rather than
        # customer-identifying detail text where the original included
        # account/table-specific paths.
        ("lost connection to query engine", "lost_connection"),
        (
            "Out of Memory Error: Failed to allocate block of 15892480 bytes (bad allocation)",
            "out_of_memory",
        ),
        (
            "Out of Memory Error: could not allocate block of size 76.5 MiB (1.9 GiB/2.0 GiB used)",
            "out_of_memory",
        ),
        ("Out of Memory Error: Allocation failure", "out_of_memory"),
        ("IO Error: AzureStorageFileSystem could not open file", "io_error"),
        (
            "IO Error: AzureBlobStorageFileSystem Delete of azure://example-container/example-path",
            "io_error",
        ),
        ("Invalid Error: Invalid header name: content-length", "invalid_error"),
        (
            "HTTP Error: HTTP GET error reading 's3://example-bucket/example/path'",
            "http_error",
        ),
        # Negative cases: real strings seen in production that do NOT
        # match the "<Category> Error: " convention, so they must fall
        # through to "other" with zero text leaked.
        ("unable to read avro record: Error decompressing block with deflate", "other"),
        ("ERROR Thread creation failed with 11", "other"),
        ("some unrelated message", "other"),
        (None, "other"),
    ],
)
def test_classify_pgduck_error_message(
    superuser_conn, classify_fn, message, expected_class
):
    cur = superuser_conn.cursor()
    cur.execute(
        "SELECT test_error_classification.classify_pgduck_error_message(%s)",
        (message,),
    )
    result = cur.fetchone()[0]
    assert (
        result == expected_class
    ), f"{message!r} classified as {result!r}, expected {expected_class!r}"
    superuser_conn.rollback()
