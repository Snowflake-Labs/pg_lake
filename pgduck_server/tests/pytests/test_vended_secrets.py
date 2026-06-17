"""
Tests for vended credential secrets in pgduck_server.

Validates that DuckDB scoped secrets can be created, replaced, and dropped
via the same SQL patterns used by pg_lake's EnsureVendedSecretInPGDuck.
"""

from utils_pytest import *


def test_create_scoped_s3_secret(pgduck_conn):
    """Create a scoped S3 secret and verify it appears in duckdb_secrets()."""
    perform_query(
        """
        CREATE OR REPLACE SECRET pglake_vended_test_1 (
            TYPE S3,
            KEY_ID 'AKIAIOSFODNN7EXAMPLE',
            SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            SESSION_TOKEN 'FwoGZXIvYXdzEBYaDHqa0AP',
            REGION 'us-west-2',
            SCOPE 's3://test-bucket/test-prefix/'
        );
        """,
        pgduck_conn,
    )

    secrets = run_query(
        "SELECT name, type, scope FROM duckdb_secrets()",
        pgduck_conn,
    )

    vended = [s for s in secrets if s["name"] == "pglake_vended_test_1"]
    assert len(vended) == 1
    assert vended[0]["type"] == "s3"
    assert "test-bucket/test-prefix" in vended[0]["scope"]

    # Cleanup
    perform_query("DROP SECRET pglake_vended_test_1", pgduck_conn)
    pgduck_conn.rollback()


def test_replace_scoped_s3_secret(pgduck_conn):
    """Verify CREATE OR REPLACE updates an existing secret idempotently."""
    perform_query(
        """
        CREATE OR REPLACE SECRET pglake_vended_test_replace (
            TYPE S3,
            KEY_ID 'OLD_KEY',
            SECRET 'OLD_SECRET',
            SESSION_TOKEN 'OLD_TOKEN',
            REGION 'us-east-1',
            SCOPE 's3://bucket/prefix/'
        );
        """,
        pgduck_conn,
    )

    # Replace with new credentials
    perform_query(
        """
        CREATE OR REPLACE SECRET pglake_vended_test_replace (
            TYPE S3,
            KEY_ID 'NEW_KEY',
            SECRET 'NEW_SECRET',
            SESSION_TOKEN 'NEW_TOKEN',
            REGION 'us-west-2',
            SCOPE 's3://bucket/prefix/'
        );
        """,
        pgduck_conn,
    )

    secrets = run_query(
        "SELECT name, type, scope FROM duckdb_secrets()",
        pgduck_conn,
    )

    vended = [s for s in secrets if s["name"] == "pglake_vended_test_replace"]
    assert len(vended) == 1

    # Cleanup
    perform_query("DROP SECRET pglake_vended_test_replace", pgduck_conn)
    pgduck_conn.rollback()


def test_scoped_secret_does_not_override_default(pgduck_conn):
    """
    A scoped secret should coexist with the default s3 secret.
    The default (s3default) should still be returned for paths outside
    the scoped secret's prefix.
    """
    perform_query(
        """
        CREATE OR REPLACE SECRET pglake_vended_scoped (
            TYPE S3,
            KEY_ID 'SCOPED_KEY',
            SECRET 'SCOPED_SECRET',
            SESSION_TOKEN 'SCOPED_TOKEN',
            REGION 'eu-west-1',
            SCOPE 's3://scoped-bucket/specific-path/'
        );
        """,
        pgduck_conn,
    )

    secrets = run_query(
        "SELECT name, type FROM duckdb_secrets()",
        pgduck_conn,
    )

    names = [s["name"] for s in secrets]
    assert "pglake_vended_scoped" in names
    assert "s3default" in names

    # Cleanup
    perform_query("DROP SECRET pglake_vended_scoped", pgduck_conn)
    pgduck_conn.rollback()


def test_drop_nonexistent_secret_if_exists(pgduck_conn):
    """DROP SECRET IF EXISTS should not error on a missing secret."""
    perform_query(
        "DROP SECRET IF EXISTS pglake_vended_nonexistent",
        pgduck_conn,
    )
    pgduck_conn.rollback()


def test_multiple_scoped_secrets(pgduck_conn):
    """Multiple scoped secrets with different prefixes can coexist."""
    for i in range(3):
        perform_query(
            f"""
            CREATE OR REPLACE SECRET pglake_vended_multi_{i} (
                TYPE S3,
                KEY_ID 'KEY_{i}',
                SECRET 'SECRET_{i}',
                SESSION_TOKEN 'TOKEN_{i}',
                REGION 'us-west-2',
                SCOPE 's3://bucket/table_{i}/'
            );
            """,
            pgduck_conn,
        )

    secrets = run_query(
        "SELECT name FROM duckdb_secrets()",
        pgduck_conn,
    )

    names = [s["name"] for s in secrets]
    for i in range(3):
        assert f"pglake_vended_multi_{i}" in names

    # Cleanup
    for i in range(3):
        perform_query(f"DROP SECRET pglake_vended_multi_{i}", pgduck_conn)
    pgduck_conn.rollback()


def test_secret_with_special_chars_in_credentials(pgduck_conn):
    """
    Credentials containing single quotes or other special characters
    must be properly escaped in the SQL.
    """
    perform_query(
        """
        CREATE OR REPLACE SECRET pglake_vended_special (
            TYPE S3,
            KEY_ID 'KEY_WITH''QUOTE',
            SECRET 'SECRET/WITH+SPECIAL=CHARS',
            SESSION_TOKEN 'TOKEN_WITH''DOUBLE''QUOTES',
            REGION 'us-east-1',
            SCOPE 's3://bucket/special/'
        );
        """,
        pgduck_conn,
    )

    secrets = run_query(
        "SELECT name, type FROM duckdb_secrets()",
        pgduck_conn,
    )

    vended = [s for s in secrets if s["name"] == "pglake_vended_special"]
    assert len(vended) == 1

    # Cleanup
    perform_query("DROP SECRET pglake_vended_special", pgduck_conn)
    pgduck_conn.rollback()
