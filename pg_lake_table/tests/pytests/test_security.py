"""
Security regression tests for pg_lake privilege escalation vulnerabilities
and SQL injection findings.
"""

import json

import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def setup_lake_write_perms(superuser_conn, app_user):
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
        GRANT lake_read_write TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module", autouse=True)
def setup_object_store_settings(superuser_conn):
    """Configure the GUC settings needed for object_store catalog tests."""
    superuser_conn.autocommit = True
    run_command(
        f"ALTER SYSTEM SET pg_lake_iceberg.object_store_catalog_location_prefix = 's3://{TEST_BUCKET}';",
        superuser_conn,
    )
    run_command(
        "ALTER SYSTEM SET pg_lake_iceberg.internal_object_store_catalog_prefix = 'sec_test_fromsf';",
        superuser_conn,
    )
    run_command(
        "ALTER SYSTEM SET pg_lake_iceberg.external_object_store_catalog_prefix = 'sec_test_fromsf';",
        superuser_conn,
    )
    superuser_conn.autocommit = False
    run_command("SELECT pg_reload_conf()", superuser_conn)
    run_command("SELECT pg_sleep(0.1)", superuser_conn)
    superuser_conn.commit()

    yield

    superuser_conn.autocommit = True
    run_command(
        "ALTER SYSTEM RESET pg_lake_iceberg.object_store_catalog_location_prefix;",
        superuser_conn,
    )
    run_command(
        "ALTER SYSTEM RESET pg_lake_iceberg.internal_object_store_catalog_prefix;",
        superuser_conn,
    )
    run_command(
        "ALTER SYSTEM RESET pg_lake_iceberg.external_object_store_catalog_prefix;",
        superuser_conn,
    )
    superuser_conn.autocommit = False
    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.commit()


def _upload_catalog_json(s3, catalog_name, catalog_content_str):
    """
    Upload catalog.json to the path that GetExternalObjectStoreCatalogFilePath builds:
      s3://<bucket>/sec_test_fromsf/catalog/<catalog_name>/catalog.json
    """
    key = f"sec_test_fromsf/catalog/{catalog_name}/catalog.json"
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key=key,
        Body=catalog_content_str.encode("utf-8"),
    )
    return f"s3://{TEST_BUCKET}/{key}"


def test_dollar_quote_injection_in_location_does_not_escalate_privileges(
    s3, pg_conn, superuser_conn, app_user
):
    """
    Regression test for: lake_write role -> superuser RCE via $$ in table location.

    InsertInProgressFileRecordExtended() wrapped the inner INSERT query in
    $$...$$  without escaping $$ present in the path value.  Any $$ in the
    user-supplied location option terminates the outer dollar-quote and lets
    the caller inject arbitrary SQL executed as the pg_lake_engine extension
    owner (superuser) with readOnly=false.

    Fix: pass the inner query as a $1 text parameter via DECLARE_SPI_ARGS /
    SPI_ARG_VALUE / SPI_EXECUTE instead of embedding it in $$...$$  .
    """
    # Precondition: app_user is not a superuser.
    res = run_query(
        f"SELECT rolsuper FROM pg_roles WHERE rolname = '{app_user}'",
        superuser_conn,
    )
    assert not res[0]["rolsuper"], "precondition: app_user must not be superuser"

    # Build the injected location using $loc$ quoting so that the $$ payload
    # is stored as a literal string in PostgreSQL catalogs rather than being
    # interpreted at the CREATE TABLE level.  Only the C code's subsequent
    # $$%s$$ interpolation is the vulnerable step.
    #
    # The chr(39) trick reconstructs the single-quote that quote_literal_cstr
    # opened, keeping the first run_attached() argument syntactically valid so
    # that SPI_execute reaches the second (injected) statement.
    injected_location = (
        f"s3://{TEST_BUCKET}/sec_test"
        f"$$ || chr(39) || $q$,1,false)$q$); ALTER ROLE {app_user} SUPERUSER; -- "
    )

    # CREATE the writable iceberg table as the app_user, then commit so the
    # INSERT below actually exercises InsertInProgressFileRecordExtended.
    # IsSupportedURL() only checks the s3:// prefix so the injected location
    # passes the validator.  Use $loc$...$loc$ to embed the injected payload
    # without breaking the CREATE TABLE SQL string.
    run_command("DROP TABLE IF EXISTS sec_privesc_test", pg_conn)
    pg_conn.commit()
    run_command(
        f"CREATE TABLE sec_privesc_test (id int) USING iceberg "
        f"WITH (location = $loc${injected_location}$loc$)",
        pg_conn,
    )
    pg_conn.commit()

    # Trigger the INSERT path that calls InsertInProgressFileRecordExtended.
    # On patched code this runs the parameterized SPI insert with the location
    # bound as $1; on vulnerable code the $$%s$$ interpolation lets the $$
    # in the location terminate the outer dollar-quote and execute the
    # injected ALTER ROLE.  The data-file write on top of the malformed path
    # may itself fail; we only need to reach the SPI insert before the
    # privilege assertion below.
    run_command(
        "INSERT INTO sec_privesc_test VALUES (1)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # Critical assertion: the app_user must NOT have been escalated.
    res = run_query(
        f"SELECT rolsuper FROM pg_roles WHERE rolname = '{app_user}'",
        superuser_conn,
    )
    assert not res[0]["rolsuper"], (
        "SECURITY REGRESSION: app_user was escalated to superuser via $$ injection "
        "in InsertInProgressFileRecordExtended(). "
        "Ensure the run_attached() call uses parameterized SPI ($1) instead "
        "of $$%s$$ string interpolation."
    )

    run_command("DROP TABLE sec_privesc_test", pg_conn)
    pg_conn.commit()


def test_dollar_sign_in_location_is_stored_as_literal(
    s3, pg_conn, superuser_conn, app_user
):
    """
    After the fix, a $$ sequence in a table location must be stored and
    retrieved as literal characters, not interpreted as a SQL delimiter.
    This confirms that the parameterized SPI path handles the value correctly.
    """
    dollar_location = f"s3://{TEST_BUCKET}/path$$with$$dollars"

    run_command("DROP TABLE IF EXISTS sec_dollar_literal_test", pg_conn)
    pg_conn.commit()
    run_command(
        f"CREATE TABLE sec_dollar_literal_test (id int) USING iceberg "
        f"WITH (location = $loc${dollar_location}$loc$)",
        pg_conn,
    )
    pg_conn.commit()

    res = run_query(
        """
        SELECT ftoptions
        FROM pg_foreign_table
        WHERE ftrelid = 'sec_dollar_literal_test'::regclass
        """,
        pg_conn,
    )
    options_str = str(res[0]["ftoptions"])
    assert (
        "$$" in options_str
    ), "Expected literal $$ to be preserved in stored table options"

    run_command("DROP TABLE sec_dollar_literal_test", pg_conn)
    pg_conn.commit()


def test_catalog_json_dollar_quote_injection_is_blocked(
    pg_conn, superuser_conn, s3, extension
):
    """
    Regression test for: S3 catalog.json -> $$-quoted SQLi runs as extension
    owner; DB-wide read + outbound exfil.

    GetTableMetadataLocationFromExternalObjectStoreCatalog() fetched the raw
    bytes of catalog.json and embedded them directly into:
        SELECT $$<content>$$::jsonb AS j
    Any $$ sequence in the JSON terminates the outer dollar-quote and lets the
    caller inject an arbitrary SELECT executed as the pg_lake_iceberg extension
    owner (typically the bootstrap superuser).

    The crafted catalog.json contains $$ inside a JSON string value.

    On VULNERABLE code:
        catalogContent = '{"tables":[{"table-name":"test$$table",...}]}'
        => SQL becomes: SELECT $${"tables":[{"table-name":"test$$table  ← terminated!
        => SQL parse error or unexpected execution as the extension owner.

    On FIXED code (catalogContent passed as $3 text parameter):
        => $$ is treated as two literal dollar-sign characters in the JSON text.
        => The JSONB cast succeeds; query runs correctly; returns
           "no table found" because no table named 'injected' exists.
    """
    # Craft a catalog.json whose string values contain $$ — valid JSON, but
    # would terminate a $$...$$ dollar-quoted PostgreSQL literal.
    crafted_catalog = json.dumps(
        {
            "tables": [
                {
                    "namespace": "public",
                    "table-name": "test$$table",
                    "metadata-location": "s3://legit/meta.json",
                }
            ]
        }
    )

    _upload_catalog_json(s3, "sec_sqli_test", crafted_catalog)

    # CREATE TABLE with catalog='object_store', read_only=True triggers
    # GetTableMetadataLocationFromExternalObjectStoreCatalog().
    # We look up a table that does NOT exist in the catalog so we get a
    # deterministic "no table found" error on fixed code.
    error = run_command(
        """
        CREATE SCHEMA IF NOT EXISTS sec_sqli_schema;
        CREATE TABLE sec_sqli_schema.ro_tbl()
          USING iceberg
          WITH (
            catalog = 'object_store',
            read_only = True,
            catalog_name = 'sec_sqli_test',
            catalog_namespace = 'public',
            catalog_table_name = 'nonexistent_table'
          )
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # On fixed code: the $$ in the JSON is treated as literal characters;
    # the parameterized query runs without injection and returns "no table found".
    assert error is not None, "Expected an error (no matching table), got success"
    assert (
        "no table found" in str(error).lower() or "does not exist" in str(error).lower()
    ), (
        "SECURITY REGRESSION: expected 'no table found' error from parameterized query "
        f"but got: {error}\n\n"
        "If this is a SQL syntax error, the $$ in catalog.json likely terminated "
        "the outer dollar-quote. "
        "Ensure catalogContent is passed as $3 in DECLARE_SPI_ARGS(3), "
        "not interpolated via $$%s$$."
    )


def test_catalog_json_with_dollar_sequence_parses_correctly(
    pg_conn, superuser_conn, s3, extension
):
    """
    A catalog.json that contains $$ in a string value must be parseable as
    valid JSONB after the fix.  On vulnerable code the $$ terminates the
    dollar-quote before the content is cast, causing either a SQL parse error
    or an injection.  On fixed code the content is passed as $3::jsonb and $$ is
    treated as literal text.
    """
    # This catalog has a valid entry so the function can actually find a table —
    # we just want to confirm the JSONB cast succeeds on content with $$.
    crafted_catalog = json.dumps(
        {
            "tables": [
                {
                    "namespace": "sec$$ns",
                    "table-name": "sec$$tbl",
                    "metadata-location": "s3://sec/meta.json",
                }
            ]
        }
    )

    _upload_catalog_json(s3, "sec_parse_test", crafted_catalog)

    # Look up the table with the literal $$ namespace and table name.
    # Fixed code: lookup succeeds (finds the table) or fails cleanly with
    # "no table found" (wrong namespace/table mismatch) — not a SQL error.
    # Vulnerable code: SQL parse error due to premature $$ termination.
    error = run_command(
        """
        CREATE SCHEMA IF NOT EXISTS sec_parse_schema;
        CREATE TABLE sec_parse_schema.ro_tbl()
          USING iceberg
          WITH (
            catalog = 'object_store',
            read_only = True,
            catalog_name = 'sec_parse_test',
            catalog_namespace = 'sec$$ns',
            catalog_table_name = 'sec$$tbl'
          )
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # Accept either "found and attempted metadata fetch" (URL error) or
    # "no table found" — both indicate the JSONB cast succeeded without injection.
    # Reject any PostgreSQL syntax/parse error, which would indicate injection.
    if error is not None:
        err_lower = str(error).lower()
        assert "syntax error" not in err_lower and "parse error" not in err_lower, (
            "SECURITY REGRESSION: SQL syntax/parse error while parsing catalog.json "
            f"with $$ in string values: {error}\n"
            "This indicates $$ in catalogContent terminated the $$...$$ dollar-quote. "
            "Use parameterized SPI $3 for catalogContent."
        )


def test_catalog_json_dollar_quote_roundtrip_through_writer(
    superuser_conn, s3, extension, installcheck
):
    """
    End-to-end coverage of the catalog.json injection: instead of a hand-crafted
    catalog.json, drive the write path through pg_lake's own writer.

    Create a writable object_store table whose schema and table name contain
    $$, insert rows, wait for the catalog.json to be pushed, then create a
    read-only object_store table pointing at it from another database and
    assert the rows round-trip.

    On vulnerable code, the read-only side errors with a SQL syntax / parse
    error when the lookup query embeds the $$-bearing catalog.json into
    SELECT $$<content>$$::jsonb.  On fixed code (catalogContent passed as $3
    text parameter) the rows come back unchanged.

    Modeled on test_object_store_catalog.py::test_iceberg_multiple_dbs, which
    only uses a single $ in identifiers and would not catch this bug.
    """
    if installcheck:
        return

    src_db = "sec_dollar_src_db"
    dst_db = "sec_dollar_dst_db"
    schema_name = "sec$$schema"
    table_name = "sec$$table"

    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE IF EXISTS {src_db} WITH (FORCE);", superuser_conn)
    run_command(f"DROP DATABASE IF EXISTS {dst_db} WITH (FORCE);", superuser_conn)
    run_command(f"CREATE DATABASE {src_db};", superuser_conn)
    run_command(f"CREATE DATABASE {dst_db};", superuser_conn)
    superuser_conn.autocommit = False

    src_conn = open_pg_conn_to_db(src_db)
    dst_conn = open_pg_conn_to_db(dst_db)
    try:
        for conn in (src_conn, dst_conn):
            run_command("CREATE EXTENSION pg_lake CASCADE", conn)
            run_command(
                f"SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}'",
                conn,
            )
            conn.commit()

        # Writable table with $$ in schema and table name.  The writer pushes
        # this name into catalog.json verbatim, which is exactly the input
        # that broke the vulnerable lookup query.
        run_command(f'CREATE SCHEMA "{schema_name}";', src_conn)
        run_command(
            f'CREATE TABLE "{schema_name}"."{table_name}"(a int) '
            "USING iceberg WITH (catalog='object_store');",
            src_conn,
        )
        run_command(
            f'INSERT INTO "{schema_name}"."{table_name}" VALUES (1), (2), (3);',
            src_conn,
        )
        src_conn.commit()
        run_command("SELECT lake_iceberg.force_push_object_store_catalog()", src_conn)
        src_conn.commit()
        wait_until_object_store_writable_table_pushed(src_conn, schema_name, table_name)

        # Read-only table in the other database, going through the lookup path
        # that embeds catalog.json into a $$-quoted SQL literal on vulnerable
        # code.  On fixed code, the rows round-trip.
        run_command(f'CREATE SCHEMA "{schema_name}";', dst_conn)
        run_command(
            f'CREATE TABLE "{schema_name}"."{table_name}"() '
            "USING iceberg WITH ("
            "catalog='object_store', read_only=True, "
            f"catalog_name='{src_db}'"
            ");",
            dst_conn,
        )

        rows = run_query(
            f'SELECT a FROM "{schema_name}"."{table_name}" ORDER BY a',
            dst_conn,
        )
        assert [r[0] for r in rows] == [1, 2, 3], (
            "SECURITY REGRESSION: rows did not round-trip through a $$-named "
            "object_store catalog.  On vulnerable code the "
            "read side typically fails with a SQL syntax / parse error because "
            "$$ in catalog.json terminates the outer dollar-quote."
        )
    finally:
        src_conn.close()
        dst_conn.close()
        superuser_conn.autocommit = True
        run_command(f"DROP DATABASE IF EXISTS {src_db} WITH (FORCE);", superuser_conn)
        run_command(f"DROP DATABASE IF EXISTS {dst_db} WITH (FORCE);", superuser_conn)
        superuser_conn.autocommit = False
