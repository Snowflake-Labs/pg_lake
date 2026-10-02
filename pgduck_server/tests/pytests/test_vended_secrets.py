"""
Tests for vended credential secrets in pgduck_server.

Validates that DuckDB scoped secrets can be created, replaced, and dropped
via the same SQL patterns used by pg_lake's PushVendedSecretToPGDuck.
"""

from utils_pytest import *

from datetime import datetime, timedelta, timezone

from azure.storage.blob import ContainerSasPermissions, generate_container_sas


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
    """DROP SECRET IF EXISTS should not error on a missing secret.

    Completing without an exception is the assertion: secret cleanup is
    best-effort and runs against secrets that may already be gone.
    """
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


_AZURITE = dict(part.split("=", 1) for part in AZURITE_CONNECTION_STRING.split(";"))


def _azurite_container_sas(signature_suffix=""):
    """A read/list SAS for the test container, as a catalog would vend it."""
    sas = generate_container_sas(
        account_name=_AZURITE["AccountName"],
        container_name=TEST_BUCKET,
        account_key=_AZURITE["AccountKey"],
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    return sas + signature_suffix


def _create_azure_sas_secret(pgduck_conn, name, scope, sas):
    connection_string = (
        f"AccountName={_AZURITE['AccountName']};"
        f"BlobEndpoint={_AZURITE['BlobEndpoint']};"
        f"DfsEndpoint={_AZURITE['BlobEndpoint']};"
        f"SharedAccessSignature={sas}"
    )
    perform_query(
        f"""
        CREATE OR REPLACE SECRET {name} (
            TYPE AZURE,
            CONNECTION_STRING '{connection_string}',
            SCOPE '{scope}'
        );
        """,
        pgduck_conn,
    )


def test_scoped_azure_sas_secret_reads_blob(azure, pgduck_conn):
    """
    A vended ADLS credential is a SAS token.  DuckDB's Azure secret has no SAS
    parameter, but hands CONNECTION_STRING to the Azure SDK, which accepts
    SharedAccessSignature there.
    """
    prefix = "vended_sas_ok"
    azure.upload_blob(name=f"{prefix}/data.csv", data=b"x\n1\n2\n3\n", overwrite=True)

    _create_azure_sas_secret(
        pgduck_conn,
        "pglake_vended_azure_ok",
        f"az://{TEST_BUCKET}/{prefix}/",
        _azurite_container_sas(),
    )
    try:
        result = run_query(
            f"SELECT count(*) AS n FROM read_csv('az://{TEST_BUCKET}/{prefix}/data.csv')",
            pgduck_conn,
        )
        assert result[0]["n"] == 3
    finally:
        pgduck_conn.rollback()
        perform_query("DROP SECRET IF EXISTS pglake_vended_azure_ok", pgduck_conn)
        pgduck_conn.rollback()


@pytest.mark.parametrize(
    "location",
    [
        pytest.param(f"az://{TEST_BUCKET}", id="az"),
        pytest.param(
            f"abfss://{TEST_BUCKET}@{_AZURITE['AccountName']}.dfs.core.windows.net",
            id="abfss",
        ),
    ],
)
def test_scoped_azure_sas_secret_is_the_one_used(azure, pgduck_conn, location):
    """
    The static aztest secret can read the whole emulator account, so a read
    succeeding proves nothing on its own.  A scoped secret carrying a SAS with
    a broken signature must make the read fail: that is what shows the scoped
    secret, and its SAS, are what the read went through, through the blob
    client for az:// and the DataLake one for abfss://.
    """
    prefix = "vended_sas_tampered"
    azure.upload_blob(name=f"{prefix}/data.csv", data=b"x\n1\n", overwrite=True)

    _create_azure_sas_secret(
        pgduck_conn,
        "pglake_vended_azure_bad",
        f"{location}/{prefix}/",
        _azurite_container_sas(signature_suffix="tampered"),
    )
    try:
        error = run_query(
            f"SELECT count(*) FROM read_csv('{location}/{prefix}/data.csv')",
            pgduck_conn,
            raise_error=False,
        )
        assert "AuthorizationFailure" in str(error), error
    finally:
        pgduck_conn.rollback()
        perform_query("DROP SECRET IF EXISTS pglake_vended_azure_bad", pgduck_conn)
        pgduck_conn.rollback()


def test_scoped_azure_sas_secret_reads_abfss(azure, pgduck_conn):
    """
    abfss:// goes through the DataLake client rather than the blob one, and a
    fully qualified URL additionally requires AccountName in the connection
    string to match the account the URL names.
    """
    prefix = "vended_sas_abfss"
    azure.upload_blob(name=f"{prefix}/data.csv", data=b"x\n1\n2\n", overwrite=True)

    location = f"abfss://{TEST_BUCKET}@{_AZURITE['AccountName']}.dfs.core.windows.net"
    _create_azure_sas_secret(
        pgduck_conn,
        "pglake_vended_azure_abfss",
        f"{location}/{prefix}/",
        _azurite_container_sas(),
    )
    try:
        result = run_query(
            f"SELECT count(*) AS n FROM read_csv('{location}/{prefix}/data.csv')",
            pgduck_conn,
        )
        assert result[0]["n"] == 2
    finally:
        pgduck_conn.rollback()
        perform_query("DROP SECRET IF EXISTS pglake_vended_azure_abfss", pgduck_conn)
        pgduck_conn.rollback()
