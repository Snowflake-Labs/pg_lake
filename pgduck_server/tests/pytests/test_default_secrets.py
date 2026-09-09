"""
Tests for the secrets pgduck_server creates at startup.

These pin the provider of each default secret. The GCS default used to be
created with PROVIDER CREDENTIAL_CHAIN, which the aws extension registers for
type gcs and fills from the *AWS* credential chain, so every GCS request was
signed with an AWS key and came back 403. A provider regression is invisible
from the outside until someone points pg_lake at a real GCS bucket, so assert
on it directly.
"""

from utils_pytest import *


def default_secrets(pgduck_conn):
    """Startup secrets, keyed by name. Excludes the ones the test harness adds."""
    rows = run_query(
        "SELECT name, type, provider FROM duckdb_secrets()",
        pgduck_conn,
    )
    return {r["name"]: r for r in rows}


def test_s3_default_uses_aws_credential_chain(pgduck_conn):
    """s3default is meant to follow the AWS credential chain."""
    secrets = default_secrets(pgduck_conn)

    assert "s3default" in secrets
    assert secrets["s3default"]["type"] == "s3"
    assert secrets["s3default"]["provider"] == "credential_chain"


def test_no_default_gcs_secret(pgduck_conn):
    """
    There is no default GCS secret.

    Without one, httpfs sends unauthenticated requests, so public buckets work
    and private ones give a plain 403 until the user creates a secret. Any
    default we add here has to hold a GCP credential, not an AWS one.
    """
    assert "gcsdefault" not in default_secrets(pgduck_conn)


def test_no_gcs_secret_from_the_aws_credential_chain(pgduck_conn):
    """
    No GCS secret anywhere may use the AWS credential chain.

    {gcs, credential_chain} resolves to an AWS key. This catches both the old
    gcsdefault and anyone re-adding the same provider under a different name.
    """
    offenders = [
        name
        for name, s in default_secrets(pgduck_conn).items()
        if s["type"] == "gcs" and s["provider"] == "credential_chain"
    ]

    assert offenders == []
