"""End-to-end tests of pg_lake_cortex against a real Snowflake account.

Skipped unless the account is configured in the environment, with the same
variables as the pg_lake_snowflake end-to-end tests. The role needs the
SNOWFLAKE.CORTEX_USER database role and the account access to the models.
"""

import os

import pytest
from utils_pytest import *

account = os.getenv("SNOWFLAKE_ACCOUNT")
token = os.getenv("SNOWFLAKE_TOKEN")
database = os.getenv("SNOWFLAKE_DATABASE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

pytestmark = pytest.mark.skipif(
    not (account and token and database and warehouse),
    reason="no Snowflake account configured in the environment",
)


@pytest.fixture(scope="module")
def cortex(postgres):
    conn = open_pg_conn()
    conn.autocommit = True

    options = [
        f"account '{account}'",
        f"database '{database}'",
        f"warehouse '{warehouse}'",
    ]
    if os.getenv("SNOWFLAKE_ACCOUNT_URL"):
        options.append(f"account_url '{os.getenv('SNOWFLAKE_ACCOUNT_URL')}'")
    if os.getenv("SNOWFLAKE_ROLE"):
        options.append(f"role '{os.getenv('SNOWFLAKE_ROLE')}'")

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_cortex CASCADE;
        DROP SERVER IF EXISTS e2e_cortex CASCADE;
        CREATE SERVER e2e_cortex FOREIGN DATA WRAPPER snowflake OPTIONS ({", ".join(options)});
        CREATE USER MAPPING FOR CURRENT_USER SERVER e2e_cortex OPTIONS (token '{token}');
        SET lake_cortex.default_server TO e2e_cortex;
        """,
        conn,
    )

    yield conn

    run_command("DROP SERVER e2e_cortex CASCADE", conn)
    conn.close()


def scalar(conn, query):
    return run_query(query, conn)[0][0]


def test_functions(cortex):
    assert scalar(
        cortex, "SELECT lake_cortex.complete('llama3.1-8b', $$Reply with: it's \\ok$$)"
    )
    assert (
        scalar(cortex, "SELECT lake_cortex.sentiment('I love this product')")
        == "positive"
    )
    assert scalar(
        cortex, "SELECT lake_cortex.classify('I love it', ARRAY['praise','complaint'])"
    ) == ["praise"]
    assert (
        scalar(
            cortex, "SELECT lake_cortex.filter('Is this text positive?', 'I love it')"
        )
        is True
    )
    assert scalar(cortex, "SELECT lake_cortex.translate('I love it', 'en', 'de')")
    assert (
        len(
            scalar(
                cortex, "SELECT lake_cortex.embed('snowflake-arctic-embed-m-v1.5', 'x')"
            )
        )
        == 768
    )
    assert (
        scalar(cortex, "SELECT lake_cortex.similarity('I love it', 'I like it')") > 0.5
    )
    assert "[NAME]" in scalar(cortex, "SELECT lake_cortex.redact('Call John Smith')")
    assert (
        scalar(
            cortex,
            """SELECT lake_cortex.complete_json('llama3.1-8b', 'Is the sky blue?',
             '{"type":"json","schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}}')""",
        )
        == {"ok": True}
    )
