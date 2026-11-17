"""End-to-end tests of pg_lake_cortex against a real Snowflake account.

Skipped unless the account is configured in the environment, with the same
variables as the pg_lake_snowflake end-to-end tests. SNOWFLAKE_USER and
SNOWFLAKE_PRIVATE_KEY_PATH may replace SNOWFLAKE_TOKEN to use key-pair
authentication. No warehouse is needed. The default role of the user needs the
SNOWFLAKE.CORTEX_USER database role and the account access to the models.
"""

import os

import pytest
from utils_pytest import *

account = os.getenv("SNOWFLAKE_ACCOUNT")
token = os.getenv("SNOWFLAKE_TOKEN")
user = os.getenv("SNOWFLAKE_USER")
private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

pytestmark = pytest.mark.skipif(
    not (account and (token or (user and private_key_path))),
    reason="no Snowflake account configured in the environment",
)


@pytest.fixture(scope="module")
def cortex(postgres):
    conn = open_pg_conn()
    conn.autocommit = True

    options = [f"account '{account}'"]
    if os.getenv("SNOWFLAKE_ACCOUNT_URL"):
        options.append(f"account_url '{os.getenv('SNOWFLAKE_ACCOUNT_URL')}'")

    if token:
        mapping = f"token '{token}'"
    else:
        mapping = f"user '{user}', private_key_path '{private_key_path}'"

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_cortex CASCADE;
        DROP SERVER IF EXISTS e2e_cortex CASCADE;
        CREATE SERVER e2e_cortex FOREIGN DATA WRAPPER snowflake OPTIONS ({", ".join(options)});
        CREATE USER MAPPING FOR CURRENT_USER SERVER e2e_cortex OPTIONS ({mapping});
        SET pg_lake_cortex.default_server TO e2e_cortex;
        """,
        conn,
    )

    yield conn

    run_command("DROP SERVER e2e_cortex CASCADE", conn)
    conn.close()


def scalar(conn, query):
    return run_query(query, conn)[0][0]


def test_complete(cortex):
    assert scalar(
        cortex, "SELECT cortex.complete('llama3.1-8b', $$Reply with: it's \\ok$$)"
    )
    assert scalar(
        cortex,
        """SELECT cortex.complete('llama3.1-8b', 'Say hi', '{"temperature":0,"max_tokens":10}')""",
    )


def test_complete_json(cortex):
    for model in ["llama3.1-70b", "claude-sonnet-4-5"]:
        assert (
            scalar(
                cortex,
                f"""SELECT cortex.complete_json('{model}', 'Is the sky blue?',
                 '{{"type":"json","schema":{{"type":"object","properties":{{"ok":{{"type":"boolean"}}}},"required":["ok"]}}}}')""",
            )
            == {"ok": True}
        )


def test_embed(cortex):
    assert (
        len(scalar(cortex, "SELECT cortex.embed('snowflake-arctic-embed-m-v1.5', 'x')"))
        == 768
    )

    rows = run_query(
        """SELECT index, cardinality(embedding)
           FROM cortex.embed_batch('snowflake-arctic-embed-m-v1.5',
                                   ARRAY(SELECT 'text ' || i FROM generate_series(1, 50) i))
           ORDER BY index""",
        cortex,
    )
    assert [tuple(row) for row in rows] == [(i, 768) for i in range(1, 51)]


def test_similarity(cortex):
    close = scalar(cortex, "SELECT cortex.similarity('I love it', 'I like it')")
    far = scalar(
        cortex, "SELECT cortex.similarity('I love it', 'The invoice is overdue')"
    )
    assert close > far


def test_error(cortex):
    error = run_query(
        "SELECT cortex.embed('no-such-model', 'x')", cortex, raise_error=False
    )
    assert "Cortex request failed" in error
