"""Tests for pg_lake_cortex against the mock of the Snowflake SQL API.

The mock answers with what TO_JSON would, which lets the tests assert on the
expression each function sends and on how the JSON answer becomes a value.
"""

import json

import pytest
from utils_pytest import *

from mock_snowflake import MockSnowflake, column


@pytest.fixture(scope="function")
def cortex(postgres):
    mock = MockSnowflake()
    account_url = mock.start()

    conn = open_pg_conn()
    conn.autocommit = True

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_cortex CASCADE;
        SET pg_lake_snowflake.allow_plain_http TO on;
        CREATE SERVER mock_sf FOREIGN DATA WRAPPER snowflake
          OPTIONS (account 'mock-account', account_url '{account_url}',
                   database 'MOCKDB', schema_name 'PUBLIC', warehouse 'MOCK_WH');
        CREATE USER MAPPING FOR CURRENT_USER SERVER mock_sf
          OPTIONS (token 'mock-token');
        SET lake_cortex.default_server TO mock_sf;
        """,
        conn,
    )

    yield mock, conn

    run_command("DROP SERVER IF EXISTS mock_sf CASCADE", conn)
    conn.close()
    mock.stop()


def answer(mock, function, value):
    """Answer statements calling function with value, as TO_JSON would."""
    mock.route(function + "(", [column("RESULT", "text")], [[[json.dumps(value)]]])


def scalar(conn, query):
    return run_query(query, conn)[0][0]


def test_complete(cortex):
    mock, conn = cortex
    answer(mock, "AI_COMPLETE", "Hello!")

    assert (
        scalar(conn, "SELECT lake_cortex.complete('llama3.1-8b', 'Say hi')") == "Hello!"
    )
    assert mock.last_statement() == (
        "SELECT TO_JSON(TO_VARIANT(AI_COMPLETE(model => 'llama3.1-8b', "
        "prompt => 'Say hi')))"
    )


def test_literals_are_escaped(cortex):
    """Quotes and backslashes cannot end the literal they are in."""
    mock, conn = cortex
    answer(mock, "AI_COMPLETE", "ok")

    run_query(r"""SELECT lake_cortex.complete('m', 'it''s \'') || 1)) --')""", conn)
    assert r"prompt => 'it''s \\'') || 1)) --')))" in mock.last_statement()


def test_complete_with_parameters(cortex):
    mock, conn = cortex
    answer(mock, "AI_COMPLETE", {"ok": True})

    result = scalar(
        conn,
        """SELECT lake_cortex.complete_json('m', 'Is the sky blue?',
             '{"type":"json","schema":{"type":"object"}}',
             '{"temperature":0}')""",
    )
    assert result == {"ok": True}
    statement = mock.last_statement()
    assert "model_parameters => PARSE_JSON('{\"temperature\": 0}')" in statement
    assert "response_format => PARSE_JSON(" in statement


def test_classify(cortex):
    mock, conn = cortex
    answer(mock, "AI_CLASSIFY", {"labels": ["praise"]})

    assert scalar(
        conn, "SELECT lake_cortex.classify('I love it', ARRAY['praise','complaint'])"
    ) == ["praise"]
    assert "AI_CLASSIFY('I love it', PARSE_JSON('[\"praise\", \"complaint\"]'))" in (
        mock.last_statement()
    )


def test_filter_sentiment_similarity(cortex):
    mock, conn = cortex
    answer(mock, "AI_FILTER", True)
    answer(
        mock,
        "AI_SENTIMENT",
        {"categories": [{"name": "overall", "sentiment": "positive"}]},
    )
    answer(mock, "AI_SIMILARITY", 0.87)
    answer(mock, "AI_COUNT_TOKENS", 14)

    assert (
        scalar(conn, "SELECT lake_cortex.filter('Is this positive?', 'I love it')")
        is True
    )
    assert scalar(conn, "SELECT lake_cortex.sentiment('I love it')") == "positive"
    assert scalar(conn, "SELECT lake_cortex.similarity('a', 'b')") == pytest.approx(
        0.87
    )
    assert (
        scalar(conn, "SELECT lake_cortex.count_tokens('ai_complete', 'm', 'x')") == 14
    )


def test_text_functions(cortex):
    mock, conn = cortex
    answer(mock, "AI_TRANSLATE", "Ich liebe es")
    answer(mock, "SUMMARIZE", "short")
    answer(mock, "AI_REDACT", "Call [NAME]")
    answer(mock, "AI_EXTRACT", {"error": None, "response": {"sentiment": "positive"}})

    assert (
        scalar(conn, "SELECT lake_cortex.translate('I love it', NULL, 'de')")
        == "Ich liebe es"
    )
    assert "AI_TRANSLATE('I love it', '', 'de')" in mock.last_statement()
    assert scalar(conn, "SELECT lake_cortex.summarize('long')") == "short"
    assert scalar(conn, "SELECT lake_cortex.redact('Call John')") == "Call [NAME]"
    assert scalar(
        conn, """SELECT lake_cortex.extract('I love it', '{"sentiment":"?"}')"""
    ) == {"sentiment": "positive"}


def test_embed(cortex):
    mock, conn = cortex
    answer(mock, "AI_EMBED", [0.5, -1.25, 3e-2])

    assert scalar(conn, "SELECT lake_cortex.embed('e5-base-v2', 'x')") == [
        0.5,
        -1.25,
        0.03,
    ]
    assert "AI_EMBED('e5-base-v2', 'x')::ARRAY" in mock.last_statement()


def test_null_input_skips_round_trip(cortex):
    mock, conn = cortex

    assert scalar(conn, "SELECT lake_cortex.complete('m', NULL)") is None
    assert scalar(conn, "SELECT lake_cortex.sentiment(NULL)") is None
    assert mock.statements == []


def test_explicit_server_and_missing_server(cortex):
    mock, conn = cortex
    answer(
        mock,
        "AI_SENTIMENT",
        {"categories": [{"name": "overall", "sentiment": "neutral"}]},
    )

    run_command("RESET lake_cortex.default_server", conn)
    assert (
        scalar(conn, "SELECT lake_cortex.sentiment('meh', server_name => 'mock_sf')")
        == "neutral"
    )

    error = run_query("SELECT lake_cortex.sentiment('meh')", conn, raise_error=False)
    assert "no Snowflake server to run Cortex functions on" in error


def test_snowflake_error_is_reported(cortex):
    mock, conn = cortex
    mock.route(
        "AI_COMPLETE(",
        [],
        [],
        status=422,
        error={"code": "001088", "sqlState": "42601", "message": "unknown model"},
    )

    error = run_query(
        "SELECT lake_cortex.complete('nope', 'x')", conn, raise_error=False
    )
    assert "unknown model" in error
