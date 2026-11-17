"""Tests for pg_lake_cortex against the mock of the Snowflake APIs.

The mock records the body of every Cortex REST request and answers from a
route per path, which lets the tests assert on the request each function sends
and on how the JSON answer becomes a value.
"""

import pytest
from utils_pytest import *

from mock_snowflake import MockSnowflake

CHAT = "/api/v2/cortex/v1/chat/completions"
EMBED = "/api/v2/cortex/inference:embed"


@pytest.fixture(scope="function")
def cortex(postgres):
    mock = MockSnowflake()
    account_url = mock.start()

    conn = open_pg_conn()
    conn.autocommit = True

    # no warehouse: Cortex REST requests do not need one
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_cortex CASCADE;
        SET pg_lake_snowflake.allow_plain_http TO on;
        CREATE SERVER mock_sf FOREIGN DATA WRAPPER snowflake
          OPTIONS (account 'mock-account', account_url '{account_url}');
        CREATE USER MAPPING FOR CURRENT_USER SERVER mock_sf
          OPTIONS (token 'mock-token');
        SET pg_lake_cortex.default_server TO mock_sf;
        """,
        conn,
    )

    yield mock, conn

    run_command("DROP SERVER IF EXISTS mock_sf CASCADE", conn)
    conn.close()
    mock.stop()


def chat_answer(content):
    return {
        "choices": [{"index": 0, "message": {"role": "assistant", "content": content}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


def embed_answer(vectors):
    """Answer an embed request with one of vectors per input text, as Cortex does."""

    def answer(body):
        return {
            "object": "list",
            "data": [
                {"object": "embedding", "index": i, "embedding": [vectors[text]]}
                for i, text in enumerate(body["text"])
            ],
            "usage": {"total_tokens": len(body["text"])},
        }

    return answer


def scalar(conn, query):
    return run_query(query, conn)[0][0]


def test_complete(cortex):
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer("Hello!"))

    assert scalar(conn, "SELECT cortex.complete('llama3.1-8b', 'Say hi')") == "Hello!"
    assert mock.cortex_requests == [
        (
            CHAT,
            {
                "model": "llama3.1-8b",
                "messages": [{"role": "user", "content": "Say hi"}],
            },
        )
    ]
    assert mock.statements == []


def test_text_is_sent_verbatim(cortex):
    """Quotes and backslashes travel inside JSON, so nothing is escaped by hand."""
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer("ok"))

    run_query(r"""SELECT cortex.complete('m', 'it''s \'') || 1)) --')""", conn)
    assert (
        mock.cortex_requests[-1][1]["messages"][0]["content"] == r"it's \') || 1)) --"
    )


def test_model_parameters(cortex):
    """max_tokens is renamed to what the REST API calls it, the rest is passed on."""
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer("ok"))

    run_query(
        """SELECT cortex.complete('m', 'x', '{"temperature":0,"top_p":0.5,"max_tokens":20}')""",
        conn,
    )
    body = mock.cortex_requests[-1][1]
    assert body["temperature"] == 0
    assert body["top_p"] == 0.5
    assert body["max_completion_tokens"] == 20
    assert "max_tokens" not in body


def test_complete_json(cortex):
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer('{"ok": true}'))

    result = scalar(
        conn,
        """SELECT cortex.complete_json('m', 'Is the sky blue?',
             '{"type":"json","schema":{"type":"object"}}')""",
    )
    assert result == {"ok": True}
    assert mock.cortex_requests[-1][1]["response_format"] == {
        "type": "json_schema",
        "json_schema": {"name": "response", "schema": {"type": "object"}},
    }


def test_complete_json_passes_json_schema_on(cortex):
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer("{}"))

    run_query(
        """SELECT cortex.complete_json('m', 'x',
             '{"type":"json_schema","json_schema":{"name":"r","schema":{}}}')""",
        conn,
    )
    assert mock.cortex_requests[-1][1]["response_format"] == {
        "type": "json_schema",
        "json_schema": {"name": "r", "schema": {}},
    }


def test_complete_json_that_is_not_json(cortex):
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer("Sure! Here is your JSON"))

    error = run_query(
        """SELECT cortex.complete_json('m', 'x', '{"type":"json","schema":{}}')""",
        conn,
        raise_error=False,
    )
    assert "answered with something other than JSON" in error


def test_embed(cortex):
    mock, conn = cortex
    mock.cortex_route(EMBED, embed_answer({"x": [0.5, -1.25, 3e-2]}))

    assert scalar(conn, "SELECT cortex.embed('e5-base-v2', 'x')") == [
        0.5,
        -1.25,
        0.03,
    ]
    assert mock.cortex_requests == [(EMBED, {"model": "e5-base-v2", "text": ["x"]})]


def test_embed_batch_is_one_request(cortex):
    mock, conn = cortex
    mock.cortex_route(EMBED, embed_answer({"a": [1.0], "b": [2.0], "c": [3.0]}))

    rows = run_query(
        "SELECT index, embedding FROM cortex.embed_batch('m', ARRAY['a','b','c']) ORDER BY index",
        conn,
    )
    assert [tuple(row) for row in rows] == [(1, [1.0]), (2, [2.0]), (3, [3.0])]
    assert len(mock.cortex_requests) == 1


def test_similarity(cortex):
    mock, conn = cortex
    mock.cortex_route(EMBED, embed_answer({"a": [1.0, 0.0], "b": [1.0, 1.0]}))

    assert scalar(conn, "SELECT cortex.similarity('a', 'b')") == pytest.approx(
        0.70710678
    )
    assert mock.cortex_requests == [
        (EMBED, {"model": "snowflake-arctic-embed-m-v1.5", "text": ["a", "b"]})
    ]


def test_null_input_skips_round_trip(cortex):
    mock, conn = cortex

    assert scalar(conn, "SELECT cortex.complete('m', NULL)") is None
    assert scalar(conn, "SELECT cortex.embed('m', NULL)") is None
    assert scalar(conn, "SELECT cortex.similarity('a', NULL)") is None
    assert (
        run_query("SELECT * FROM cortex.embed_batch('m', ARRAY[]::text[])", conn) == []
    )
    assert mock.cortex_requests == []


def test_explicit_server_and_missing_server(cortex):
    mock, conn = cortex
    mock.cortex_route(CHAT, chat_answer("hi"))

    run_command("RESET pg_lake_cortex.default_server", conn)
    assert (
        scalar(conn, "SELECT cortex.complete('m', 'x', server_name => 'mock_sf')")
        == "hi"
    )

    error = run_query("SELECT cortex.complete('m', 'x')", conn, raise_error=False)
    assert "no Snowflake server to run Cortex functions on" in error


def test_cortex_error_is_reported(cortex):
    mock, conn = cortex
    mock.cortex_route(
        EMBED,
        {
            "code": "399304",
            "message": '{"message":"unknown model \\"nope\\""}\n',
            "request_id": "r1",
        },
        status=400,
    )

    error = run_query("SELECT cortex.embed('nope', 'x')", conn, raise_error=False)
    assert 'Cortex request failed: unknown model "nope"' in error


def test_only_cortex_paths(cortex):
    mock, conn = cortex

    for path in [
        "/api/v2/statements",
        "/api/v2/cortex/../statements",
        "/api/v2/cortex/x?y",
    ]:
        error = run_query(
            f"SELECT cortex.request('{path}', '{{}}')", conn, raise_error=False
        )
        assert "is not a Cortex REST API path" in error

    assert mock.cortex_requests == []
    assert mock.statements == []


def test_request(cortex):
    mock, conn = cortex
    mock.cortex_route("/api/v2/cortex/v1/messages", {"content": [{"text": "hi"}]})

    assert (
        scalar(
            conn,
            """SELECT cortex.request('/api/v2/cortex/v1/messages',
             '{"model":"claude-sonnet-4-5","max_tokens":10,"messages":[]}')""",
        )
        == {"content": [{"text": "hi"}]}
    )
