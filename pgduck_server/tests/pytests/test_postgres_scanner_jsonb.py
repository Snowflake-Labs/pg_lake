"""Verify postgres_scan normalizes PostgreSQL JSON values for VARIANT."""

import pytest
from utils_pytest import *


def _connstr():
    return (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )


@pytest.fixture(scope="module")
def pg_json_table(postgres):
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_json_types")
    cur.execute("CREATE TABLE scanner_json_types (j json, jb jsonb)")
    cur.execute(
        """
        INSERT INTO scanner_json_types VALUES (
            '{"kind":"json","value":41}',
            '{"kind":"jsonb","nested":{"value":42}}'
        )
        """
    )
    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_json_types")
    cur.close()
    conn.close()


def test_json_and_jsonb_can_cast_to_variant(pg_json_table, pgduck_conn):
    scan = f"postgres_scan('{_connstr()}', 'public', 'scanner_json_types')"
    rows = perform_query_on_cursor(
        f"""
        SELECT typeof(j),
               typeof(jb),
               CAST(
                   variant_extract(CAST(j AS VARIANT), 'value')
                   AS INT
               ),
               CAST(
                   json_extract(
                       CAST(CAST(CAST(jb AS JSON) AS VARIANT) AS JSON),
                       '$.nested.value'
                   ) AS INT
               )
        FROM {scan}
        """,
        pgduck_conn,
    )
    assert rows == [("JSON", "VARCHAR", 41, 42)]
