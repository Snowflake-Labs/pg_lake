import io

import psycopg2
from utils_pytest import *

BINARY_COPY_SIGNATURE = b"PGCOPY\n\377\r\n\0"


def _transmit(query):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    try:
        output = io.BytesIO()
        conn.cursor().copy_expert(query, output)
        return output.getvalue()
    finally:
        conn.close()


def test_binary_transmit_plain_blob(pgduck_server):
    output = _transmit("transmit binary (17) SELECT '\\x01\\x02'::blob")
    assert output.startswith(BINARY_COPY_SIGNATURE)


def test_binary_transmit_aliased_blob_falls_back(pgduck_server):
    """WKB_BLOB has a different text form than its bytes, so it uses CSV."""
    query = "SELECT ST_AsWKB(ST_Point(1, 2))::WKB_BLOB"

    binary_output = _transmit("transmit binary (17) " + query)
    csv_output = _transmit("transmit " + query)

    assert not binary_output.startswith(BINARY_COPY_SIGNATURE)
    assert binary_output == csv_output
