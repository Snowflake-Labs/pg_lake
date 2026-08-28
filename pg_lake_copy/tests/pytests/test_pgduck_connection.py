"""Regression test: releasing the same pgduck_server connection twice.

ReleasePGDuckConnection() used to dereference the entry returned by
HASH_REMOVE, and on the !found path it closed the connection through the
caller's own handle. A second release therefore passed the same PGconn to
PQfinish() twice, which aborts the backend and restarts the whole cluster,
or closed a newer connection that had reused the hash entry.

See https://github.com/Snowflake-Labs/pg_lake/issues/557.
"""

import pytest
from utils_pytest import *


@pytest.fixture()
def release_pgduck_connection_twice(superuser_conn):
    run_command(
        """
        CREATE OR REPLACE FUNCTION release_pgduck_connection_twice()
        RETURNS void LANGUAGE C
        AS 'pg_lake_copy', $$release_pgduck_connection_twice$$;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command("DROP FUNCTION release_pgduck_connection_twice()", superuser_conn)
    superuser_conn.commit()


def test_double_release_is_a_noop(superuser_conn, release_pgduck_connection_twice):
    superuser_conn.notices.clear()

    run_command("SELECT release_pgduck_connection_twice()", superuser_conn)
    superuser_conn.commit()

    # The second release must recognize the connection as already released
    # instead of taking the untracked path that closed the handle again.
    assert not any(
        "untracked connection" in notice for notice in superuser_conn.notices
    )

    # The first release must still have removed the hash entry, or the
    # end-of-transaction sweep would have found a connection to close.
    assert not any(
        "connections on transaction commit" in notice
        for notice in superuser_conn.notices
    )

    # The backend is still alive, so PQfinish() was not called twice.
    assert run_query("SELECT 1 AS ok", superuser_conn)[0]["ok"] == 1
