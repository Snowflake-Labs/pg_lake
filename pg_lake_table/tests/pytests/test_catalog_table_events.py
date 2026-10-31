"""
Tests for the "pg_lake_catalog: " lines about Iceberg tables: table_created
and table_dropped.

The lines go to the server log only, and they must not name the table, its
schema or its location, so they report the relation OID instead.
"""

import os
import uuid

from utils_pytest import *


def _logfile():
    return f"{server_params.PG_DIR}/logfile"


def _log_offset():
    return os.path.getsize(_logfile())


def _catalog_lines(offset):
    with open(_logfile()) as f:
        f.seek(offset)
        delta = f.read()

    return [line for line in delta.splitlines() if "pg_lake_catalog: " in line]


def _events(offset, event):
    events = []
    for line in _catalog_lines(offset):
        words = line.split("pg_lake_catalog: ", 1)[1].split()
        if words[0] == event:
            events.append(dict(word.split("=", 1) for word in words[1:]))
    return events


def _relid(conn, name):
    return str(run_query(f"SELECT '{name}'::regclass::oid", conn)[0][0])


def test_table_created_and_dropped_are_logged_at_commit(
    s3, pg_conn, extension, installcheck
):
    if installcheck:
        return

    schema = f"catlog_{uuid.uuid4().hex[:8]}"
    location = f"s3://{TEST_BUCKET}/{schema}"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    offset = _log_offset()

    run_command(
        f"CREATE TABLE {schema}.kept (id int) USING iceberg "
        f"WITH (location = '{location}/kept')",
        pg_conn,
    )
    relid = _relid(pg_conn, f"{schema}.kept")

    assert _events(offset, "table_created") == [], "logged before commit"

    pg_conn.commit()

    created = _events(offset, "table_created")
    assert created == [{"catalog": "postgres", "relid": relid}]

    run_command(f"DROP TABLE {schema}.kept", pg_conn)
    pg_conn.commit()

    dropped = _events(offset, "table_dropped")
    assert dropped == [{"catalog": "postgres", "relid": relid, "read_only": "false"}]

    for line in _catalog_lines(offset):
        assert schema not in line
        assert TEST_BUCKET not in line.split("pg_lake_catalog: ", 1)[1]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_rolled_back_table_is_not_logged(s3, pg_conn, extension, installcheck):
    if installcheck:
        return

    schema = f"catlog_{uuid.uuid4().hex[:8]}"
    location = f"s3://{TEST_BUCKET}/{schema}"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    offset = _log_offset()

    run_command(
        f"CREATE TABLE {schema}.gone (id int) USING iceberg "
        f"WITH (location = '{location}/gone')",
        pg_conn,
    )
    pg_conn.rollback()

    run_command("SAVEPOINT before_create", pg_conn)
    run_command(
        f"CREATE TABLE {schema}.undone (id int) USING iceberg "
        f"WITH (location = '{location}/undone')",
        pg_conn,
    )
    run_command("ROLLBACK TO SAVEPOINT before_create", pg_conn)
    run_command("SAVEPOINT kept", pg_conn)
    run_command(
        f"CREATE TABLE {schema}.kept (id int) USING iceberg "
        f"WITH (location = '{location}/kept')",
        pg_conn,
    )
    run_command("RELEASE SAVEPOINT kept", pg_conn)
    relid = _relid(pg_conn, f"{schema}.kept")
    pg_conn.commit()

    assert _events(offset, "table_created") == [{"catalog": "postgres", "relid": relid}]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()
