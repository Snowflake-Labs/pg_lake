import pytest
import psycopg2
import time
import duckdb
from fixtures.delta import *
from utils_pytest import *


@pytest.mark.skipif(
    os.getenv("PG_LAKE_DELTA_SUPPORT") != "1",
    reason="Delta support not enabled",
)
def test_delta_definition_from(pg_conn, sample_delta_table):
    run_command(
        f"""
		create table people_countries () with (definition_from = '{sample_delta_table}', format = 'delta')
	""",
        pg_conn,
    )

    result = run_query(
        """
		select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'people_countries'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 4
    assert result == [
        ["first_name", "text"],
        ["last_name", "text"],
        ["country", "text"],
        ["continent", "text"],
    ]

    pg_conn.rollback()


@pytest.mark.skipif(
    os.getenv("PG_LAKE_DELTA_SUPPORT") != "1",
    reason="Delta support not enabled",
)
def test_delta_load_from(pg_conn, sample_delta_table):
    run_command(
        f"""
		create table people_countries () with (load_from = '{sample_delta_table}', format = 'delta')
	""",
        pg_conn,
    )

    result = run_query(
        "select * from people_countries order by first_name, last_name", pg_conn
    )
    assert len(result) == 5
    assert result[0]["first_name"] == "Bruce"

    pg_conn.rollback()


@pytest.mark.skipif(
    os.getenv("PG_LAKE_DELTA_SUPPORT") != "1",
    reason="Delta support not enabled",
)
def test_delta_copy(pg_conn, sample_delta_table):
    run_command(
        f"""
		create table people_countries (id bigserial, first_name text, last_name text, country text, continent text);
		/* does not autodetect format
		copy people_countries (first_name, last_name, country, continent) from '{sample_delta_table}'; */
		copy people_countries (first_name, last_name, country, continent) from '{sample_delta_table}' with (format 'delta');
	""",
        pg_conn,
    )

    result = run_query(
        "select * from people_countries order by first_name, last_name", pg_conn
    )
    assert len(result) == 5
    assert result[0]["first_name"] == "Bruce"
    assert result[0]["last_name"] == "Lee"

    error = run_command(
        f"""
		copy people_countries (first_name, last_name, country, continent) to '{sample_delta_table}' with (format 'delta');
	""",
        pg_conn,
        raise_error=False,
    )
    assert "COPY TO in Delta format is not supported" in error

    pg_conn.rollback()
