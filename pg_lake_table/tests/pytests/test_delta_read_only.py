import pytest
from utils_pytest import *
from fixtures.delta import *
from datetime import datetime, date, timezone


@pytest.mark.skipif(
    os.getenv("PG_LAKE_DELTA_SUPPORT") != "1",
    reason="Delta support not enabled",
)
def test_delta_pg_lake_table(pg_conn, sample_delta_table, extension):
    run_command(
        f"""
        create foreign table people_countries ()
        server pg_lake
        options (path '{sample_delta_table}', format 'delta')
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

    result = run_query("select * from people_countries order by first_name", pg_conn)
    assert len(result) == 5
    assert result[0]["first_name"] == "Bruce"

    # Test an empty result
    result = run_query(
        "select a.first_name from people_countries a where continent = 'Pangea'",
        pg_conn,
    )
    assert len(result) == 0

    # Test a join query
    result = run_query(
        "select a.first_name || ' ' || b.last_name from people_countries a join people_countries b on (a.country = b.country) where a.continent ^@ 'N' order by b.continent",
        pg_conn,
    )
    assert len(result) == 5

    # Test an aggregation query
    result = run_query(
        "select first_name, count(*) from people_countries group by 1 order by 2 desc",
        pg_conn,
    )
    assert len(result) == 5

    pg_conn.rollback()


@pytest.mark.skipif(
    os.getenv("PG_LAKE_DELTA_SUPPORT") != "1",
    reason="Delta support not enabled",
)
def test_delta_with_filename(pg_conn, sample_delta_table, extension):

    run_command(
        f"""
        create foreign table people_countries ()
        server pg_lake
        options (path '{sample_delta_table}', format 'delta', filename 'true')
    """,
        pg_conn,
    )

    # Test selecting from a specific file
    url = f"s3://{TEST_BUCKET}/test_delta_tables/people_countries/country=Argentina/part-00000-8d0390a3-f797-4265-b9c2-da1c941680a3.c000.snappy.parquet"

    # Currently disabled due to a crash in delta extension
    result = run_query(
        f"select count(*) from people_countries where _filename = '{url}'",
        pg_conn,
        raise_error=False,
    )
    # assert result[0]["count"] == 1
    assert "unsupported" in result

    pg_conn.rollback()
