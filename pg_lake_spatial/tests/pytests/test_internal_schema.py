import pytest
import psycopg2
from utils_pytest import *


def test_internal_schema(
    pg_conn,
    s3,
    spatial_analytics_extension,
    pg_lake_table_extension,
    extension,
    with_default_location,
):

    run_command("SET pg_lake_table.hide_objects_created_by_lake to false;", pg_conn)

    # all functions should be in the new internal schema
    result = run_query(
        """
        SELECT COUNT(*) FROM pg_proc WHERE pronamespace = '__lake__internal__nsp__'::regnamespace; 
    """,
        pg_conn,
    )[0][0]
    assert result == 63

    pg_conn.rollback()
