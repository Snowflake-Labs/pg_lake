"""Value / row comparison, sorting, and query-result assertion utilities."""

import datetime
from decimal import Decimal

from .db import (
    perform_query_on_cursor,
    run_command,
    run_query,
)


# ---------------------------------------------------------------------------
# Value / row comparison helpers
# ---------------------------------------------------------------------------

def compare_values(val1, val2, tolerance):
    if isinstance(val1, float) and isinstance(val2, float):
        return abs(val1 - val2) <= tolerance
    elif isinstance(val1, Decimal) and isinstance(val2, Decimal):
        return abs(val1 - val2) <= Decimal(str(tolerance))
    elif isinstance(val1, list) and isinstance(val2, list):
        for v1, v2 in zip(val1, val2):
            if not compare_values(v1, v2, tolerance):
                return False
        return True
    elif isinstance(val1, memoryview) and isinstance(val2, memoryview):
        return bytes(val1) == bytes(val2)

    # Add more comparison strategies here for other data types if needed
    return val1 == val2


def compare_rows(row1, row2, tolerance):
    return all(compare_values(val1, val2, tolerance) for val1, val2 in zip(row1, row2))


def transform_item_for_sort(item):
    if item is None:
        return "RESERVED_KEY_FOR_NULLS_AS_STRING"

    if isinstance(item, memoryview):
        return bytes(item)

    return str(item)


def custom_key(x):
    # Transform the tuple by converting all items to strings, except None, which gets a placeholder that sorts last
    return tuple(transform_item_for_sort(item) for item in x)


def sort_with_none_at_end(lst):
    return sorted(lst, key=custom_key)


def date_days_since_epoch(d: datetime.date) -> int:
    return (d - datetime.date(1970, 1, 1)).days


def compare_rows_as_string_or_float(row_pg_lake, row_other, tolerance):
    for val_pg_lake, val_other in zip(row_pg_lake, row_other):
        if isinstance(val_pg_lake, memoryview) and isinstance(val_other, bytearray):
            val_pg_lake = bytearray(val_pg_lake)
        elif isinstance(val_pg_lake, int) and isinstance(val_other, datetime.date):
            # sometimes val_pg_lake is day count from unix epoch but spark returns it as datetime
            val_other = date_days_since_epoch(val_other)
        elif isinstance(val_pg_lake, datetime.datetime) and isinstance(
            val_other, datetime.datetime
        ):
            # remove timezone info from both values by adjusting time
            val_pg_lake_tzinfo = val_pg_lake.tzinfo
            if val_pg_lake_tzinfo is not None:
                val_pg_lake = val_pg_lake.replace(tzinfo=None) + datetime.timedelta(
                    seconds=val_pg_lake_tzinfo.utcoffset(val_pg_lake).total_seconds()
                )

                # spark already adjust the time to the local timezone (we set utc when creating the session)

        # Convert both values to strings for comparison
        if str(val_pg_lake) != str(val_other):
            try:
                # If they can't be compared as floats, just return False
                if abs(float(val_pg_lake) - float(val_other)) > tolerance:
                    print(f"Mismatch: {val_pg_lake} vs {val_other}")
                    return False
            except ValueError:
                # If they aren't numeric, they must be exactly equal as strings
                print(f"Mismatch: {val_pg_lake} vs {val_other}")
                return False
            except TypeError:
                print(f"Mismatch: {val_pg_lake} vs {val_other}")
                return False
    return True


def compare_results_with_duckdb(
    pg_conn, duckdb_conn, table_name, table_namespace, metadata_location, query
):

    pg_lake_result = run_query(query, pg_conn)

    if table_namespace is not None:
        query = query.replace(
            table_namespace + "." + table_name, f"iceberg_scan('{metadata_location}')"
        )
    else:
        query = query.replace(table_name, f"iceberg_scan('{metadata_location}')")

    duckdb_conn.execute(query)
    duckdb_result = duckdb_conn.fetchall()

    pg_lake_result = sort_with_none_at_end(pg_lake_result)
    duckdb_result = sort_with_none_at_end(duckdb_result)
    assert (
        len(pg_lake_result) > 0
    ), "No rows returned, make sure at least one row returns"
    assert len(pg_lake_result) == len(
        duckdb_result
    ), "Result sets have different lengths"

    for row_pg_lake, row_duckdb in zip(pg_lake_result, duckdb_result):
        assert compare_rows_as_string_or_float(
            row_pg_lake, row_duckdb, 0.001
        ), f"Results do not match: {row_pg_lake} and {row_duckdb}"


# ---------------------------------------------------------------------------
# Query-result assertion helpers
# ---------------------------------------------------------------------------

def assert_query_results_on_tables(
    query, pg_conn, first_table_names, second_table_names, tolerance=0.001
):

    if len(first_table_names) != len(second_table_names):
        raise ValueError(
            "The lists of first and second table names must have the same length."
        )

    fdw_query_result = perform_query_on_cursor(query, pg_conn)

    heap_query = query
    for first_table_name, second_table_name in zip(
        first_table_names, second_table_names
    ):
        heap_query = heap_query.replace(first_table_name, second_table_name)

    heap_query_result = perform_query_on_cursor(heap_query, pg_conn)

    sorted_fdw_result = sort_with_none_at_end(fdw_query_result)
    sorted_heap_result = sort_with_none_at_end(heap_query_result)

    assert (
        len(sorted_heap_result) > 0
    ), "No rows returned, make sure at least one row returns"
    assert len(sorted_fdw_result) == len(
        sorted_heap_result
    ), "Result sets have different lengths"

    for row_fdw, row_heap in zip(sorted_fdw_result, sorted_heap_result):
        assert compare_rows(
            row_fdw, row_heap, tolerance
        ), f"Results do not match: {row_fdw} and {row_heap} ({sorted_fdw_result} vs {sorted_heap_result})"


def assert_query_results_on_search_path(
    query, pg_conn, first_search_path, second_search_path, tolerance=0.001
):

    run_command("SET search_path TO " + first_search_path + ";", pg_conn)
    first_query_result = perform_query_on_cursor(query, pg_conn)

    run_command("SET search_path TO " + second_search_path + ";", pg_conn)
    second_query_result = perform_query_on_cursor(query, pg_conn)

    sorted_first_result = sort_with_none_at_end(first_query_result)
    sorted_second_result = sort_with_none_at_end(second_query_result)

    assert len(sorted_first_result) == len(
        sorted_second_result
    ), "Result sets have different lengths"

    for row_fdw, row_heap in zip(sorted_first_result, sorted_second_result):
        assert compare_rows(
            row_fdw, row_heap, tolerance
        ), f"Results do not match: {row_fdw} and {row_heap}"


def assert_query_result_on_duckdb_and_pg(duckdb_conn, pg_conn, duckdb_query, pg_query):
    duckdb_conn.execute(duckdb_query)
    duckdb_result = duckdb_conn.fetchall()

    pg_result = run_query(pg_query, pg_conn)
    # duckdb returns [(), (), ...] while pg returns [[], [], ...]
    pg_result = [tuple(row) for row in pg_result]

    assert duckdb_result == pg_result

    return pg_result


def check_table_size(pg_conn, table_name, count):
    result = run_query(f"SELECT count(*) FROM {table_name}", pg_conn)
    assert result[0]["count"] == count
