import datetime

import pytest

from utils_pytest import *


def test_iso_date_survives_us_style_date_in_text_column(s3, pg_conn, extension):
    """A US-format date string in a text column must not make the CSV reader
    wrongly parse a real ISO date column on the same row.

    pg_lake round-trips table writes through an internal CSV (always ISO for
    dates) read back with DuckDB read_csv. The read passes an explicit columns
    type map, which pins column *types* but not the date *format* -- DuckDB
    still auto-detects the format. A text value like "12/25/2020" made the
    sniffer choose date_format=%m/%d/%Y for the whole file, which then failed to
    parse the ISO value "2021-03-10" in the real date column
    (Could not convert string "2021-03-10" to 'DATE').
    """
    location = f"s3://{TEST_BUCKET}/test_iso_date_us_text/"
    run_command(
        f"""CREATE TABLE t_iso_date (txt text, d date)
            USING iceberg WITH (location = '{location}')""",
        pg_conn,
    )
    run_command(
        "INSERT INTO t_iso_date VALUES ('12/25/2020', DATE '2021-03-10')",
        pg_conn,
    )
    result = run_query("SELECT txt, d FROM t_iso_date", pg_conn)
    assert result == [["12/25/2020", datetime.date(2021, 3, 10)]]
    pg_conn.rollback()
