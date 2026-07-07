import re

from utils_pytest import *


def _extract_min_max(column_statistics):
    """Pull min/max out of a RETURN_STATS column_statistics value (single column)."""
    text = str(column_statistics)
    stat_min = re.search(r"\(min,(-?\d+)\)", text)
    stat_max = re.search(r"\(max,(-?\d+)\)", text)
    assert stat_min is not None and stat_max is not None, column_statistics
    return stat_min.group(1), stat_max.group(1)


def test_return_stats_decimal38_multi_row_group(pgduck_conn, tmp_path):
    """DECIMAL(38,0) spanning >1 row group must report the true file-level min/max.

    128-bit decimal stats are big-endian two's complement; a little-endian
    comparison while unifying row-group bounds swaps min and max (SNOW-3701832).
    """
    path = str(tmp_path / "dec38.parquet")

    copy = f"""
        COPY (
            SELECT CASE WHEN i < 2048 THEN 126619 ELSE 19308 END::DECIMAL(38,0) AS c
            FROM range(4096) t(i)
        ) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, RETURN_STATS)
    """

    stats = run_query(copy, pgduck_conn)
    stat_min, stat_max = _extract_min_max(stats[0]["column_statistics"])
    assert (stat_min, stat_max) == ("19308", "126619")

    # Ground truth from the Parquet footer, which is always correct.
    footer = run_query(
        f"""
        SELECT min(stats_min::DECIMAL(38,0)), max(stats_max::DECIMAL(38,0)),
               count(*)
        FROM parquet_metadata('{path}') WHERE path_in_schema = 'c'
        """,
        pgduck_conn,
    )
    assert footer[0][0] == 19308
    assert footer[0][1] == 126619
    assert footer[0][2] >= 2
