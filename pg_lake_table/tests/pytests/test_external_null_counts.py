"""Null counts that other Iceberg engines write, and the ones pg_lake writes,
read the same everywhere.

Spark and PyIceberg write tables through the REST catalog that pg_lake attaches
read-only, and the null counts pg_lake parses from their manifests have to be
the ones Spark itself reports for each file. Only top-level columns with
usable bounds get a count, as for the files pg_lake writes.

The other way around, Spark has to report the counts pg_lake writes, and both
Spark and PyIceberg, which skip files on them, have to return the right rows.
"""

import datetime
import math

import pyarrow
import pytest
from pyiceberg.expressions import GreaterThan, IsNull, NotNull
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    StructType,
    IntegerType,
)
from helpers import server_params
from helpers.polaris import *
from helpers.spark import (
    create_spark_catalog_database,
    spark_register_table,
    spark_rest_catalog,
    spark_session,
    spark_unregister_table,
)
from test_read_only_more_external_writers import iceberg_table
from test_read_only_spark_written_tables import SparkTable, spark_table
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def external_column_stats_function(superuser_conn, app_user, extension):
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION public.external_column_stats(regclass)
        RETURNS TABLE(path text, field_id int, lower_bound text,
                      upper_bound text, null_count bigint)
        LANGUAGE C STRICT
        AS 'pg_lake_table', $function$external_column_stats$function$;
        GRANT EXECUTE ON FUNCTION public.external_column_stats(regclass) TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield
    run_command("DROP FUNCTION public.external_column_stats(regclass)", superuser_conn)
    superuser_conn.commit()


def parsed_stats(table):
    """{path: {field_id: null_count}} as pg_lake reads them for pruning."""
    rows = run_query(
        f"SELECT path, field_id, null_count FROM external_column_stats('{table.pg_name}')",
        table.pg_conn,
    )
    table.pg_conn.commit()
    stats = {}
    for path, field_id, null_count in rows:
        stats.setdefault(path, {})[field_id] = null_count
    return stats


def spark_file_metrics(table):
    """{path: (null_value_counts, nan_value_counts, bounded field ids)} for the
    live data files, as Spark reports them."""
    rows = table.spark_sql(
        "SELECT file_path, null_value_counts, nan_value_counts, lower_bounds, "
        "upper_bounds FROM {table}.files WHERE content = 0"
    )
    return {
        row.file_path: (
            dict(row.null_value_counts or {}),
            dict(row.nan_value_counts or {}),
            set(row.lower_bounds or {}) & set(row.upper_bounds or {}),
        )
        for row in rows
    }


def field_ids(table, *names):
    schema = iceberg_table(table).schema()
    return [schema.findField(name).fieldId() for name in names]


def assert_counts_match_spark(table):
    """
    Every count pg_lake parses is the one Spark reports for the file, and
    pg_lake only has stats for fields Spark wrote both bounds for, without
    NaN. Returns pg_lake's stats and Spark's metrics.
    """
    parsed = parsed_stats(table)
    spark = spark_file_metrics(table)

    assert set(parsed) <= set(spark)
    assert len(spark) > 0

    for path, counts in parsed.items():
        null_counts, nan_counts, bounded = spark[path]
        for field_id, null_count in counts.items():
            assert field_id in bounded, (path, field_id)
            assert nan_counts.get(field_id, 0) == 0, (path, field_id)
            assert null_count == null_counts[field_id], (path, field_id)

    return parsed, spark


SPARK_COLUMNS = """id BIGINT, i INT, s STRING, d DOUBLE, amount DECIMAL(12,2),
    ts TIMESTAMP_NTZ, b BINARY, st STRUCT<a: INT, z: STRING>, arr ARRAY<INT>,
    m MAP<STRING, INT>, allnull INT"""

# {nulls} picks which columns are NULL in which rows
SPARK_ROWS = """SELECT id,
       CASE WHEN {i_null} THEN NULL ELSE CAST(id AS INT) END AS i,
       CASE WHEN {s_null} THEN NULL ELSE concat('s', id) END AS s,
       CASE WHEN {d_nan} THEN CAST('NaN' AS DOUBLE) ELSE id * 1.5 END AS d,
       CAST(id * 1.25 AS DECIMAL(12,2)) AS amount,
       TIMESTAMP_NTZ'2024-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0) AS ts,
       CAST(concat('b', id) AS BINARY) AS b,
       CASE WHEN id % 5 = 0 THEN NULL
            ELSE named_struct('a', CAST(id AS INT), 'z', concat('z', id)) END AS st,
       array(CAST(id AS INT), NULL) AS arr,
       map('k', CAST(id AS INT)) AS m,
       CAST(NULL AS INT) AS allnull
FROM range({start}, {end})"""


def spark_insert(table, start, end, i_null="false", s_null="false", d_nan="false"):
    rows = SPARK_ROWS.format(
        i_null=i_null, s_null=s_null, d_nan=d_nan, start=start, end=end
    )
    # one file per insert, so each NULL pattern stays in its own files
    table.spark_sql(f"INSERT INTO {{table}} SELECT /*+ COALESCE(1) */ * FROM ({rows})")


SPARK_FILTERS = [
    "i IS NULL",
    "i IS NOT NULL",
    "i > 150",
    "s IS NULL",
    "s >= 's2'",
    "d > 100",
    "d IS NOT NULL",
    "amount > 100",
    "allnull IS NULL",
    "st IS NULL",
]


def test_spark_written_null_counts(spark_table):
    spark_table.create(columns=SPARK_COLUMNS)
    spark_insert(spark_table, 0, 100, i_null="id % 10 = 0")
    spark_insert(spark_table, 100, 200, d_nan="id = 150")
    spark_insert(spark_table, 200, 300, s_null="true")
    spark_table.attach()

    parsed, spark = assert_counts_match_spark(spark_table)
    assert len(parsed) == 3

    id_, i, s, d, amount, ts, b, allnull = field_ids(
        spark_table, "id", "i", "s", "d", "amount", "ts", "b", "allnull"
    )
    nested = field_ids(spark_table, "st.a", "st.z", "arr.element", "m.key", "m.value")

    # Spark writes counts for every field, pg_lake keeps only the ones it can
    # use: no binary, nested or all-NULL fields, and no double with a NaN
    for path, counts in parsed.items():
        null_counts, nan_counts, _ = spark[path]
        assert allnull in null_counts and nested[0] in null_counts
        assert set(counts) & {b, allnull, *nested} == set()
        assert {id_, i, amount, ts} <= set(counts)
        # s has no bounds in the file where it is all NULL
        assert (s in counts) == (s in spark[path][2])

        if nan_counts.get(d, 0) > 0:
            assert d not in counts
        else:
            assert counts[d] == 0

    # the three NULL patterns are all there
    assert sorted(counts[i] for counts in parsed.values()) == [0, 0, 10]
    assert sorted(counts.get(s, -1) for counts in parsed.values()) == [-1, 0, 0]

    for where in SPARK_FILTERS:
        spark_table.assert_same_rows(where, columns=["id"])


@pytest.mark.parametrize(
    "properties,expected",
    [
        # counts without bounds give pruning nothing to use
        ({"write.metadata.metrics.default": "counts"}, set()),
        ({"write.metadata.metrics.default": "none"}, set()),
        (
            {
                "write.metadata.metrics.default": "counts",
                "write.metadata.metrics.column.s": "truncate(4)",
            },
            {"s"},
        ),
        (
            {
                "write.metadata.metrics.default": "full",
                "write.metadata.metrics.column.i": "none",
                "write.metadata.metrics.column.s": "counts",
            },
            {"id", "d", "amount", "ts"},
        ),
    ],
    ids=["counts", "none", "counts_and_truncate", "full_without_some"],
)
def test_spark_metrics_modes(spark_table, properties, expected):
    spark_table.create(columns=SPARK_COLUMNS, properties=properties)
    spark_insert(spark_table, 0, 100, i_null="id % 10 = 0", s_null="id % 3 = 0")
    spark_table.attach()

    parsed, _ = assert_counts_match_spark(spark_table)

    expected_ids = set(field_ids(spark_table, *expected)) if expected else set()
    if expected_ids:
        assert [set(counts) for counts in parsed.values()] == [expected_ids]
    else:
        assert parsed == {}

    for where in ["i IS NULL", "s IS NULL", "s >= 's5'", "i > 50"]:
        spark_table.assert_same_rows(where, columns=["id"])


@pytest.mark.parametrize("mode", ["copy-on-write", "merge-on-read"])
def test_spark_row_level_changes(spark_table, mode):
    spark_table.create(
        columns=SPARK_COLUMNS,
        properties={"write.delete.mode": mode, "write.update.mode": mode},
    )
    spark_insert(spark_table, 0, 300, i_null="id % 10 = 0")
    spark_table.attach()

    spark_table.spark_sql("DELETE FROM {table} WHERE id % 7 = 0")
    spark_table.spark_sql("UPDATE {table} SET i = NULL WHERE id % 11 = 0")

    # rewritten files carry their own counts, delete files are not data files
    assert_counts_match_spark(spark_table)
    for where in ["i IS NULL", "i IS NOT NULL", "i > 100"]:
        spark_table.assert_same_rows(where, columns=["id"])


PYICEBERG_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "i", IntegerType(), required=False),
    NestedField(3, "s", StringType(), required=False),
    NestedField(4, "d", DoubleType(), required=False),
    NestedField(
        5,
        "st",
        StructType(NestedField(6, "a", IntegerType(), required=False)),
        required=False,
    ),
    NestedField(7, "allnull", IntegerType(), required=False),
)


def pyiceberg_rows(table, start, end, i_null=lambda i: False, d_nan=lambda i: False):
    return pyarrow.Table.from_pylist(
        [
            {
                "id": i,
                "i": None if i_null(i) else i,
                "s": None if i % 3 == 0 else f"s{i}",
                "d": math.nan if d_nan(i) else i * 1.5,
                "st": None if i % 5 == 0 else {"a": None if i % 2 else i},
                "allnull": None,
            }
            for i in range(start, end)
        ],
        table.schema().as_arrow(),
    )


def test_pyiceberg_written_null_counts(spark_table):
    catalog = create_iceberg_rest_catalog(spark_table.namespace)
    table = catalog.create_table(spark_table.name, schema=PYICEBERG_SCHEMA)
    table.append(pyiceberg_rows(table, 0, 100, i_null=lambda i: i % 10 == 0))
    table.append(pyiceberg_rows(table, 100, 200, d_nan=lambda i: i == 150))
    table.append(pyiceberg_rows(table, 200, 300))
    spark_table.attach()

    files = {task.file.file_path: task.file for task in table.scan().plan_files()}
    parsed = parsed_stats(spark_table)
    assert set(parsed) == set(files)

    for path, counts in parsed.items():
        data_file = files[path]
        # PyIceberg counts nested fields too, including rows whose struct is
        # NULL, and all-NULL fields, which pg_lake does not use
        assert {6, 7} <= set(data_file.null_value_counts)
        assert set(counts) == {1, 2, 3}, path
        for field_id, null_count in counts.items():
            assert null_count == data_file.null_value_counts[field_id]

        # PyIceberg writes double bounds without a NaN count, even for the
        # file with a NaN, so pg_lake cannot trust them
        assert 4 in data_file.lower_bounds
        assert 4 not in (data_file.nan_value_counts or {})

    assert sorted(counts[2] for counts in parsed.values()) == [0, 0, 10]

    # Spark reads the same files through the REST catalog
    for where in ["i IS NULL", "i > 150", "s IS NULL", "d > 200", "d IS NULL"]:
        spark_table.assert_same_rows(where, columns=["id"])


# pg_lake writes, Spark and PyIceberg read


@pytest.fixture
def pg_lake_written_table(
    installcheck, pg_conn, superuser_conn, s3, extension, with_default_location
):
    if installcheck:
        pytest.skip("requires Spark")

    run_command(
        """
        CREATE SCHEMA null_counts_interop;
        CREATE TABLE null_counts_interop.t (
            id int, a int, s text, d float8, ts timestamptz, allnull int
        ) USING iceberg;
        INSERT INTO null_counts_interop.t
        SELECT i, NULLIF(i % 10, 0), 's' || i, i * 1.5,
               '2026-01-01'::timestamptz + i * interval '1 hour', NULL
        FROM generate_series(1, 100) i;
        INSERT INTO null_counts_interop.t
        SELECT i, i, NULL, CASE WHEN i = 150 THEN 'NaN' ELSE i * 1.5 END,
               NULL, NULL
        FROM generate_series(101, 200) i;
        INSERT INTO null_counts_interop.t
        SELECT i, i, 's' || i, i * 1.5,
               '2026-01-01'::timestamptz + i * interval '1 hour', NULL
        FROM generate_series(201, 300) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_namespace = 'null_counts_interop' AND table_name = 't'",
        pg_conn,
    )[0][0]

    yield metadata_location

    pg_conn.rollback()
    run_command("DROP SCHEMA null_counts_interop CASCADE", pg_conn)
    pg_conn.commit()


def catalog_null_counts(superuser_conn):
    rows = run_query(
        """
        SELECT s.path, s.field_id, s.null_count
        FROM lake_table.data_file_column_stats s
        WHERE s.table_name = 'null_counts_interop.t'::regclass
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    counts = {}
    for path, field_id, null_count in rows:
        counts.setdefault(path, {})[field_id] = null_count
    return counts


def test_spark_reads_pg_lake_null_counts(
    installcheck, pg_conn, superuser_conn, spark_session, pg_lake_written_table
):
    spark_register_table(
        installcheck, spark_session, "t", "null_counts_interop", pg_lake_written_table
    )
    try:
        files = spark_session.sql(
            "SELECT file_path, null_value_counts FROM null_counts_interop.t.files "
            "WHERE content = 0"
        ).collect()
        spark = {row.file_path: dict(row.null_value_counts or {}) for row in files}
        catalog = catalog_null_counts(superuser_conn)

        # Spark sees exactly the counts pg_lake keeps
        assert spark == catalog
        # field ids: id 1, a 2, s 3, d 4, ts 5, allnull 6
        assert sorted(counts.get(4, -1) for counts in spark.values()) == [-1, 0, 0]

        for where in [
            "a IS NULL",
            "a IS NOT NULL",
            "s IS NULL",
            "ts IS NULL",
            "d IS NULL",
            "allnull IS NULL",
        ]:
            spark_rows = spark_session.sql(
                f"SELECT id FROM null_counts_interop.t WHERE {where} ORDER BY id"
            ).collect()
            pg_rows = run_query(
                f"SELECT id FROM null_counts_interop.t WHERE {where} ORDER BY id",
                pg_conn,
            )
            assert [row.id for row in spark_rows] == [row[0] for row in pg_rows], where
    finally:
        spark_unregister_table(installcheck, spark_session, "t", "null_counts_interop")


def test_pyiceberg_reads_pg_lake_null_counts(
    pg_conn, superuser_conn, pg_lake_written_table
):
    """
    PyIceberg skips a file on an IS NULL filter when its null count is zero,
    so a wrong count would lose rows.
    """
    table = StaticTable.from_metadata(
        pg_lake_written_table,
        properties={
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.region": TEST_AWS_REGION,
        },
    )
    catalog = catalog_null_counts(superuser_conn)
    field_ids = {"a": 2, "s": 3, "ts": 5}

    for column, field_id in field_ids.items():
        scan = table.scan(row_filter=IsNull(column), selected_fields=("id",))

        # only the files with NULLs in the column, or without a count for it
        planned = {task.file.file_path for task in scan.plan_files()}
        expected = {
            path
            for path, counts in catalog.items()
            if counts.get(field_id) is None or counts[field_id] > 0
        }
        assert planned == expected, column

        pg_ids = [
            row[0]
            for row in run_query(
                f"SELECT id FROM null_counts_interop.t WHERE {column} IS NULL ORDER BY id",
                pg_conn,
            )
        ]
        assert sorted(scan.to_arrow()["id"].to_pylist()) == pg_ids, column

    for expression, where in [
        (NotNull("a"), "a IS NOT NULL"),
        (GreaterThan("a", 150), "a > 150"),
    ]:
        rows = table.scan(row_filter=expression, selected_fields=("id",)).to_arrow()
        pg_ids = [
            row[0]
            for row in run_query(
                f"SELECT id FROM null_counts_interop.t WHERE {where} ORDER BY id",
                pg_conn,
            )
        ]
        assert sorted(rows["id"].to_pylist()) == pg_ids, where
