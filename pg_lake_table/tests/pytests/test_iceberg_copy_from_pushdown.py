import pytest
import psycopg2
import io
from utils_pytest import *

simple_file_url = (
    f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.parquet"
)
iceberg_location = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/iceberg/"

# Most tests load 10 rows into simple_table (an Iceberg table) via COPY and then roll back


# Test COPY happy path
def test_pushdown(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        COPY simple_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test COPY happy path with pg_lake_table.target_file_size_mb = -1;
def test_pushdown_no_split(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):

    run_command("SET LOCAL pg_lake_table.target_file_size_mb TO -1;", pg_conn)

    run_command(
        f"""
        COPY simple_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test load_from happy path
def test_load_from(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE with_pushdown ()
        USING pg_lake_iceberg
        WITH (load_from = '{simple_file_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM with_pushdown",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test column list
def test_columns(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        COPY simple_table (id, val, tags) FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test WHERE
def test_where(pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location):
    run_command(
        f"""
        COPY simple_table FROM '{simple_file_url}' WHERE id > 5;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 5
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test CSV with an array
def test_csv_array(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    csv_url = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.csv"

    run_command(
        f"""
        COPY (SELECT s id, 'hello' val, array['test',NULL] tags FROM generate_series(1,10) s) TO '{csv_url}' WITH (header);

        CREATE TABLE array_table (id int, val text, tags text[]) USING iceberg WITH (load_from = '{csv_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM array_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test CSV with a composite type
def test_csv_composite(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    csv_url = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.csv"

    run_command(
        f"""
        CREATE TYPE coords AS (x int, y int);
        COPY (SELECT s id, 'hello' val, (3,4)::coords AS pos FROM generate_series(1,10) s) TO '{csv_url}' WITH (header);

        CREATE TABLE comp_type_table (id int, val text, pos coords) USING iceberg WITH (load_from = '{csv_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM comp_type_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test CSV in the happy path
def test_csv(pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location):
    csv_url = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.csv"

    run_command(
        f"""
        COPY (SELECT s id, 'hello' val FROM generate_series(1,10) s) TO '{csv_url}' WITH (header);

        CREATE TABLE csv_table (id int, val text) USING iceberg WITH (load_from = '{csv_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM csv_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test with a constraint
def test_constraint(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE constraint_table(
            id int check(id > 0),
            val text,
            tags text[]
        )
        USING iceberg;
        COPY constraint_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM constraint_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test with triggers
def test_triggers(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE trigger_table(
            id int,
            val text
        )
        USING iceberg;

        CREATE OR REPLACE FUNCTION worldize()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.val := 'world';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_insert
        BEFORE INSERT ON trigger_table
        FOR EACH ROW EXECUTE FUNCTION worldize();

        COPY trigger_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM trigger_table WHERE val = 'world'",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test with partitioning
def test_partitioning(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE partitioned_table (
            id int,
            val text
        )
        PARTITION BY RANGE (id);
        CREATE TABLE child_1 PARTITION OF partitioned_table FOR VALUES FROM (0) TO (50) USING iceberg;
        CREATE TABLE child_2 PARTITION OF partitioned_table FOR VALUES  FROM (50) TO (100) USING iceberg;

        COPY partitioned_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    # Copy into partitioned table is not pushed down
    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM partitioned_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    # Copy into partition is also not pushed down, even if it's Iceberg
    run_command(
        f"""
        COPY child_1 FROM '{simple_file_url}';
    """,
        pg_conn,
    )
    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM partitioned_table",
        pg_conn,
    )
    assert result[0]["count"] == 20
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


def test_copy_from_reject_limit(pg_conn, extension, s3, with_default_location):
    if get_pg_version_num(pg_conn) < 180000:
        return

    run_command(
        "create table test_copy_from_reject_limit(a int) using iceberg ;", pg_conn
    )

    copy_command_without_error_ignore = f"COPY test_copy_from_reject_limit FROM STDIN WITH (on_error ignore, reject_limit 3);"

    data = """\
        'a'
        'b'
        'c'
        'd'
        \.
        """

    try:
        with pg_conn.cursor() as cursor:
            cursor.copy_expert(copy_command_without_error_ignore, io.StringIO(data))

        assert False  # We expect an error to be raised
    except psycopg2.DatabaseError as error:
        assert "skipped more" in str(error)

    pg_conn.rollback()


_COPY_LOC = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown"

_COPY_FROM_UNSUITABLE_CASES = [
    # --- domain nested inside an array ---
    pytest.param(
        f"""
        CREATE DOMAIN pos_int AS INT CHECK (VALUE > 0);
        CREATE TABLE src_da (id INT, vals INT[]);
        INSERT INTO src_da VALUES (1, ARRAY[1,2]), (2, ARRAY[3]);
        CREATE FOREIGN TABLE tgt_da (id INT, vals pos_int[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_COPY_LOC}/domain_in_array/', format 'parquet');
        """,
        "src_da",
        "tgt_da",
        2,
        None,
        id="domain-in-array",
    ),
    # --- bad numeric (scale>precision) nested inside an array ---
    pytest.param(
        f"""
        CREATE TABLE src_bna (id INT, vals numeric(25,26)[]);
        INSERT INTO src_bna VALUES
            (1, ARRAY[0::numeric(25,26)]),
            (2, ARRAY[0::numeric(25,26)]);
        CREATE FOREIGN TABLE tgt_bna (id INT, vals numeric(25,26)[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_COPY_LOC}/bad_numeric_in_array/', format 'parquet');
        """,
        "src_bna",
        "tgt_bna",
        2,
        None,
        id="bad-numeric-in-array",
    ),
    # --- bad numeric nested inside a composite ---
    pytest.param(
        f"""
        CREATE TYPE has_bad_num AS (v numeric(25,26));
        CREATE TABLE src_bns (id INT, d has_bad_num);
        INSERT INTO src_bns VALUES
            (1, ROW(0::numeric(25,26))),
            (2, ROW(0::numeric(25,26)));
        CREATE FOREIGN TABLE tgt_bns (id INT, d has_bad_num)
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_COPY_LOC}/bad_numeric_in_struct/', format 'parquet');
        """,
        "src_bns",
        "tgt_bns",
        2,
        None,
        id="bad-numeric-in-struct",
    ),
    # --- bad numeric inside a composite inside an array ---
    pytest.param(
        f"""
        CREATE TYPE with_bad_num AS (a INT, b numeric(25,26));
        CREATE TABLE src_bnsa (id INT, vals with_bad_num[]);
        INSERT INTO src_bnsa VALUES
            (1, ARRAY[ROW(1, 0::numeric(25,26))::with_bad_num]),
            (2, ARRAY[ROW(2, 0::numeric(25,26))::with_bad_num]);
        CREATE FOREIGN TABLE tgt_bnsa (id INT, vals with_bad_num[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_COPY_LOC}/bad_numeric_in_struct_in_array/', format 'parquet');
        """,
        "src_bnsa",
        "tgt_bnsa",
        2,
        None,
        id="bad-numeric-in-struct-in-array",
    ),
]


@pytest.mark.parametrize(
    "setup_sql, src_table, tgt_table, expected_count, map_type",
    _COPY_FROM_UNSUITABLE_CASES,
)
def test_nested_unsuitable_types(
    pg_conn,
    extension,
    s3,
    copy_from_pushdown_setup,
    with_default_location,
    setup_sql,
    src_table,
    tgt_table,
    expected_count,
    map_type,
):
    """COPY FROM must NOT be pushed down when the target table has a column
    containing a type unsuitable for pushdown — whether at the top level,
    inside an array, composite, or map.
    """
    base = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown"
    url = f"{base}/{src_table}.parquet"

    if map_type:
        create_map_type(*map_type)

    run_command(setup_sql, pg_conn)
    run_command(f"COPY (SELECT * FROM {src_table}) TO '{url}'", pg_conn)
    run_command(f"COPY {tgt_table} FROM '{url}'", pg_conn)

    result = run_query(
        f"SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM {tgt_table}",
        pg_conn,
    )
    assert result[0]["count"] == expected_count
    assert not result[0]["pushed_down"]
    pg_conn.rollback()


def test_copy_from_domain_in_map_value(
    pg_conn, superuser_conn, extension, s3, copy_from_pushdown_setup
):
    """Domain as map value type must block COPY FROM pushdown."""
    run_command(
        "DROP DOMAIN IF EXISTS bounded_text CASCADE;"
        "CREATE DOMAIN bounded_text AS TEXT CHECK (LENGTH(VALUE) <= 10);",
        superuser_conn,
    )
    superuser_conn.commit()

    src_map = create_map_type("int", "text")
    tgt_map = create_map_type("int", "bounded_text")

    base = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown"
    loc = f"{base}/domain_in_map_value"
    run_command(
        f"""
        CREATE TABLE src_dmv (id INT, m {src_map});
        INSERT INTO src_dmv VALUES
            (1, ARRAY[(1, 'hi')]::{src_map}),
            (2, ARRAY[(2, 'bye')]::{src_map});
        CREATE FOREIGN TABLE tgt_dmv (id INT, m {tgt_map})
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/', format 'parquet');
        """,
        pg_conn,
    )
    run_command(f"COPY (SELECT * FROM src_dmv) TO '{loc}/src.parquet'", pg_conn)
    run_command(f"COPY tgt_dmv FROM '{loc}/src.parquet'", pg_conn)

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM tgt_dmv",
        pg_conn,
    )
    assert result[0]["count"] == 2
    assert not result[0]["pushed_down"]
    pg_conn.rollback()


def test_copy_from_domain_in_map_key(
    pg_conn, superuser_conn, extension, s3, copy_from_pushdown_setup
):
    """Domain as map key type must block COPY FROM pushdown."""
    run_command(
        "DROP DOMAIN IF EXISTS small_int CASCADE;"
        "CREATE DOMAIN small_int AS INT CHECK (VALUE < 1000);",
        superuser_conn,
    )
    superuser_conn.commit()

    src_map = create_map_type("int", "text")
    tgt_map = create_map_type("small_int", "text")

    base = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown"
    loc = f"{base}/domain_in_map_key"
    run_command(
        f"""
        CREATE TABLE src_dmk (id INT, m {src_map});
        INSERT INTO src_dmk VALUES
            (1, ARRAY[(1, 'hi')]::{src_map}),
            (2, ARRAY[(2, 'bye')]::{src_map});
        CREATE FOREIGN TABLE tgt_dmk (id INT, m {tgt_map})
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/', format 'parquet');
        """,
        pg_conn,
    )
    run_command(f"COPY (SELECT * FROM src_dmk) TO '{loc}/src.parquet'", pg_conn)
    run_command(f"COPY tgt_dmk FROM '{loc}/src.parquet'", pg_conn)

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM tgt_dmk",
        pg_conn,
    )
    assert result[0]["count"] == 2
    assert not result[0]["pushed_down"]
    pg_conn.rollback()


@pytest.fixture(scope="module")
def copy_from_pushdown_setup(superuser_conn):
    run_command(
        f"""
        COPY (SELECT s id, 'hello' val, array['test',NULL] tags FROM generate_series(1,10) s) TO '{simple_file_url}';

        CREATE OR REPLACE FUNCTION pg_lake_last_copy_pushed_down_test()
          RETURNS bool
          LANGUAGE C
        AS 'pg_lake_copy', $function$pg_lake_last_copy_pushed_down_test$function$;

        CREATE TABLE simple_table (
           id int,
           val text,
           tags text[]
        )
        USING pg_lake_iceberg
        WITH (location = '{iceberg_location}');
        GRANT ALL ON simple_table TO public;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the functions after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION pg_lake_last_copy_pushed_down_test;
        DROP TABLE simple_table;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
