import psycopg2
import pytest

from utils_pytest import *


def test_insert_select_pushdown(s3, pg_conn, extension, with_default_location):

    # create the tables
    run_command(
        """
		CREATE SCHEMA test_insert_select_pushdown;
		SET search_path TO test_insert_select_pushdown;

		CREATE TABLE test_table_1 (id bigint, value_1 int, value_2 int, value_3 float, value_4 bigint, value_5 text, value_6 int DEfAULT 250, value_7 int, happens_at date DEfAULT now(), jsonb_data jsonb) USING iceberg;
		CREATE TABLE test_table_1_local (LIKE test_table_1 INCLUDING ALL);

		CREATE TABLE test_table_2 (id bigint, value_1 int, value_2 int, value_3 float, value_4 bigint, value_5 text, value_6 float DEfAULT 250.555, value_7 int, happens_at date DEfAULT now()) USING iceberg;
		CREATE TABLE test_table_2_local (LIKE test_table_2 INCLUDING ALL);

		CREATE TABLE target_table (id bigint, sum_value_1 bigint, average_value_2 float, average_value_3 float, sum_value_4 bigint, sum_value_5 float, average_value_6 int, rollup_hour date) USING iceberg;
		CREATE TABLE target_table_local (LIKE target_table INCLUDING ALL);
	""",
        pg_conn,
    )

    # generate some random data
    run_command(
        """ 

			INSERT INTO test_table_1 (id, value_1, value_2, value_3, value_4, value_5, value_7) SELECT i, (random()*100)::int, (random()*100)::int, (random()*100), (random()*100)::bigint, (random()*100)::int::text, (random()*100)::int FROM generate_series(0,25)i;  
			INSERT INTO test_table_1_local SELECT * FROM test_table_1;

			INSERT INTO test_table_2 (id, value_1, value_2, value_3, value_4, value_5, value_7) SELECT i, (random()*100)::int, (random()*100)::int, (random()*100), (random()*100)::bigint, (random()*100)::int::text, (random()*100)::int FROM generate_series(0,25)i;  
			INSERT INTO test_table_2_local SELECT * FROM test_table_2;

			INSERT INTO target_table SELECT i, (random()*100)::int, (random()*100)::int, (random()*100), (random()*100)::bigint, (random()*100), (random()*100)::int FROM generate_series(0,25)i;  
			INSERT INTO target_table_local SELECT * FROM target_table;
		""",
        pg_conn,
    )

    queries = [
        # simplest cast
        "INSERT INTO test_table_1 SELECT * FROM test_table_1;",
        # subset of the columns
        "INSERT INTO test_table_1(id, value_4) SELECT id, value_4 FROM test_table_1;",
        # now that shuffle columns a on a single table
        "INSERT INTO test_table_1(value_5, value_2, id, value_4) SELECT value_2::text, value_5::int, id, value_4 FROM test_table_1;",
        # similar test on two different tables
        "INSERT INTO test_table_1(value_5, value_2, id, value_4) SELECT value_2::text, value_5::int, id, value_4 FROM test_table_2;",
        # aggregations
        "INSERT INTO target_table (id, rollup_hour, sum_value_1, average_value_3, average_value_6, sum_value_4) SELECT id, date_trunc('hour', happens_at) , sum(value_1), avg(value_3), avg(value_6), sum(value_4) FROM test_table_1 GROUP BY id, date_trunc('hour', happens_at);",
        # some subqueries, JOINS
        "INSERT INTO test_table_1 (value_3, id) SELECT test_table_2.value_3, test_table_1.id FROM test_table_1, test_table_2 WHERE test_table_1.id = test_table_2.id;",
        # join with aggs
        "INSERT INTO test_table_1 (value_3, id) SELECT max(test_table_2.value_3), avg(test_table_1.value_3) FROM test_table_1, test_table_2  WHERE test_table_1.id = test_table_2.id GROUP BY test_table_1.happens_at;",
        # queries with CTEs can be pushdown
        "WITH some_vals AS (SELECT happens_at, value_5, id FROM test_table_1) INSERT INTO target_table (rollup_hour, sum_value_5, id) SELECT happens_at, sum(value_5::int), id FROM some_vals GROUP BY happens_at, id;",
        # even if CTE is unreferenced, we should be fine
        "WITH some_vals AS (SELECT happens_at, value_5, id FROM test_table_1) INSERT INTO target_table (rollup_hour, sum_value_5, id) SELECT happens_at, sum(value_5::int), id FROM test_table_1 GROUP BY happens_at, id;",
        # recursive CTEs are also fine
        "INSERT INTO target_table (sum_value_1, sum_value_5, id) WITH RECURSIVE hierarchy as ( SELECT value_1, 1 AS LEVEL, id FROM test_table_1 WHERE id = 1 UNION SELECT re.value_2, (h.level+1), re.id FROM hierarchy h JOIN test_table_1 re ON (h.id = re.id AND h.value_1 = re.value_6)) SELECT * FROM hierarchy WHERE LEVEL <= 50;",
        # distinct is fine
        "INSERT INTO target_table (sum_value_1) SELECT DISTINCT value_1 FROM test_table_1;",
        # window functions is fine
        "INSERT INTO target_table (sum_value_5, id) SELECT rank() OVER (PARTITION BY id ORDER BY value_6), id FROM test_table_1 WHERE happens_at < now();",
        # functions/operators are fine
        "INSERT INTO target_table (sum_value_5, id, sum_value_4) SELECT 100, 10 * max(value_1), value_6 FROM test_table_1 WHERE happens_at <= now() GROUP BY happens_at, value_7, value_6;",
        # distinct / case etc. is fine
        "INSERT INTO target_table (sum_value_1, id) SELECT count(DISTINCT CASE WHEN value_1 < 100 THEN id ELSE value_6 END) as c, max(id) FROM test_table_1;",
        # subqueries inside is fine
        "INSERT INTO test_table_1(value_7, value_1, id) SELECT value_7, value_1, id FROM (SELECT id, value_2 as value_7, value_1 FROM test_table_2 ) as foo;",
        # use the same column multiple times
        "INSERT INTO test_table_1(id, value_7, value_4) SELECT id, value_7, value_7 FROM test_table_1 ORDER BY value_2, value_1;",
        # shuffle columns
        "INSERT INTO test_table_2(id, value_1, value_2, value_3, value_4) SELECT id, value_1, value_2, value_3, value_4 FROM (SELECT value_2, value_4, id, value_1, value_3 FROM test_table_1 ) as foo;",
        # union is fine
        "INSERT INTO test_table_1 SELECT * FROM test_table_1 UNION SELECT * FROM test_table_1",
        # union all is fine
        "INSERT INTO test_table_1 SELECT * FROM test_table_2 UNION ALL SELECT * FROM test_table_2",
        # except all is fine
        "INSERT INTO test_table_1 SELECT * FROM test_table_2 EXCEPT SELECT * FROM test_table_2",
        # subquery on top of a set operations is fine
        "INSERT INTO test_table_1(id, value_1, value_2, value_3) SELECT max(id), min(value_1), max(value_2), avg(value_3)  FROM (SELECT * FROM test_table_1 UNION SELECT * FROM test_table_1)",
        # insert with constant json and jsonb
        'INSERT INTO test_table_1 (jsonb_data) SELECT \'{"key": "jsonb_data"}\' FROM generate_series(0,10)',
    ]

    first_table_names, second_table_names = [
        "test_table_1",
        "test_table_2",
        "target_table",
    ], ["test_table_1_local", "test_table_2_local", "target_table_local"]

    for query in queries:

        # ensure all the tests here are pushdown tests
        results = run_query("EXPLAIN (VERBOSE) " + query, pg_conn)
        assert "Custom Scan (Query Pushdown)" in str(results)

        # run the actual command
        run_command(query, pg_conn)

        # replace the table names with the local table names
        # and run the same queries on heap tables
        heap_query = query
        for first_table_name, second_table_name in zip(
            first_table_names, second_table_names
        ):
            heap_query = heap_query.replace(first_table_name, second_table_name)
        run_command(heap_query, pg_conn)

        # ensure the iceberg and heap tables have the same
        assert_tables_equal(pg_conn, "test_table_1", "test_table_1_local")
        assert_tables_equal(pg_conn, "test_table_2", "test_table_2_local")
        assert_tables_equal(pg_conn, "target_table", "target_table_local")

    run_command("DROP SCHEMA test_insert_select_pushdown CASCADE", pg_conn)


def assert_tables_equal(pg_conn, table_1, table_2):

    query = f"SELECT * FROM {table_1}"
    assert_query_results_on_tables(query, pg_conn, [f"{table_1}"], [f"{table_2}"])


def test_parametrized_insert_select_pushdown(
    s3, pg_conn, extension, with_default_location
):

    run_command(
        """
				CREATE SCHEMA test_parametrized_insert_select_pushdown;
				SET search_path TO test_parametrized_insert_select_pushdown;

				CREATE TABLE target_table (key int, value text) USING iceberg;
		""",
        pg_conn,
    )

    run_command(
        "PREPARE p1(int) AS INSERT INTO target_table SELECT i,i::text FROM generate_series(0, $1)i",
        pg_conn,
    )
    for i in range(0, 10):
        run_command(f"EXECUTE p1({i})", pg_conn)

    res = run_query("SELECT max(key), count(*) FROM target_table", pg_conn)
    assert res == [[9, 55]]

    run_command("TRUNCATE target_table", pg_conn)

    # now, the same with EXPLAIN ANALYZE
    for i in range(0, 10):
        run_command(f"EXPLAIN ANALYZE EXECUTE p1({i})", pg_conn)

    res = run_query("SELECT max(key), count(*) FROM target_table", pg_conn)
    assert res == [[9, 55]]

    run_command("TRUNCATE target_table", pg_conn)

    # now, with unused parameters
    run_command(
        "PREPARE p2(int, int) AS INSERT INTO target_table SELECT i,i::text FROM generate_series(0, $1)i",
        pg_conn,
    )
    for i in range(0, 10):
        run_command(f"EXECUTE p2({i}, {i})", pg_conn)

    res = run_query("SELECT max(key), count(*) FROM target_table", pg_conn)
    assert res == [[9, 55]]

    pg_conn.rollback()


# implicit/explicit casts or constant values are all fine in the target list
def test_insert_select_target_list_transforms(
    s3, pg_conn, extension, with_default_location
):

    run_command(
        """
			CREATE SCHEMA test_insert_select_unusual_target_lists;
			SET search_path TO test_insert_select_unusual_target_lists;

			CREATE TABLE t1 (key int, value varchar) USING iceberg;
			INSERT INTO t1 VALUES ('1', 'test1');

			CREATE TABLE t2 (key bigint, value text) USING iceberg;
			INSERT INTO t2 VALUES ('2', 'test2');
	""",
        pg_conn,
    )

    # implicit casts
    results = run_query("EXPLAIN ANALYZE INSERT INTO t1 SELECT * FROM t2", pg_conn)
    assert "Custom Scan (Query Pushdown)" in str(results)

    results = run_query("EXPLAIN ANALYZE INSERT INTO t2 SELECT * FROM t1", pg_conn)
    assert "Custom Scan (Query Pushdown)" in str(results)

    # explicit casts
    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO t1 SELECT key::int, value::varchar FROM t2",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO t2 SELECT key::bigint, value::text FROM t1",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    # constants in the target list
    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO t1 SELECT 1, value FROM t2", pg_conn
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO t1 SELECT key, 'value const' FROM t2", pg_conn
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    pg_conn.rollback()


# table names tell why the case is not supported
def test_insert_select_pushdown_unsupported(
    s3, pg_conn, extension, with_default_location
):

    run_command(
        """
		CREATE SCHEMA test_insert_select_pushdown_unsupported;
		SET search_path TO test_insert_select_pushdown_unsupported;

		CREATE TABLE table_with_generated_columns (height_cm numeric, height_in numeric GENERATED ALWAYS AS (height_cm / 2.54) STORED) USING iceberg;

		CREATE TABLE table_with_serial(id bigserial, value text) USING iceberg;

		CREATE TABLE table_with_not_null(id int NOT NULL) USING iceberg;
		CREATE TABLE table_with_check(id int CHECK (id >= 0)) USING iceberg;
		
		CREATE TABLE table_with_trigger (data text,created_at TIMESTAMP);

		CREATE OR REPLACE FUNCTION update_timestamp()
		RETURNS TRIGGER AS $$ BEGIN NEW.created_at := NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER set_timestamp BEFORE UPDATE ON table_with_trigger FOR EACH ROW EXECUTE FUNCTION update_timestamp();

		CREATE TABLE parent_table (id INT, data TEXT) PARTITION BY RANGE (id);
		CREATE TABLE parent_table_partition_1 PARTITION OF parent_table FOR VALUES FROM (1) TO (100) USING iceberg;

		CREATE DOMAIN simple_text AS TEXT CHECK (LENGTH(VALUE) <= 50);
		CREATE TABLE table_with_domain (id INT, description simple_text) USING iceberg;

        -- scale>precision is not safe to pushdown
		CREATE TABLE numeric_table (value numeric(25,26)) USING iceberg;

        -- plain numeric is safe to pushdown
        CREATE TABLE numeric_table_2 (value numeric) USING iceberg;
        CREATE TABLE heap_numeric(value numeric);

		CREATE SEQUENCE test_seq;
		CREATE TABLE table_use_sequence(key int default nextval('test_seq'), value int) USING iceberg;
		CREATE TABLE table_use_sequence_2(key int);

        CREATE TABLE test_collation (name text) USING iceberg;
        CREATE COLLATION s_coll (LOCALE="C");

""",
        pg_conn,
    )

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_with_generated_columns SELECT i FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_with_serial(value) SELECT i::text FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_with_not_null SELECT i FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_with_check SELECT i FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_with_trigger SELECT i FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO parent_table_partition_1 SELECT i,i::text FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_with_domain SELECT i,i::text FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO numeric_table SELECT random()*0.01 FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_use_sequence(value) SELECT i FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO table_use_sequence_2 SELECT nextval('test_seq') FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO numeric_table_2 SELECT * FROM heap_numeric",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    results = run_query(
        """EXPLAIN ANALYZE INSERT INTO test_collation SELECT i::text COLLATE "s_coll" FROM generate_series(0,10)i """,
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    # this time supported case
    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO numeric_table_2 SELECT random()*0.01 FROM generate_series(0,10)i",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    pg_conn.rollback()


_UNSUITABLE_LOC = f"s3://{TEST_BUCKET}/test_unsuitable"

_INSERT_SELECT_UNSUITABLE_CASES = [
    # --- domain nested inside an array ---
    pytest.param(
        f"""
        CREATE DOMAIN pos_int AS INT CHECK (VALUE > 0);
        CREATE FOREIGN TABLE src (id INT, vals pos_int[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/domain_in_array/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, vals pos_int[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/domain_in_array/tgt/', format 'parquet');
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="domain-in-array",
    ),
    # --- bad numeric (scale>precision) nested inside an array ---
    pytest.param(
        f"""
        CREATE FOREIGN TABLE src (id INT, vals numeric(25,26)[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/bad_numeric_in_array/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, vals numeric(25,26)[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/bad_numeric_in_array/tgt/', format 'parquet');
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="bad-numeric-in-array",
    ),
    # --- bad numeric nested inside a composite ---
    pytest.param(
        f"""
        CREATE TYPE has_bad_num AS (v numeric(25,26));
        CREATE FOREIGN TABLE src (id INT, d has_bad_num)
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/bad_numeric_in_struct/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, d has_bad_num)
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/bad_numeric_in_struct/tgt/', format 'parquet');
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="bad-numeric-in-struct",
    ),
    # --- bad numeric inside a composite inside an array (deep nesting) ---
    pytest.param(
        f"""
        CREATE TYPE with_bad_num AS (a INT, b numeric(25,26));
        CREATE FOREIGN TABLE src (id INT, vals with_bad_num[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/bad_numeric_in_struct_in_array/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, vals with_bad_num[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{_UNSUITABLE_LOC}/bad_numeric_in_struct_in_array/tgt/', format 'parquet');
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="bad-numeric-in-struct-in-array",
    ),
    # --- interval column (Iceberg stores as struct, needs special serde) ---
    pytest.param(
        """
        CREATE TABLE src (id INT, d INTERVAL) USING iceberg;
        CREATE TABLE tgt (id INT, d INTERVAL) USING iceberg;
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="interval-iceberg",
    ),
    # --- interval inside an array (Iceberg) ---
    pytest.param(
        """
        CREATE TABLE src (id INT, vals INTERVAL[]) USING iceberg;
        CREATE TABLE tgt (id INT, vals INTERVAL[]) USING iceberg;
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="interval-in-array-iceberg",
    ),
    # --- interval inside a composite (Iceberg) ---
    pytest.param(
        """
        CREATE TYPE has_interval AS (a INT, b INTERVAL);
        CREATE TABLE src (id INT, d has_interval) USING iceberg;
        CREATE TABLE tgt (id INT, d has_interval) USING iceberg;
        """,
        "INSERT INTO tgt SELECT * FROM src",
        None,
        id="interval-in-struct-iceberg",
    ),
]


@pytest.mark.parametrize(
    "setup_sql, insert_query, map_type", _INSERT_SELECT_UNSUITABLE_CASES
)
def test_insert_select_nested_unsuitable_types(
    s3,
    pg_conn,
    extension,
    with_default_location,
    setup_sql,
    insert_query,
    map_type,
):
    """INSERT..SELECT must NOT be pushed down when a column contains a type
    unsuitable for pushdown — whether at the top level, inside an array,
    composite, or map.
    """
    if map_type:
        create_map_type(*map_type)

    run_command(setup_sql, pg_conn)
    assert_query_not_pushdownable(insert_query, pg_conn)
    pg_conn.rollback()


def test_insert_select_domain_in_map_value(s3, pg_conn, superuser_conn, extension):
    """Domain as map value type must block pushdown."""
    run_command(
        "DROP DOMAIN IF EXISTS bounded_text CASCADE;"
        "CREATE DOMAIN bounded_text AS TEXT CHECK (LENGTH(VALUE) <= 10);",
        superuser_conn,
    )
    superuser_conn.commit()

    map_typename = create_map_type("int", "bounded_text")

    loc = f"s3://{TEST_BUCKET}/test_unsuitable/domain_in_map_value"
    run_command(
        f"""
        CREATE FOREIGN TABLE src (id INT, m {map_typename})
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, m {map_typename})
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/tgt/', format 'parquet');
        """,
        pg_conn,
    )
    assert_query_not_pushdownable("INSERT INTO tgt SELECT * FROM src", pg_conn)
    pg_conn.rollback()


def test_insert_select_domain_in_map_key(s3, pg_conn, superuser_conn, extension):
    """Domain as map key type must block pushdown."""
    run_command(
        "DROP DOMAIN IF EXISTS small_int CASCADE;"
        "CREATE DOMAIN small_int AS INT CHECK (VALUE < 1000);",
        superuser_conn,
    )
    superuser_conn.commit()

    map_typename = create_map_type("small_int", "text")

    loc = f"s3://{TEST_BUCKET}/test_unsuitable/domain_in_map_key"
    run_command(
        f"""
        CREATE FOREIGN TABLE src (id INT, m {map_typename})
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, m {map_typename})
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/tgt/', format 'parquet');
        """,
        pg_conn,
    )
    assert_query_not_pushdownable("INSERT INTO tgt SELECT * FROM src", pg_conn)
    pg_conn.rollback()


def test_insert_select_domain_in_struct(s3, pg_conn, extension):
    """Domain inside a composite: even EXPLAIN fails because the FDW
    planning path cannot resolve domain types in DuckDB struct
    definitions.  The error itself proves the query cannot be pushed down.
    """
    loc = f"s3://{TEST_BUCKET}/test_unsuitable/domain_in_struct"
    run_command(
        f"""
        CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0);
        CREATE TYPE has_domain AS (id INT, val positive_int);
        CREATE FOREIGN TABLE domain_src (id INT, d has_domain)
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/src/', format 'parquet');
        CREATE FOREIGN TABLE domain_tgt (id INT, d has_domain)
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/tgt/', format 'parquet');
        """,
        pg_conn,
    )
    with pytest.raises(psycopg2.errors.IndeterminateDatatype):
        run_query(
            "EXPLAIN INSERT INTO domain_tgt SELECT * FROM domain_src",
            pg_conn,
        )
    pg_conn.rollback()


def test_insert_select_domain_in_struct_in_array(s3, pg_conn, extension):
    """Domain inside a composite inside an array: EXPLAIN fails because
    the FDW planning path cannot resolve domain types in DuckDB struct
    definitions.  The error itself proves the query cannot be pushed down.
    """
    loc = f"s3://{TEST_BUCKET}/test_unsuitable/domain_in_struct_in_array"
    run_command(
        f"""
        CREATE DOMAIN nonneg AS INT CHECK (VALUE >= 0);
        CREATE TYPE with_domain AS (a INT, b nonneg);
        CREATE FOREIGN TABLE src (id INT, vals with_domain[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/src/', format 'parquet');
        CREATE FOREIGN TABLE tgt (id INT, vals with_domain[])
            SERVER pg_lake OPTIONS (writable 'true',
            location '{loc}/tgt/', format 'parquet');
        """,
        pg_conn,
    )
    with pytest.raises(psycopg2.errors.IndeterminateDatatype):
        run_query(
            "EXPLAIN INSERT INTO tgt SELECT * FROM src",
            pg_conn,
        )
    pg_conn.rollback()


# we can only pushdown INSERT .. SELECT as the top level command, not inside a CTE
def test_insert_select_ctes(s3, pg_conn, extension, with_default_location):

    run_command(
        """
            CREATE SCHEMA test_insert_select_ctes;
            SET search_path TO test_insert_select_ctes;

            CREATE TABLE source_table (x int, y int) USING iceberg;
            CREATE TABLE heap_source_table (x int, y int) USING heap;
            CREATE TABLE target_table (x int, y int) USING iceberg;

            INSERT INTO heap_source_table SELECT i FROM generate_series(1,10) i;
	""",
        pg_conn,
    )

    results = run_query(
        "EXPLAIN ANALYZE WITH cte_1 AS (INSERT INTO source_table SELECT i FROM generate_series(1,10) i RETURNING *) SELECT count(*) FROM cte_1;",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    # Modifying CTE with only Iceberg table
    results = run_query(
        "EXPLAIN ANALYZE WITH del AS (DELETE FROM source_table RETURNING *) INSERT INTO target_table SELECT * FROM del",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[10]]

    # Non-modifying CTEs with not-pushdownable heap table
    results = run_query(
        "EXPLAIN ANALYZE WITH sel AS (SELECT * FROM heap_source_table) INSERT INTO target_table SELECT * FROM sel",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[20]]

    # CTE with heap table in the subquery part of the insert..select
    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO target_table WITH sel AS (SELECT * FROM heap_source_table) SELECT * FROM sel",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[30]]

    # materialized CTE with heap table
    results = run_query(
        "EXPLAIN ANALYZE WITH sel AS MATERIALIZED (SELECT * FROM heap_source_table) INSERT INTO target_table SELECT * FROM sel",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[40]]

    # nested CTE with heap table
    results = run_query(
        """
         EXPLAIN ANALYZE
         WITH sel AS (WITH double_sel as (SELECT *,random() FROM heap_source_table) SELECT x,y from double_sel)
         INSERT INTO target_table SELECT * FROM sel
        """,
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[50]]

    pg_conn.rollback()


def test_insert_select_disabled(s3, pg_conn, extension, with_default_location):

    run_command(
        """
            CREATE SCHEMA test_insert_select_disabled;
            SET search_path TO test_insert_select_disabled;

            CREATE TABLE source_table (x int, y int) USING iceberg;
            CREATE TABLE target_table (x int, y int) USING iceberg;

            INSERT INTO source_table VALUES (1,2);

            SET pg_lake_table.enable_insert_select_pushdown TO off;
    """,
        pg_conn,
    )

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO target_table SELECT * FROM source_table",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[1]]

    pg_conn.rollback()


def test_insert_select_dropped_cols(s3, pg_conn, extension, with_default_location):

    run_command(
        """
            CREATE SCHEMA test_insert_select_dropped_cols;
            SET search_path TO test_insert_select_dropped_cols;

            CREATE TABLE source_table (drop_col_1 int, key int, drop_col_2 json, value text, drop_col_3 numeric) USING iceberg;
            CREATE TABLE target_table (drop_col_1 text, key int, drop_col_2 numeric, value text, drop_col_3 json) USING iceberg;

            INSERT INTO source_table SELECT i, i, '{"a":"b"}'::json, 'test', i FROM generate_series(0,10)i;
            INSERT INTO target_table SELECT 'test', i,  i, 'test', '{"c":"d"}'::json FROM generate_series(0,10)i;
            
            ALTER TABLE source_table DROP COLUMN drop_col_1, DROP COLUMN drop_col_2, DROP COLUMN drop_col_3;
            ALTER TABLE target_table DROP COLUMN drop_col_1, DROP COLUMN drop_col_2, DROP COLUMN drop_col_3;

    """,
        pg_conn,
    )

    results = run_query(
        "EXPLAIN ANALYZE INSERT INTO target_table SELECT * FROM source_table",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    res = run_query("SELECT count(*) FROM target_table", pg_conn)
    assert res == [[22]]

    pg_conn.rollback()


def test_bc_dates_insert_select_pushdown(
    pg_conn,
    extension,
    s3,
    with_default_location,
):
    """Verify BC dates roundtrip correctly through INSERT SELECT pushdown.

    INSERT INTO iceberg_table SELECT * FROM iceberg_table goes through
    WriteQueryResultTo (DuckDB-side COPY), bypassing PGDuckSerialize.
    This test ensures the ISO-year conversion produces correct results
    in the pushed-down path.
    """
    run_command(
        """
        CREATE SCHEMA test_bc_insert_pushdown;
        SET search_path TO test_bc_insert_pushdown;

        CREATE TABLE source (
            col_date date,
            col_ts timestamp,
            col_tstz timestamptz
        ) USING iceberg;

        CREATE TABLE target (
            col_date date,
            col_ts timestamp,
            col_tstz timestamptz
        ) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET TIME ZONE 'UTC';", pg_conn)
    run_command(
        """INSERT INTO source VALUES
            ('4712-01-01 BC', '0001-01-01 00:00:00', '0001-01-01 00:00:00+00'),
            ('0001-01-01 BC', '0001-06-15 12:30:00', '0001-06-15 12:30:00+00'),
            ('2021-01-01', '2021-01-01 00:00:00', '2021-01-01 00:00:00+00');""",
        pg_conn,
    )
    pg_conn.commit()

    # INSERT SELECT pushdown: data flows through DuckDB without PGDuckSerialize
    results = run_query(
        "EXPLAIN (VERBOSE) INSERT INTO target SELECT * FROM source",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(results)

    run_command("INSERT INTO target SELECT * FROM source", pg_conn)
    pg_conn.commit()

    result = run_query(
        "SELECT col_date::text AS d, col_ts::text AS ts, col_tstz::text AS tstz "
        "FROM target ORDER BY col_date;",
        pg_conn,
    )

    assert normalize_bc(result) == [
        ["4712-01-01 BC", "0001-01-01 00:00:00", "0001-01-01 00:00:00+00"],
        ["0001-01-01 BC", "0001-06-15 12:30:00", "0001-06-15 12:30:00+00"],
        ["2021-01-01", "2021-01-01 00:00:00", "2021-01-01 00:00:00+00"],
    ]

    run_command("RESET TIME ZONE;", pg_conn)
    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_bc_insert_pushdown CASCADE;", pg_conn)
    pg_conn.commit()
