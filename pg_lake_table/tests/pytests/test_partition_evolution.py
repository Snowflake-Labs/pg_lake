"""
Verify that changing the partition spec with ALTER FOREIGN TABLE produces the
expected *new* Parquet files.

Step-by-step
------------

 1. Start with   partition_by = truncate(10,a), b
    → 4 partitions, 2 rows each → 4 files

 2. ALTER … SET  truncate(5,a), truncate(1,b), c
    → 8 new partitions, 2 rows each → 8 **new** files

 3. ALTER … DROP partition_by
    → table becomes un-partitioned → 1 **new** file with 4 rows
"""

import uuid
from utils_pytest import *

SCHEMA = "test_part_alter_files"
PART1_WIDTH = 10  # first truncate width for column a
PART2_WIDTH = 5  # second truncate width for column a
PARTB_WIDTH = 1  # second truncate width for column b


# ─── helpers ────────────────────────────────────────────────────────────────
def _rand_table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)


def _current_files(tbl: str, pg_conn):
    """return set of file paths belonging to *tbl*."""
    res = run_query(
        f"SELECT path FROM lake_table.files " f"WHERE table_name = '{tbl}'::regclass;",
        pg_conn,
    )
    return {r[0] for r in res}


def _file_holds_single_key(path: str, key_sql: str, duck_conn) -> None:
    """assert that <path> contains rows from exactly one distinct key."""
    cnt = run_query(
        f"SELECT count(*) FROM (SELECT DISTINCT {key_sql} FROM '{path}') t;",
        duck_conn,
    )[0][0]
    assert int(cnt) == 1


# partition-key SQL snippets for DuckDB checks
KEY_STAGE1 = (
    f"floor(a / {PART1_WIDTH}) * {PART1_WIDTH}  AS a_trunc, "
    f"b                                         AS b_part"
)

KEY_STAGE2 = (
    f"floor(a / {PART2_WIDTH}) * {PART2_WIDTH}  AS a_trunc, "
    f"substr(b,1,{PARTB_WIDTH})                 AS b_trunc, "
    f"c"
)


# ─── main test ──────────────────────────────────────────────────────────────
def test_partition_evolution(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
):
    _setup_schema(pg_conn)
    tbl = f"{SCHEMA}.{_rand_table('alter_part')}"

    run_command(
        f"""
        CREATE TABLE {tbl}(a INT, b TEXT, c BIGINT)
        USING iceberg
        WITH (partition_by = 'truncate({PART1_WIDTH}, a), b',
              autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # ─── stage 1  ── initial insert (4 partitions, 2 rows each) ────────────
    before = _current_files(tbl, pg_conn)

    run_command(
        f"""
        INSERT INTO {tbl}(a,b,c) VALUES
          ( 1,'x',10),( 1,'x',10),
          ( 2,'y',20),( 2,'y',20),
          (15,'x',30),(15,'x',30),
          (16,'y',40),(16,'y',40);
        """,
        pg_conn,
    )

    files_stage1 = _current_files(tbl, pg_conn) - before
    assert len(files_stage1) == 4

    for p in files_stage1:
        rows = run_query(f"SELECT count(*) FROM '{p}'", pgduck_conn)[0][0]
        assert rows == 2
        _file_holds_single_key(p, KEY_STAGE1, pgduck_conn)

    # ─── stage 2  ── ALTER SET … + second insert (8 new partitions) ────────
    run_command(
        f"ALTER TABLE {tbl} OPTIONS "
        f"(SET partition_by 'truncate({PART2_WIDTH}, a), truncate({PARTB_WIDTH}, b), c');",
        pg_conn,
    )

    before = _current_files(tbl, pg_conn)

    run_command(
        f"""
        INSERT INTO {tbl}(a,b,c) VALUES
          ( 1,'x',1 ),( 1,'x',1 ),
          ( 6,'x',2 ),( 6,'x',2 ),
          (11,'y',1 ),(11,'y',1 ),
          (16,'y',2 ),(16,'y',2 ),
          (21,'x',1 ),(21,'x',1 ),
          (26,'x',2 ),(26,'x',2 ),
          (31,'y',1 ),(31,'y',1 ),
          (36,'y',2 ),(36,'y',2 );
        """,
        pg_conn,
    )

    files_stage2 = _current_files(tbl, pg_conn) - before
    assert len(files_stage2) == 8

    for p in files_stage2:
        assert run_query(f"SELECT count(*) FROM '{p}'", pgduck_conn)[0][0] == 2
        _file_holds_single_key(p, KEY_STAGE2, pgduck_conn)

    # ─── stage 3  ── ALTER DROP partition_by + final insert (un-partitioned) ─
    run_command(f"ALTER TABLE {tbl} OPTIONS (DROP partition_by);", pg_conn)

    before = _current_files(tbl, pg_conn)

    run_command(
        f"INSERT INTO {tbl}(a,b,c) VALUES "
        f"(100,'z',9),(101,'z',9),(102,'z',9),(103,'z',9);",
        pg_conn,
    )

    files_stage3 = _current_files(tbl, pg_conn) - before
    assert len(files_stage3) == 1

    path = next(iter(files_stage3))
    assert run_query(f"SELECT count(*) FROM '{path}'", pgduck_conn)[0][0] == 4

    # ─── cleanup session GUC ────────────────────────────────────────────────
    pg_conn.rollback()
