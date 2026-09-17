"""
VACUUM data-file compaction must preserve geometry EWKB.

Compaction reads WKB into DuckDB's native GEOMETRY representation before
rewriting Parquet files.  Pin the round trip here so the persisted geometry
encoding does not depend on DuckDB's GEOMETRY -> BLOB serialization.
"""

from utils_pytest import *


def test_geometry_survives_vacuum_compaction(
    user_conn, spatial_analytics_extension, pg_lake_extension
):
    user_conn.rollback()

    run_command(
        f"""
        CREATE FOREIGN TABLE geom_compact (id int, geom geometry)
        SERVER pg_lake_iceberg
        OPTIONS (
            location 's3://{TEST_BUCKET}/test_geometry_vacuum_compaction/',
            autovacuum_enabled 'false'
        );
        """,
        user_conn,
    )
    user_conn.commit()

    for i in range(5):
        run_command(
            f"""
            INSERT INTO geom_compact
            SELECT
                g,
                ST_SetSRID(ST_MakeEnvelope(g, 0, g + 1, 1), 4326)
            FROM generate_series({i * 40 + 1}, {(i + 1) * 40}) g;
            """,
            user_conn,
        )
        user_conn.commit()

    geometries = """
        SELECT id, encode(ST_AsEWKB(geom), 'hex') AS ewkb
        FROM geom_compact
        ORDER BY id
    """

    data_files = """
        SELECT count(*)
        FROM lake_iceberg.files(
            (
                SELECT metadata_location
                FROM iceberg_tables
                WHERE table_name = 'geom_compact'
            )
        )
        WHERE content = 'DATA'
    """

    before = run_query(geometries, user_conn)
    assert run_query(data_files, user_conn)[0][0] == 5
    user_conn.commit()

    user_conn.autocommit = True
    run_command(
        "SET pg_lake_table.vacuum_compact_min_input_files TO 1",
        user_conn,
    )
    run_command("VACUUM geom_compact", user_conn)
    run_command(
        "RESET pg_lake_table.vacuum_compact_min_input_files",
        user_conn,
    )
    user_conn.autocommit = False

    assert run_query(data_files, user_conn)[0][0] == 1
    assert run_query(geometries, user_conn) == before

    run_command("DROP FOREIGN TABLE geom_compact", user_conn)
    user_conn.commit()
