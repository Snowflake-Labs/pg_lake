import pytest
from utils_pytest import *


# The temporary CSV that carries rows from PostgreSQL to the query engine can be
# compressed (pg_lake_engine.temp_file_compression).  It is an internal exchange
# format, so the setting must make no difference to anything a user can observe.

COMPRESSION_MODES = ["none", "gzip"]

# Values chosen to stress the CSV dialect rather than the type system: embedded
# delimiters and quotes, the literal null sentinel next to a real NULL, an empty
# string, and a value carrying its own newlines.
ADVERSARIAL_QUERY = r"""
	SELECT g AS id,
		   'has,comma and "quote"' AS punctuated,
		   '\N' AS null_sentinel,
		   NULL::text AS real_null,
		   '' AS empty,
		   E'multi\nline\ttab' AS whitespace,
		   ('\x' || md5(g::text))::bytea AS blob,
		   ARRAY[1.5, NULL, 3.25]::numeric[] AS nums,
		   '2020-01-01 00:00:00+00'::timestamptz + (g || ' seconds')::interval AS ts,
		   jsonb_build_object('k', g) AS j
	FROM generate_series(1, 2000) g
"""


def write_parquet(conn, tmp_path, mode, name):
    """COPY the adversarial rows out as Parquet with the given compression mode."""
    run_command(f"SET pg_lake_engine.temp_file_compression TO '{mode}'", conn)

    parquet_path = tmp_path / f"{name}.parquet"
    copy_to_file(
        f"COPY ({ADVERSARIAL_QUERY}) TO STDOUT WITH (format 'parquet')",
        parquet_path,
        conn,
    )

    return parquet_path


@pytest.mark.parametrize("mode", COMPRESSION_MODES)
def test_temp_file_compression_preserves_values(pg_conn, duckdb_conn, tmp_path, mode):
    parquet_path = write_parquet(pg_conn, tmp_path, mode, f"values_{mode}")

    duckdb_conn.execute(
        """
		SELECT count(*),
			   count(DISTINCT punctuated),
			   count(DISTINCT null_sentinel),
			   count(real_null),
			   count(DISTINCT empty),
			   count(DISTINCT whitespace)
		FROM read_parquet($1)
	""",
        [str(parquet_path)],
    )

    # The three text columns each hold one distinct value across all rows, and
    # real_null is NULL everywhere, so count() over it is zero.
    assert duckdb_conn.fetchall() == [(2000, 1, 1, 0, 1, 1)]

    duckdb_conn.execute(
        """
		SELECT DISTINCT punctuated, null_sentinel, empty, whitespace
		FROM read_parquet($1)
	""",
        [str(parquet_path)],
    )

    # The null sentinel has to survive as its own text rather than becoming NULL.
    assert duckdb_conn.fetchall() == [
        ('has,comma and "quote"', "\\N", "", "multi\nline\ttab")
    ]

    pg_conn.rollback()


def test_temp_file_compression_matches_uncompressed(pg_conn, duckdb_conn, tmp_path):
    paths = {
        mode: write_parquet(pg_conn, tmp_path, mode, f"compare_{mode}")
        for mode in COMPRESSION_MODES
    }

    # The Parquet writer does not promise byte-identical files, so compare
    # contents rather than checksums.
    contents = {}
    for mode, path in paths.items():
        duckdb_conn.execute("SELECT * FROM read_parquet($1) ORDER BY id", [str(path)])
        contents[mode] = duckdb_conn.fetchall()

    assert contents["gzip"] == contents["none"]

    pg_conn.rollback()


@pytest.mark.parametrize("mode", COMPRESSION_MODES)
def test_temp_file_compression_iceberg_dml(
    extension, s3, pg_conn, with_default_location, mode
):
    # DELETE and UPDATE write their own exchange CSVs (the position list and the
    # re-inserted rows), and a large enough DELETE rewrites the data file by
    # reading that CSV back, so all three exchange paths run here.
    run_command(
        f"""
		SET pg_lake_engine.temp_file_compression TO '{mode}';
		CREATE TABLE temp_file_compression_{mode} (id int, txt text) USING iceberg;
		INSERT INTO temp_file_compression_{mode}
			SELECT g, 'row-' || g FROM generate_series(1, 5000) g;
		DELETE FROM temp_file_compression_{mode} WHERE id % 3 = 0;
		UPDATE temp_file_compression_{mode} SET txt = txt || '-updated'
			WHERE id % 7 = 0;
	""",
        pg_conn,
    )

    result = run_query(
        f"""
		SELECT count(*),
			   count(*) FILTER (WHERE txt LIKE '%-updated'),
			   sum(id)
		FROM temp_file_compression_{mode}
	""",
        pg_conn,
    )

    updated = len([g for g in range(1, 5001) if g % 3 != 0 and g % 7 == 0])
    remaining = [g for g in range(1, 5001) if g % 3 != 0]
    assert result == [[len(remaining), updated, sum(remaining)]]

    run_command(f"DROP TABLE temp_file_compression_{mode}", pg_conn)
    pg_conn.commit()
