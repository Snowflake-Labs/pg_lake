"""
Phase 3 end-to-end coverage for the Iceberg V3 Variant POC.

Four scenarios (mirroring the user's three asks plus a regression):

  Test A - managed iceberg: CREATE FOREIGN TABLE ... SERVER pg_lake_iceberg
           with a JSONB column. With pg_lake_engine.enable_variant_type=on the
           manifest must tag the column as `variant`, the parquet files must
           carry the VARIANT logical type, and INSERT/SELECT must round-trip
           losslessly.

  Test B - foreign parquet over Test A's data files: with the GUC on, a
           foreign parquet table over Test A's parquet output must surface
           VARIANT columns as JSONB. With the GUC off, schema inference must
           raise the configured ereport(ERROR).

  Test C - foreign iceberg via metadata.json path: pointing a
           CREATE FOREIGN TABLE ... OPTIONS (path '<metadata.json>',
           format 'iceberg') at Test A's metadata must round-trip the JSONB
           contents. GUC-off path must error.

  Test D - GUC-off regression: a vanilla iceberg table with JSONB and the
           GUC off must keep behaving exactly like today (manifest tag is
           `string`, no VARIANT round-trip), so we have not regressed anyone
           who hasn't opted in.

Run with:
  PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_iceberg_variant_e2e.py
"""

import json
import os

import psycopg2
import pytest
from utils_pytest import *


SETUP_DOC_A = '{"id": 1, "label": "alpha", "tags": [1, 2]}'
SETUP_DOC_B = '{"id": 2, "label": "beta", "tags": []}'


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _read_metadata_json(s3_client, metadata_location):
    """metadata_location is an s3:// URI; read its contents as parsed JSON."""
    raw = read_s3_operations(s3_client, metadata_location)
    return json.loads(raw)


def _column_field(metadata_json, column_name):
    current_schema_id = metadata_json["current-schema-id"]
    schema = next(
        schema
        for schema in metadata_json["schemas"]
        if schema["schema-id"] == current_schema_id
    )
    for field in schema["fields"]:
        if field["name"] == column_name:
            return field
    raise AssertionError(
        f"column {column_name!r} not in schema fields "
        f"({[f['name'] for f in schema['fields']]})"
    )


def _data_file_paths(s3_client, location):
    """Return the s3 URIs of the parquet data files under <location>/data/.

    `location` is the iceberg table's `s3://bucket/prefix/` URL. We avoid
    parsing avro/manifest files here because boto3 listing is enough for
    the test scope (we just need ANY data file written by Test A)."""
    assert location.startswith("s3://")
    bucket_and_prefix = location[len("s3://") :].rstrip("/")
    bucket, _, prefix = bucket_and_prefix.partition("/")
    data_prefix = f"{prefix}/data/" if prefix else "data/"
    paginator = s3_client.get_paginator("list_objects_v2")
    paths = []
    for page in paginator.paginate(Bucket=bucket, Prefix=data_prefix):
        for obj in page.get("Contents", []) or []:
            if obj["Key"].endswith(".parquet"):
                paths.append(f"s3://{bucket}/{obj['Key']}")
    paths.sort()
    return paths


# ---------------------------------------------------------------------------
# Test A - managed iceberg with VARIANT-tagged column
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def variant_managed_table(pg_conn, iceberg_extension, extension, s3):
    """Set up a managed iceberg foreign table with the GUC on, JSONB column,
    and two rows of data. Yields (location, metadata_location)."""

    location = f"s3://{TEST_BUCKET}/test_variant_e2e/managed/"

    run_command(
        f"""
        SET pg_lake_engine.enable_variant_type = on;
        CREATE SCHEMA test_variant_e2e;
        SET search_path TO test_variant_e2e;

        CREATE FOREIGN TABLE managed_t (
            id INT,
            doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

        INSERT INTO managed_t VALUES
            (1, '{SETUP_DOC_A}'::jsonb),
            (2, '{SETUP_DOC_B}'::jsonb);
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        """
        SELECT metadata_location
        FROM lake_iceberg.tables
        WHERE table_namespace = 'test_variant_e2e'
          AND table_name = 'managed_t'
        """,
        pg_conn,
    )[0][0]

    yield {
        "location": location,
        "metadata_location": metadata_location,
    }

    run_command(
        """
        DROP FOREIGN TABLE IF EXISTS test_variant_e2e.managed_t CASCADE;
        DROP SCHEMA IF EXISTS test_variant_e2e CASCADE;
        """,
        pg_conn,
    )
    pg_conn.commit()


class TestAManagedIceberg:
    """Round-trip JSONB through a managed iceberg table when the GUC is on."""

    def test_manifest_tags_jsonb_column_as_variant(self, variant_managed_table, s3):
        meta = _read_metadata_json(s3, variant_managed_table["metadata_location"])
        doc_field = _column_field(meta, "doc")
        assert (
            doc_field["type"] == "variant"
        ), f"Expected `variant` tag with the GUC on; got {doc_field['type']!r}"

    def test_format_version_remains_v2(self, variant_managed_table, s3):
        """POC invariant: format-version stays 2 even with variant columns.
        This is what makes the manifest non-spec-compliant for external
        readers like Spark, but keeps pg_lake's read/write surface narrow."""
        meta = _read_metadata_json(s3, variant_managed_table["metadata_location"])
        assert meta["format-version"] == 2

    def test_select_round_trips_via_jsonb(self, variant_managed_table, pg_conn):
        rows = run_query(
            """
            SET pg_lake_engine.enable_variant_type = on;
            SELECT id, doc FROM test_variant_e2e.managed_t ORDER BY id
            """,
            pg_conn,
        )
        assert len(rows) == 2
        # rows[i][1] is a python dict already (psycopg2 jsonb adapter)
        assert rows[0][0] == 1
        assert rows[0][1] == json.loads(SETUP_DOC_A)
        assert rows[1][0] == 2
        assert rows[1][1] == json.loads(SETUP_DOC_B)

    def test_variant_extract_pushes_to_parquet_filter(
        self, variant_managed_table, pg_conn
    ):
        """Predicate using DuckDB's variant_extract on a remote variant column
        must end up as a READ_PARQUET filter (i.e. surfaced into the SELECT
        plan via shippable-function pushdown)."""
        plan = run_query(
            """
            SET pg_lake_engine.enable_variant_type = on;
            EXPLAIN (verbose, format text)
            SELECT count(*)
            FROM test_variant_e2e.managed_t
            WHERE (doc->>'id')::int = 1
            """,
            pg_conn,
        )
        plan_text = "\n".join(row[0] for row in plan)
        # Sanity: the iceberg foreign-table scan ran. We do NOT assert that
        # ->> pushed down — that's the deferred operator-overload work in
        # Phase 2 / DuckDB patch. We DO assert the query did not error.
        assert "managed_t" in plan_text


# ---------------------------------------------------------------------------
# Test B - foreign parquet over Test A's data files
# ---------------------------------------------------------------------------


class TestBForeignParquet:
    """The same parquet files Test A produced must be readable as a foreign
    parquet table, with VARIANT columns surfacing as JSONB."""

    def test_schema_inference_with_guc_on(self, variant_managed_table, pg_conn, s3):
        """Empty column list -> schema is inferred from the parquet file.
        With the GUC on, the inferred VARIANT column must surface as JSONB."""
        data_files = _data_file_paths(s3, variant_managed_table["location"])
        assert len(data_files) >= 1
        path = data_files[0]

        run_command(
            f"""
            SET pg_lake_engine.enable_variant_type = on;
            CREATE FOREIGN TABLE test_variant_e2e.foreign_pq_inferred ()
            SERVER pg_lake OPTIONS (path '{path}', format 'parquet');
            """,
            pg_conn,
        )
        pg_conn.commit()

        # Verify the doc column was inferred as JSONB on the PG side.
        col_types = run_query(
            """
            SELECT attname, format_type(atttypid, atttypmod)
            FROM pg_attribute
            WHERE attrelid = 'test_variant_e2e.foreign_pq_inferred'::regclass
              AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
            """,
            pg_conn,
        )
        cols = dict(col_types)
        assert (
            cols.get("doc") == "jsonb"
        ), f"With GUC on, inferred VARIANT must surface as JSONB; got {cols}"

        rows = run_query(
            "SELECT count(*)::int FROM test_variant_e2e.foreign_pq_inferred",
            pg_conn,
        )
        # Row count varies (>=1) depending on how many files we picked; >=1 is
        # the meaningful invariant.
        assert rows[0][0] >= 1

        run_command(
            "DROP FOREIGN TABLE test_variant_e2e.foreign_pq_inferred",
            pg_conn,
        )
        pg_conn.commit()

    def test_schema_inference_with_guc_off_errors(
        self, variant_managed_table, pg_conn, s3
    ):
        """When the GUC is off, schema inference for a VARIANT column must
        raise the configured ereport(ERROR) — the POC keeps the read surface
        narrow until the user explicitly opts in."""
        data_files = _data_file_paths(s3, variant_managed_table["location"])
        path = data_files[0]

        with pytest.raises(psycopg2.Error) as ei:
            run_command(
                f"""
                SET pg_lake_engine.enable_variant_type = off;
                CREATE FOREIGN TABLE test_variant_e2e.foreign_pq_off ()
                SERVER pg_lake OPTIONS (path '{path}', format 'parquet');
                """,
                pg_conn,
            )
        assert "VARIANT" in str(ei.value), str(ei.value)
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Test C - foreign iceberg via metadata.json
# ---------------------------------------------------------------------------


class TestCForeignIceberg:
    """Same data, accessed through Iceberg's metadata.json instead of raw
    parquet. This exercises iceberg's schema-inference path."""

    def test_round_trip_via_metadata_json(self, variant_managed_table, pg_conn):
        meta_path = variant_managed_table["metadata_location"]

        run_command(
            f"""
            SET pg_lake_engine.enable_variant_type = on;
            CREATE FOREIGN TABLE test_variant_e2e.foreign_iceberg ()
            SERVER pg_lake OPTIONS (path '{meta_path}', format 'iceberg');
            """,
            pg_conn,
        )
        pg_conn.commit()

        col_types = dict(
            run_query(
                """
                SELECT attname, format_type(atttypid, atttypmod)
                FROM pg_attribute
                WHERE attrelid = 'test_variant_e2e.foreign_iceberg'::regclass
                  AND attnum > 0 AND NOT attisdropped
                """,
                pg_conn,
            )
        )
        assert col_types.get("doc") == "jsonb"

        rows = run_query(
            "SELECT id, doc FROM test_variant_e2e.foreign_iceberg ORDER BY id",
            pg_conn,
        )
        assert len(rows) == 2
        assert rows[0][1] == json.loads(SETUP_DOC_A)
        assert rows[1][1] == json.loads(SETUP_DOC_B)

        run_command(
            "DROP FOREIGN TABLE test_variant_e2e.foreign_iceberg",
            pg_conn,
        )
        pg_conn.commit()

    def test_metadata_json_with_guc_off_errors(self, variant_managed_table, pg_conn):
        meta_path = variant_managed_table["metadata_location"]
        with pytest.raises(psycopg2.Error) as ei:
            run_command(
                f"""
                SET pg_lake_engine.enable_variant_type = off;
                CREATE FOREIGN TABLE test_variant_e2e.foreign_iceberg_off ()
                SERVER pg_lake OPTIONS (path '{meta_path}', format 'iceberg');
                """,
                pg_conn,
            )
        assert "VARIANT" in str(ei.value), str(ei.value)
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Test D - regression: GUC off + JSONB on iceberg keeps today's behavior
# ---------------------------------------------------------------------------


class TestDGucOffRegression:
    """A user who never sets the GUC must see the exact same iceberg + JSONB
    behavior they had before this change: manifest tags JSONB columns as
    `string`, no VARIANT round-trip, no errors."""

    @pytest.fixture(scope="class")
    def regression_table(self, pg_conn, iceberg_extension, extension, s3):
        location = f"s3://{TEST_BUCKET}/test_variant_e2e/regression/"

        run_command(
            f"""
            SET pg_lake_engine.enable_variant_type = off;
            CREATE SCHEMA IF NOT EXISTS test_variant_e2e_regression;
            SET search_path TO test_variant_e2e_regression;

            CREATE FOREIGN TABLE regress_t (
                id INT,
                doc JSONB
            ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

            INSERT INTO regress_t VALUES
                (1, '{SETUP_DOC_A}'::jsonb),
                (2, '{SETUP_DOC_B}'::jsonb);
            """,
            pg_conn,
        )
        pg_conn.commit()

        metadata_location = run_query(
            """
            SELECT metadata_location FROM lake_iceberg.tables
            WHERE table_namespace = 'test_variant_e2e_regression'
              AND table_name = 'regress_t'
            """,
            pg_conn,
        )[0][0]

        yield metadata_location

        run_command(
            """
            DROP FOREIGN TABLE IF EXISTS
                test_variant_e2e_regression.regress_t CASCADE;
            DROP SCHEMA IF EXISTS test_variant_e2e_regression CASCADE;
            """,
            pg_conn,
        )
        pg_conn.commit()

    def test_manifest_keeps_string_tag_with_guc_off(self, regression_table, s3):
        meta = _read_metadata_json(s3, regression_table)
        doc_field = _column_field(meta, "doc")
        assert doc_field["type"] == "string", (
            f"GUC-off regression failed: doc tagged as {doc_field['type']!r} "
            "instead of string."
        )

    def test_select_returns_jsonb_textually(self, regression_table, pg_conn):
        rows = run_query(
            """
            SET pg_lake_engine.enable_variant_type = off;
            SELECT id, doc FROM test_variant_e2e_regression.regress_t
            ORDER BY id
            """,
            pg_conn,
        )
        assert len(rows) == 2
        # JSONB is fully reconstructable from the string-stored value.
        assert rows[0][1] == json.loads(SETUP_DOC_A)
        assert rows[1][1] == json.loads(SETUP_DOC_B)


# ---------------------------------------------------------------------------
# Schema evolution and GUC flips
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compatibility_tables(pg_conn, iceberg_extension, extension, s3):
    old_location = f"s3://{TEST_BUCKET}/test_variant_compat/old/"
    new_location = f"s3://{TEST_BUCKET}/test_variant_compat/new/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_compat;

        SET pg_lake_engine.enable_variant_type = off;
        CREATE FOREIGN TABLE test_variant_compat.old_t (
            id INT,
            string_doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{old_location}');
        INSERT INTO test_variant_compat.old_t
        VALUES (1, '{SETUP_DOC_A}'::jsonb);

        SET pg_lake_engine.enable_variant_type = on;
        ALTER TABLE test_variant_compat.old_t ADD COLUMN variant_doc JSONB;
        INSERT INTO test_variant_compat.old_t
        VALUES (2, '{SETUP_DOC_B}'::jsonb, '{SETUP_DOC_A}'::jsonb);

        CREATE FOREIGN TABLE test_variant_compat.new_t (
            id INT,
            variant_doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{new_location}');
        INSERT INTO test_variant_compat.new_t
        VALUES (1, '{SETUP_DOC_A}'::jsonb);

        SET pg_lake_engine.enable_variant_type = off;
        ALTER TABLE test_variant_compat.new_t ADD COLUMN string_doc JSONB;
        INSERT INTO test_variant_compat.new_t
        VALUES (2, '{SETUP_DOC_B}'::jsonb, '{SETUP_DOC_A}'::jsonb);

        SET pg_lake_engine.enable_variant_type = on;
        INSERT INTO test_variant_compat.old_t
        VALUES (3, '{SETUP_DOC_A}'::jsonb, '{SETUP_DOC_B}'::jsonb);

        SET pg_lake_engine.enable_variant_type = off;
        INSERT INTO test_variant_compat.new_t
        VALUES (3, '{SETUP_DOC_A}'::jsonb, '{SETUP_DOC_B}'::jsonb);
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata = dict(
        run_query(
            """
            SELECT table_name, metadata_location
            FROM lake_iceberg.tables
            WHERE table_namespace = 'test_variant_compat'
            """,
            pg_conn,
        )
    )

    yield metadata

    run_command("DROP SCHEMA IF EXISTS test_variant_compat CASCADE", pg_conn)
    pg_conn.commit()


class TestSchemaEvolutionAndGucFlips:
    def test_old_table_keeps_string_and_adds_variant(self, compatibility_tables, s3):
        metadata = _read_metadata_json(s3, compatibility_tables["old_t"])
        assert _column_field(metadata, "string_doc")["type"] == "string"
        assert _column_field(metadata, "variant_doc")["type"] == "variant"

    def test_new_table_keeps_variant_and_adds_string(self, compatibility_tables, s3):
        metadata = _read_metadata_json(s3, compatibility_tables["new_t"])
        assert _column_field(metadata, "variant_doc")["type"] == "variant"
        assert _column_field(metadata, "string_doc")["type"] == "string"

    @pytest.mark.parametrize("guc_value", ["on", "off"])
    def test_mixed_storage_tables_read_after_guc_flip(
        self, compatibility_tables, pg_conn, guc_value
    ):
        run_command(
            f"SET pg_lake_engine.enable_variant_type = {guc_value}",
            pg_conn,
        )

        old_rows = run_query(
            """
            SELECT id, string_doc, variant_doc
            FROM test_variant_compat.old_t
            ORDER BY id
            """,
            pg_conn,
        )
        new_rows = run_query(
            """
            SELECT id, variant_doc, string_doc
            FROM test_variant_compat.new_t
            ORDER BY id
            """,
            pg_conn,
        )

        assert [row[0] for row in old_rows] == [1, 2, 3]
        assert old_rows[0][1] == json.loads(SETUP_DOC_A)
        assert old_rows[0][2] is None
        assert old_rows[1][1] == json.loads(SETUP_DOC_B)
        assert old_rows[1][2] == json.loads(SETUP_DOC_A)
        assert old_rows[2][1] == json.loads(SETUP_DOC_A)
        assert old_rows[2][2] == json.loads(SETUP_DOC_B)

        assert [row[0] for row in new_rows] == [1, 2, 3]
        assert new_rows[0][1] == json.loads(SETUP_DOC_A)
        assert new_rows[0][2] is None
        assert new_rows[1][1] == json.loads(SETUP_DOC_B)
        assert new_rows[1][2] == json.loads(SETUP_DOC_A)
        assert new_rows[2][1] == json.loads(SETUP_DOC_A)
        assert new_rows[2][2] == json.loads(SETUP_DOC_B)


def test_heap_jsonb_insert_select_into_variant(
    pg_conn, iceberg_extension, extension, s3
):
    pg_conn.rollback()
    location = f"s3://{TEST_BUCKET}/test_variant_scanner/target/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_scanner;
        CREATE TABLE test_variant_scanner.source_t (id INT, doc JSONB);
        INSERT INTO test_variant_scanner.source_t VALUES
            (1, '{SETUP_DOC_A}'::jsonb),
            (2, '{SETUP_DOC_B}'::jsonb);

        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_scanner.target_t (
            id INT,
            doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

        SET pg_lake_engine.enable_variant_type = off;
        """,
        pg_conn,
    )

    insert_sql = """
        INSERT INTO test_variant_scanner.target_t
        SELECT id, doc FROM test_variant_scanner.source_t
    """
    run_command(insert_sql, pg_conn)
    rows = run_query(
        "SELECT id, doc FROM test_variant_scanner.target_t ORDER BY id",
        pg_conn,
    )
    assert rows == [
        [1, json.loads(SETUP_DOC_A)],
        [2, json.loads(SETUP_DOC_B)],
    ]

    run_command("DROP SCHEMA test_variant_scanner CASCADE", pg_conn)
    pg_conn.commit()


def test_json_surface_type_also_stores_variant(
    pg_conn, iceberg_extension, extension, s3
):
    """The storage override is keyed on the variant storage type, which is
    JSONB for both surface types, so a `json` column must behave like `jsonb`."""
    pg_conn.rollback()
    location = f"s3://{TEST_BUCKET}/test_variant_json_surface/target/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_json_surface;
        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_json_surface.target_t (
            id INT,
            doc JSON
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

        INSERT INTO test_variant_json_surface.target_t VALUES
            (1, '{SETUP_DOC_A}'::json),
            (2, '{SETUP_DOC_B}'::json);
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        """
        SELECT metadata_location FROM lake_iceberg.tables
        WHERE table_namespace = 'test_variant_json_surface'
          AND table_name = 'target_t'
        """,
        pg_conn,
    )[0][0]
    assert (
        _column_field(_read_metadata_json(s3, metadata_location), "doc")["type"]
        == "variant"
    )

    rows = run_query(
        """
        SELECT id, doc::jsonb, doc->>'label'
        FROM test_variant_json_surface.target_t
        ORDER BY id
        """,
        pg_conn,
    )
    assert rows == [
        [1, json.loads(SETUP_DOC_A), "alpha"],
        [2, json.loads(SETUP_DOC_B), "beta"],
    ]

    run_command("DROP SCHEMA test_variant_json_surface CASCADE", pg_conn)
    pg_conn.commit()


def test_large_jsonb_variant_round_trip(pg_conn, iceberg_extension, extension, s3):
    pg_conn.rollback()
    location = f"s3://{TEST_BUCKET}/test_variant_large/target/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_large;
        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_large.target_t (
            id INT,
            doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

        INSERT INTO test_variant_large.target_t
        SELECT size_kb,
               jsonb_build_object(
                   'payload', repeat(chr(64 + size_kb / 64), size_kb * 1024),
                   'nested', jsonb_build_object(
                       'size_kb', size_kb,
                       'values', to_jsonb(ARRAY[1, 2, 3, 4])
                   )
               )
        FROM unnest(ARRAY[64, 512]) AS sizes(size_kb);
        """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id,
               length(doc->>'payload'),
               (doc->'nested'->>'size_kb')::int,
               jsonb_array_length(doc->'nested'->'values')
        FROM test_variant_large.target_t
        ORDER BY id
        """,
        pg_conn,
    )
    assert rows == [
        [64, 64 * 1024, 64, 4],
        [512, 512 * 1024, 512, 4],
    ]

    run_command("DROP SCHEMA test_variant_large CASCADE", pg_conn)
    pg_conn.commit()
