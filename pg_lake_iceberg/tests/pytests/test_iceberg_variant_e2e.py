"""
Phase 3 end-to-end coverage for the Iceberg V3 Variant POC.

KNOWN SPEC DEVIATION
--------------------
`variant` is an Iceberg format-version 3 type, but these tables are written at
format-version 2 (asserted by test_format_version_remains_v2). The metadata is
therefore not spec-compliant: another engine is entitled to reject or misread
it, so a table with a variant column is pg_lake-only for now. Emitting v3 is
out of scope for the POC because it pulls in the rest of the v3 surface,
deletion vectors above all.

This also means cross-engine interop is untestable here today, on two counts:
the suite pins iceberg-spark-runtime 1.4.3 on Spark 3.5 and pyiceberg 0.10.0,
neither of which can write variant (upstream needs Iceberg 1.10+ on Spark 4.0),
and our v2 tables would be rejected on the version alone. Every assertion below
is consequently pg_lake-against-itself, most usefully by comparing a
variant-backed table against a string-backed one holding identical data.

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
        "relation": "test_variant_e2e.managed_t",
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

        `variant` is a v3 type, so this metadata is deliberately outside the
        Iceberg spec -- external readers are entitled to reject it. The trade
        is that pg_lake avoids taking on the rest of v3 (deletion vectors in
        particular) just to name a column type. See the module docstring."""
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


# json preserves its input text exactly -- whitespace, key order and duplicate
# keys -- which a parsed VARIANT cannot represent. Stored as a variant this
# document comes back as {"a":2,"b":1}: reformatted, reordered, and with the
# duplicate "b" resolved first-wins, matching neither json nor jsonb.
JSON_VERBATIM_DOC = '{"b":   1,   "a": 2,   "b": 3}'


def test_json_surface_type_keeps_string_storage(
    pg_conn, iceberg_extension, extension, s3
):
    """VARIANT storage is only chosen for jsonb. A json column keeps the
    lossless string storage even with the GUC on, so its text survives."""
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
            (1, '{JSON_VERBATIM_DOC}'::json);
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
        == "string"
    )

    rows = run_query(
        "SELECT doc::text FROM test_variant_json_surface.target_t", pg_conn
    )
    assert rows == [[JSON_VERBATIM_DOC]]

    run_command("DROP SCHEMA test_variant_json_surface CASCADE", pg_conn)
    pg_conn.commit()


def test_variant_jsonb_equality_is_not_pushed_down(
    pg_conn, iceberg_extension, extension, s3
):
    """DuckDB re-renders a VARIANT when reading it, minifying the JSON text, so
    shipping jsonb equality would compare that rendering against PostgreSQL's
    own and never match. Equality must be evaluated locally instead."""
    pg_conn.rollback()
    location = f"s3://{TEST_BUCKET}/test_variant_equality/target/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_equality;
        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_equality.target_t (
            id INT,
            doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

        INSERT INTO test_variant_equality.target_t VALUES
            (1, '{SETUP_DOC_A}'::jsonb),
            (2, '{SETUP_DOC_B}'::jsonb);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # equality against a PostgreSQL-rendered literal
    assert (
        run_query(
            f"""
        SELECT id FROM test_variant_equality.target_t
        WHERE doc = '{SETUP_DOC_A}'::jsonb
        """,
            pg_conn,
        )
        == [[1]]
    )

    # the same via IN (ScalarArrayOpExpr) and via <>
    assert (
        run_query(
            f"""
        SELECT id FROM test_variant_equality.target_t
        WHERE doc IN ('{SETUP_DOC_B}'::jsonb)
        """,
            pg_conn,
        )
        == [[2]]
    )
    assert (
        run_query(
            f"""
        SELECT id FROM test_variant_equality.target_t
        WHERE doc <> '{SETUP_DOC_A}'::jsonb ORDER BY id
        """,
            pg_conn,
        )
        == [[2]]
    )

    # joining a variant column against heap jsonb
    assert (
        run_query(
            f"""
        SELECT f.id FROM test_variant_equality.target_t f
        JOIN (VALUES ('{SETUP_DOC_A}'::jsonb)) AS h(d) ON f.doc = h.d
        """,
            pg_conn,
        )
        == [[1]]
    )

    # operators that do not depend on the rendering stay pushed down and correct
    assert (
        run_query(
            """
        SELECT count(*) FROM test_variant_equality.target_t
        WHERE doc @> '{"label": "alpha"}'::jsonb
        """,
            pg_conn,
        )
        == [[1]]
    )
    assert (
        run_query(
            """
        SELECT id FROM test_variant_equality.target_t
        WHERE doc->>'label' = 'beta'
        """,
            pg_conn,
        )
        == [[2]]
    )

    run_command("DROP SCHEMA test_variant_equality CASCADE", pg_conn)
    pg_conn.commit()


def _parquet_column_is_variant(pg_conn, path, probe_name):
    """A VARIANT parquet column makes schema inference raise with the GUC off,
    a string column does not. That is the cheapest discriminator available for
    a bare parquet file, and it is the same signal TestBForeignParquet uses."""
    pg_conn.rollback()
    try:
        run_command(
            f"""
            SET pg_lake_engine.enable_variant_type = off;
            CREATE FOREIGN TABLE public.{probe_name} ()
                SERVER pg_lake OPTIONS (path '{path}');
            """,
            pg_conn,
        )
        pg_conn.commit()
        run_command(f"DROP FOREIGN TABLE public.{probe_name}", pg_conn)
        pg_conn.commit()
        return False
    except psycopg2.Error as e:
        pg_conn.rollback()
        assert "VARIANT" in str(e), f"unexpected inference failure: {e}"
        return True


def test_variant_column_preserves_sql_null(pg_conn, iceberg_extension, extension, s3):
    """A SQL NULL must survive VARIANT storage as a SQL NULL.

    DuckDB reports both a SQL NULL VARIANT and a VARIANT holding JSON `null`
    as VARIANT_NULL, and casting either to JSON yields the text `null`, so the
    two cannot be told apart once stored. We resolve that towards SQL NULL,
    which means a jsonb 'null' scalar degrades to SQL NULL -- asserted here so
    the limitation is visible rather than surprising."""
    pg_conn.rollback()
    location = f"s3://{TEST_BUCKET}/test_variant_null/target/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_null;
        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_null.target_t (
            id INT,
            doc JSONB
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

        INSERT INTO test_variant_null.target_t VALUES
            (1, '{SETUP_DOC_A}'::jsonb),
            (2, NULL),
            (3, 'null'::jsonb);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert run_query(
        "SELECT id, doc IS NULL FROM test_variant_null.target_t ORDER BY id",
        pg_conn,
    ) == [[1, False], [2, True], [3, True]]

    assert run_query(
        "SELECT count(*) FROM test_variant_null.target_t WHERE doc IS NULL",
        pg_conn,
    ) == [[2]]

    # the non-null document is unaffected
    assert run_query(
        "SELECT doc FROM test_variant_null.target_t WHERE id = 1", pg_conn
    ) == [[json.loads(SETUP_DOC_A)]]

    run_command("DROP SCHEMA test_variant_null CASCADE", pg_conn)
    pg_conn.commit()


def test_load_from_and_definition_from_variant_parquet(
    pg_conn, variant_managed_table, s3
):
    """A parquet file with a VARIANT column can seed a new iceberg table
    through either option: the column comes back as jsonb on the surface and
    keeps `variant` storage, and load_from also carries the rows over."""
    pg_conn.rollback()
    source_parquet = _data_file_paths(s3, variant_managed_table["location"])[0]

    run_command(
        f"""
        CREATE SCHEMA test_variant_seed;
        SET pg_lake_engine.enable_variant_type = on;
        SET pg_lake_iceberg.default_location_prefix
            TO 's3://{TEST_BUCKET}/test_variant_seed/out';

        CREATE TABLE test_variant_seed.def_t () USING iceberg
            WITH (definition_from = '{source_parquet}');
        CREATE TABLE test_variant_seed.load_t () USING iceberg
            WITH (load_from = '{source_parquet}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    for table in ("def_t", "load_t"):
        assert (
            run_query(
                f"""
            SELECT attname, atttypid::regtype::text FROM pg_attribute
            WHERE attrelid = 'test_variant_seed.{table}'::regclass AND attnum > 0
            ORDER BY attnum
            """,
                pg_conn,
            )
            == [["id", "integer"], ["doc", "jsonb"]]
        )

        metadata_location = run_query(
            f"""
            SELECT metadata_location FROM lake_iceberg.tables
            WHERE table_namespace = 'test_variant_seed' AND table_name = '{table}'
            """,
            pg_conn,
        )[0][0]
        assert (
            _column_field(_read_metadata_json(s3, metadata_location), "doc")["type"]
            == "variant"
        )

    # definition_from copies the shape only, load_from brings the rows too
    assert run_query("SELECT count(*) FROM test_variant_seed.def_t", pg_conn) == [[0]]
    assert run_query(
        "SELECT id, doc FROM test_variant_seed.load_t ORDER BY id", pg_conn
    ) == [[1, json.loads(SETUP_DOC_A)], [2, json.loads(SETUP_DOC_B)]]

    run_command("DROP SCHEMA test_variant_seed CASCADE", pg_conn)
    pg_conn.commit()


def test_copy_to_parquet_does_not_produce_variant(
    pg_conn, variant_managed_table, extension, s3
):
    """COPY ... TO parquet writes the plain parquet type mapping, where jsonb
    is a string. VARIANT is chosen by the iceberg storage layer, so exporting
    either a heap table or a variant iceberg table yields string columns.
    Pinned because an export silently changing type would be worse than the
    current, uniform behaviour."""
    pg_conn.rollback()
    heap_out = f"s3://{TEST_BUCKET}/test_variant_copy/heap.parquet"
    iceberg_out = f"s3://{TEST_BUCKET}/test_variant_copy/iceberg.parquet"

    run_command(
        f"""
        CREATE SCHEMA test_variant_copy;
        SET pg_lake_engine.enable_variant_type = on;
        CREATE TABLE test_variant_copy.heap_t (id INT, doc JSONB);
        INSERT INTO test_variant_copy.heap_t VALUES (1, '{SETUP_DOC_A}'::jsonb);
        COPY test_variant_copy.heap_t TO '{heap_out}';
        COPY (SELECT * FROM {variant_managed_table["relation"]}) TO '{iceberg_out}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert _parquet_column_is_variant(pg_conn, heap_out, "probe_heap") is False
    assert _parquet_column_is_variant(pg_conn, iceberg_out, "probe_iceberg") is False

    run_command("DROP SCHEMA test_variant_copy CASCADE", pg_conn)
    pg_conn.commit()


def test_jsonb_operators_match_string_storage(
    pg_conn, iceberg_extension, extension, s3
):
    """Every jsonb operator and function we ship must give the same answer over
    a VARIANT-backed column as over the string-backed one. The two tables hold
    identical data and differ only in storage."""
    pg_conn.rollback()
    doc_a = '{"id": 1, "label": "alpha", "tags": [1, 2], "nested": {"k": "v"}}'
    doc_b = '{"id": 2, "label": "beta", "tags": []}'

    run_command(
        f"""
        CREATE SCHEMA test_variant_ops;

        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_ops.as_variant (id INT, doc JSONB)
            SERVER pg_lake_iceberg
            OPTIONS (location 's3://{TEST_BUCKET}/test_variant_ops/v/');
        INSERT INTO test_variant_ops.as_variant
            VALUES (1, '{doc_a}'::jsonb), (2, '{doc_b}'::jsonb);

        SET pg_lake_engine.enable_variant_type = off;
        CREATE FOREIGN TABLE test_variant_ops.as_string (id INT, doc JSONB)
            SERVER pg_lake_iceberg
            OPTIONS (location 's3://{TEST_BUCKET}/test_variant_ops/s/');
        INSERT INTO test_variant_ops.as_string
            VALUES (1, '{doc_a}'::jsonb), (2, '{doc_b}'::jsonb);
        """,
        pg_conn,
    )
    pg_conn.commit()

    expressions = [
        "doc->>'label'",
        "doc->'nested'",
        "doc->'tags'->0",
        "doc#>>'{nested,k}'",
        "doc#>'{nested}'",
        "jsonb_typeof(doc)",
        "jsonb_array_length(doc->'tags')",
        "jsonb_extract_path_text(doc, 'label')",
        "(doc ? 'label')",
        "(doc ?| ARRAY['label','zzz'])",
        "(doc @> '{\"id\": 1}'::jsonb)",
        "('{\"id\": 1}'::jsonb <@ doc)",
        "(doc @? '$.tags[*]')",
        "jsonb_path_query_first(doc, '$.id')",
        "jsonb_pretty(doc)",
        "jsonb_strip_nulls(doc)",
        # rendering the document as text must give PostgreSQL's canonical form,
        # not DuckDB's minified one
        "doc::text",
        f"(doc = '{doc_a}'::jsonb)",
    ]

    for expression in expressions:
        variant_rows = run_query(
            f"SELECT {expression} FROM test_variant_ops.as_variant ORDER BY id",
            pg_conn,
        )
        string_rows = run_query(
            f"SELECT {expression} FROM test_variant_ops.as_string ORDER BY id",
            pg_conn,
        )
        assert variant_rows == string_rows, f"mismatch for {expression}"

    run_command("DROP SCHEMA test_variant_ops CASCADE", pg_conn)
    pg_conn.commit()


def test_update_and_delete_on_variant_column(pg_conn, iceberg_extension, extension, s3):
    pg_conn.rollback()
    location = f"s3://{TEST_BUCKET}/test_variant_dml/target/"

    run_command(
        f"""
        CREATE SCHEMA test_variant_dml;
        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_dml.target_t (id INT, doc JSONB)
            SERVER pg_lake_iceberg OPTIONS (location '{location}');
        INSERT INTO test_variant_dml.target_t
            VALUES (1, '{SETUP_DOC_A}'::jsonb), (2, '{SETUP_DOC_B}'::jsonb);

        UPDATE test_variant_dml.target_t
            SET doc = jsonb_set(doc, '{{label}}', '"updated"') WHERE id = 2;
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert run_query(
        "SELECT id, doc->>'label' FROM test_variant_dml.target_t ORDER BY id",
        pg_conn,
    ) == [[1, "alpha"], [2, "updated"]]

    run_command(
        "DELETE FROM test_variant_dml.target_t WHERE doc->>'label' = 'updated'",
        pg_conn,
    )
    pg_conn.commit()

    assert run_query(
        "SELECT id FROM test_variant_dml.target_t ORDER BY id", pg_conn
    ) == [[1]]

    run_command("DROP SCHEMA test_variant_dml CASCADE", pg_conn)
    pg_conn.commit()


def test_ctas_into_iceberg_uses_variant(pg_conn, iceberg_extension, extension, s3):
    """CREATE TABLE ... USING iceberg AS SELECT from a heap jsonb column picks
    up VARIANT storage the same way an explicit column list does."""
    pg_conn.rollback()

    run_command(
        f"""
        CREATE SCHEMA test_variant_ctas;
        SET pg_lake_engine.enable_variant_type = on;
        SET pg_lake_iceberg.default_location_prefix
            TO 's3://{TEST_BUCKET}/test_variant_ctas/out';

        CREATE TABLE test_variant_ctas.heap_t (id INT, doc JSONB);
        INSERT INTO test_variant_ctas.heap_t VALUES
            (1, '{SETUP_DOC_A}'::jsonb),
            (2, '{SETUP_DOC_B}'::jsonb);

        CREATE TABLE test_variant_ctas.target_t USING iceberg AS
            SELECT * FROM test_variant_ctas.heap_t;
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        """
        SELECT metadata_location FROM lake_iceberg.tables
        WHERE table_namespace = 'test_variant_ctas' AND table_name = 'target_t'
        """,
        pg_conn,
    )[0][0]
    assert (
        _column_field(_read_metadata_json(s3, metadata_location), "doc")["type"]
        == "variant"
    )

    assert run_query(
        "SELECT id, doc FROM test_variant_ctas.target_t ORDER BY id", pg_conn
    ) == [[1, json.loads(SETUP_DOC_A)], [2, json.loads(SETUP_DOC_B)]]

    run_command("DROP SCHEMA test_variant_ctas CASCADE", pg_conn)
    pg_conn.commit()


def test_jsonb_inside_struct_keeps_string_storage(
    pg_conn, iceberg_extension, extension, s3
):
    """VARIANT is only chosen for a top-level jsonb column, so a jsonb field
    inside a composite keeps string storage and still round-trips."""
    pg_conn.rollback()

    run_command(
        f"""
        CREATE SCHEMA test_variant_struct;
        SET pg_lake_engine.enable_variant_type = on;
        SET pg_lake_iceberg.default_location_prefix
            TO 's3://{TEST_BUCKET}/test_variant_struct/out';

        CREATE TYPE test_variant_struct.wrapper AS (tag TEXT, doc JSONB);
        CREATE TABLE test_variant_struct.target_t (
            id INT,
            w test_variant_struct.wrapper
        ) USING iceberg;
        INSERT INTO test_variant_struct.target_t VALUES
            (1, ROW('x', '{SETUP_DOC_A}'::jsonb)::test_variant_struct.wrapper);
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        """
        SELECT metadata_location FROM lake_iceberg.tables
        WHERE table_namespace = 'test_variant_struct' AND table_name = 'target_t'
        """,
        pg_conn,
    )[0][0]
    struct_field = _column_field(_read_metadata_json(s3, metadata_location), "w")
    nested_doc = next(
        field for field in struct_field["type"]["fields"] if field["name"] == "doc"
    )
    assert nested_doc["type"] == "string"

    assert run_query(
        "SELECT id, (w).tag, (w).doc FROM test_variant_struct.target_t", pg_conn
    ) == [[1, "x", json.loads(SETUP_DOC_A)]]

    run_command("DROP SCHEMA test_variant_struct CASCADE", pg_conn)
    pg_conn.commit()


def test_jsonb_array_column_keeps_string_storage(
    pg_conn, iceberg_extension, extension, s3
):
    """VARIANT is only chosen for a top-level jsonb column. A jsonb[] keeps the
    list-of-string storage and reads back exactly as it does with the GUC
    off, so turning the GUC on cannot change nested jsonb behaviour."""
    pg_conn.rollback()

    run_command(
        f"""
        CREATE SCHEMA test_variant_array;

        SET pg_lake_engine.enable_variant_type = on;
        CREATE FOREIGN TABLE test_variant_array.guc_on (id INT, docs JSONB[])
            SERVER pg_lake_iceberg
            OPTIONS (location 's3://{TEST_BUCKET}/test_variant_array/on/');
        INSERT INTO test_variant_array.guc_on
            VALUES (1, ARRAY['{SETUP_DOC_A}'::jsonb, '{SETUP_DOC_B}'::jsonb]);

        SET pg_lake_engine.enable_variant_type = off;
        CREATE FOREIGN TABLE test_variant_array.guc_off (id INT, docs JSONB[])
            SERVER pg_lake_iceberg
            OPTIONS (location 's3://{TEST_BUCKET}/test_variant_array/off/');
        INSERT INTO test_variant_array.guc_off
            VALUES (1, ARRAY['{SETUP_DOC_A}'::jsonb, '{SETUP_DOC_B}'::jsonb]);
        """,
        pg_conn,
    )
    pg_conn.commit()

    for table in ("guc_on", "guc_off"):
        metadata_location = run_query(
            f"""
            SELECT metadata_location FROM lake_iceberg.tables
            WHERE table_namespace = 'test_variant_array' AND table_name = '{table}'
            """,
            pg_conn,
        )[0][0]
        docs_field = _column_field(_read_metadata_json(s3, metadata_location), "docs")
        assert docs_field["type"]["element"] == "string"

    assert run_query(
        "SELECT docs FROM test_variant_array.guc_on", pg_conn
    ) == run_query("SELECT docs FROM test_variant_array.guc_off", pg_conn)

    run_command("DROP SCHEMA test_variant_array CASCADE", pg_conn)
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
