"""Generate Iceberg format-version 3 test fixtures.

This script is a one-shot generator. The fixtures it produces are committed
to the repo under ``test_common/sample/iceberg/sample_tables_v3/`` so that
tests can load them without depending on a running Spark cluster. Re-run
this script only when:

  * we intentionally change which v3 fixtures we keep,
  * a new Iceberg spec revision lands that changes the on-disk shape, or
  * a higher-priority bug needs a wider corpus.

Each run is deterministic in directory layout but **not** in file contents
(Spark generates random UUIDs and current-timestamp snapshot ids on every
write). Treat regenerated fixtures the same way one treats regenerated
golden files: review the diff carefully before committing.

Usage from the repo root::

    JAVA_HOME=$(/usr/libexec/java_home -v21) \
        pipenv run python test_common/sample/iceberg/scripts/generate_v3_fixtures.py

The script uses the Hadoop catalog so it can run without S3 / a JDBC
catalog. The committed files use Hadoop's ``vN.metadata.json`` naming
convention; pg_lake's reader keys off the JSON body's ``format-version``
field, so the filename convention is irrelevant.

Coverage goals
--------------

This corpus is designed so that every code path in pg_lake's existing v2
serializer (``pg_lake_iceberg/src/iceberg/write_table_metadata.c``,
``write_manifest.c``) plus every metadata-json field listed in the v3
spec that Spark 1.10.1 actually emits has at least one Spark-generated
fixture driving it. When the v3 reader/writer work lands (PLAN Stages 7,
11, 13, 18), every table here gets a round-trip assertion in
``test_v3_fixtures_smoke.py``; until then the round-trips are gated by
``xfail(strict=True)``.

Fixtures produced
-----------------

Foundational
~~~~~~~~~~~~

``public/v3_minimal``
    Empty v3 table with a single column. Smallest possible v3 metadata.
    Drives ``current-snapshot-id: null``, empty ``refs: {}``, empty
    ``snapshots`` / ``snapshot-log`` / ``metadata-log``, ``next-row-id: 0``,
    empty ``statistics: []`` and ``partition-statistics: []``.

``public/v3_row_lineage_appends``
    Multiple INSERTs of varying row counts, no partition, no sort. Drives
    monotonic ``next-row-id`` advancement, per-snapshot ``first-row-id``,
    per-snapshot ``added-rows`` summary, growing ``metadata-log``.

``public/v3_primitives``
    One column per Iceberg primitive that Spark 1.10.1 can emit through
    its DDL: bool, int, long, float, double, decimal(18,4),
    decimal(38,10), date, timestamp_ntz, timestamp (tz), string, binary.
    Three rows are inserted (one all-null, one min-bound-ish, one
    max-bound-ish) so manifest ``lower_bounds`` / ``upper_bounds`` are
    non-trivial for every type.

Schema evolution
~~~~~~~~~~~~~~~~

``public/v3_evolution_full``
    Walks every ALTER COLUMN flavour Spark 3.5 + Iceberg 1.10.1 supports:
    ADD, RENAME, type-promote int -> long and float -> double, decimal
    widen, AFTER reorder, DROP, plus an ADD of a decimal column. Snapshots
    are interleaved between ALTERs so the snapshot list references
    multiple schema-ids.

Partitioning
~~~~~~~~~~~~

``public/v3_partitioned_identity``
    Identity transform on a string column. Drives string-typed partition
    tuples in the manifest ``r102`` record.

``public/v3_partitioned_bucket``
    ``bucket(8, id)`` transform on a BIGINT column. Drives int-typed
    partition tuples (bucket result is always int) and the
    ``transform: "bucket[8]"`` serialization.

``public/v3_partitioned_truncate``
    ``truncate(3, name)`` transform on a string column. Drives the
    ``transform: "truncate[3]"`` serialization and string-typed truncated
    partition tuples.

``public/v3_partitioned_temporal``
    Walks ``years``, ``months``, ``days``, ``hours`` on the same
    TIMESTAMP column via ``REPLACE PARTITION FIELD``. Drives the four
    temporal transform strings plus partition-spec evolution (multiple
    entries under ``partition-specs`` with ``spec-id`` 0..3 and
    corresponding ``default-spec-id`` flips).

``public/v3_partitioned_multi``
    Three-field partition spec ``(region, days(ts), bucket(4, id))``.
    Drives multi-field partition tuples and mixed transform types within
    a single spec.

Sort orders
~~~~~~~~~~~

``public/v3_sorted_single``
    ``WRITE ORDERED BY name ASC NULLS LAST``. Drives a non-default
    ``sort-orders[1]`` entry, ``default-sort-order-id`` flip, and the
    manifest-entry ``sort_order_id`` propagation.

``public/v3_sorted_multi``
    ``WRITE ORDERED BY name ASC NULLS FIRST, id DESC NULLS LAST``. Drives
    the four-way matrix (asc/desc x nulls-first/nulls-last) of sort-field
    direction/null-order encoding.

Nested types
~~~~~~~~~~~~

``public/v3_nested_basic``
    One column per nested-type flavour: ``struct``, ``array<string>``,
    ``map<string, bigint>``, ``array<struct>``, ``map<string, struct>``.
    Drives every branch of the recursive type parser in the reader and
    the recursive type serializer in the writer, plus the element-id /
    key-id / value-id bookkeeping in ``last-column-id``.

``public/v3_nested_evolution``
    ALTERs on nested fields: ``ADD COLUMN point.z``, ``ADD COLUMN
    addresses.element.country``, ``RENAME COLUMN point.x TO point.lat``.
    Drives nested-field id allocation without top-level column changes.

Deletes (deletion vectors)
~~~~~~~~~~~~~~~~~~~~~~~~~~

``public/v3_pos_dv``
    ``write.delete.mode='merge-on-read'`` plus a row-level ``DELETE``.
    Spark 1.10.1 emits a Puffin-blob deletion vector for v3 tables. Drives
    the manifest-entry fields ``referenced_data_file``, ``content_offset``,
    ``content_size_in_bytes``, ``first_row_id`` (v3 row-lineage on the
    data file the DV references), and the snapshot-summary keys
    ``added-deletion-vectors`` / ``total-position-deletes``.

    The Puffin blob is binary and lands in the committed corpus; until
    the v3 DV reader lands (PLAN Stage 22) the smoke-test round-trip
    stays gated by ``xfail(strict=True)``. Committing now gives the
    Stage-22 work a forcing function.

Branches & tags
~~~~~~~~~~~~~~~

``public/v3_branches_tags``
    Creates one tag (``v1``, with ``max-ref-age-ms`` set via ``RETAIN 60
    DAYS``) and one named branch (``audit``, with all three optional
    retention fields set). Drives the full ``SnapshotReference`` field set
    (``snapshot-id``, ``type``, ``min-snapshots-to-keep``,
    ``max-snapshot-age-ms``, ``max-ref-age-ms``) and a multi-entry ``refs``
    map containing both branch and tag kinds.

Properties / codec round-trip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``public/v3_props_compressed``
    ``write.parquet.compression-codec='gzip'`` plus several free-form
    properties. Drives a non-trivial ``properties`` map round-trip and
    confirms that the codec switch actually produces gzip-compressed
    parquet data (size visible in the per-table summary printed below).

Explicitly skipped
------------------

The following v3-spec features are **not** driven from Spark today and
are scheduled for a later stage:

* ``initial-default`` / ``write-default`` -- Spark 3.5 + Iceberg 1.10.1
  rejects every DDL surface for column defaults (both ``CREATE TABLE
  (c TYPE DEFAULT lit)`` and ``ALTER TABLE ADD COLUMN c TYPE DEFAULT lit``
  fail with ``UNSUPPORTED_FEATURE.TABLE_OPERATION: Table does not support
  column default value``). PyIceberg-driven in PLAN Stage 15.
* Multi-arg partition transforms (``source-ids`` array length > 1) --
  no Spark DDL surface in 1.10.1. PyIceberg-driven in PLAN Stage 18.
* New v3 types (``variant``, ``geometry``, ``geography``, ``unknown``,
  ``timestamp_ns``, ``timestamptz_ns``) -- no Spark DDL surface in 1.10.1.
  PyIceberg-driven in PLAN Stages 17, 25-27.
* ``encryption-keys`` / per-snapshot ``key-id`` -- no Spark DDL surface
  in 1.10.1. PLAN Stage 28.
* Equality deletes -- Spark only emits DVs (positional) for v3. Equality
  deletes only appear in Trino/Flink-produced tables and are tested via
  the existing v2 corpus on read.
* ``last-row-id`` (top-level) -- Spark 1.10.1 emits only ``next-row-id``
  even though the spec lists both.
* Iceberg ``time`` and ``uuid`` types -- Spark has no DDL literal for
  these.
* ``fixed[N]`` -- Spark has no DDL surface for fixed-size binary.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
TARGET_ROOT = REPO_ROOT / "test_common" / "sample" / "iceberg" / "sample_tables_v3"

ICEBERG_VERSION = "1.10.1"
SCALA_VERSION = "2.12"
SPARK_MAJOR_MINOR = "3.5"
CATALOG_NAME = "local"


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------


def _make_spark():
    """Lazily import + build a SparkSession.

    Imported lazily so ``--help`` works without pyspark installed.
    """
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        (
            f"--packages org.apache.iceberg:iceberg-spark-runtime-"
            f"{SPARK_MAJOR_MINOR}_{SCALA_VERSION}:{ICEBERG_VERSION} "
            "pyspark-shell"
        ),
    )

    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    warehouse = tempfile.mkdtemp(prefix="pg_lake_v3_fixtures_")
    spark = (
        SparkSession.builder.appName("pg_lake iceberg v3 fixture generator")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse)
        # Force UTC so timestamp-typed sample rows round-trip identically
        # regardless of the host's TZ. Matches the production-test session
        # in test_common/helpers/spark.py.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark, Path(warehouse)


def _qualified(db: str, table: str) -> str:
    return f"{CATALOG_NAME}.{db}.{table}"


def _drop_create(spark, fq: str, ddl: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    spark.sql(ddl)


# ---------------------------------------------------------------------------
# Foundational
# ---------------------------------------------------------------------------


def _gen_v3_minimal(spark, db: str) -> None:
    """Empty v3 table: format-version=3, one column, no rows, no snapshots."""

    fq = _qualified(db, "v3_minimal")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )


def _gen_v3_row_lineage_appends(spark, db: str) -> None:
    """Row-lineage allocation across multiple appends of varying row counts.

    Drives monotonic ``next-row-id`` advancement at the table level and
    per-snapshot ``first-row-id`` / ``added-rows`` summary entries.
    Intentionally has no partition, no sort, no schema evolution -- the
    schema-evolution fixture covers that surface separately.
    """

    fq = _qualified(db, "v3_row_lineage_appends")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            payload STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"INSERT INTO {fq} VALUES (1, 'a'), (2, 'b')")
    spark.sql(f"INSERT INTO {fq} VALUES (3, 'c'), (4, 'd'), (5, 'e')")
    spark.sql(f"INSERT INTO {fq} VALUES (6, 'f')")
    # Larger batch to push first-row-id past trivial values.
    spark.sql(f"INSERT INTO {fq} SELECT id + 6, CAST(id AS STRING) FROM range(20)")


def _gen_v3_primitives(spark, db: str) -> None:
    """One column per Iceberg primitive Spark 1.10.1 can emit through DDL."""

    fq = _qualified(db, "v3_primitives")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            c_bool    BOOLEAN,
            c_int     INT,
            c_long    BIGINT,
            c_float   FLOAT,
            c_double  DOUBLE,
            c_dec_s   DECIMAL(18, 4),
            c_dec_l   DECIMAL(38, 10),
            c_date    DATE,
            c_ts_ntz  TIMESTAMP_NTZ,
            c_ts_tz   TIMESTAMP,
            c_str     STRING,
            c_bin     BINARY
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    # Three rows: one all-NULL, one near min-bound, one near max-bound.
    # The bounds drive non-trivial lower_bounds / upper_bounds in the
    # manifest, which exercises the binary-serde for every primitive.
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (NULL, NULL, NULL, NULL, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL),
            (false, -2147483648, -9223372036854775808, -3.4028235E38, -1.7976931348623157E308,
             CAST(-99999999999999.9999 AS DECIMAL(18, 4)),
             CAST(-9999999999999999999999999999.9999999999 AS DECIMAL(38, 10)),
             DATE '0001-01-01',
             TIMESTAMP_NTZ '0001-01-01 00:00:00',
             TIMESTAMP '0001-01-01 00:00:00 UTC',
             'a',
             CAST('\\x00' AS BINARY)),
            (true, 2147483647, 9223372036854775807, 3.4028235E38, 1.7976931348623157E308,
             CAST(99999999999999.9999 AS DECIMAL(18, 4)),
             CAST(9999999999999999999999999999.9999999999 AS DECIMAL(38, 10)),
             DATE '9999-12-31',
             TIMESTAMP_NTZ '9999-12-31 23:59:59',
             TIMESTAMP '9999-12-31 23:59:59 UTC',
             'zzz',
             CAST('\\xFF' AS BINARY))
        """)


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


def _gen_v3_evolution_full(spark, db: str) -> None:
    """Walk every ALTER COLUMN flavour Spark 3.5 + Iceberg 1.10.1 supports."""

    fq = _qualified(db, "v3_evolution_full")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            payload INT
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"INSERT INTO {fq} VALUES (1, 10), (2, 20)")

    spark.sql(f"ALTER TABLE {fq} ADD COLUMN flag BOOLEAN")
    spark.sql(f"INSERT INTO {fq} VALUES (3, 30, true)")

    spark.sql(f"ALTER TABLE {fq} RENAME COLUMN payload TO amount")
    spark.sql(f"INSERT INTO {fq} VALUES (4, 40, false)")

    spark.sql(f"ALTER TABLE {fq} ALTER COLUMN amount TYPE BIGINT")
    spark.sql(f"INSERT INTO {fq} VALUES (5, 50, true)")

    spark.sql(f"ALTER TABLE {fq} ALTER COLUMN amount AFTER flag")
    spark.sql(f"INSERT INTO {fq} VALUES (6, true, 60)")

    spark.sql(f"ALTER TABLE {fq} DROP COLUMN flag")
    spark.sql(f"INSERT INTO {fq} VALUES (7, 70)")

    spark.sql(f"ALTER TABLE {fq} ADD COLUMN extra DECIMAL(20, 5)")
    spark.sql(f"INSERT INTO {fq} VALUES (8, 80, CAST(1.23 AS DECIMAL(20, 5)))")

    spark.sql(f"ALTER TABLE {fq} ALTER COLUMN extra TYPE DECIMAL(28, 5)")
    spark.sql(f"INSERT INTO {fq} VALUES (9, 90, CAST(4.56 AS DECIMAL(28, 5)))")


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


def _gen_v3_partitioned_identity(spark, db: str) -> None:
    fq = _qualified(db, "v3_partitioned_identity")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            region STRING
        ) USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (1, 'us-east'), (2, 'us-east'),
            (3, 'eu-west'), (4, 'eu-west')
        """)


def _gen_v3_partitioned_bucket(spark, db: str) -> None:
    fq = _qualified(db, "v3_partitioned_bucket")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            name STRING
        ) USING iceberg
        PARTITIONED BY (bucket(8, id))
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(
        f"INSERT INTO {fq} SELECT id, CONCAT('row-', CAST(id AS STRING)) FROM range(16)"
    )


def _gen_v3_partitioned_truncate(spark, db: str) -> None:
    fq = _qualified(db, "v3_partitioned_truncate")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            name STRING
        ) USING iceberg
        PARTITIONED BY (truncate(3, name))
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (1, 'aardvark'), (2, 'aargh'),
            (3, 'banana'),   (4, 'banshee'),
            (5, 'cat'),      (6, 'caterpillar')
        """)


def _gen_v3_partitioned_temporal(spark, db: str) -> None:
    """All four temporal transforms via REPLACE PARTITION FIELD."""

    fq = _qualified(db, "v3_partitioned_temporal")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            ts TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (years(ts))
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"INSERT INTO {fq} VALUES (1, TIMESTAMP '2023-01-15 10:00:00')")

    spark.sql(f"ALTER TABLE {fq} REPLACE PARTITION FIELD years(ts) WITH months(ts)")
    spark.sql(f"INSERT INTO {fq} VALUES (2, TIMESTAMP '2023-06-15 10:00:00')")

    spark.sql(f"ALTER TABLE {fq} REPLACE PARTITION FIELD months(ts) WITH days(ts)")
    spark.sql(f"INSERT INTO {fq} VALUES (3, TIMESTAMP '2023-06-21 10:00:00')")

    spark.sql(f"ALTER TABLE {fq} REPLACE PARTITION FIELD days(ts) WITH hours(ts)")
    spark.sql(f"INSERT INTO {fq} VALUES (4, TIMESTAMP '2023-06-21 12:00:00')")


def _gen_v3_partitioned_multi(spark, db: str) -> None:
    fq = _qualified(db, "v3_partitioned_multi")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            region STRING,
            ts TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (region, days(ts), bucket(4, id))
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (1, 'us-east', TIMESTAMP '2024-01-15 10:00:00'),
            (2, 'us-east', TIMESTAMP '2024-01-15 11:00:00'),
            (3, 'us-east', TIMESTAMP '2024-01-16 10:00:00'),
            (4, 'eu-west', TIMESTAMP '2024-01-15 10:00:00'),
            (5, 'eu-west', TIMESTAMP '2024-01-15 11:00:00'),
            (6, 'eu-west', TIMESTAMP '2024-01-16 10:00:00'),
            (7, 'ap-south', TIMESTAMP '2024-01-15 10:00:00'),
            (8, 'ap-south', TIMESTAMP '2024-01-16 10:00:00')
        """)


# ---------------------------------------------------------------------------
# Sort orders
# ---------------------------------------------------------------------------


def _gen_v3_sorted_single(spark, db: str) -> None:
    fq = _qualified(db, "v3_sorted_single")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            name STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"ALTER TABLE {fq} WRITE ORDERED BY name ASC NULLS LAST")
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (1, 'charlie'),
            (2, 'alpha'),
            (3, NULL),
            (4, 'bravo')
        """)


def _gen_v3_sorted_multi(spark, db: str) -> None:
    fq = _qualified(db, "v3_sorted_multi")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            name STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(
        f"ALTER TABLE {fq} WRITE ORDERED BY name ASC NULLS FIRST, id DESC NULLS LAST"
    )
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (10, 'beta'),
            (1, 'alpha'),
            (5, NULL),
            (2, 'alpha'),
            (3, 'gamma')
        """)


# ---------------------------------------------------------------------------
# Nested types
# ---------------------------------------------------------------------------


def _gen_v3_nested_basic(spark, db: str) -> None:
    fq = _qualified(db, "v3_nested_basic")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            point     STRUCT<x: DOUBLE, y: DOUBLE>,
            tags      ARRAY<STRING>,
            attrs     MAP<STRING, BIGINT>,
            addresses ARRAY<STRUCT<street: STRING, zip: INT>>,
            by_region MAP<STRING, STRUCT<count: BIGINT, avg: DOUBLE>>
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (1,
             named_struct('x', 1.0, 'y', 2.0),
             array('alpha', 'beta'),
             map('k1', 10, 'k2', 20),
             array(named_struct('street', '101 Main', 'zip', 94001),
                   named_struct('street', '202 Oak',  'zip', 94002)),
             map('us-east', named_struct('count', 100, 'avg', 1.5),
                 'eu-west', named_struct('count', 200, 'avg', 2.5))),
            (2,
             named_struct('x', 3.0, 'y', 4.0),
             array('gamma'),
             map('k3', 30),
             array(named_struct('street', '303 Pine', 'zip', 94003)),
             map('ap-south', named_struct('count', 300, 'avg', 3.5)))
        """)


def _gen_v3_nested_evolution(spark, db: str) -> None:
    fq = _qualified(db, "v3_nested_evolution")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            point     STRUCT<x: DOUBLE, y: DOUBLE>,
            addresses ARRAY<STRUCT<street: STRING, zip: INT>>
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (1, named_struct('x', 1.0, 'y', 2.0),
             array(named_struct('street', '101 Main', 'zip', 94001)))
        """)

    # Drives nested-field id allocation without top-level column changes.
    spark.sql(f"ALTER TABLE {fq} ADD COLUMN point.z DOUBLE")
    spark.sql(f"ALTER TABLE {fq} ADD COLUMN addresses.element.country STRING")
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (2, named_struct('x', 3.0, 'y', 4.0, 'z', 5.0),
             array(named_struct('street', '202 Oak', 'zip', 94002,
                                'country', 'US')))
        """)

    # Spark 3.5 + Iceberg RENAME COLUMN nested syntax: the source path is
    # dotted but the target is just the new identifier within the same
    # parent struct (`point.x -> lat`, not `point.x -> point.lat`).
    spark.sql(f"ALTER TABLE {fq} RENAME COLUMN point.x TO lat")
    spark.sql(f"""
        INSERT INTO {fq} VALUES
            (3, named_struct('lat', 7.0, 'y', 8.0, 'z', 9.0),
             array(named_struct('street', '303 Pine', 'zip', 94003,
                                'country', 'US')))
        """)


# ---------------------------------------------------------------------------
# Deletes (deletion vectors)
# ---------------------------------------------------------------------------


def _gen_v3_pos_dv(spark, db: str) -> None:
    """v3 merge-on-read DELETE -- positional deletes or deletion vectors.

    Iceberg-spark-runtime 1.10.1 with ``format-version=3`` +
    ``write.delete.mode=merge-on-read`` is expected to emit a deletion
    vector (Puffin blob) for a sub-file delete, falling back to a
    positional-delete file if DV emission is gated. Either way the
    fixture drives a v3 DELETE snapshot with
    ``operation: delete`` and the file-level delete bookkeeping fields.

    We pre-load a single large parquet file (50 rows, coalesced to one
    Spark partition) so the DELETE WHERE id IN (...) cannot just drop
    whole files -- it has to either emit a DV or rewrite the file.
    Re-check the v3_pos_dv/metadata/v3.metadata.json snapshot summary
    after regeneration to confirm which delete strategy fired
    (look for ``added-delete-files`` / ``added-deletion-vectors`` vs
    ``deleted-data-files``).
    """

    fq = _qualified(db, "v3_pos_dv")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            payload STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
        """,
    )

    # Load 50 rows in a single coalesced partition so Spark writes them
    # into one parquet file. Without this, range(50) shards into many
    # tasks and produces many one-row files, which causes the DELETE
    # below to drop whole files instead of emitting DVs/position-deletes.
    spark.sql(f"""
        INSERT INTO {fq}
        SELECT /*+ COALESCE(1) */ id, CONCAT('row-', CAST(id AS STRING))
        FROM range(50)
        """)
    spark.sql(f"DELETE FROM {fq} WHERE id IN (5, 12, 21, 33, 47)")


# ---------------------------------------------------------------------------
# Branches & tags
# ---------------------------------------------------------------------------


def _gen_v3_branches_tags(spark, db: str) -> None:
    fq = _qualified(db, "v3_branches_tags")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            payload STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """,
    )
    spark.sql(f"INSERT INTO {fq} VALUES (1, 'a')")

    # Tag with max-ref-age-ms only (drives just the tag-flavoured ref entry).
    spark.sql(f"ALTER TABLE {fq} CREATE TAG v1 RETAIN 60 DAYS")

    spark.sql(f"INSERT INTO {fq} VALUES (2, 'b')")

    # Branch with all three optional retention fields. Drives the full
    # SnapshotReference field set.
    spark.sql(f"""
        ALTER TABLE {fq}
        CREATE BRANCH audit
        RETAIN 30 DAYS
        WITH SNAPSHOT RETENTION 5 SNAPSHOTS 7 DAYS
        """)

    spark.sql(f"INSERT INTO {fq} VALUES (3, 'c')")


# ---------------------------------------------------------------------------
# Properties / codec round-trip
# ---------------------------------------------------------------------------


def _gen_v3_props_compressed(spark, db: str) -> None:
    fq = _qualified(db, "v3_props_compressed")
    _drop_create(
        spark,
        fq,
        f"""
        CREATE TABLE {fq} (
            id BIGINT,
            payload STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'gzip',
            'pg_lake.test.kind' = 'props_compressed',
            'pg_lake.test.expected_codec' = 'gzip',
            'mykey' = 'myvalue'
        )
        """,
    )
    spark.sql(
        f"INSERT INTO {fq} SELECT id, CONCAT('row-', CAST(id AS STRING)) FROM range(50)"
    )


# ---------------------------------------------------------------------------
# Generator registry + driver
# ---------------------------------------------------------------------------

# Order matters only for the printed summary; each generator is independent.
GENERATORS = [
    ("v3_minimal", _gen_v3_minimal),
    ("v3_row_lineage_appends", _gen_v3_row_lineage_appends),
    ("v3_primitives", _gen_v3_primitives),
    ("v3_evolution_full", _gen_v3_evolution_full),
    ("v3_partitioned_identity", _gen_v3_partitioned_identity),
    ("v3_partitioned_bucket", _gen_v3_partitioned_bucket),
    ("v3_partitioned_truncate", _gen_v3_partitioned_truncate),
    ("v3_partitioned_temporal", _gen_v3_partitioned_temporal),
    ("v3_partitioned_multi", _gen_v3_partitioned_multi),
    ("v3_sorted_single", _gen_v3_sorted_single),
    ("v3_sorted_multi", _gen_v3_sorted_multi),
    ("v3_nested_basic", _gen_v3_nested_basic),
    ("v3_nested_evolution", _gen_v3_nested_evolution),
    ("v3_pos_dv", _gen_v3_pos_dv),
    ("v3_branches_tags", _gen_v3_branches_tags),
    ("v3_props_compressed", _gen_v3_props_compressed),
]


def _copy_table(warehouse: Path, db: str, table: str) -> Path:
    src = warehouse / db / table
    if not src.exists():
        raise FileNotFoundError(
            f"Spark did not produce table directory: {src}. "
            "Check Spark stderr above."
        )

    dst = TARGET_ROOT / db / table
    if dst.exists():
        shutil.rmtree(dst)

    def _ignore(_dir, names):
        # Skip Hadoop's per-file CRC sidecars; pg_lake's reader never
        # consults them and committing them doubles the fixture size.
        return [n for n in names if n.startswith(".") and n.endswith(".crc")]

    shutil.copytree(src, dst, ignore=_ignore)
    return dst


def _summarize(table_dir: Path) -> None:
    rel = table_dir.relative_to(REPO_ROOT)
    files = sorted(
        (p for p in table_dir.rglob("*") if p.is_file()),
        key=lambda p: p.relative_to(table_dir).as_posix(),
    )
    print(f"\n  {rel}/")
    for f in files:
        size = f.stat().st_size
        print(f"    {f.relative_to(table_dir).as_posix():<70} {size:>10} bytes")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default="public",
        help="namespace to write fixtures under (default: public)",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help=(
            "subset of tables to (re)generate (default: all). Useful when "
            "iterating on a single fixture without re-running the entire "
            "corpus."
        ),
    )
    args = parser.parse_args(argv)

    selected = GENERATORS
    if args.only:
        wanted = set(args.only)
        unknown = wanted - {name for name, _ in GENERATORS}
        if unknown:
            print(f"unknown table names: {sorted(unknown)}", file=sys.stderr)
            return 2
        selected = [(name, fn) for name, fn in GENERATORS if name in wanted]

    print(f"target: {TARGET_ROOT.relative_to(REPO_ROOT)}")
    print(f"tables: {[name for name, _ in selected]}")
    TARGET_ROOT.mkdir(parents=True, exist_ok=True)

    spark, warehouse = _make_spark()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{args.db}")
        for name, fn in selected:
            print(f"\n[generate] {args.db}.{name}")
            fn(spark, args.db)
    finally:
        spark.stop()

    written = [_copy_table(warehouse, args.db, name) for name, _ in selected]

    for d in written:
        _summarize(d)

    shutil.rmtree(warehouse, ignore_errors=True)
    print("\nDone. Stage the new files and commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
