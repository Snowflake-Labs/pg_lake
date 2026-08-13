"""Tests for the DDL interface of a tiered table (DESIGN.md section 5).

One CREATE TABLE ... USING timeseries is the whole interface: it makes the
partitioned heap, the Iceberg tier next to it, the registration that the planner
hook reads, and the partitions that make the table writable straight away. From
then on the table is altered, indexed and dropped like any other, and the only
thing the extension adds is keeping the Iceberg tier the same shape -- which is
where Iceberg's own rules on schema evolution start to show.

The properties worth testing are therefore that one statement produces a
complete tiered table, that the shape of the two tiers never drifts apart, and
that a statement the pair cannot honour is refused rather than half-applied.
"""

import os
import subprocess
from datetime import timedelta

from utils_pytest import *


COLUMNS = "ts timestamptz NOT NULL, device int, value float8"


def is_tiered(conn, name):
    return run_query(f"SELECT timeseries.is_tiered('{name}'::regclass)", conn)[0][0]


def registration(conn, name):
    row = run_query(f"SELECT * FROM timeseries.tiered_table('{name}'::regclass)", conn)[
        0
    ]
    # an unregistered relation gives one all-NULL row, not an empty result
    return None if row["cold_table"] is None else row


def registered(conn):
    return [
        row[0]
        for row in run_query(
            "SELECT relation::text FROM timeseries.tiered_tables() ORDER BY 1", conn
        )
    ]


def relkind(conn, name):
    return run_query(
        f"SELECT relkind FROM pg_class WHERE oid = '{name}'::regclass", conn
    )[0][0]


def exists(conn, name):
    return run_query(f"SELECT to_regclass('{name}') IS NOT NULL", conn)[0][0]


def columns(conn, name):
    """The live columns of a relation, in attribute order."""
    return [
        (row["attname"], row["type"])
        for row in run_query(
            "SELECT attname, format_type(atttypid, atttypmod) AS type"
            f" FROM pg_attribute WHERE attrelid = '{name}'::regclass"
            " AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            conn,
        )
    ]


def foreign_options(conn, name):
    return run_query(
        f"SELECT ftoptions FROM pg_foreign_table WHERE ftrelid = '{name}'::regclass",
        conn,
    )[0][0]


def partitions(conn, name):
    return [
        row[0]
        for row in run_query(
            f"SELECT inhrelid::regclass::text FROM pg_inherits"
            f" WHERE inhparent = '{name}'::regclass ORDER BY 1",
            conn,
        )
    ]


def fails(conn, statement):
    """A statement that has to be refused, with the transaction left usable."""
    error = run_command(statement, conn, raise_error=False)
    conn.rollback()
    assert error is not None, statement
    return error


def test_create_table_builds_a_whole_tiered_table(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    # a range-partitioned heap on the time column, made from the user's statement
    assert relkind(pg_conn, "metrics") == "p"
    assert (
        run_query("SELECT pg_get_partkeydef('metrics'::regclass)", pg_conn)[0][0]
        == "RANGE (ts)"
    )

    # an Iceberg tier next to it with the same columns in the same order
    assert relkind(pg_conn, "metrics_cold") == "f"
    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")

    # the registration the planner hook and maintenance read
    row = registration(pg_conn, "metrics")
    assert row["cold_table"] == "metrics_cold"
    assert row["time_column"] == "ts"
    assert row["partition_interval"] == timedelta(hours=1)
    assert row["hot_retention"] == timedelta(hours=2)
    assert is_tiered(pg_conn, "metrics")

    # the mark is on the heap only: the Iceberg tier is not itself tiered
    assert not is_tiered(pg_conn, "metrics_cold")
    assert registration(pg_conn, "metrics_cold") is None

    # a new table is authoritative in PostgreSQL for everything it holds
    assert (
        run_query(
            "SELECT boundary::text FROM timeseries.tiered_table('metrics'::regclass)",
            pg_conn,
        )[0][0]
        == "-infinity"
    )

    # and it is writable immediately, without waiting for maintenance
    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    pg_conn.commit()

    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1
    assert len(partitions(pg_conn, "metrics")) > 1


def test_the_options_have_defaults(tiered, pg_conn):
    tiered("metrics")

    row = registration(pg_conn, "metrics")

    assert row["partition_interval"] == timedelta(days=1)
    assert row["hot_retention"] == timedelta(days=7)
    assert row["cold_retention"] is None
    assert row["precreate_ahead"] == 7

    # day-long heap partitions are written a day at a time, so day granularity
    assert "partition_by=day(ts)" in foreign_options(pg_conn, "metrics_cold")


def test_the_options_are_honoured(tiered, pg_conn):
    tiered(
        "metrics",
        partition_interval="1 hour",
        hot_retention="6 hours",
        cold_retention="30 days",
        precreate_ahead=3,
    )

    row = registration(pg_conn, "metrics")

    assert row["partition_interval"] == timedelta(hours=1)
    assert row["hot_retention"] == timedelta(hours=6)
    assert row["cold_retention"] == timedelta(days=30)
    assert row["precreate_ahead"] == 3

    # partitions finer than a day are copied over an hour at a time
    assert "partition_by=hour(ts)" in foreign_options(pg_conn, "metrics_cold")

    # precreate_ahead bounds the frontier the initial partitions reach
    assert (
        max(
            row[0]
            for row in run_query(
                "SELECT part_end FROM timeseries.heap_ranges('metrics'::regclass)",
                pg_conn,
            )
        )
        <= run_query("SELECT now() + interval '4 hours'", pg_conn)[0][0]
    )

    # an option the extension does not know belongs to the Iceberg tier, which
    # validates it: max_snapshot_age is a number of seconds
    assert "max_snapshot_age=3600" in foreign_options(
        pg_conn, tiered("other", max_snapshot_age="3600") + "_cold"
    )


def test_the_time_column_is_the_one_timestamptz_column(tiered, pg_conn):
    tiered("metrics", "id bigint, ts timestamptz NOT NULL, value float8")

    assert registration(pg_conn, "metrics")["time_column"] == "ts"
    assert (
        run_query("SELECT pg_get_partkeydef('metrics'::regclass)", pg_conn)[0][0]
        == "RANGE (ts)"
    )


def test_the_time_column_can_be_named(tiered, pg_conn):
    tiered(
        "metrics",
        "recorded timestamptz NOT NULL, ingested timestamptz NOT NULL, value float8",
        time_column="recorded",
    )

    assert registration(pg_conn, "metrics")["time_column"] == "recorded"
    assert (
        run_query("SELECT pg_get_partkeydef('metrics'::regclass)", pg_conn)[0][0]
        == "RANGE (recorded)"
    )


def test_an_explicit_partition_by_names_the_time_column(tiered, pg_conn):
    """A user-written PARTITION BY settles the time column, ambiguity or not."""
    tiered(
        "metrics",
        "recorded timestamptz NOT NULL, ingested timestamptz NOT NULL, value float8",
        clauses="PARTITION BY RANGE (ingested)",
    )

    assert registration(pg_conn, "metrics")["time_column"] == "ingested"


def test_the_time_column_has_to_be_unambiguous(timeseries_extension, pg_conn):
    two = "recorded timestamptz NOT NULL, ingested timestamptz NOT NULL, value float8"

    error = fails(pg_conn, f"CREATE TABLE ambiguous ({two}) USING timeseries")
    assert "more than one timestamp with time zone column" in error

    error = fails(pg_conn, "CREATE TABLE untimed (id bigint) USING timeseries")
    assert "needs a timestamp with time zone column" in error

    # a named column still has to be one a tiered table can be split on
    error = fails(
        pg_conn,
        f"CREATE TABLE untyped ({COLUMNS}) USING timeseries"
        " WITH (time_column = 'device')",
    )
    assert "must be timestamp with time zone" in error

    error = fails(
        pg_conn,
        "CREATE TABLE nullable (ts timestamptz, device int)" " USING timeseries",
    )
    assert "must be NOT NULL" in error

    error = fails(
        pg_conn,
        "CREATE TABLE local_time (ts timestamp NOT NULL, device int)"
        " USING timeseries",
    )
    assert "needs a timestamp with time zone column" in error

    # and the partitioning has to be the one the boundary can be drawn on
    error = fails(
        pg_conn,
        f"CREATE TABLE hashed ({COLUMNS}) PARTITION BY HASH (device)"
        " USING timeseries",
    )
    assert "partitioned by range on a single column" in error

    error = fails(
        pg_conn,
        f"CREATE TABLE two_keys ({COLUMNS}) PARTITION BY RANGE (ts, device)"
        " USING timeseries",
    )
    assert "partitioned by range on a single column" in error

    error = fails(
        pg_conn,
        f"CREATE TABLE wrong_key ({COLUMNS}) PARTITION BY RANGE (device)"
        " USING timeseries WITH (time_column = 'ts')",
    )
    assert "must be partitioned by range on ts" in error

    # nothing above left a table, a tier or a registration behind
    for name in (
        "ambiguous",
        "untimed",
        "untyped",
        "nullable",
        "local_time",
        "hashed",
        "two_keys",
        "wrong_key",
    ):
        assert not exists(pg_conn, name), name
        assert not exists(pg_conn, f"{name}_cold"), name

    assert registered(pg_conn) == []


def test_the_option_values_are_validated(timeseries_extension, pg_conn):
    error = fails(
        pg_conn,
        f"CREATE TABLE monthly ({COLUMNS}) USING timeseries"
        " WITH (partition_interval = '1 month')",
    )
    assert "must be a fixed-length interval" in error

    error = fails(
        pg_conn,
        f"CREATE TABLE backwards ({COLUMNS}) USING timeseries"
        " WITH (partition_interval = '-1 hour')",
    )
    assert "must be positive" in error

    error = fails(
        pg_conn,
        f"CREATE TABLE zero ({COLUMNS}) USING timeseries"
        " WITH (partition_interval = '0 seconds')",
    )
    assert "must be positive" in error

    error = fails(
        pg_conn,
        f"CREATE TABLE ahead ({COLUMNS}) USING timeseries"
        " WITH (precreate_ahead = -1)",
    )
    assert "must not be negative" in error


def test_unsupported_create_variants_are_refused(timeseries_extension, pg_conn, tiered):
    tiered("metrics", partition_interval="1 hour")

    error = fails(pg_conn, f"CREATE TEMP TABLE t ({COLUMNS}) USING timeseries")
    assert "cannot be temporary or unlogged" in error

    error = fails(pg_conn, f"CREATE UNLOGGED TABLE u ({COLUMNS}) USING timeseries")
    assert "cannot be temporary or unlogged" in error

    error = fails(
        pg_conn, f"CREATE TABLE c ({COLUMNS}) INHERITS (metrics) USING timeseries"
    )
    assert "cannot inherit from another table" in error

    error = fails(
        pg_conn,
        "CREATE TABLE p PARTITION OF metrics"
        " FOR VALUES FROM ('2000-01-01') TO ('2000-01-02')"
        " USING timeseries",
    )
    assert "a partition cannot use the timeseries access method" in error

    error = fails(pg_conn, "CREATE TABLE a USING timeseries AS SELECT * FROM metrics")
    assert "not supported in CREATE TABLE AS" in error

    # an existing table cannot be turned into one, since the tier and the
    # partitioning would both have to appear underneath it
    error = fails(pg_conn, "ALTER TABLE metrics SET ACCESS METHOD timeseries")
    assert "cannot set the timeseries access method on an existing table" in error


def test_create_index_works_as_usual(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    run_command("CREATE INDEX metrics_device_idx ON metrics (device)", pg_conn)
    run_command(
        "CREATE UNIQUE INDEX metrics_ts_device_key ON metrics (ts, device)", pg_conn
    )
    pg_conn.commit()

    # the partitioned index reaches every partition, as it would on any heap
    parts = partitions(pg_conn, "metrics")
    indexes = run_query(
        "SELECT count(*) FROM pg_index WHERE indrelid = ANY ("
        " SELECT inhrelid FROM pg_inherits WHERE inhparent = 'metrics'::regclass)",
        pg_conn,
    )[0][0]

    assert indexes == 2 * len(parts)

    # the index is on the heap only, and the table still reads over both tiers
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1

    run_command("DROP INDEX metrics_device_idx", pg_conn)
    pg_conn.commit()

    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1


def test_add_and_drop_column_reach_both_tiers(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    run_command("ALTER TABLE metrics ADD COLUMN site text", pg_conn)
    pg_conn.commit()

    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")
    assert ("site", "text") in columns(pg_conn, "metrics")
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1

    run_command("ALTER TABLE metrics DROP COLUMN value", pg_conn)
    pg_conn.commit()

    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")
    assert "value" not in [name for name, _ in columns(pg_conn, "metrics")]

    # a column the heap does not have is PostgreSQL's error, not a cold-tier one
    error = fails(pg_conn, "ALTER TABLE metrics DROP COLUMN nosuchcolumn")
    assert 'column "nosuchcolumn" of relation "metrics" does not exist' in error

    # IF EXISTS is still a notice on both sides
    run_command("ALTER TABLE metrics DROP COLUMN IF EXISTS nosuchcolumn", pg_conn)
    pg_conn.commit()

    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")


def test_a_default_on_a_new_column_is_a_hot_tier_default(tiered, pg_conn):
    """Iceberg reads a field its data files lack as NULL, so only the heap fills."""
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    run_command("ALTER TABLE metrics ADD COLUMN site text DEFAULT 'unknown'", pg_conn)
    pg_conn.commit()

    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")
    assert run_query("SELECT site FROM metrics", pg_conn)[0][0] == "unknown"

    # the cold tier took the column but not the default
    assert (
        run_query(
            "SELECT count(*) FROM pg_attrdef d"
            " WHERE d.adrelid = 'metrics_cold'::regclass",
            pg_conn,
        )[0][0]
        == 0
    )


def test_a_type_change_iceberg_allows_reaches_both_tiers(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    pg_conn.commit()

    # int -> long is an Iceberg type promotion, so it is metadata-only there
    run_command("ALTER TABLE metrics ALTER COLUMN device TYPE bigint", pg_conn)
    pg_conn.commit()

    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")
    assert ("device", "bigint") in columns(pg_conn, "metrics")
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1


def test_a_type_change_iceberg_refuses_changes_nothing(tiered, pg_conn):
    """The tiers are altered in one transaction, so Iceberg's rules stop the heap."""
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    before = columns(pg_conn, "metrics")

    error = fails(pg_conn, "ALTER TABLE metrics ALTER COLUMN device TYPE text")
    assert "not supported for pg_lake_iceberg tables" in error

    assert columns(pg_conn, "metrics") == before
    assert columns(pg_conn, "metrics_cold") == before

    # a USING clause rewrites the values of a column, which Iceberg cannot do, so
    # applying it to the heap alone would compute the column two ways
    error = fails(
        pg_conn,
        "ALTER TABLE metrics ALTER COLUMN device TYPE bigint USING device + 1",
    )
    assert 'cannot change the type of column "device"' in error
    assert "USING expression" in error

    assert columns(pg_conn, "metrics") == before
    assert columns(pg_conn, "metrics_cold") == before


def test_rename_column_reaches_both_tiers(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    run_command("ALTER TABLE metrics RENAME COLUMN device TO sensor", pg_conn)
    pg_conn.commit()

    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")
    assert (
        run_query("SELECT count(*) FROM metrics WHERE sensor = 1", pg_conn)[0][0] == 1
    )

    # the time column is the field the Iceberg tier is partitioned on, and that
    # cannot be renamed, so the tiers are kept from disagreeing about its name
    error = fails(pg_conn, "ALTER TABLE metrics RENAME COLUMN ts TO recorded")
    assert 'cannot rename column "ts"' in error
    assert "time column of a tiered table" in error

    assert registration(pg_conn, "metrics")["time_column"] == "ts"
    assert columns(pg_conn, "metrics") == columns(pg_conn, "metrics_cold")

    # and the table still reads on both branches
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1
    assert (
        run_query(
            "SELECT count(*) FROM metrics WHERE ts < now() + interval '1 day'",
            pg_conn,
        )[0][0]
        == 1
    )


def test_the_time_column_cannot_be_dropped_or_retyped(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    before = columns(pg_conn, "metrics")

    error = fails(pg_conn, "ALTER TABLE metrics DROP COLUMN ts")
    assert 'cannot drop column "ts"' in error
    assert "time column of a tiered table" in error

    error = fails(pg_conn, "ALTER TABLE metrics ALTER COLUMN ts TYPE timestamp")
    assert 'cannot change the type of column "ts"' in error

    assert columns(pg_conn, "metrics") == before
    assert columns(pg_conn, "metrics_cold") == before


def test_the_settings_can_be_altered(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command(
        "ALTER TABLE metrics SET (hot_retention = '3 days',"
        " cold_retention = '30 days', precreate_ahead = 3)",
        pg_conn,
    )
    pg_conn.commit()

    row = registration(pg_conn, "metrics")

    assert row["hot_retention"] == timedelta(days=3)
    assert row["cold_retention"] == timedelta(days=30)
    assert row["precreate_ahead"] == 3
    # what was not named is untouched
    assert row["partition_interval"] == timedelta(hours=1)

    # a setting is not a relation option, so nothing lands in reloptions
    assert (
        run_query(
            "SELECT reloptions FROM pg_class WHERE oid = 'metrics'::regclass", pg_conn
        )[0][0]
        is None
    )

    run_command("ALTER TABLE metrics RESET (hot_retention, cold_retention)", pg_conn)
    pg_conn.commit()

    row = registration(pg_conn, "metrics")

    assert row["hot_retention"] == timedelta(days=7)
    assert row["cold_retention"] is None

    # a wider partition interval applies to the partitions added from now on
    run_command("ALTER TABLE metrics SET (partition_interval = '1 day')", pg_conn)
    pg_conn.commit()

    assert registration(pg_conn, "metrics")["partition_interval"] == timedelta(days=1)


def test_the_settings_are_validated(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    before = registration(pg_conn, "metrics")

    # the time column is the partition key, which no ALTER can move
    error = fails(pg_conn, "ALTER TABLE metrics SET (time_column = 'device')")
    assert "time_column of a tiered table cannot be changed" in error

    error = fails(pg_conn, "ALTER TABLE metrics SET (partition_interval = '1 month')")
    assert "must be a fixed-length interval" in error

    error = fails(pg_conn, "ALTER TABLE metrics SET (hot_retention = '-1 day')")
    assert "must not be negative" in error

    error = fails(pg_conn, "ALTER TABLE metrics SET (precreate_ahead = -1)")
    assert "must not be negative" in error

    # a setting is absorbed rather than executed, so it cannot be mixed with a
    # change PostgreSQL has to make
    error = fails(
        pg_conn,
        "ALTER TABLE metrics SET (hot_retention = '3 days'), ADD COLUMN site text",
    )
    assert "cannot change the settings of a tiered table together" in error

    assert registration(pg_conn, "metrics") == before
    assert "site" not in [name for name, _ in columns(pg_conn, "metrics")]

    # an option that is neither ours nor a heap option is PostgreSQL's error
    error = fails(pg_conn, "ALTER TABLE metrics SET (nosuchoption = 1)")
    assert "unrecognized parameter" in error


def test_drop_table_drops_both_tiers(tiered, pg_conn, test_s3_path):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")
    tiered("other", partition_interval="1 hour", hot_retention="2 hours")

    # the Iceberg tier belongs to the heap, so it cannot be dropped on its own
    error = fails(pg_conn, "DROP TABLE metrics_cold")
    assert "cannot drop" in error
    assert exists(pg_conn, "metrics_cold")

    run_command("DROP TABLE metrics", pg_conn)
    pg_conn.commit()

    # both tables are gone, and so is the registration row
    assert not exists(pg_conn, "metrics")
    assert not exists(pg_conn, "metrics_cold")
    assert registered(pg_conn) == ["other"]

    # a cascading drop reaches them as well
    run_command("CREATE SCHEMA ts_ddl", pg_conn)
    run_command(
        f"CREATE TABLE ts_ddl.nested ({COLUMNS}) USING timeseries"
        f" WITH (location = '{test_s3_path}/nested')",
        pg_conn,
    )
    pg_conn.commit()

    assert registered(pg_conn) == ["other", "ts_ddl.nested"]

    run_command("DROP SCHEMA ts_ddl CASCADE", pg_conn)
    pg_conn.commit()

    assert not exists(pg_conn, "ts_ddl.nested")
    assert registered(pg_conn) == ["other"]


def test_a_rolled_back_create_leaves_nothing(
    timeseries_extension, pg_conn, test_s3_path
):
    """The two tiers and the registration are made in one transaction."""
    run_command(
        f"CREATE TABLE rolled ({COLUMNS}) USING timeseries"
        f" WITH (location = '{test_s3_path}/rolled')",
        pg_conn,
    )

    assert is_tiered(pg_conn, "rolled")

    pg_conn.rollback()

    assert not exists(pg_conn, "rolled")
    assert not exists(pg_conn, "rolled_cold")
    assert registered(pg_conn) == []


def test_a_registration_from_another_backend_is_seen(tiered, pg_conn, superuser_conn):
    """The membership cache is backend-local, so it has to be invalidated."""
    # make the observer cache the negative answer, and an empty registry with it
    assert registered(superuser_conn) == []
    superuser_conn.commit()

    tiered("metrics", partition_interval="1 hour")

    assert is_tiered(superuser_conn, "metrics")
    assert registration(superuser_conn, "metrics")["cold_table"] == "metrics_cold"
    superuser_conn.commit()

    run_command("DROP TABLE metrics", pg_conn)
    pg_conn.commit()

    assert registered(superuser_conn) == []
    superuser_conn.commit()


def test_truncate_is_refused(tiered, pg_conn):
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    pg_conn.commit()

    error = fails(pg_conn, "TRUNCATE metrics")
    assert "cannot truncate tiered table metrics" in error

    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 1

    # a single partition is the user's to empty, since it is entirely hot
    partition = run_query(
        "SELECT partition::text FROM timeseries.heap_ranges('metrics'::regclass)"
        " WHERE part_start <= now() AND now() < part_end",
        pg_conn,
    )[0][0]

    run_command(f"TRUNCATE {partition}", pg_conn)
    pg_conn.commit()

    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == 0


def test_a_tiered_table_can_be_dumped(tiered, pg_conn):
    """pg_dump reaches both tiers: neither is hidden by the dependency between them."""
    tiered("metrics", partition_interval="1 hour", hot_retention="2 hours")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1.0)", pg_conn)
    pg_conn.commit()

    environment = dict(os.environ, PGPASSWORD=server_params.PG_PASSWORD)
    dump = subprocess.run(
        [
            os.path.join(PG_BINDIR, "pg_dump"),
            "-h",
            server_params.PG_HOST,
            "-p",
            str(server_params.PG_PORT),
            "-U",
            server_params.PG_USER,
            "-d",
            server_params.PG_DATABASE,
            "-t",
            "metrics",
            "-t",
            "metrics_*",
        ],
        capture_output=True,
        text=True,
        env=environment,
    )

    assert dump.returncode == 0, dump.stderr

    # the heap, its partitions and the Iceberg tier are all in the dump
    assert "CREATE TABLE public.metrics (" in dump.stdout
    assert "PARTITION BY RANGE (ts)" in dump.stdout
    assert "ATTACH PARTITION" in dump.stdout
    assert "CREATE FOREIGN TABLE public.metrics_cold (" in dump.stdout
