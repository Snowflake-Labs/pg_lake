import pytest
from utils_pytest import *


# the schema of the tables the tiered fixture builds
TIERED_COLUMNS = "ts timestamptz NOT NULL, device int, value float8"


@pytest.fixture(scope="module")
def timeseries_extension(superuser_conn, extension, app_user):
    """pg_lake_timeseries with the maintenance worker held back.

    Maintenance is driven explicitly from the tests so that assertions about the
    authority boundary and the partition states are deterministic; the one test
    that exercises the background worker re-enables it for its own duration.
    """
    run_command(
        "CREATE EXTENSION IF NOT EXISTS pg_lake_timeseries CASCADE;", superuser_conn
    )
    superuser_conn.commit()

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_timeseries.enable TO off",
            "ALTER SYSTEM SET pg_lake_timeseries.maintenance_naptime TO '200ms'",
            "SELECT pg_reload_conf()",
        ]
    )

    yield

    superuser_conn.rollback()
    run_command("DROP EXTENSION pg_lake_timeseries CASCADE;", superuser_conn)
    superuser_conn.commit()

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_timeseries.enable",
            "ALTER SYSTEM RESET pg_lake_timeseries.maintenance_naptime",
            "SELECT pg_reload_conf()",
        ]
    )


@pytest.fixture(scope="function")
def tiered(request, pg_conn, timeseries_extension, test_s3_path):
    """Factory for a tiered table, dropped afterwards.

    One CREATE TABLE ... USING timeseries is the whole interface: it makes the
    partitioned heap, the Iceberg tier next to it, the registration and the
    partitions covering the hot window. Options given as keywords land in the WITH
    clause, so a test spells out only what it cares about; `clauses` is for the
    ones that are grammar rather than options, such as PARTITION BY.
    """
    created = []
    # a location per test: DROP TABLE leaves the Iceberg files behind, and
    # CREATE TABLE ... USING iceberg refuses a non-empty location
    prefix = f"{test_s3_path}/{request.node.name}"

    def _create(name, columns=TIERED_COLUMNS, clauses="", **options):
        options.setdefault("location", f"{prefix}/{name}")
        with_clause = ", ".join(f"{key} = '{value}'" for key, value in options.items())

        run_command(
            f"CREATE TABLE {name} ({columns}) {clauses}"
            f" USING timeseries WITH ({with_clause})",
            pg_conn,
        )
        pg_conn.commit()

        created.append(name)
        return name

    yield _create

    pg_conn.rollback()
    for name in reversed(created):
        # the Iceberg tier is dropped along with the heap it depends on
        run_command(f"DROP TABLE IF EXISTS {name}", pg_conn, raise_error=False)
        pg_conn.commit()
