import pytest
from utils_pytest import *


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
