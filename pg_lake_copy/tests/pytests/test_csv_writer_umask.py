"""Regression test: StartCopyTo must create the temp CSV with mode 0640.

The intermediate CSV that pg_lake_copy writes inside
$PGDATA/base/pgsql_tmp/ holds the full row image of every row being
copied, including columns the SQL caller is not allowed to read.  On
hosts where data_directory_mode=0750 (the RPM/DEB default since PG 11)
pgsql_tmp is group-traversable, so any OS user in the postgres group
can read those files.  StartCopyTo must therefore strip world-read.

The fix in csv_writer.c sets umask(0037), so files created via
fopen()/AllocateFile() land as 0640 (owner rw, group r, other none).
Group-read is intentional: pgduck_server runs as a different OS user
in the same group and reads these files via read_csv().

The temp file is unlinked when the COPY's memory context resets, so
it can only be observed mid-flight.  This test attaches the
'csv-writer-file-opened' injection point in 'wait' mode, runs a COPY
in a worker thread so StartCopyTo blocks with the temp file on disk,
then stat()s it and asserts the mode.
"""

import glob
import os
import stat
import threading
import time

import pytest
from utils_pytest import *

INJECTION_POINT = "csv-writer-file-opened"


def _wait_for_backend_at_injection_point(superuser_conn, timeout_seconds=30):
    """Poll pg_stat_activity until a backend is paused at our injection point."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        superuser_conn.rollback()
        rows = run_query(
            f"""
            SELECT count(*) AS n FROM pg_stat_activity
            WHERE wait_event_type = 'InjectionPoint'
              AND wait_event = '{INJECTION_POINT}'
            """,
            superuser_conn,
        )
        if int(rows[0]["n"]) > 0:
            return True
        time.sleep(0.05)
    return False


def test_csv_writer_temp_file_not_world_readable(
    s3,
    superuser_conn,
    extension,
    create_injection_extension,
):
    """The mid-flight temp CSV in pgsql_tmp must be 0640, not 0644."""
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("injection_points 'wait' requires PG17+")

    tmp_glob = os.path.join(
        server_params.PG_DIR, "base", "pgsql_tmp", "pgsql_tmp.pg_lake_copy_*"
    )

    out_url = f"s3://{TEST_BUCKET}/test_csv_writer_umask/data.parquet"

    run_command(
        f"SELECT injection_points_attach('{INJECTION_POINT}', 'wait')",
        superuser_conn,
    )
    superuser_conn.commit()

    copy_exception = []

    def run_copy():
        # The module-scoped fixtures must not be used from a background thread.
        conn = open_pg_conn()
        try:
            run_command(
                f"COPY (SELECT g FROM generate_series(1, 1000) g) TO '{out_url}'",
                conn,
            )
        except Exception as e:
            copy_exception.append(e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    worker = threading.Thread(target=run_copy, daemon=True)
    detached = False
    try:
        worker.start()

        assert _wait_for_backend_at_injection_point(
            superuser_conn
        ), f"No backend reached injection point '{INJECTION_POINT}'"

        files = glob.glob(tmp_glob)
        assert (
            files
        ), f"temp CSV file did not appear under {tmp_glob} while StartCopyTo was paused"

        mode = stat.S_IMODE(os.stat(files[0]).st_mode)

        # Properties asserted individually so a regression message names the
        # broken property.
        assert mode & 0o007 == 0, (
            f"SECURITY REGRESSION: temp CSV is world-accessible: {oct(mode)}. "
            f"StartCopyTo umask must strip S_IROTH|S_IWOTH|S_IXOTH."
        )
        assert mode & 0o040, (
            f"temp CSV dropped group-read ({oct(mode)}); pgduck_server "
            f"running as a different OS user in the same group can no longer "
            f"open it via read_csv()."
        )
        assert mode == 0o640, f"unexpected temp CSV mode: {oct(mode)} (want 0o640)"

        run_command(
            f"SELECT injection_points_wakeup('{INJECTION_POINT}')",
            superuser_conn,
        )
        run_command(
            f"SELECT injection_points_detach('{INJECTION_POINT}')",
            superuser_conn,
        )
        superuser_conn.commit()
        detached = True

        worker.join(timeout=30)
        assert not worker.is_alive(), "worker did not finish after wakeup"
        assert not copy_exception, f"COPY raised: {copy_exception[0]!r}"

    finally:
        if not detached:
            try:
                run_command(
                    f"SELECT injection_points_wakeup('{INJECTION_POINT}')",
                    superuser_conn,
                )
                run_command(
                    f"SELECT injection_points_detach('{INJECTION_POINT}')",
                    superuser_conn,
                )
                superuser_conn.commit()
            except Exception:
                superuser_conn.rollback()
            worker.join(timeout=30)
