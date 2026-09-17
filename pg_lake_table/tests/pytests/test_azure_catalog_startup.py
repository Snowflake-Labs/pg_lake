import json
from pathlib import Path
import time

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobType
from utils_pytest import *


@pytest.mark.parametrize(
    "enabled, leased, scheme",
    [
        (True, False, "azure"),
        (False, False, "azure"),
        (True, True, "azure"),
        (True, False, "az"),
    ],
)
def test_azure_catalog_startup_migrates_append_blob(
    superuser_conn, azure, extension, enabled, leased, scheme, installcheck
):
    if leased and installcheck:
        pytest.skip("startup retry assertion needs the test cluster log")
    database = "azure_catalog_startup"
    root = f"test_azure_catalog_startup/{enabled}/{leased}/{scheme}"
    key = f"{root}/frompg/catalog/{database}/catalog.json"
    blob = azure.get_blob_client(key)
    blob.create_append_blob()
    blob.append_block(b'{"tables":[],"legacy":true}')
    original_etag = blob.get_blob_properties().etag
    lease = blob.acquire_lease(lease_duration=-1) if leased else None
    superuser_conn.autocommit = True
    database_conn = None
    try:
        run_command(f"CREATE DATABASE {database}", superuser_conn)
        run_command(
            "ALTER SYSTEM SET pg_lake_iceberg.object_store_catalog_location_prefix "
            f"= '{scheme}://{TEST_BUCKET}/{root}'",
            superuser_conn,
        )
        run_command(
            "ALTER SYSTEM SET pg_lake_iceberg.enable_object_store_catalog "
            f"= '{'on' if enabled else 'off'}'",
            superuser_conn,
        )
        run_command("SELECT pg_reload_conf()", superuser_conn)
        time.sleep(0.2)
        database_conn = open_pg_conn_to_db(database)
        run_command("CREATE EXTENSION pg_lake_table CASCADE", database_conn)
        database_conn.commit()

        def worker_pid():
            database_conn.rollback()
            rows = run_query(
                "SELECT extension_base.get_worker_pid(worker_id) "
                "FROM extension_base.workers WHERE worker_name = 'iceberg vacuum worker'",
                database_conn,
            )
            return rows[0][0] if rows else 0

        if lease:
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                log_lines = (
                    Path(f"{server_params.PG_DIR}/logfile").read_text().splitlines()
                )
                if any(key in line and "LeaseIdMissing" in line for line in log_lines):
                    break
                time.sleep(0.2)
            else:
                pytest.fail("startup deletion failure was not recorded")
            assert blob.get_blob_properties().etag == original_etag
            previous_pid = worker_pid()
            assert previous_pid
            lease.release()
            lease = None
            run_command(f"SELECT pg_terminate_backend({previous_pid})", superuser_conn)

        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if worker_pid():
                try:
                    if (
                        not enabled
                        or blob.get_blob_properties().blob_type == BlobType.BLOCKBLOB
                    ):
                        break
                except ResourceNotFoundError:
                    pass
            time.sleep(0.2)
        else:
            pytest.fail("worker did not start and publish the migrated Azure catalog")

        if enabled:
            content = json.loads(blob.download_blob().readall())
            assert content["tables"] == []
            assert "catalog-snapshot-time" in content
            assert "legacy" not in content
            previous_pid = worker_pid()
            run_command(f"SELECT pg_terminate_backend({previous_pid})", superuser_conn)
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                current_pid = worker_pid()
                if current_pid and current_pid != previous_pid:
                    break
                time.sleep(0.2)
            else:
                pytest.fail("autovacuum worker did not restart")
            run_command(
                "SELECT lake_iceberg.force_push_object_store_catalog()", database_conn
            )
            database_conn.commit()
            assert blob.get_blob_properties().blob_type == BlobType.BLOCKBLOB
        else:
            time.sleep(2)
            assert blob.get_blob_properties().etag == original_etag
    finally:
        if database_conn:
            database_conn.close()
        if lease:
            lease.release()
        run_command(
            "ALTER SYSTEM RESET pg_lake_iceberg.object_store_catalog_location_prefix",
            superuser_conn,
        )
        run_command(
            "ALTER SYSTEM RESET pg_lake_iceberg.enable_object_store_catalog",
            superuser_conn,
        )
        run_command("SELECT pg_reload_conf()", superuser_conn)
        run_command(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)", superuser_conn)
        superuser_conn.autocommit = False
        if blob.exists():
            blob.delete_blob()
