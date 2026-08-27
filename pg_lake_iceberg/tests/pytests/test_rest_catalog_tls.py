import psycopg2
import pytest

from utils_pytest import *

# The mutual TLS settings are global while catalogs are not, and curl reads the
# files when it builds the TLS context rather than when a server asks for a
# certificate.  A path it cannot read therefore fails every REST catalog,
# including catalogs that never asked for mutual TLS.  These tests pin the
# check that turns that into an error where the mistake is made.

TLS_SETTINGS = [
    "pg_lake_iceberg.tls_ca_file",
    "pg_lake_iceberg.tls_cert_file",
    "pg_lake_iceberg.tls_key_file",
]


@pytest.mark.parametrize("setting", TLS_SETTINGS)
def test_an_unreadable_tls_file_is_refused(superuser_conn, setting):
    with pytest.raises(psycopg2.Error) as excinfo:
        run_command_outside_tx(
            [f"ALTER SYSTEM SET {setting} TO '/nonexistent/pg_lake_tls_test.pem'"]
        )

    assert "Cannot read file" in str(excinfo.value)


@pytest.mark.parametrize("setting", TLS_SETTINGS)
def test_a_readable_tls_file_is_accepted(superuser_conn, setting, tmp_path):
    readable = tmp_path / "tls.pem"
    readable.write_text("not a certificate, but readable\n")

    try:
        run_command_outside_tx([f"ALTER SYSTEM SET {setting} TO '{readable}'"])
    finally:
        run_command_outside_tx([f"ALTER SYSTEM RESET {setting}"])


@pytest.mark.parametrize("setting", TLS_SETTINGS)
def test_leaving_a_tls_setting_empty_is_accepted(superuser_conn, setting):
    """Empty means unconfigured, which is how every non-mTLS catalog runs."""
    try:
        run_command_outside_tx([f"ALTER SYSTEM SET {setting} TO ''"])
    finally:
        run_command_outside_tx([f"ALTER SYSTEM RESET {setting}"])
