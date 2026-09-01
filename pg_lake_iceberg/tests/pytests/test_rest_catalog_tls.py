import datetime
import ipaddress
import socket
import ssl
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from utils_pytest import *

# The mutual TLS settings describe the deployment rather than any one catalog,
# and curl reads the files when it builds the TLS context rather than when a
# server asks for a certificate.  A path it cannot read therefore fails every
# REST catalog, including catalogs that never asked for mutual TLS.  These
# tests pin the check that turns that into an error where the mistake is made.

TLS_SETTINGS = [
    "pg_lake_iceberg.horizon_tls_ca_file",
    "pg_lake_iceberg.horizon_tls_cert_file",
    "pg_lake_iceberg.horizon_tls_key_file",
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


# ---------------------------------------------------------------------------
# A real mutual handshake
#
# The tests below run against a TLS server that demands a client certificate,
# so they answer two questions the settings alone cannot: whether the
# certificate reaches the server at all, and whether it reaches servers that
# have no business seeing it.  Both catalogs below are configured identically
# apart from rest_auth_type, which is what decides.
# ---------------------------------------------------------------------------

CLIENT_COMMON_NAME = "pg-lake-test-client"


def _issue_certificate(common_name, issuer=None, is_ca=False, ip_address=None):
    """Issue a certificate, self-signed unless an (cert, key) issuer is given."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    issuer_cert, issuer_key = issuer if issuer else (None, None)
    now = datetime.datetime.now(datetime.timezone.utc)

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_cert.subject if issuer_cert else subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )

    if ip_address:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ip_address)]), critical=False
        )

    return builder.sign(issuer_key or key, hashes.SHA256()), key


def _write_pem(directory, name, cert=None, key=None):
    path = directory / name
    if cert is not None:
        path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    else:
        path.write_bytes(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    return path


@pytest.fixture(scope="module")
def tls_material(tmp_path_factory):
    """One authority signing a server certificate and a client certificate."""
    directory = tmp_path_factory.mktemp("pg_lake_mtls")

    ca_cert, ca_key = _issue_certificate("pg-lake-test-ca", is_ca=True)
    server_cert, server_key = _issue_certificate(
        "127.0.0.1",
        issuer=(ca_cert, ca_key),
        ip_address=ipaddress.ip_address("127.0.0.1"),
    )
    client_cert, client_key = _issue_certificate(
        CLIENT_COMMON_NAME, issuer=(ca_cert, ca_key)
    )

    return {
        "ca": _write_pem(directory, "ca.pem", cert=ca_cert),
        "server_cert": _write_pem(directory, "server.pem", cert=server_cert),
        "server_key": _write_pem(directory, "server.key", key=server_key),
        "client_cert": _write_pem(directory, "client.pem", cert=client_cert),
        "client_key": _write_pem(directory, "client.key", key=client_key),
    }


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="function")
def mtls_catalog_server(tls_material):
    """
    A catalog that refuses to speak to a client without a certificate, and
    records the name on the one it is shown.
    """

    class _Handler(BaseHTTPRequestHandler):
        peer_common_names = []

        def _handle(self):
            subject = dict(
                pair
                for entry in self.connection.getpeercert()["subject"]
                for pair in entry
            )
            _Handler.peer_common_names.append(subject.get("commonName"))

            if "/oauth/tokens" in self.path:
                body = (
                    b'{"access_token": "t", "token_type": "bearer", "expires_in": 3600}'
                )
            else:
                body = b'{"namespace": ["myns"], "properties": {}}'

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tls_material["server_cert"], tls_material["server_key"])
    context.load_verify_locations(tls_material["ca"])
    context.verify_mode = ssl.CERT_REQUIRED

    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), _Handler)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    yield port, _Handler

    httpd.shutdown()
    thread.join(timeout=5)


_ALL_TLS_FILES = ("ca", "client_cert", "client_key")


def _use_catalog(conn, port, tls_material, auth_type, tls_files=_ALL_TLS_FILES):
    """
    Point the built-in catalog at the test server.  tls_files names which of
    the three pieces of client-certificate material to configure, so a test
    can leave one out.
    """
    host = f"https://127.0.0.1:{port}/api/catalog"

    tls_settings = {
        f"pg_lake_iceberg.horizon_tls_{setting}": (
            str(tls_material[key]) if key in tls_files else ""
        )
        for key, setting in zip(_ALL_TLS_FILES, ("ca_file", "cert_file", "key_file"))
    }

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{host}'",
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_auth_type TO '{auth_type}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
        ]
        + [
            f"ALTER SYSTEM SET {name} TO '{value}'"
            for name, value in tls_settings.items()
        ]
        + ["SELECT pg_reload_conf()"]
    )

    wait_for_reloaded_settings(
        [conn],
        {
            "pg_lake_iceberg.rest_catalog_host": host,
            "pg_lake_iceberg.rest_catalog_auth_type": auth_type,
            **tls_settings,
        },
    )


def _stop_using_catalog():
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_auth_type",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret",
            "ALTER SYSTEM RESET pg_lake_iceberg.horizon_tls_ca_file",
            "ALTER SYSTEM RESET pg_lake_iceberg.horizon_tls_cert_file",
            "ALTER SYSTEM RESET pg_lake_iceberg.horizon_tls_key_file",
            "SELECT pg_reload_conf()",
        ]
    )


def _register_namespace(superuser_conn):
    run_command(
        """
        CREATE OR REPLACE FUNCTION register_namespace_to_rest_catalog(TEXT,TEXT)
        RETURNS void
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'register_namespace_to_rest_catalog';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        "SELECT register_namespace_to_rest_catalog('mycat', 'myns')", superuser_conn
    )
    superuser_conn.commit()


def test_a_horizon_catalog_completes_a_mutual_handshake(
    superuser_conn, iceberg_extension, installcheck, tls_material, mtls_catalog_server
):
    if installcheck:
        return

    port, handler = mtls_catalog_server
    _use_catalog(superuser_conn, port, tls_material, "horizon")

    try:
        _register_namespace(superuser_conn)

        assert handler.peer_common_names, "the catalog was never reached"
        assert set(handler.peer_common_names) == {CLIENT_COMMON_NAME}
    finally:
        superuser_conn.rollback()
        _stop_using_catalog()


def test_a_third_party_catalog_is_not_shown_the_certificate(
    superuser_conn, iceberg_extension, installcheck, tls_material, mtls_catalog_server
):
    """
    The same server, the same settings, an ordinary catalog: it is refused,
    having been offered neither the certificate nor the authority that would
    let it be trusted.
    """
    if installcheck:
        return

    port, handler = mtls_catalog_server
    _use_catalog(superuser_conn, port, tls_material, "oauth2")

    try:
        with pytest.raises(psycopg2.Error):
            _register_namespace(superuser_conn)

        assert handler.peer_common_names == []
    finally:
        superuser_conn.rollback()
        _stop_using_catalog()


def test_a_half_configured_certificate_is_refused(
    superuser_conn, iceberg_extension, installcheck, tls_material, mtls_catalog_server
):
    """
    The certificate and the authority that signed the edge are one credential.
    Presenting the certificate while verifying the peer against the public
    bundle would offer the deployment's identity to any publicly signed host a
    catalog names, so half a configuration is reported rather than half used.
    """
    if installcheck:
        return

    port, handler = mtls_catalog_server
    _use_catalog(
        superuser_conn,
        port,
        tls_material,
        "horizon",
        tls_files=("client_cert", "client_key"),
    )

    try:
        with pytest.raises(psycopg2.Error, match="only partly configured"):
            _register_namespace(superuser_conn)

        assert handler.peer_common_names == []
    finally:
        superuser_conn.rollback()
        _stop_using_catalog()
