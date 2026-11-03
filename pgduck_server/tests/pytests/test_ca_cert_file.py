import datetime
import ipaddress
import socket
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from utils_pytest import *

# --ca_cert_file decides which certificates DuckDB's extensions trust when they
# verify a TLS connection. The tests below check that the file reaches
# httpfs, and that an OpenSSL trusted-certificate bundle, the format in which
# distributions express distrust of an intermediate, is honoured rather than
# reduced to its plain certificates.

CA_TEST_PORT = 8259


def _issue(common_name, issuer=None, is_ca=False):
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

    if not is_ca:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )

    return builder.sign(issuer_key or key, hashes.SHA256()), key


def _pem(cert):
    return cert.public_bytes(serialization.Encoding.PEM)


def _trusted_pem(path, trust_flag):
    """Re-encode a certificate as an OpenSSL TRUSTED CERTIFICATE."""
    return subprocess.run(
        ["openssl", "x509", "-in", str(path), trust_flag, "serverAuth", "-trustout"],
        check=True,
        capture_output=True,
    ).stdout


@pytest.fixture(scope="module")
def tls_material(tmp_path_factory):
    """A root, an intermediate it signs, and a server certificate below that."""
    directory = tmp_path_factory.mktemp("pgduck_ca_cert_file")

    root_cert, root_key = _issue("pgduck-test-root", is_ca=True)
    intermediate_cert, intermediate_key = _issue(
        "pgduck-test-intermediate", issuer=(root_cert, root_key), is_ca=True
    )
    server_cert, server_key = _issue(
        "127.0.0.1", issuer=(intermediate_cert, intermediate_key)
    )
    other_cert, _ = _issue("pgduck-test-unrelated-root", is_ca=True)

    paths = {}
    for name, data in {
        "root": _pem(root_cert),
        "intermediate": _pem(intermediate_cert),
        "chain": _pem(server_cert) + _pem(intermediate_cert),
        "unrelated": _pem(other_cert),
    }.items():
        paths[name] = directory / f"{name}.pem"
        paths[name].write_bytes(data)

    paths["key"] = directory / "server.key"
    paths["key"].write_bytes(
        server_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    # the root trusted, the intermediate below it rejected for server auth
    paths["distrusted"] = directory / "distrusted.trust.crt"
    paths["distrusted"].write_bytes(
        _trusted_pem(paths["root"], "-addtrust")
        + _trusted_pem(paths["intermediate"], "-addreject")
    )

    return paths


class _Handler(BaseHTTPRequestHandler):
    def _respond(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        return body

    def do_HEAD(self):
        self._respond(b"hello\n")

    def do_GET(self):
        self.wfile.write(self._respond(b"hello\n"))

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def https_url(tls_material):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tls_material["chain"], tls_material["key"])
    server.socket = context.wrap_socket(server.socket, server_side=True)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield f"https://127.0.0.1:{server.server_address[1]}/hello.txt"

    server.shutdown()
    server.server_close()


def _start_server(ca_cert_file):
    server = PgDuckServer(
        port=CA_TEST_PORT,
        extra_args=["--ca_cert_file", str(ca_cert_file)],
    )
    # a cold start maps a large libduckdb, so allow more than the default
    assert is_server_listening(server.socket_path, timeout=60)
    conn = psycopg2.connect(host="/tmp", port=CA_TEST_PORT)
    return server, conn


def _read(conn, url):
    return run_query(f"SELECT content FROM read_text('{url}')", conn)[0][0]


def test_ca_cert_file_is_logged():
    returncode, stdout, stderr = run_cli_command(["--ca_cert_file", "/dev/null"])
    assert returncode == 0
    assert "CA certificate file is set to: /dev/null" in stderr


def test_empty_ca_cert_file_uses_duckdb_defaults():
    returncode, stdout, stderr = run_cli_command(["--ca_cert_file", ""])
    assert returncode == 0
    assert "Using DuckDB's default CA certificate locations" in stderr


def test_unreadable_ca_cert_file_is_refused():
    returncode, stdout, stderr = run_cli_command(
        ["--ca_cert_file", "/nonexistent/pgduck_ca.pem"]
    )
    assert returncode != 0
    assert 'could not read ca_cert_file "/nonexistent/pgduck_ca.pem"' in stderr


def test_ca_cert_file_is_applied_to_extensions(tls_material):
    server, conn = _start_server(tls_material["root"])
    try:
        assert run_query("SELECT current_setting('ca_cert_file')", conn)[0][0] == str(
            tls_material["root"]
        )
        assert (
            run_query("SELECT current_setting('azure_transport_option_type')", conn)[0][
                0
            ]
            == "curl"
        )
    finally:
        conn.close()
        server.cleanup()


def test_ca_cert_file_verifies_https(tls_material, https_url):
    server, conn = _start_server(tls_material["root"])
    try:
        assert _read(conn, https_url) == "hello\n"
    finally:
        conn.close()
        server.cleanup()


def test_ca_cert_file_without_the_issuer_refuses_https(tls_material, https_url):
    server, conn = _start_server(tls_material["unrelated"])
    try:
        with pytest.raises(psycopg2.Error) as excinfo:
            _read(conn, https_url)
        assert "SSL" in str(excinfo.value)
    finally:
        conn.close()
        server.cleanup()


def test_distrusted_intermediate_refuses_https(tls_material, https_url):
    server, conn = _start_server(tls_material["distrusted"])
    try:
        with pytest.raises(psycopg2.Error) as excinfo:
            _read(conn, https_url)
        assert "SSL" in str(excinfo.value)
    finally:
        conn.close()
        server.cleanup()
