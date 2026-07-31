"""MinIO helpers and fixtures for *credential-enforcing* S3 tests.

Moto (used by the other cloud-storage fixtures) is intentionally
permissive: it accepts any access key and never denies a request.  That
is fine for functional tests, but it cannot prove that a scoped
credential is actually *load-bearing* -- e.g. that pg_lake's vended
credential is the thing that unlocks a data scan, or that an
out-of-scope credential is rejected.

MinIO is a real S3 implementation with a real policy engine, so it lets
us write hermetic tests that assert enforcement:

    * an unknown access key is rejected (InvalidAccessKeyId)
    * a scoped user can read/write only inside its allowed prefix
    * an out-of-prefix access is denied (AccessDenied)

These fixtures are gated on MinIO being available (the ``minio`` server
binary on PATH and the ``minio`` Python admin SDK importable).  When it is
not available -- e.g. in a CI image that has not installed it -- tests
that request them are skipped rather than failed, mirroring the
skip-if-absent pattern used by the e2e suites.
"""

import atexit
import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import boto3
import pytest
from botocore.client import Config as _BotoConfig

# The MinIO Python SDK is an optional dependency (declared in the dev
# Pipfile).  Import it lazily so this module can still be imported -- and
# the fixtures cleanly *skip* -- on environments that do not have it.
try:
    from minio.minioadmin import MinioAdmin
    from minio.credentials.providers import StaticProvider

    _HAVE_MINIO_SDK = True
    _MINIO_SDK_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - depends on environment
    MinioAdmin = None
    StaticProvider = None
    _HAVE_MINIO_SDK = False
    _MINIO_SDK_IMPORT_ERROR = exc


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin-pglake-test"
MINIO_BUCKET = "pglakeminio"
MINIO_REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Availability detection + skip marker
# ---------------------------------------------------------------------------


def minio_binary_path():
    """Return the path to the ``minio`` server binary, or None."""
    return shutil.which("minio")


def minio_available():
    """True when both the MinIO server binary and admin SDK are present."""
    return _HAVE_MINIO_SDK and minio_binary_path() is not None


def _skip_reason():
    if not _HAVE_MINIO_SDK:
        return f"minio Python SDK not importable ({_MINIO_SDK_IMPORT_ERROR})"
    if minio_binary_path() is None:
        return "minio server binary not found on PATH"
    return ""


# Decorator: skip a test when MinIO is unavailable.
requires_minio = pytest.mark.skipif(
    not minio_available(),
    reason=_skip_reason() or "MinIO not available",
)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------


class MinioServer:
    """A running local MinIO server with helpers for scoped users.

    Attributes
    ----------
    endpoint : str
        ``host:port`` (no scheme) -- convenient for DuckDB ``ENDPOINT``.
    endpoint_url : str
        ``http://host:port`` -- convenient for boto3.
    root_user / root_password : str
        Root credentials (full access).
    bucket : str
        The pre-created test bucket.
    admin : MinioAdmin
        Admin client for user/policy management.
    """

    def __init__(self, process, port, console_port, data_dir):
        self._process = process
        self._data_dir = data_dir
        self.port = port
        self.console_port = console_port
        self.endpoint = f"127.0.0.1:{port}"
        self.endpoint_url = f"http://127.0.0.1:{port}"
        self.root_user = MINIO_ROOT_USER
        self.root_password = MINIO_ROOT_PASSWORD
        self.bucket = MINIO_BUCKET
        self.region = MINIO_REGION
        self.admin = MinioAdmin(
            endpoint=self.endpoint,
            credentials=StaticProvider(self.root_user, self.root_password),
            secure=False,
        )
        self._policy_files = []

    # -- boto3 clients -----------------------------------------------------

    def client(self, access_key=None, secret_key=None, session_token=None):
        """Return a boto3 S3 client (defaults to root credentials)."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=access_key or self.root_user,
            aws_secret_access_key=secret_key or self.root_password,
            aws_session_token=session_token,
            region_name=self.region,
            config=_BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )

    # -- scoped users / policies ------------------------------------------

    def create_scoped_user(
        self,
        access_key,
        secret_key,
        allowed_prefixes,
        actions=("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"),
    ):
        """Create a MinIO user restricted to ``allowed_prefixes``.

        ``allowed_prefixes`` is a list of key prefixes inside the test
        bucket (e.g. ``["wh/ns/tbl/"]``).  The generated policy allows the
        given ``actions`` on those prefixes only; everything else is
        implicitly denied by MinIO's policy engine.
        """
        object_resources = [
            f"arn:aws:s3:::{self.bucket}/{prefix.rstrip('/')}/*"
            for prefix in allowed_prefixes
        ]
        statements = [
            {
                "Effect": "Allow",
                "Action": [a for a in actions if a != "s3:ListBucket"],
                "Resource": object_resources,
            }
        ]
        if "s3:ListBucket" in actions:
            # ListBucket is a bucket-level action; scope it with a prefix
            # condition so listing is limited to the allowed prefixes.
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{self.bucket}"],
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [
                                f"{prefix.rstrip('/')}/*" for prefix in allowed_prefixes
                            ]
                        }
                    },
                }
            )
        policy = {"Version": "2012-10-17", "Statement": statements}

        policy_name = f"pol_{access_key}"
        policy_path = os.path.join(self._data_dir, f"{policy_name}.json")
        with open(policy_path, "w") as f:
            json.dump(policy, f)
        self._policy_files.append(policy_path)

        self.admin.user_add(access_key, secret_key)
        self.admin.policy_add(policy_name, policy_path)
        self.admin.policy_set(policy_name, user=access_key)

    # -- teardown ----------------------------------------------------------

    def stop(self):
        proc = self._process
        if proc is not None and proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    pass
        shutil.rmtree(self._data_dir, ignore_errors=True)


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_until_ready(port, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def create_minio_server():
    """Start a local MinIO server, create the test bucket, and return it."""
    binary = minio_binary_path()
    if binary is None:
        raise RuntimeError("minio server binary not found on PATH")

    data_dir = tempfile.mkdtemp(prefix="pgl_tests_minio_")
    port = _free_port()
    console_port = _free_port()

    env = dict(os.environ)
    env["MINIO_ROOT_USER"] = MINIO_ROOT_USER
    env["MINIO_ROOT_PASSWORD"] = MINIO_ROOT_PASSWORD

    process = subprocess.Popen(
        [
            binary,
            "server",
            "--address",
            f"127.0.0.1:{port}",
            "--console-address",
            f"127.0.0.1:{console_port}",
            data_dir,
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if not _wait_until_ready(port):
        process.kill()
        shutil.rmtree(data_dir, ignore_errors=True)
        raise RuntimeError("MinIO server did not become ready in time")

    server = MinioServer(process, port, console_port, data_dir)

    # Create the test bucket with root credentials.
    server.client().create_bucket(Bucket=MINIO_BUCKET)

    return server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_minio_cache = None


@pytest.fixture(scope="session")
def minio_server():
    """Session-scoped MinIO server (skips when MinIO is unavailable)."""
    if not minio_available():
        pytest.skip(_skip_reason() or "MinIO not available")

    global _minio_cache
    if _minio_cache is not None:
        yield _minio_cache
        return

    server = create_minio_server()
    _minio_cache = server
    atexit.register(server.stop)
    yield server
    # Teardown handled by atexit to survive cross-file session-fixture
    # re-instantiation (same pattern as the moto fixtures).
