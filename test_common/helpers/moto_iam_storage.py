"""Credential-enforcing S3 fixtures backed by moto's own IAM access control.

Moto is permissive *by default*, but it does implement IAM policy
evaluation for S3: ``INITIAL_NO_AUTH_ACTION_COUNT`` (``moto/settings.py``)
is checked from every handler in ``moto/s3/responses.py``.  In server mode
the counter can be flipped at runtime through ``POST /moto-api/reset-auth``,
so a fixture can run its setup unauthenticated and then turn enforcement on
for the part of the test that needs it.

That gives the three properties an enforcement test needs:

    * an unknown access key is rejected (InvalidAccessKeyId)
    * a wrong secret is rejected (SignatureDoesNotMatch)
    * a scoped user can read only inside its allowed prefix (AccessDenied)

and it needs no server binary beyond the ``moto`` we already depend on, so
nothing gets skipped for want of an optional dependency.

Two differences from the permissive ``cloud_storage`` fixtures:

    * Moto generates access key ids, so ``create_scoped_user`` returns the
      ``(access_key_id, secret_access_key)`` pair instead of taking one.
    * Enforcement is per moto process, so this fixture runs its own moto
      server rather than sharing the session-wide one.

Known gap: moto's access control resolves IAM user keys only, so STS
temporary credentials come back as InvalidAccessKeyId.  Static keys are
enough for the tests here; session-token coverage would need a different
mock.
"""

import json
import os
import socket
import subprocess
import sys
import time

import boto3
import pytest
import requests
from botocore.client import Config as _BotoConfig

from .db import terminate_process

MOTO_BUCKET = "protobucket"
MOTO_REGION = "us-east-1"

# Credentials used for the unauthenticated setup phase.  Any string works
# while enforcement is off.
SETUP_KEY = "setup"
SETUP_SECRET = "setup"


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_until_ready(port, timeout=20.0):
    deadline = time.time() + timeout
    url = f"http://127.0.0.1:{port}/moto-api/"
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=1).status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


class MotoEnforcingServer:
    """A moto server with IAM enforcement that can be switched on and off.

    Attributes follow the ``cloud_storage`` fixtures so tests can read the
    same way: ``endpoint`` (host:port, for DuckDB), ``endpoint_url`` (for
    boto3), ``bucket``, ``region``, and ``root_user`` / ``root_password``
    for a full-access principal that keeps working once enforcement is on.
    """

    def __init__(self, process, port):
        self._process = process
        self.port = port
        self.endpoint = f"127.0.0.1:{port}"
        self.endpoint_url = f"http://127.0.0.1:{port}"
        self.bucket = MOTO_BUCKET
        self.region = MOTO_REGION
        self._enforcing = False

        self.relax()
        self._iam = boto3.client(
            "iam",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=SETUP_KEY,
            aws_secret_access_key=SETUP_SECRET,
        )
        self.client().create_bucket(Bucket=self.bucket)

        # A full-access principal, so materializing fixtures keep working
        # after enforcement is switched on.
        self.root_user, self.root_password = self._create_user(
            "root",
            [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
        )

    # -- boto3 clients -----------------------------------------------------

    def client(self, access_key=None, secret_key=None, session_token=None):
        """Return a boto3 S3 client (defaults to the setup credentials)."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=access_key or SETUP_KEY,
            aws_secret_access_key=secret_key or SETUP_SECRET,
            aws_session_token=session_token,
            region_name=self.region,
            config=_BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )

    # -- enforcement -------------------------------------------------------

    def enforce(self):
        """Require a valid, authorized credential for every request."""
        requests.post(f"{self.endpoint_url}/moto-api/reset-auth", data="0", timeout=5)
        self._enforcing = True

    def relax(self):
        """Accept anything again (setup phase, and teardown)."""
        requests.post(f"{self.endpoint_url}/moto-api/reset-auth", data="inf", timeout=5)
        self._enforcing = False

    # -- scoped users / policies ------------------------------------------

    def _create_user(self, name, statements):
        was_enforcing = self._enforcing
        if was_enforcing:
            self.relax()
        try:
            self._iam.create_user(UserName=name)
            self._iam.put_user_policy(
                UserName=name,
                PolicyName=f"pol_{name}",
                PolicyDocument=json.dumps(
                    {"Version": "2012-10-17", "Statement": statements}
                ),
            )
            key = self._iam.create_access_key(UserName=name)["AccessKey"]
        finally:
            if was_enforcing:
                self.enforce()
        return key["AccessKeyId"], key["SecretAccessKey"]

    def create_scoped_user(
        self,
        name,
        allowed_prefixes,
        actions=("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"),
    ):
        """Create a user restricted to ``allowed_prefixes``.

        ``allowed_prefixes`` is a list of key prefixes inside the test
        bucket (e.g. ``["wh/ns/tbl"]``).  Returns the generated
        ``(access_key_id, secret_access_key)``; everything outside those
        prefixes is denied by moto's policy evaluation.
        """
        object_actions = [a for a in actions if a != "s3:ListBucket"]
        statements = []
        if object_actions:
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": object_actions,
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket}/{p.rstrip('/')}/*"
                        for p in allowed_prefixes
                    ],
                }
            )
        if "s3:ListBucket" in actions:
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                    "Resource": [f"arn:aws:s3:::{self.bucket}"],
                }
            )
        return self._create_user(name, statements)

    # -- teardown ----------------------------------------------------------

    def stop(self):
        self.relax()
        terminate_process(self._process)


def create_moto_enforcing_server():
    port = _free_port()
    env = dict(os.environ)
    # Enforcement is toggled at runtime; start out permissive.
    env.pop("INITIAL_NO_AUTH_ACTION_COUNT", None)
    process = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(port), "-H", "127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    if not _wait_until_ready(port):
        process.kill()
        raise RuntimeError(f"moto server did not come up on port {port}")
    return MotoEnforcingServer(process, port)


@pytest.fixture(scope="session")
def moto_enforcing_server():
    server = create_moto_enforcing_server()
    yield server
    server.stop()
