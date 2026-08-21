"""S3 fixtures that actually enforce credentials, backed by moto's IAM.

The other cloud-storage fixtures run moto at its default setting, where it
accepts any access key and never denies a request.  That is fine for
functional tests, but it cannot show that a credential is *load-bearing*
-- that pg_lake's vended credential is the thing unlocking a data scan,
or that an out-of-scope credential is refused.

Moto can do that, though: it implements IAM policy evaluation for S3
behind ``INITIAL_NO_AUTH_ACTION_COUNT`` (``moto/settings.py``), consulted
by every handler in ``moto/s3/responses.py``, and in server mode the
counter can be flipped at runtime through ``POST /moto-api/reset-auth``.
So a fixture can run its setup unauthenticated and turn enforcement on
for the part of a test where the credential has to matter, giving:

    * an unknown access key is rejected (InvalidAccessKeyId)
    * a wrong secret is rejected (SignatureDoesNotMatch)
    * a scoped user reads only inside its allowed prefix (AccessDenied)

Enforcement is per moto *process*, and it is global within one: turning
it on affects every request that process serves.  That is why this starts
a moto of its own instead of sharing the session-wide server the rest of
the suite uses with dummy credentials -- those tests would start failing
the moment one of these turned enforcement on.

Known gap: moto's access control resolves IAM user keys only, so STS
temporary credentials come back as InvalidAccessKeyId.  Real vended
credentials are STS triples, so what is exercised here is the plumbing
around the credential rather than the session token itself.

Derived from the prototype in
https://github.com/Snowflake-Labs/pg_lake/pull/549.
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
from botocore.exceptions import ClientError

from .db import terminate_process

MOTO_BUCKET = "pglakemoto"
MOTO_REGION = "us-east-1"

# Credentials for the phase before any IAM user exists.  Any string works
# while enforcement is off, and none of them work once it is on.
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
    """A moto server whose IAM enforcement can be switched on and off.

    ``endpoint`` is host:port (what DuckDB and pg_lake want), while
    ``endpoint_url`` carries the scheme (what boto3 and pyiceberg want).
    ``root_user`` / ``root_password`` name a full-access principal that
    keeps working once enforcement is on, so fixtures can go on writing
    to the bucket while a test asserts that some other credential cannot.
    """

    def __init__(self, process, port):
        self._process = process
        self.port = port
        self.endpoint = f"127.0.0.1:{port}"
        self.endpoint_url = f"http://127.0.0.1:{port}"
        self.bucket = MOTO_BUCKET
        self.region = MOTO_REGION
        self._enforcing = False
        self.root_user = SETUP_KEY
        self.root_password = SETUP_SECRET

        self.relax()
        self._iam = boto3.client(
            "iam",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=SETUP_KEY,
            aws_secret_access_key=SETUP_SECRET,
        )
        self.client().create_bucket(Bucket=self.bucket)

        self.root_user, self.root_password = self._create_user(
            "root", [{"Effect": "Allow", "Action": "*", "Resource": "*"}]
        )

    # -- boto3 clients -----------------------------------------------------

    def client(self, access_key=None, secret_key=None, session_token=None):
        """An S3 client, by default for the full-access root principal.

        Defaulting to root rather than to the setup credentials keeps
        fixture code (materializing tables, reading a metadata document
        back) working on both sides of ``enforce()``.
        """
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

    # -- enforcement -------------------------------------------------------

    def enforce(self):
        """Require a valid, authorized credential for every request."""
        requests.post(f"{self.endpoint_url}/moto-api/reset-auth", data="0", timeout=5)
        self._enforcing = True
        self._assert_enforcing()

    def relax(self):
        """Accept anything again (setup phase, and teardown)."""
        requests.post(f"{self.endpoint_url}/moto-api/reset-auth", data="inf", timeout=5)
        self._enforcing = False

    def _assert_enforcing(self):
        """Fail loudly if moto is not actually refusing a bogus credential.

        The reset-auth endpoint is a moto test API rather than part of
        S3, so a moto upgrade could stop honouring it.  Without this
        check that would not fail any test that asserts a *successful*
        read: those would keep passing while quietly proving nothing.
        """
        try:
            self.client("bogus_key", "bogus_secret").list_objects_v2(
                Bucket=self.bucket, MaxKeys=1
            )
        except ClientError:
            return
        raise RuntimeError(
            "moto accepted a bogus access key while enforcement was on; "
            "POST /moto-api/reset-auth is no longer taking effect"
        )

    # -- scoped users / policies ------------------------------------------

    def _create_user(self, name, statements):
        """Create an IAM user with an inline policy, returning its keys.

        IAM itself is subject to enforcement, and the setup credentials
        are not a principal, so this drops enforcement for the duration
        when a test creates a user mid-flight.
        """
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
        """Create a user that can only reach ``allowed_prefixes``.

        ``allowed_prefixes`` are key prefixes inside the test bucket
        (e.g. ``["wh/ns/tbl"]``).  Returns the generated
        ``(access_key_id, secret_access_key)``, since moto assigns the
        key id.  Note that ``s3:ListBucket`` is granted on the bucket as
        a whole: object reads are what these prefixes restrict.
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
def enforcing_s3_server():
    server = create_moto_enforcing_server()
    yield server
    server.stop()
