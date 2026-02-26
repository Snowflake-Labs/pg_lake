"""S3, GCS, and Azure mock-storage helpers, constants, and fixtures."""

import atexit
import json
import os
import shutil
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb
import pytest
from azure.storage.blob import BlobServiceClient
from moto.server import ThreadedMotoServer

from . import server_params
from .db import terminate_process


# ---------------------------------------------------------------------------
# Cloud-storage constants
# ---------------------------------------------------------------------------

# S3 configuration — should match test secret in duckdb.c
MOTO_PORT = 5999
TEST_BUCKET = "testbucketcdw"
MANAGED_STORAGE_BUCKET = "pglakemanaged1"
TEST_AWS_ACCESS_KEY_ID = "testing"
TEST_AWS_SECRET_ACCESS_KEY = "testing"
TEST_AWS_REGION = "us-west-1"
TEST_AWS_FAKE_ROLE_NAME = "FakeRoleForTest"
AWS_ROLE_ARN = f"arn:aws:iam::000000000000:role/{TEST_AWS_FAKE_ROLE_NAME}"

MOTO_PORT_GCS = 5998
TEST_BUCKET_GCS = "testbucketgcs"
TEST_GCS_REGION = "europe-west4"

AZURITE_CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1"


# ---------------------------------------------------------------------------
# DuckDB in-memory connection (with cloud secrets)
# ---------------------------------------------------------------------------

def create_duckdb_conn():
    """Create an in-memory DuckDB connection pre-configured with S3/GCS/Azure secrets."""
    conn = duckdb.connect(database=":memory:")
    conn.execute(
        """
        CREATE SECRET s3test (
            TYPE S3, KEY_ID 'testing', SECRET 'testing',
            ENDPOINT 'localhost:5999',
            SCOPE 's3://testbucketcdw', URL_STYLE 'path', USE_SSL false
        );
    """
    )
    conn.execute(
        """
        CREATE SECRET gcstest (
            TYPE GCS, KEY_ID 'testing', SECRET 'testing',
            ENDPOINT 'localhost:5998',
            SCOPE 'gs://testbucketgcs', URL_STYLE 'path', USE_SSL false
        );
    """
    )
    conn.execute(
        f"""
        CREATE SECRET ztest (
            TYPE AZURE,
            CONNECTION_STRING '{AZURITE_CONNECTION_STRING}'
        );
    """
    )
    return conn


# ---------------------------------------------------------------------------
# Mock-storage creation helpers
# ---------------------------------------------------------------------------

def start_azurite():
    azurite_tmp_dir = Path("/tmp/pgl_tests_azurite")
    if azurite_tmp_dir.exists():
        shutil.rmtree(azurite_tmp_dir, ignore_errors=True)

    process = subprocess.Popen(
        ["azurite", "--location", str(azurite_tmp_dir), "--skipApiVersionCheck"]
    )

    # Wait a bit for blob storage to start
    # If it doesn't, the SDK will still retry, but that might add more delay
    time.sleep(1)

    return process


# Start Azurite in the background
def create_mock_azure_blob_storage():
    process = start_azurite()

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURITE_CONNECTION_STRING
    )
    container_client = blob_service_client.create_container(TEST_BUCKET)

    return container_client, process


def dump_test_s3_object(key):
    client = mock_s3_client()
    response = client.get_object(Bucket=TEST_BUCKET, Key=key)
    print(response["Body"].read())


def mock_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )


# Start a background server that pretends to be Amazon Simple Storage Service (S3)
def create_mock_s3():

    # Service-specific endpoints so SDK v2 (used by Polaris) knows where to call
    os.environ["AWS_ENDPOINT_URL_S3"] = f"http://127.0.0.1:{MOTO_PORT}"
    os.environ["AWS_ENDPOINT_URL_STS"] = f"http://127.0.0.1:{MOTO_PORT}"
    os.environ["AWS_REGION"] = TEST_AWS_REGION
    os.environ["AWS_ACCESS_KEY_ID"] = TEST_AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = TEST_AWS_SECRET_ACCESS_KEY

    # ---- start Moto ----
    server = ThreadedMotoServer(port=MOTO_PORT)
    server.start()

    client = mock_s3_client()
    client.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
    )
    client.create_bucket(
        Bucket=MANAGED_STORAGE_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
    )

    # allow public reads to our test bucket
    client.put_bucket_policy(
        Bucket=TEST_BUCKET,
        Policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicReadGetObject",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{TEST_BUCKET}/*",
                    }
                ],
            }
        ),
    )

    # Create a customer-managed key
    kms_client = create_kms_client()
    response = kms_client.create_key(
        Description="Customer Managed Key for testing",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )

    # Extract the KeyId from the response
    server_params.MANAGED_STORAGE_CMK_ID = response["KeyMetadata"]["KeyId"]

    # Setting up STS + assume-role is not strictly required for Polaris version 1.2+
    # But we prefer to keep for now, as that's more closer to production workloads

    # Create IAM role + STS assume-role
    # required for Polaris
    iam = boto3.client(
        "iam",
        endpoint_url=f"http://127.0.0.1:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )

    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}
        ],
    }

    role = iam.create_role(
        RoleName=f"{TEST_AWS_FAKE_ROLE_NAME}",
        AssumeRolePolicyDocument=json.dumps(trust),
        Description="Moto test role for Polaris",
    )["Role"]

    # attach wide S3 policy; Moto is lax but this mirrors real life
    iam.put_role_policy(
        RoleName=f"{TEST_AWS_FAKE_ROLE_NAME}",
        PolicyName="S3All",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        ),
    )

    # Prove STS works
    # if not, should throw exception
    sts = boto3.client(
        "sts",
        endpoint_url=f"http://127.0.0.1:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )
    assumed = sts.assume_role(
        RoleArn=role["Arn"], RoleSessionName="polaris-test-session"
    )["Credentials"]

    return client, server


# Start a background server that pretends to be Google Cloud Storage (GCS)
#
# GCS offers S3 API compatibility, which is what DuckDB uses to access GCS.
# Hence, we can also use moto to mock GCS, but we run it on a separate port
# to not get confused with mock S3.
def create_mock_gcs():
    server = ThreadedMotoServer(port=MOTO_PORT_GCS)
    server.start()

    # "s3" refers to the name of the AWS API within boto
    client = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{MOTO_PORT_GCS}",
        region_name=TEST_GCS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )
    client.create_bucket(
        Bucket=TEST_BUCKET_GCS,
        CreateBucketConfiguration={"LocationConstraint": TEST_GCS_REGION},
    )
    return client, server


def create_kms_client():
    return boto3.client(
        "kms",
        endpoint_url=f"http://localhost:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )


def get_object_size(client, bucket_name, key):
    response = client.head_object(Bucket=bucket_name, Key=key)
    return int(response["ContentLength"])


def list_objects(client, bucket_name, prefix=""):
    keys = []

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" in response:
        for content in response["Contents"]:
            print(content["Key"])
            keys.append(content["Key"])

    return keys


def stop_moto_server(server, timeout=5):
    """Stop a ThreadedMotoServer, handling edge cases.

    - If the server thread failed to bind (``_server`` is None),
      skip shutdown to avoid blocking forever.
    - ``shutdown()`` is run in a helper thread with a *timeout* so we
      never block indefinitely.
    - ``server_close()`` is called afterwards to close the listening
      socket immediately (avoids TCP TIME_WAIT that blocks the next
      test session from binding the same port).
    """
    if server is None:
        return

    inner = getattr(server, "_server", None)
    if inner is None:
        # Server thread never created the WSGI server (e.g. bind failed)
        return

    # Run stop() in a thread so we can enforce a timeout
    t = threading.Thread(target=server.stop, daemon=True)
    t.start()
    t.join(timeout=timeout)

    # Close the listening socket to release the port immediately
    try:
        inner.server_close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Singleton caches for session-scoped fixtures.
#
# Every test file does ``from utils_pytest import *`` which imports
# @pytest.fixture-decorated functions into the *test module* namespace.
# Pytest discovers them there as module-local fixtures that shadow the
# conftest versions.  When transitioning between test files, pytest may
# create a *second* instance of the same session fixture.
#
# The caches below ensure the underlying resource (mock server, process,
# …) is only created once, regardless of how many times pytest calls the
# fixture function.
# ---------------------------------------------------------------------------
_mock_s3_cache = None
_gcs_cache = None
_azure_cache = None


@pytest.fixture(scope="session")
def mock_s3():
    """Creates a single Moto S3 instance shared across the session."""
    global _mock_s3_cache
    if _mock_s3_cache is not None:
        yield _mock_s3_cache
        return
    client, server = create_mock_s3()
    _mock_s3_cache = (client, server)
    atexit.register(stop_moto_server, server)
    yield client, server
    stop_moto_server(server)


@pytest.fixture(scope="session")
def s3(mock_s3):
    """Returns the S3 client from the shared mock S3 instance."""
    client, _ = mock_s3
    return client


@pytest.fixture(scope="session")
def s3_server(mock_s3):
    """Returns the server from the shared mock S3 instance."""
    _, server = mock_s3
    return server


@pytest.fixture(scope="session")
def gcs():
    global _gcs_cache
    if _gcs_cache is not None:
        yield _gcs_cache
        return
    client, server = create_mock_gcs()
    _gcs_cache = client
    atexit.register(stop_moto_server, server)
    yield client
    stop_moto_server(server)


@pytest.fixture(scope="session")
def azure():
    global _azure_cache
    if _azure_cache is not None:
        yield _azure_cache
        return
    client, server = create_mock_azure_blob_storage()
    _azure_cache = client
    atexit.register(terminate_process, server)
    yield client
    terminate_process(server)


# ---------------------------------------------------------------------------
# S3 path / upload / read helpers
# ---------------------------------------------------------------------------

# Function to parse the S3 path and return the bucket name and key
def parse_s3_path(s3_path):
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    return bucket_name, key


# Utility function to upload an entire local dir to s3 rooted at the target path
def s3_upload_dir(s3, local_dir, s3_bucket, target_dir):
    for root, _, files in os.walk(local_dir):
        for filename in files:
            # we need to remove the original localdir from the root as a prefix
            dirFrag = root.removeprefix(local_dir + "/")
            s3.upload_file(
                os.path.join(root, filename),
                s3_bucket,
                f"{target_dir}/{dirFrag}/{filename}",
            )


def read_s3_operations(s3, spath, is_text=True):
    bucket, s3_key = parse_s3_path(spath)
    # Read from the S3 bucket
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    read_content = response["Body"].read()

    if is_text:
        read_content = read_content.decode("utf-8")

    return read_content


# Need subprocess for start_azurite — import at module level after other
# imports so the rest of the file is readable.
import subprocess  # noqa: E402
