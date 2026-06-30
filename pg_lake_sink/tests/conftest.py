# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Fixtures for the pg_lake_sink test suite.

pg_lake_sink is an opt-in Rust/pgrx extension, so its tests are genuine
integration tests: they need the extension installed *and* a Kafka broker. Both
prerequisites are detected at fixture setup and the affected tests skip cleanly
when either is missing, so this file is safe to collect in an environment where
pg_lake_sink was never built (e.g. the default all-C CI).

Kafka is driven through the single-node Redpanda container defined in
``docker/docker-compose.yml`` via ``rpk`` (``docker exec``), mirroring
``scripts/produce.sh``. That keeps the Python test harness free of any Kafka
client dependency and free of a host librdkafka (the extension vendors its own).
The default broker address (``localhost:19092``) matches the
``pg_lake_sink.brokers`` GUC default, so subscriptions need no explicit brokers.
"""

import os
import shutil
import subprocess
import time

import psycopg2
import pytest
from utils_pytest import *  # noqa: F401,F403  (shared helpers + core fixtures)

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE_FILE = os.path.normpath(
    os.path.join(HERE, "..", "docker", "docker-compose.yml")
)
CONTAINER = "pg_lake_sink_redpanda"
BROKER = "localhost:19092"


def _compose_cmd():
    """Return the docker compose invocation to use, or None if unavailable.

    Prefers ``docker compose`` (v2); falls back to the legacy ``docker-compose``.
    """
    if shutil.which("docker"):
        probe = subprocess.run(
            ["docker", "compose", "version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if probe.returncode == 0:
            return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    return None


def _container_running():
    probe = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return probe.returncode == 0 and probe.stdout.strip() == "true"


def _cluster_healthy():
    probe = subprocess.run(
        ["docker", "exec", CONTAINER, "rpk", "cluster", "health"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    out = probe.stdout
    return probe.returncode == 0 and "Healthy:" in out and "true" in out.lower()


@pytest.fixture(scope="session")
def redpanda():
    """Ensure a single-node Redpanda broker is up at ``localhost:19092``.

    Brings the ``docker/docker-compose.yml`` service up if it is not already
    running and waits for the cluster to report healthy. Skips (rather than
    fails) when Docker is unavailable or the broker never comes up, since these
    are opt-in integration tests. A broker we started is torn down afterwards; a
    pre-existing one is left running.
    """
    compose = _compose_cmd()
    if compose is None:
        pytest.skip(
            "pg_lake_sink tests need Docker + a Kafka broker (docker not found)"
        )

    started_by_us = not _container_running()
    up = subprocess.run(
        compose + ["-f", COMPOSE_FILE, "up", "-d"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if up.returncode != 0:
        pytest.skip(f"could not start Redpanda via docker compose:\n{up.stdout}")

    # Wait for the cluster to become healthy (first boot pulls the image + elects
    # a controller). Poll with a wall-clock deadline rather than sleeping blindly.
    deadline = time.time() + 120
    while time.time() < deadline and not _cluster_healthy():
        time.sleep(2)

    if not _cluster_healthy():
        if started_by_us:
            subprocess.run(
                compose + ["-f", COMPOSE_FILE, "down", "-v"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        pytest.skip("Redpanda did not become healthy in time")

    yield BROKER

    if started_by_us:
        subprocess.run(
            compose + ["-f", COMPOSE_FILE, "down", "-v"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


class Kafka:
    """Tiny producer helper backed by ``rpk`` inside the Redpanda container."""

    broker = BROKER

    @staticmethod
    def produce(topic, records, keyed=True):
        """Create ``topic`` (single partition) and produce ``records`` into it.

        With ``keyed=True`` (default) ``records`` is a list of ``(key, value)``
        tuples; with ``keyed=False`` it is a list of values and the messages are
        produced with no key (so the ``key`` column comes out NULL). Keys and
        values must be free of tab/newline characters (our JSON test payloads
        are), because rpk's record framing uses them as delimiters.

        A single partition keeps offsets deterministic: the Nth produced record
        lands at offset N-1.
        """
        subprocess.run(
            ["docker", "exec", CONTAINER, "rpk", "topic", "create", topic, "-p", "1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if keyed:
            data = "".join(f"{k}\t{v}\n" for k, v in records)
            fmt = "%k\t%v\n"
        else:
            data = "".join(f"{v}\n" for v in records)
            fmt = "%v\n"
        produced = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                CONTAINER,
                "rpk",
                "topic",
                "produce",
                topic,
                "--format",
                fmt,
            ],
            input=data,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert produced.returncode == 0, f"rpk produce failed: {produced.stdout}"


@pytest.fixture(scope="session")
def kafka(redpanda):
    return Kafka()


@pytest.fixture(scope="module")
def sink_extension(postgres):
    """Create the ``pg_lake_sink`` extension on a dedicated superuser connection.

    Skips the module if the extension is not installed (the common case: it is an
    opt-in Rust build not present in the all-C CI). Yields the connection so
    tests can run the ``lake_sink.*`` management functions on it. On teardown,
    every remaining subscription is dropped first (which deregisters its base
    worker) so no orphaned worker survives ``DROP EXTENSION``.
    """
    conn = open_pg_conn()
    try:
        run_command("CREATE EXTENSION IF NOT EXISTS pg_lake_sink CASCADE", conn)
        conn.commit()
    except psycopg2.Error as exc:
        conn.rollback()
        conn.close()
        pytest.skip(f"pg_lake_sink extension not installed (opt-in build): {exc}")

    yield conn

    conn.rollback()
    try:
        leftovers = run_query("SELECT name FROM lake_sink.subscriptions", conn)
        for row in leftovers:
            run_command(
                f"SELECT lake_sink.drop_subscription('{row['name']}', true)",
                conn,
                raise_error=False,
            )
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    run_command("DROP EXTENSION IF EXISTS pg_lake_sink CASCADE", conn)
    conn.commit()
    conn.close()
