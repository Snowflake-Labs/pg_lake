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
"""End-to-end tests for pg_lake_sink.

These exercise the full path Kafka -> streaming COPY -> target table, including
the binary COPY encoders (jsonb version byte, timestamptz epoch offset, bytea,
NULL), transactional offset tracking (exactly-once resume with no duplicates),
the async base worker (consume / disable / enable), subscription management
(create / drop / cascade), input validation, and a real pg_lake ``USING iceberg``
target.

Every test needs both the extension and a broker, provided by the ``sink_extension``
and ``kafka`` fixtures (see conftest.py); the module skips when either is absent.
The validation tests need only ``sink_extension`` (no broker), since they never
reach Kafka.
"""

import time

from utils_pytest import *  # noqa: F401,F403

# Imported after the star-import above: utils_pytest re-exports the `datetime`
# *module*, which would otherwise shadow these names.
from datetime import datetime, timezone


def _scalar(conn, sql, raise_error=True):
    """Run a query returning a single value."""
    rows = run_query(sql, conn, raise_error=raise_error)
    if not isinstance(rows, list):  # error string when raise_error=False
        return rows
    return rows[0][0]


def _wait_for_count(conn, table, expected, timeout=60):
    """Poll ``table`` until it has at least ``expected`` rows or ``timeout`` s.

    Rolls back before each read so a fresh (post-worker-commit) snapshot is taken.
    Returns the final observed count.
    """
    deadline = time.time() + timeout
    count = 0
    while time.time() < deadline:
        conn.rollback()
        count = _scalar(conn, f"SELECT count(*) FROM {table}")
        if count >= expected:
            return count
        time.sleep(1)
    conn.rollback()
    return _scalar(conn, f"SELECT count(*) FROM {table}")


# ---------------------------------------------------------------------------
# pull() into an auto-created heap target
# ---------------------------------------------------------------------------


def test_pull_fills_defaults_and_maps_types(sink_extension, kafka):
    """A JSON-keyed batch pulled into a heap target: identity + consumed_at are
    filled by COPY, and topic/partition/offset/key/payload/kafka_timestamp all
    map to the right Postgres types (exercises every binary encoder)."""
    conn = sink_extension
    topic = "t_pull_types"
    records = [(f"k{i}", f'{{"n": {i}, "event": "click"}}') for i in range(1, 6)]

    before = datetime.now(timezone.utc)
    kafka.produce(topic, records)

    # enabled => false: no competing worker, so pull() is fully deterministic.
    run_command(
        f"SELECT lake_sink.create_subscription('s_types', '{topic}', enabled => false)",
        conn,
    )
    conn.commit()

    assert _scalar(conn, "SELECT lake_sink.pull('s_types', 100)") == 5
    conn.commit()

    rows = run_query(
        'SELECT id, topic, partition, "offset", key, payload, headers, kafka_timestamp, consumed_at '
        'FROM lake_sink.s_types ORDER BY "offset"',
        conn,
    )
    after = datetime.now(timezone.utc)

    assert len(rows) == 5
    # Identity column filled by COPY, in insertion (= offset) order.
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5]
    assert [r["offset"] for r in rows] == [0, 1, 2, 3, 4]
    assert all(r["topic"] == topic for r in rows)
    assert all(r["partition"] == 0 for r in rows)
    # bytea key round-trips exactly.
    assert bytes(rows[0]["key"]) == b"k1"
    # jsonb payload parses back to the produced object (jsonb version byte + text).
    assert rows[0]["payload"] == {"n": 1, "event": "click"}
    assert rows[4]["payload"] == {"n": 5, "event": "click"}
    # No headers were produced.
    assert rows[0]["headers"] is None
    # consumed_at default now() filled; kafka_timestamp decoded near real time
    # (a wrong Postgres-epoch offset would be ~30 years off, so this is a strong
    # check on the timestamptz encoder).
    assert all(r["consumed_at"] is not None for r in rows)
    ts = rows[0]["kafka_timestamp"]
    assert ts is not None
    assert before.timestamp() - 300 <= ts.timestamp() <= after.timestamp() + 300

    run_command("SELECT lake_sink.drop_subscription('s_types', true)", conn)
    conn.commit()


def test_non_json_payload_wrapped_as_json_string(sink_extension, kafka):
    """A non-JSON Kafka value is stored as a JSON string, not rejected."""
    conn = sink_extension
    topic = "t_nonjson"
    kafka.produce(topic, [("k", "hello world")])

    run_command(
        f"SELECT lake_sink.create_subscription('s_nonjson', '{topic}', enabled => false)",
        conn,
    )
    conn.commit()
    assert _scalar(conn, "SELECT lake_sink.pull('s_nonjson', 100)") == 1
    conn.commit()

    payload = _scalar(conn, "SELECT payload FROM lake_sink.s_nonjson")
    assert payload == "hello world"  # wrapped as a JSON string -> Python str

    run_command("SELECT lake_sink.drop_subscription('s_nonjson', true)", conn)
    conn.commit()


def test_null_key(sink_extension, kafka):
    """Messages produced without a key land with a NULL key column."""
    conn = sink_extension
    topic = "t_nullkey"
    kafka.produce(topic, ['{"a": 1}', '{"a": 2}'], keyed=False)

    run_command(
        f"SELECT lake_sink.create_subscription('s_nullkey', '{topic}', enabled => false)",
        conn,
    )
    conn.commit()
    assert _scalar(conn, "SELECT lake_sink.pull('s_nullkey', 100)") == 2
    conn.commit()

    assert (
        _scalar(conn, "SELECT count(*) FROM lake_sink.s_nullkey WHERE key IS NULL") == 2
    )

    run_command("SELECT lake_sink.drop_subscription('s_nullkey', true)", conn)
    conn.commit()


# ---------------------------------------------------------------------------
# Exactly-once: offsets advance transactionally, resume without duplicates
# ---------------------------------------------------------------------------


def test_idempotent_resume_no_duplicates(sink_extension, kafka):
    """Re-pulling with no new messages inserts nothing; new messages resume from
    the stored offset. Offsets stay dense and duplicate-free across pulls."""
    conn = sink_extension
    topic = "t_resume"
    kafka.produce(topic, [(f"k{i}", f'{{"n": {i}}}') for i in range(1, 6)])

    run_command(
        f"SELECT lake_sink.create_subscription('s_resume', '{topic}', enabled => false)",
        conn,
    )
    conn.commit()

    assert _scalar(conn, "SELECT lake_sink.pull('s_resume', 100)") == 5
    conn.commit()
    # Nothing new to consume -> a second pull is a no-op (offsets already advanced).
    assert _scalar(conn, "SELECT lake_sink.pull('s_resume', 100)") == 0
    conn.commit()

    # Produce more and resume from the stored offset.
    kafka.produce(topic, [(f"k{i}", f'{{"n": {i}}}') for i in range(6, 9)])
    assert _scalar(conn, "SELECT lake_sink.pull('s_resume', 100)") == 3
    conn.commit()

    offsets = [
        r["offset"]
        for r in run_query(
            'SELECT "offset" FROM lake_sink.s_resume ORDER BY "offset"', conn
        )
    ]
    assert offsets == [0, 1, 2, 3, 4, 5, 6, 7]  # dense, no duplicates
    assert (
        _scalar(
            conn,
            "SELECT last_offset FROM lake_sink.subscription_offsets WHERE subscription = 's_resume'",
        )
        == 7
    )

    run_command("SELECT lake_sink.drop_subscription('s_resume', true)", conn)
    conn.commit()


# ---------------------------------------------------------------------------
# The async base worker: consume, disable (pause), enable (resume)
# ---------------------------------------------------------------------------


def test_worker_consume_disable_enable(sink_extension, kafka):
    """The dedicated base worker consumes automatically; disabling pauses it and
    releases its partitions; enabling resumes from the stored offset with no
    duplicates (the same load-offsets + assign path used on restart)."""
    conn = sink_extension
    topic = "t_worker"
    kafka.produce(topic, [(f"k{i}", f'{{"n": {i}}}') for i in range(1, 5)])

    # enabled (default), snappy poll so the test doesn't wait long.
    run_command(
        f"SELECT lake_sink.create_subscription('s_worker', '{topic}', poll_interval_ms => 250)",
        conn,
    )
    conn.commit()

    assert _wait_for_count(conn, "lake_sink.s_worker", 4) == 4

    # Pause and give the worker time to observe the disable before producing more.
    run_command("SELECT lake_sink.disable_subscription('s_worker')", conn)
    conn.commit()
    time.sleep(3)
    kafka.produce(topic, [(f"k{i}", f'{{"n": {i}}}') for i in range(5, 8)])
    time.sleep(4)
    conn.rollback()
    assert _scalar(conn, "SELECT count(*) FROM lake_sink.s_worker") == 4  # still paused

    # Resume: the remaining messages arrive, and nothing is double-counted.
    run_command("SELECT lake_sink.enable_subscription('s_worker')", conn)
    conn.commit()
    assert _wait_for_count(conn, "lake_sink.s_worker", 7) == 7

    conn.rollback()
    offsets = [
        r["offset"]
        for r in run_query(
            'SELECT "offset" FROM lake_sink.s_worker ORDER BY "offset"', conn
        )
    ]
    assert offsets == [0, 1, 2, 3, 4, 5, 6]  # dense, no duplicates across the pause

    run_command("SELECT lake_sink.drop_subscription('s_worker', true)", conn)
    conn.commit()


# ---------------------------------------------------------------------------
# Subscription management
# ---------------------------------------------------------------------------


def test_drop_subscription_cascade(sink_extension, kafka):
    """drop_subscription removes the row, cascades away its stored offsets, and
    (with drop_table => true) drops the target table."""
    conn = sink_extension
    topic = "t_drop"
    kafka.produce(topic, ['{"a": 1}'], keyed=False)

    run_command(
        f"SELECT lake_sink.create_subscription('s_drop', '{topic}', enabled => false)",
        conn,
    )
    conn.commit()
    assert _scalar(conn, "SELECT lake_sink.pull('s_drop', 100)") == 1
    conn.commit()

    # Preconditions: subscription, its offsets, and the target table all exist.
    assert (
        _scalar(
            conn, "SELECT count(*) FROM lake_sink.subscriptions WHERE name = 's_drop'"
        )
        == 1
    )
    assert (
        _scalar(
            conn,
            "SELECT count(*) FROM lake_sink.subscription_offsets WHERE subscription = 's_drop'",
        )
        == 1
    )
    assert _scalar(conn, "SELECT to_regclass('lake_sink.s_drop') IS NOT NULL")

    run_command("SELECT lake_sink.drop_subscription('s_drop', true)", conn)
    conn.commit()

    assert (
        _scalar(
            conn, "SELECT count(*) FROM lake_sink.subscriptions WHERE name = 's_drop'"
        )
        == 0
    )
    # Offsets cascade-deleted via the FK ON DELETE CASCADE.
    assert (
        _scalar(
            conn,
            "SELECT count(*) FROM lake_sink.subscription_offsets WHERE subscription = 's_drop'",
        )
        == 0
    )
    # Target table dropped.
    assert _scalar(conn, "SELECT to_regclass('lake_sink.s_drop') IS NULL")


def test_invalid_subscription_name_rejected(sink_extension):
    """Names outside [A-Za-z0-9_] are rejected before anything is created."""
    conn = sink_extension
    err = run_query(
        "SELECT lake_sink.create_subscription('bad name', 'topic', enabled => false)",
        conn,
        raise_error=False,
    )
    conn.rollback()
    assert "invalid subscription name" in str(err)
    # Nothing was recorded.
    assert (
        _scalar(
            conn, "SELECT count(*) FROM lake_sink.subscriptions WHERE name = 'bad name'"
        )
        == 0
    )


def test_duplicate_subscription_rejected(sink_extension):
    """Creating two subscriptions with the same name fails on the PK."""
    conn = sink_extension
    run_command(
        "SELECT lake_sink.create_subscription('s_dup', 'topic', enabled => false)",
        conn,
    )
    conn.commit()

    err = run_query(
        "SELECT lake_sink.create_subscription('s_dup', 'topic', enabled => false)",
        conn,
        raise_error=False,
    )
    conn.rollback()
    # The unique-name (PK) violation surfaces as a raw Postgres error: it is
    # raised through ereport and longjmps past the Rust wrapper, rather than
    # coming back as a wrapped "cannot record subscription" message.
    assert "duplicate key value" in str(err)

    run_command("SELECT lake_sink.drop_subscription('s_dup', true)", conn)
    conn.commit()


def test_pull_unknown_subscription_errors(sink_extension):
    """pull() on a nonexistent subscription raises a clear error."""
    conn = sink_extension
    err = run_query(
        "SELECT lake_sink.pull('does_not_exist', 10)", conn, raise_error=False
    )
    conn.rollback()
    assert "no subscription named" in str(err)


# ---------------------------------------------------------------------------
# Real pg_lake USING iceberg target (needs S3/pgduck via the s3 + extension fixtures)
# ---------------------------------------------------------------------------


def test_iceberg_target(s3, extension, sink_extension, kafka):
    """Stream into a pre-created pg_lake ``USING iceberg`` foreign table
    (create_target_table => false). Rows land in the Iceberg table, offsets
    advance, and an incremental pull appends without duplicates."""
    conn = sink_extension
    table = "public.sink_iceberg"
    location = f"s3://{TEST_BUCKET}/test_sink_iceberg/"

    run_command(f"DROP TABLE IF EXISTS {table}", conn)
    run_command(
        f"""CREATE TABLE {table} (
                topic           text,
                partition       int,
                "offset"        bigint,
                key             bytea,
                payload         jsonb,
                headers         jsonb,
                kafka_timestamp timestamptz
            ) USING iceberg WITH (location = '{location}')""",
        conn,
    )
    conn.commit()

    topic = "t_iceberg"
    kafka.produce(topic, [(f"k{i}", f'{{"n": {i}}}') for i in range(1, 6)])

    run_command(
        f"SELECT lake_sink.create_subscription('s_ice', '{topic}', "
        f"target_table => '{table}', create_target_table => false, enabled => false)",
        conn,
    )
    conn.commit()

    assert _scalar(conn, "SELECT lake_sink.pull('s_ice', 100)") == 5
    conn.commit()  # commit flushes the Iceberg snapshot (catalog pointer update)

    assert _scalar(conn, f"SELECT count(*) FROM {table}") == 5
    assert (
        _scalar(
            conn,
            "SELECT last_offset FROM lake_sink.subscription_offsets WHERE subscription = 's_ice'",
        )
        == 4
    )
    # Payload survived the round-trip through Parquet/Iceberg.
    assert _scalar(conn, f'SELECT payload FROM {table} WHERE "offset" = 0') == {"n": 1}

    # Incremental ingest resumes from the stored offset.
    kafka.produce(topic, [(f"k{i}", f'{{"n": {i}}}') for i in range(6, 9)])
    assert _scalar(conn, "SELECT lake_sink.pull('s_ice', 100)") == 3
    conn.commit()
    assert _scalar(conn, f"SELECT count(*) FROM {table}") == 8

    run_command("SELECT lake_sink.drop_subscription('s_ice', true)", conn)
    conn.commit()
