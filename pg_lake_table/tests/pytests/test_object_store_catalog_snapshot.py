from utils_pytest import *
import json
import psycopg
import threading
import time

# What a reader of an external Iceberg catalog is entitled to see.
#
# catalog.json sits at a fixed path and names the current metadata location of
# every table of the catalog, so another cluster can replace it at any moment.
# A reader that resolves a table through that document more than once in one
# read is open to a rewrite landing in between: one table named twice can come
# back as two different snapshots, and two tables of one catalog can come back
# from two different points in time.  Neither is a state those tables ever had
# together.
#
# The rules these tests pin down:
#
#   * one statement reads one version of the catalog, whatever its isolation
#     level and however many tables and references it has, including every
#     query it runs in functions of any volatility, DO blocks, procedures,
#     triggers and rule actions, every PL/pgSQL FOR loop those run, and across
#     the Bind and Execute messages that start and run it with the extended
#     protocol;
#   * READ COMMITTED reads the catalog as it stands in each new statement,
#     including each statement of a multi-statement message, and a cursor
#     reads the version of the statement that opened it for as long as it is
#     open;
#   * REPEATABLE READ and SERIALIZABLE hold the whole transaction to the
#     version it started reading with, including for a table it touches for
#     the first time in a later statement, and including inside functions;
#   * a subtransaction that aborts, by an error or by a rollback, leaves the
#     version its statement or transaction reads in place, and so do the
#     cache invalidations other sessions send;
#   * whatever a statement or a transaction holds is released when it ends,
#     by commit, by abort, or by PREPARE TRANSACTION, including what a cursor
#     still reads with, and a procedure that commits reads anew after the
#     commit.
#
# The limits the implementation accepts, stated here as behaviour rather than
# wished away:
#
#   * the version is pinned when a transaction first reads an external table,
#     not when it took its snapshot;
#   * a statement holds its version across the queries its functions, DO
#     blocks, procedures and triggers run, where a heap table read in the same
#     place moves under READ COMMITTED, so a loop polling an external table
#     inside one statement sees the catalog move only once a procedure
#     commits;
#   * a cursor a function opens, such as a FOR loop's, reads the version of
#     its statement when the client runs that statement in the unnamed
#     portal, as the simple protocol always does, and the catalog as it stands
#     when it opens under a portal the client named;
#   * attaching a table reads the catalog as it stands, so a table published
#     since a REPEATABLE READ transaction pinned the catalog can be attached in
#     it, but not read until the pin is released.
#
# A query that reads catalog.json once cannot mix two versions of it, so most
# tests count the reads rather than racing a rewriting thread against a query
# and hoping the timing lands.  The fetch reports itself at DEBUG1, and
# _run_counting_catalog_reads and _count_catalog_reads count those reports.
# One test still races, the view one, to show the result a reader gets is right
# end to end, and the tests for functions rewrite the catalog while a function
# waits between two reads on a lock the test holds, to show which version each
# read got.


# The catalog export worker rewrites catalog.json whenever the set of pg_lake
# tables changes, and the shared settings fixture has readers read the same
# document that worker writes.  A test that hand-writes a version of the
# catalog needs one the worker leaves alone, so these tests point readers at a
# prefix of their own and keep taking real metadata locations from the
# document the worker writes.
WORKER_CATALOG_PREFIX = "tmp"
READER_CATALOG_PREFIX = "snapshot_tests"


def _set_external_catalog_prefix(superuser_conn, prefix):
    superuser_conn.autocommit = True
    run_command(
        "ALTER SYSTEM SET pg_lake_iceberg.external_object_store_catalog_prefix "
        f"= '{prefix}'",
        superuser_conn,
    )
    run_command("SELECT pg_reload_conf()", superuser_conn)

    # the background workers need a moment to pick up the reload, the same way
    # adjust_object_store_settings waits for it
    run_command("SELECT pg_sleep(0.1)", superuser_conn)
    superuser_conn.autocommit = False


@pytest.fixture(scope="function")
def reader_catalog_prefix(pg_conn, s3, superuser_conn, adjust_object_store_settings):
    _set_external_catalog_prefix(superuser_conn, READER_CATALOG_PREFIX)

    # an object_store catalog counts as existing when its document is there,
    # and both reading and listing ask that before anything else, so the
    # reader's path starts out holding an empty catalog
    _put_catalog(
        s3,
        _catalog_key(pg_conn, READER_CATALOG_PREFIX),
        json.dumps({"tables": []}).encode("utf-8"),
    )
    pg_conn.commit()

    yield READER_CATALOG_PREFIX

    # adjust_object_store_settings resets the setting on its own way out, but
    # it tears down after this fixture, so put back the value it set
    _set_external_catalog_prefix(superuser_conn, WORKER_CATALOG_PREFIX)


# Flip catalog.json between two precomputed bodies as fast as storage allows.
# The bodies are built up front so the thread only issues put_object, which
# keeps the rewrite far cheaper than the query it races.
class CatalogFlipper(threading.Thread):
    def __init__(self, s3, key, body_early, body_late):
        super().__init__(daemon=True)
        self.s3 = s3
        self.key = key
        self.bodies = [body_early, body_late]
        self.stop_event = threading.Event()
        self.flips = 0

    def run(self):
        while not self.stop_event.is_set():
            self.s3.put_object(
                Bucket=TEST_BUCKET, Key=self.key, Body=self.bodies[self.flips % 2]
            )
            self.flips += 1

    def stop(self):
        self.stop_event.set()
        self.join(timeout=30)


def _catalog_key(pg_conn, catalog_prefix):
    dbname = run_query("SELECT current_database()", pg_conn)[0][0]
    return f"{catalog_prefix}/catalog/{dbname}/catalog.json"


# Several tests create a source table of the same name in a schema of their
# own, so the namespace belongs in the lookup the way it does in
# wait_until_object_store_writable_table_pushed.
def _current_metadata_location(pg_conn, table_namespace, table_name):
    return run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = '{table_name}' "
        f"AND table_namespace ILIKE '%{table_namespace}%'",
        pg_conn,
    )[0][0]


# Build a catalog.json body whose entries point at the given locations, keyed
# by catalog table name.  The body is written straight to storage, so pg_lake
# never sees it coming.
def _catalog_body_pointing_at(s3, key, locations):
    body = json.loads(s3.get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
    for entry in body["tables"]:
        if entry["table-name"] in locations:
            entry["metadata-location"] = locations[entry["table-name"]]
    return json.dumps(body).encode("utf-8")


def _put_catalog(s3, key, body):
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)


# Turn the metadata locations of two points in time into two versions of one
# catalog, built from the document the export worker writes and published at
# the reader's own path.  Returns the reader's catalog key and the two bodies,
# which differ only in the metadata locations they name.
def _reader_catalog_versions(
    pg_conn, s3, reader_prefix, early_locations, late_locations
):
    worker_key = _catalog_key(pg_conn, WORKER_CATALOG_PREFIX)
    reader_key = _catalog_key(pg_conn, reader_prefix)

    body_early = _catalog_body_pointing_at(s3, worker_key, early_locations)
    body_late = _catalog_body_pointing_at(s3, worker_key, late_locations)

    # a read-only table resolves through the reader's copy of the catalog
    # already at creation time, so that copy has to be in place first
    _put_catalog(s3, reader_key, body_early)

    return reader_key, body_early, body_late


# The fetch of catalog.json reports itself at DEBUG1, and nothing else in the
# backend says that line, so counting it answers how many versions of the
# catalog a statement could have read.
CATALOG_READ_MESSAGE = "read external catalog document"


def _run_counting_catalog_reads(pg_conn, query):
    run_command("SET client_min_messages TO DEBUG1", pg_conn)
    pg_conn.notices.clear()

    rows = run_query(query, pg_conn)

    catalog_reads = len(
        [line for line in pg_conn.notices if CATALOG_READ_MESSAGE in str(line)]
    )
    run_command("RESET client_min_messages", pg_conn)

    return rows, catalog_reads


# The same count for a statement that returns no rows, such as a DO block, a
# CALL, or a message of several statements.
def _count_catalog_reads(pg_conn, statement):
    run_command("SET client_min_messages TO DEBUG1", pg_conn)
    pg_conn.notices.clear()

    run_command(statement, pg_conn)

    catalog_reads = len(
        [line for line in pg_conn.notices if CATALOG_READ_MESSAGE in str(line)]
    )
    run_command("RESET client_min_messages", pg_conn)

    return catalog_reads


# The functions under test wait on an advisory lock between two of their
# reads.  _rewrite_while_waiting holds that lock from a session of its own
# while it rewrites catalog.json, and nothing else takes it, so anywhere else
# the wait is over at once.
REWRITE_LOCK_KEYS = (741, 1)
REWRITE_LOCK = "%d, %d" % REWRITE_LOCK_KEYS
WAIT_FOR_REWRITE = (
    f"PERFORM pg_advisory_lock({REWRITE_LOCK}); "
    f"PERFORM pg_advisory_unlock({REWRITE_LOCK});"
)

# the same wait in a SQL function body or a message of several statements
WAIT_FOR_REWRITE_IN_SQL = WAIT_FOR_REWRITE.replace("PERFORM", "SELECT")


# Run run in a thread and rewrite catalog.json while run waits between two of
# its reads, so that the read before the wait saw the catalog as it was and the
# read after it had the rewrite to see: a read that misses the rewrite is the
# version held for it rather than a race the test lost.
def _rewrite_while_waiting(s3, key, body, run):
    lock_conn = open_pg_conn()
    lock_conn.autocommit = True
    run_command(f"SELECT pg_advisory_lock({REWRITE_LOCK})", lock_conn)

    outcome = {}

    def run_and_keep_outcome():
        try:
            outcome["result"] = run()
        except Exception as error:
            outcome["error"] = error

    runner = threading.Thread(target=run_and_keep_outcome, daemon=True)
    runner.start()
    rewritten = False
    try:
        while runner.is_alive() and not rewritten:
            if _rewrite_lock_waited_on(lock_conn):
                _put_catalog(s3, key, body)
                rewritten = True
            else:
                time.sleep(0.01)
    finally:
        run_command(f"SELECT pg_advisory_unlock({REWRITE_LOCK})", lock_conn)
        lock_conn.close()
        runner.join(timeout=60)

    if "error" in outcome:
        raise outcome["error"]
    assert rewritten, "run finished without waiting for the rewrite"
    assert not runner.is_alive(), "run did not finish after the rewrite"

    return outcome["result"]


# pg_locks shows the two keys of an advisory lock as its classid and objid
def _rewrite_lock_waited_on(lock_conn):
    return run_query(
        f"SELECT EXISTS (SELECT FROM pg_locks WHERE locktype = 'advisory' "
        f"AND classid = {REWRITE_LOCK_KEYS[0]} AND objid = {REWRITE_LOCK_KEYS[1]} "
        f"AND objsubid = 2 AND NOT granted)",
        lock_conn,
    )[0][0]


# The tests for functions record what each read saw in a table of their own,
# because a DO block, a procedure and a trigger return nothing to the client.
def _create_seen_table(pg_conn, schema):
    run_command(f"CREATE TABLE {schema}.seen (label text, n bigint)", pg_conn)


def _seen(pg_conn, schema):
    rows = run_query(f"SELECT label, n FROM {schema}.seen", pg_conn)
    run_command(f"TRUNCATE {schema}.seen", pg_conn)
    return {row[0]: row[1] for row in rows}


def _reset_schema(pg_conn, schema):
    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", pg_conn)
    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()


# Spread rows over single-row commits so each one lands in its own data file.
# The gap between the two catalog.json reads is the first reference's metadata
# and manifest read, so more manifest work widens the window.  Compaction is
# off on these tables, so the files stay separate and the metadata locations
# stay where the test pinned them.
def _insert_in_separate_commits(pg_conn, table, value_rows):
    for row in value_rows:
        run_command(f"INSERT INTO {table} VALUES {row}", pg_conn)
        pg_conn.commit()


def _create_uncompacted_writable_table(pg_conn, qualified_name, columns):
    # autovacuum_compact_data_files=false keeps the many small data files from
    # being rewritten into one, which would both narrow the window and move
    # the metadata location this test pins.
    run_command(
        f"CREATE TABLE {qualified_name}({columns}) USING iceberg "
        f"WITH (catalog='object_store', autovacuum_enabled = false, "
        f"autovacuum_compact_data_files = false)",
        pg_conn,
    )
    pg_conn.commit()


# Build two versions of one catalog over `table_names`: an earlier one where
# every table holds `early_rows` rows and a later one where every table holds
# `early_rows + extra_rows`.  Each name gets a writable table that holds the
# rows and a read-only table pointing at it by catalog table name, so a test
# picks a version by writing the matching body and reads it through the
# read-only side.  Returns the reader's catalog key and the two bodies.
#
# wide_window spreads the rows over single-row commits, for the tests that
# need a long metadata read to race against; the others take one commit.
def _two_catalog_versions(
    pg_conn,
    s3,
    schema,
    table_names,
    early_rows,
    extra_rows,
    reader_prefix,
    wide_window=False,
):
    early_locations, late_locations = {}, {}

    for table_name in table_names:
        source_name = f"{table_name}_src"
        qualified_source = f"{schema}.{source_name}"
        _create_uncompacted_writable_table(pg_conn, qualified_source, "a INT")

        for low, high in ((1, early_rows), (early_rows + 1, early_rows + extra_rows)):
            if wide_window:
                _insert_in_separate_commits(
                    pg_conn, qualified_source, [f"({n})" for n in range(low, high + 1)]
                )
            else:
                run_command(
                    f"INSERT INTO {qualified_source} "
                    f"SELECT generate_series({low}, {high})",
                    pg_conn,
                )
                pg_conn.commit()

            locations = early_locations if high == early_rows else late_locations
            locations[source_name] = _current_metadata_location(
                pg_conn, schema, source_name
            )

        assert early_locations[source_name] != late_locations[source_name]

    # the bodies take only the entries of the tables from the document the
    # worker writes, and the worker writes every table into each document, so
    # one written after the last commit has them all
    wait_until_object_store_writable_table_pushed(pg_conn, schema, source_name)

    key, body_early, body_late = _reader_catalog_versions(
        pg_conn, s3, reader_prefix, early_locations, late_locations
    )

    for table_name in table_names:
        run_command(
            f"CREATE TABLE {schema}.{table_name}(a INT) USING iceberg "
            f"WITH (catalog='object_store', read_only=True, "
            f"catalog_table_name='{table_name}_src')",
            pg_conn,
        )
    pg_conn.commit()

    return key, body_early, body_late


# Two references to the same table in one statement resolve the catalog once,
# which is what makes them one snapshot.  The second read then shows the count
# following the catalog, so the first one did not report a single read by
# reading nothing.
def test_double_reference_sees_one_snapshot(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_double_scan"
    _reset_schema(pg_conn, schema)

    early_rows, extra_rows = 5, 4
    key, body_early, body_late = _two_catalog_versions(
        pg_conn,
        s3,
        schema,
        ["read_tbl"],
        early_rows,
        extra_rows,
        reader_catalog_prefix,
    )

    doubled_query = (
        f"SELECT (SELECT count(*) FROM {schema}.read_tbl) AS first_reference, "
        f"       (SELECT count(*) FROM {schema}.read_tbl) AS second_reference"
    )

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, doubled_query)
    assert tuple(rows[0]) == (early_rows, early_rows)
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"

    _put_catalog(s3, key, body_late)

    # the next statement of this READ COMMITTED transaction is entitled to the
    # rewrite, and reads the document once for both of its references again
    late_rows = early_rows + extra_rows
    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, doubled_query)
    assert tuple(rows[0]) == (late_rows, late_rows)
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()


# The same defect through a view, where the two references are not obviously
# the same table to the reader of the query.  The view keeps the rows of a
# heap table that no later row supersedes, plus the added rows that no later
# row supersedes.  It names the read-only object_store table twice: once in
# the CTE, once in the second half of the UNION ALL.  Under one snapshot the
# view returns exactly one row per record key.
def test_view_referencing_one_table_twice(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_view_twice"
    _reset_schema(pg_conn, schema)

    columns = "record_key INT, amount INT, kind TEXT, version INT"
    _create_uncompacted_writable_table(pg_conn, f"{schema}.wrt_rows", columns)

    # Padding rows carry a kind that neither half of the view selects, so they
    # only serve to widen the window between the two catalog.json reads.
    padding = [f"({1000 + n}, {n}, 'noop', {n})" for n in range(24)]
    batch_at_version5 = ["(1, 100, 'supersede', 5)", "(1, 101, 'add', 5)"]
    batch_at_version9 = ["(1, 101, 'supersede', 9)", "(1, 102, 'add', 9)"]

    _insert_in_separate_commits(
        pg_conn, f"{schema}.wrt_rows", padding + batch_at_version5
    )
    wait_until_object_store_writable_table_pushed(pg_conn, schema, "wrt_rows")
    early_location = _current_metadata_location(pg_conn, schema, "wrt_rows")

    _insert_in_separate_commits(pg_conn, f"{schema}.wrt_rows", batch_at_version9)
    wait_until_object_store_writable_table_pushed(pg_conn, schema, "wrt_rows")
    late_location = _current_metadata_location(pg_conn, schema, "wrt_rows")
    assert early_location != late_location

    key, body_early, body_late = _reader_catalog_versions(
        pg_conn,
        s3,
        reader_catalog_prefix,
        {"wrt_rows": early_location},
        {"wrt_rows": late_location},
    )

    run_command(
        f"CREATE TABLE {schema}.read_rows({columns}) USING iceberg "
        f"WITH (catalog='object_store', read_only=True, "
        f"catalog_table_name='wrt_rows')",
        pg_conn,
    )
    run_command(f"CREATE TABLE {schema}.base_tbl(record_key INT, amount INT)", pg_conn)
    run_command(f"INSERT INTO {schema}.base_tbl VALUES (1, 100)", pg_conn)
    pg_conn.commit()

    run_command(
        f"""CREATE VIEW {schema}.effective_rows AS
            WITH superseded AS (
                SELECT record_key, MAX(version) AS version
                FROM {schema}.read_rows
                WHERE kind = 'supersede'
                GROUP BY record_key
            )
            SELECT base.record_key, base.amount
            FROM {schema}.base_tbl base
            WHERE NOT EXISTS (
                SELECT 1 FROM superseded
                WHERE superseded.record_key = base.record_key
            )
            UNION ALL
            SELECT added.record_key, added.amount
            FROM {schema}.read_rows added
            WHERE added.kind = 'add'
              AND NOT EXISTS (
                  SELECT 1 FROM superseded
                  WHERE superseded.record_key = added.record_key
                    AND superseded.version > added.version
              )""",
        pg_conn,
    )
    pg_conn.commit()

    view_query = (
        f"SELECT record_key, amount FROM {schema}.effective_rows ORDER BY amount"
    )

    # Precondition: each snapshot on its own gives exactly one row, and the two
    # differ, so anything else below comes from mixing the two.
    expected_results = []
    for body, expected in ((body_early, [(1, 101)]), (body_late, [(1, 102)])):
        _put_catalog(s3, key, body)
        rows = run_query(view_query, pg_conn)
        pg_conn.commit()
        observed = [tuple(row) for row in rows]
        assert observed == expected, f"expected {expected}, got {observed}"
        expected_results.append(expected)

    # The view names the table twice and the statement reads the catalog once,
    # which is the whole reason the rows below can only come from one version.
    _put_catalog(s3, key, body_late)
    pg_conn.commit()
    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, view_query)
    assert [tuple(row) for row in rows] == [(1, 102)]
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()

    # One test still races a rewriting thread against the query, to show that
    # what a reader gets out of the view is right and not only that the reads
    # behind it were counted right.  A build that holds nothing reads this
    # view's catalog 12 times per statement and returns rows of two versions
    # by the fourth query, so a short run is enough to catch a regression.
    flipper = CatalogFlipper(s3, key, body_early, body_late)
    flipper.start()
    wrong_results = []
    attempts = 0
    try:
        for attempts in range(1, 26):
            rows = run_query(view_query, pg_conn, raise_error=False)
            if isinstance(rows, str):
                pg_conn.rollback()
                continue
            pg_conn.commit()
            observed = [tuple(row) for row in rows]
            if observed not in expected_results:
                wrong_results.append((attempts, observed))
                break
    finally:
        flipper.stop()

    print(
        f"\nview result check: {attempts} queries, {flipper.flips} "
        f"catalog.json rewrites, {len(wrong_results)} wrong results"
    )
    for attempt, observed in wrong_results:
        print(f"  query #{attempt}: {observed}")

    assert not wrong_results, (
        "a view naming one read-only object_store table twice returned rows "
        "that no single Iceberg snapshot of that table can produce, expected "
        f"one of {expected_results}: {wrong_results}"
    )


# Two tables of one catalog read in one statement must come from one version
# of that catalog, the same way two references to one table must, and they do
# because the statement reads the document that names both of them once.
def test_two_tables_see_one_catalog_version(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_two_tables"
    _reset_schema(pg_conn, schema)

    early_rows, extra_rows = 5, 4
    key, body_early, body_late = _two_catalog_versions(
        pg_conn,
        s3,
        schema,
        ["read_one", "read_two"],
        early_rows,
        extra_rows,
        reader_catalog_prefix,
    )

    paired_query = (
        f"SELECT (SELECT count(*) FROM {schema}.read_one) AS first_table, "
        f"       (SELECT count(*) FROM {schema}.read_two) AS second_table"
    )

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, paired_query)
    assert tuple(rows[0]) == (early_rows, early_rows)
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"

    _put_catalog(s3, key, body_late)

    # both tables move to the rewritten version together in the next statement
    late_rows = early_rows + extra_rows
    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, paired_query)
    assert tuple(rows[0]) == (late_rows, late_rows)
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()


# READ COMMITTED resolves the catalog per statement, so a statement sees a
# rewrite that landed since the previous statement of its own transaction.
def test_read_committed_refreshes_per_statement(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_read_committed"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"
    transaction_id = run_query("SELECT pg_current_xact_id()", pg_conn)[0][0]
    assert run_query(count_query, pg_conn)[0][0] == 5

    _put_catalog(s3, key, body_late)

    assert run_query(count_query, pg_conn)[0][0] == 9

    # and all of that was one transaction, which is the point of the test
    assert run_query("SELECT pg_current_xact_id()", pg_conn)[0][0] == transaction_id
    pg_conn.commit()


# REPEATABLE READ and SERIALIZABLE hold a transaction to the version of the
# catalog it started reading with, including for a table it touches for the
# first time after a rewrite.
def test_transaction_snapshot_pins_catalog(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_xact_snapshot"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_first", "read_later"], 5, 4, reader_catalog_prefix
    )

    count_first = f"SELECT count(*) FROM {schema}.read_first"
    count_later = f"SELECT count(*) FROM {schema}.read_later"
    late_location = next(
        entry["metadata-location"]
        for entry in json.loads(body_late)["tables"]
        if entry["table-name"] == "read_first_src"
    )

    for isolation_level in ("REPEATABLE READ", "SERIALIZABLE"):
        _put_catalog(s3, key, body_early)
        pg_conn.commit()

        # SET TRANSACTION applies to the transaction the driver opened to run
        # it, which is the one every statement below runs in.
        run_command(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}", pg_conn)
        assert (
            run_query("SHOW transaction_isolation", pg_conn)[0][0]
            == isolation_level.lower()
        )

        assert run_query(count_first, pg_conn)[0][0] == 5

        _put_catalog(s3, key, body_late)

        # the table this transaction has read keeps the version it read
        assert run_query(count_first, pg_conn)[0][0] == 5

        # and a table first touched after the rewrite is resolved from that
        # same version, the way a transaction snapshot covers a heap table a
        # transaction reads for the first time late
        assert run_query(count_later, pg_conn)[0][0] == 5

        # the listing function answers for the catalog as it stands now
        # instead, which is what it is for
        listed = run_query(
            "SELECT metadata_location FROM "
            "lake_iceberg.list_object_store_tables(current_database(), false) "
            "WHERE catalog_table_name = 'read_first_src'",
            pg_conn,
        )[0][0]
        assert listed == late_location

        pg_conn.commit()

        # the next transaction is entitled to the rewrite
        assert run_query(count_first, pg_conn)[0][0] == 9
        pg_conn.commit()


# The version a transaction is reading with belongs to the transaction, not to
# the subtransaction that happened to resolve it, so a savepoint rollback
# leaves it in place.  READ COMMITTED holds nothing across statements to begin
# with, and a savepoint rollback must not turn that into holding something.
@pytest.mark.parametrize(
    "isolation_level,rows_after_rollback",
    [
        ("READ COMMITTED", 9),
        ("REPEATABLE READ", 5),
        ("SERIALIZABLE", 5),
    ],
)
def test_savepoint_rollback_keeps_pinned_catalog(
    isolation_level,
    rows_after_rollback,
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_savepoint"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    # the driver has already opened the transaction, so this is the way to set
    # its isolation level; assert it took, or a level would be tested twice
    run_command(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}", pg_conn)
    assert (
        run_query("SHOW transaction_isolation", pg_conn)[0][0]
        == isolation_level.lower()
    )

    run_command("SAVEPOINT resolve_here", pg_conn)
    assert run_query(count_query, pg_conn)[0][0] == 5
    run_command("ROLLBACK TO SAVEPOINT resolve_here", pg_conn)

    _put_catalog(s3, key, body_late)

    assert run_query(count_query, pg_conn)[0][0] == rows_after_rollback

    # the same through a subtransaction that aborts on an error rather than
    # one that is rolled back cleanly
    run_command("SAVEPOINT fail_here", pg_conn)
    run_query("SELECT 1 / 0", pg_conn, raise_error=False)
    run_command("ROLLBACK TO SAVEPOINT fail_here", pg_conn)

    assert run_query(count_query, pg_conn)[0][0] == rows_after_rollback
    pg_conn.commit()

    assert run_query(count_query, pg_conn)[0][0] == 9
    pg_conn.commit()


# Whatever a transaction pinned is released when that transaction ends,
# whichever way it ends.  An abort and a PREPARE TRANSACTION both end one
# without a commit, and PREPARE ends it without the backend going idle.
def test_transaction_end_releases_pinned_catalog(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_release"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)
    assert run_query(count_query, pg_conn)[0][0] == 5

    _put_catalog(s3, key, body_late)
    assert run_query(count_query, pg_conn)[0][0] == 5

    run_query("SELECT 1 / 0", pg_conn, raise_error=False)
    pg_conn.rollback()

    # the pin went with the aborted transaction
    assert run_query(count_query, pg_conn)[0][0] == 9
    pg_conn.commit()

    if run_query("SHOW max_prepared_transactions", pg_conn)[0][0] == "0":
        # nothing to say about PREPARE on a cluster that does not allow it
        pg_conn.commit()
        return

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    prepared_name = "pg_lake_object_store_catalog_snapshot"
    run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)
    assert run_query(count_query, pg_conn)[0][0] == 5

    _put_catalog(s3, key, body_late)
    run_command(f"PREPARE TRANSACTION '{prepared_name}'", pg_conn)

    try:
        # a new transaction in the same backend is entitled to the rewrite,
        # and gets it only if PREPARE released what the prepared transaction
        # pinned
        assert run_query(count_query, pg_conn)[0][0] == 9
    finally:
        pg_conn.commit()
        pg_conn.autocommit = True
        run_command(f"COMMIT PREPARED '{prepared_name}'", pg_conn)
        pg_conn.autocommit = False


# The version a transaction reads with is pinned when it first reads an
# external table, not when it took its snapshot.  A REPEATABLE READ
# transaction that takes its snapshot elsewhere, lets the catalog move, and
# only then reads the table gets the newer version, where a heap table holds
# the older one.  Resolving every external table at BEGIN is the wrong price
# for closing that gap, so this is the behaviour and the test says so.
def test_first_read_pins_catalog_not_the_snapshot(
    pg_conn,
    s3,
    superuser_conn,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_first_read"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"
    heap_count_query = f"SELECT count(*) FROM {schema}.heap_tbl"

    # a heap table another session can change, for the contrast at the end
    run_command(f"CREATE TABLE {schema}.heap_tbl(a INT)", pg_conn)
    run_command(f"INSERT INTO {schema}.heap_tbl SELECT generate_series(1, 5)", pg_conn)
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)

    # the transaction snapshot is taken by this read, which has nothing to do
    # with the external catalog
    assert run_query(heap_count_query, pg_conn)[0][0] == 5

    run_command(
        f"INSERT INTO {schema}.heap_tbl SELECT generate_series(6, 9)", superuser_conn
    )
    superuser_conn.commit()
    _put_catalog(s3, key, body_late)

    # the heap table stays at the version the snapshot covers
    assert run_query(heap_count_query, pg_conn)[0][0] == 5

    # the external table does not, because it is resolved now and the catalog
    # has moved since the snapshot was taken
    assert run_query(count_query, pg_conn)[0][0] == 9

    # from that first read on the transaction does hold its version, which is
    # what test_transaction_snapshot_pins_catalog covers
    _put_catalog(s3, key, body_early)
    assert run_query(count_query, pg_conn)[0][0] == 9
    pg_conn.commit()


# A function reads with the statement that calls it, whatever its volatility,
# so its queries read the version the rest of the statement reads, however
# many times the statement calls it and however long it runs.  A heap table a
# volatile function reads under READ COMMITTED would move in between, so for
# a volatile function this holds the catalog longer than a heap table, which
# can read an older version than the statement could, and never two at once.
@pytest.mark.parametrize("volatility", ["VOLATILE", "STABLE"])
def test_functions_read_their_statements_version(
    volatility,
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = f"test_osc_{volatility.lower()}_function"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    run_command(
        f"CREATE FUNCTION {schema}.rows_after_wait() "
        f"RETURNS TABLE (n bigint) {volatility} LANGUAGE plpgsql AS $$ BEGIN "
        f"  {WAIT_FOR_REWRITE} "
        f"  RETURN QUERY SELECT count(*) FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    run_command(
        f"CREATE FUNCTION {schema}.row_count_then_wait() "
        f"RETURNS bigint {volatility} LANGUAGE plpgsql AS $$ "
        f"DECLARE n bigint; BEGIN "
        f"  SELECT count(*) INTO n FROM {schema}.read_tbl; "
        f"  {WAIT_FOR_REWRITE} "
        f"  RETURN n; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"

    join_query = (
        f"SELECT count(*), max(f.n) "
        f"FROM {schema}.read_tbl CROSS JOIN {schema}.rows_after_wait() f"
    )

    # the outer scan and the query inside the function see one version
    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, join_query)
    assert tuple(rows[0]) == (5, 5)
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()

    # the function reads after the rewrite and still gets the statement's
    # version
    rows = _rewrite_while_waiting(
        s3, key, body_late, lambda: run_query(join_query, pg_conn)
    )
    assert tuple(rows[0]) == (5, 5)
    pg_conn.commit()

    # a caller that does not read the table calls the function three times,
    # and the catalog is rewritten between the first call and the second
    _put_catalog(s3, key, body_early)
    pg_conn.commit()
    calls_query = (
        f"SELECT array_agg({schema}.row_count_then_wait() ORDER BY g) "
        f"FROM generate_series(1, 3) g"
    )

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, calls_query)
    assert rows[0][0] == [5, 5, 5]
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()

    rows = _rewrite_while_waiting(
        s3, key, body_late, lambda: run_query(calls_query, pg_conn)
    )
    assert rows[0][0] == [5, 5, 5]
    pg_conn.commit()

    # the next statement reads the catalog as it stands
    rows = run_query(calls_query, pg_conn)
    assert rows[0][0] == [9, 9, 9]
    pg_conn.commit()


# A DO block, a procedure, a trigger function and a SQL function run their
# queries inside the statement that runs them, so they read the statement's
# version even when the catalog moves between two of their reads, where each
# of those queries would read a heap table under a new snapshot.  A procedure
# that commits in between reads anew after the commit, which
# test_open_cursor_versions_go_with_the_transaction covers.
def test_do_block_procedure_trigger_and_sql_function_read_their_statements_version(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_do_procedure_trigger"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_seen_table(pg_conn, schema)

    def read_twice(wait=WAIT_FOR_REWRITE):
        return (
            f"INSERT INTO {schema}.seen "
            f"  SELECT 'first', count(*) FROM {schema}.read_tbl; "
            f"{wait} "
            f"INSERT INTO {schema}.seen "
            f"  SELECT 'second', count(*) FROM {schema}.read_tbl; "
        )

    run_command(
        f"CREATE PROCEDURE {schema}.read_twice() "
        f"LANGUAGE plpgsql AS $$ BEGIN {read_twice()} END; $$",
        pg_conn,
    )
    run_command(
        f"CREATE FUNCTION {schema}.read_twice_on_insert() RETURNS trigger "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  {read_twice()} RETURN NULL; "
        f"END; $$",
        pg_conn,
    )
    run_command(f"CREATE TABLE {schema}.fire (a int)", pg_conn)
    run_command(
        f"CREATE TRIGGER read_twice AFTER INSERT ON {schema}.fire "
        f"FOR EACH ROW EXECUTE FUNCTION {schema}.read_twice_on_insert()",
        pg_conn,
    )

    # each statement of a SQL function body is a query of its own as well
    run_command(
        f"CREATE FUNCTION {schema}.read_twice_in_sql() "
        f"RETURNS void LANGUAGE sql AS $$ "
        f"  {read_twice(WAIT_FOR_REWRITE_IN_SQL)} $$",
        pg_conn,
    )
    pg_conn.commit()

    statements = {
        "DO block": f"DO $$ BEGIN {read_twice()} END; $$",
        "procedure": f"CALL {schema}.read_twice()",
        "trigger": f"INSERT INTO {schema}.fire VALUES (1)",
        "SQL function": f"SELECT {schema}.read_twice_in_sql()",
    }

    for kind, statement in statements.items():
        _put_catalog(s3, key, body_early)
        pg_conn.commit()

        _rewrite_while_waiting(
            s3, key, body_late, lambda: run_command(statement, pg_conn)
        )
        assert _seen(pg_conn, schema) == {"first": 5, "second": 5}, kind
        pg_conn.commit()

        # the next statement reads the rewrite
        assert run_query(f"SELECT count(*) FROM {schema}.read_tbl", pg_conn)[0][0] == 9
        pg_conn.commit()

    # and the two reads are one catalog read
    _put_catalog(s3, key, body_early)
    pg_conn.commit()
    catalog_reads = _count_catalog_reads(pg_conn, f"DO $$ BEGIN {read_twice()} END; $$")
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.rollback()


# Every statement of a message that carries several runs in a portal of its
# own, although they share one statement start timestamp, so each of them
# reads the catalog as it stands.
def test_each_statement_of_a_message_reads_with_its_own_snapshot(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_multi_statement"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_seen_table(pg_conn, schema)
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"
    catalog_reads = _count_catalog_reads(pg_conn, f"{count_query}; {count_query}")
    assert catalog_reads == 2, f"expected two catalog reads, got {catalog_reads}"
    pg_conn.commit()

    message = (
        f"INSERT INTO {schema}.seen SELECT 'first', count(*) FROM {schema}.read_tbl; "
        f"{WAIT_FOR_REWRITE_IN_SQL} "
        f"INSERT INTO {schema}.seen SELECT 'second', count(*) FROM {schema}.read_tbl"
    )
    _rewrite_while_waiting(s3, key, body_late, lambda: run_command(message, pg_conn))
    assert _seen(pg_conn, schema) == {"first": 5, "second": 9}
    pg_conn.commit()


# REPEATABLE READ and SERIALIZABLE read one snapshot for the whole transaction,
# inside functions as well, so a volatile function, which takes a new snapshot
# for every query in READ COMMITTED, reads the transaction's version.
@pytest.mark.parametrize("isolation_level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_functions_read_the_transaction_version_under_transaction_snapshots(
    isolation_level,
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_function_xact_snapshot"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    run_command(
        f"CREATE FUNCTION {schema}.read_twice("
        f"OUT first_read bigint, OUT second_read bigint) "
        f"VOLATILE LANGUAGE plpgsql AS $$ BEGIN "
        f"  SELECT count(*) INTO first_read FROM {schema}.read_tbl; "
        f"  {WAIT_FOR_REWRITE} "
        f"  SELECT count(*) INTO second_read FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    run_command(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}", pg_conn)
    assert (
        run_query("SHOW transaction_isolation", pg_conn)[0][0]
        == isolation_level.lower()
    )

    rows = _rewrite_while_waiting(
        s3,
        key,
        body_late,
        lambda: run_query(
            f"SELECT first_read, second_read FROM {schema}.read_twice()", pg_conn
        ),
    )
    assert tuple(rows[0]) == (5, 5)
    assert run_query(f"SELECT count(*) FROM {schema}.read_tbl", pg_conn)[0][0] == 5
    pg_conn.commit()

    # the next transaction is entitled to the rewrite
    assert run_query(f"SELECT count(*) FROM {schema}.read_tbl", pg_conn)[0][0] == 9
    pg_conn.commit()


# A cursor reads with the snapshot of the statement that declared it, however
# many later statements run before it is fetched, while those statements take
# snapshots of their own under READ COMMITTED.
def test_cursor_reads_the_version_it_was_declared_with(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_cursor"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"

    catalog_reads = _count_catalog_reads(
        pg_conn, f"DECLARE early_cursor CURSOR FOR {count_query}"
    )
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"

    _put_catalog(s3, key, body_late)

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, count_query)
    assert rows[0][0] == 9
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"

    assert run_query("FETCH ALL FROM early_cursor", pg_conn)[0][0] == 5
    pg_conn.commit()


# A cursor keeps its versions for as long as it is open, which can be past the
# statement that declared it, so a transaction can end with a cursor's
# versions still held.  Every way of ending the transaction drops them too,
# and the next transaction sees the catalog as it stands.  A procedure that
# commits between two reads ends a transaction in the middle of a statement,
# and what it reads after the commit it sees as the new transaction does.
def test_open_cursor_versions_go_with_the_transaction(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_held_cursor"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_seen_table(pg_conn, schema)
    run_command(
        f"CREATE PROCEDURE {schema}.read_commit_read() "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  INSERT INTO {schema}.seen "
        f"    SELECT 'before commit', count(*) FROM {schema}.read_tbl; "
        f"  COMMIT; "
        f"  {WAIT_FOR_REWRITE} "
        f"  INSERT INTO {schema}.seen "
        f"    SELECT 'after commit', count(*) FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    pg_conn.commit()

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"

    def end_with_cursor_open(end_transaction):
        _put_catalog(s3, key, body_early)
        pg_conn.commit()

        run_command(f"DECLARE held_cursor CURSOR FOR {count_query}", pg_conn)
        assert run_query("FETCH 1 FROM held_cursor", pg_conn)[0][0] == 5
        _put_catalog(s3, key, body_late)

        end_transaction()

        assert run_query(count_query, pg_conn)[0][0] == 9
        pg_conn.commit()

    end_with_cursor_open(pg_conn.commit)
    end_with_cursor_open(pg_conn.rollback)

    if run_query("SHOW max_prepared_transactions", pg_conn)[0][0] != "0":
        prepared_name = "pg_lake_held_external_catalog_cursor"

        def prepare():
            run_command(f"PREPARE TRANSACTION '{prepared_name}'", pg_conn)

            # PREPARE ended the transaction the driver opened, so close the
            # driver's side of it before switching to autocommit
            pg_conn.commit()
            pg_conn.autocommit = True
            try:
                run_command(f"COMMIT PREPARED '{prepared_name}'", pg_conn)
            finally:
                pg_conn.autocommit = False

        end_with_cursor_open(prepare)

    # a CALL outside a transaction block may commit, and what it reads after
    # the commit is read in a transaction of its own
    _put_catalog(s3, key, body_early)
    pg_conn.commit()
    pg_conn.autocommit = True
    try:
        _rewrite_while_waiting(
            s3,
            key,
            body_late,
            lambda: run_command(f"CALL {schema}.read_commit_read()", pg_conn),
        )
    finally:
        pg_conn.autocommit = False
    assert _seen(pg_conn, schema) == {"before commit": 5, "after commit": 9}
    pg_conn.commit()


# A subtransaction that aborts inside a function, as an EXCEPTION block does,
# leaves the versions of the statement that called the function in place,
# because they are still what the statement reads.  The function keeps
# seeing that version after the block, including after the catalog moved.
def test_subtransaction_abort_inside_a_function(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_function_subtransaction"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    run_command(
        f"CREATE FUNCTION {schema}.read_through_failure("
        f"OUT inside bigint, OUT after bigint) "
        f"VOLATILE LANGUAGE plpgsql AS $$ BEGIN "
        f"  BEGIN "
        f"    SELECT count(*) INTO inside FROM {schema}.read_tbl; "
        f"    PERFORM 1 / 0; "
        f"  EXCEPTION WHEN division_by_zero THEN "
        f"    NULL; "
        f"  END; "
        f"  {WAIT_FOR_REWRITE} "
        f"  SELECT count(*) INTO after FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    call_query = f"SELECT inside, after FROM {schema}.read_through_failure()"

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, call_query)
    assert tuple(rows[0]) == (5, 5)
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()

    rows = _rewrite_while_waiting(
        s3, key, body_late, lambda: run_query(call_query, pg_conn)
    )
    assert tuple(rows[0]) == (5, 5)
    pg_conn.commit()


# Attaching a table reads its catalog as it stands, the way listing it does, so
# a REPEATABLE READ transaction that has pinned the catalog can still attach a
# table published since.  The reads of that transaction keep the version it
# pinned, which does not list the new table yet, so reading the table waits
# for the next transaction.
def test_attach_reads_catalog_as_it_stands(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_attach"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    # a table that only the later version of the reader's catalog publishes
    _create_uncompacted_writable_table(pg_conn, f"{schema}.attach_src", "a INT")
    run_command(
        f"INSERT INTO {schema}.attach_src SELECT generate_series(1, 3)", pg_conn
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, schema, "attach_src")

    worker_key = _catalog_key(pg_conn, WORKER_CATALOG_PREFIX)
    worker_document = json.loads(
        s3.get_object(Bucket=TEST_BUCKET, Key=worker_key)["Body"].read()
    )
    late_document = json.loads(body_late)
    late_document["tables"] += [
        entry
        for entry in worker_document["tables"]
        if entry["namespace"] == schema and entry["table-name"] == "attach_src"
    ]
    body_late = json.dumps(late_document).encode("utf-8")

    count_read = f"SELECT count(*) FROM {schema}.read_tbl"
    count_attached = f"SELECT count(*) FROM {schema}.attached_tbl"

    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)
    assert run_query(count_read, pg_conn)[0][0] == 5

    _put_catalog(s3, key, body_late)

    run_command(
        f"CREATE TABLE {schema}.attached_tbl(a INT) USING iceberg "
        f"WITH (catalog='object_store', read_only=True, "
        f"catalog_table_name='attach_src')",
        pg_conn,
    )

    # attaching did not move the version this transaction reads
    assert run_query(count_read, pg_conn)[0][0] == 5

    # and that version does not list the attached table
    run_command("SAVEPOINT read_attached", pg_conn)
    error = run_query(count_attached, pg_conn, raise_error=False)
    assert "no table found" in str(error)
    run_command("ROLLBACK TO SAVEPOINT read_attached", pg_conn)
    pg_conn.commit()

    assert run_query(count_attached, pg_conn)[0][0] == 3
    assert run_query(count_read, pg_conn)[0][0] == 9
    pg_conn.commit()


# A rule that adds an action to a statement makes Postgres run the action
# right after the statement under the same snapshot, which it moves to a new
# command in place, and it does that at the top level, in a PL/pgSQL statement
# and in a SQL function alike.  Each place is one statement, so the action
# sees the version the statement it was added to sees.  Holding the snapshot
# itself to tell statements apart crashed assert-enabled builds in all three.
def test_rule_actions_read_one_version(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_rule_actions"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    run_command(f"CREATE TABLE {schema}.counted (n bigint)", pg_conn)
    run_command(f"CREATE TABLE {schema}.counted_log (n bigint)", pg_conn)

    # NEW in the action is the statement's SELECT, so the action reads the
    # external table a second time
    run_command(
        f"CREATE RULE log_counted AS ON INSERT TO {schema}.counted "
        f"DO ALSO INSERT INTO {schema}.counted_log VALUES (NEW.n)",
        pg_conn,
    )

    insert_count = (
        f"INSERT INTO {schema}.counted SELECT count(*) FROM {schema}.read_tbl"
    )
    run_command(
        f"CREATE FUNCTION {schema}.insert_count() RETURNS void "
        f"LANGUAGE sql AS $$ {insert_count} $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"

    statements = {
        "top level": insert_count,
        "PL/pgSQL": f"DO $$ BEGIN {insert_count}; END; $$",
        "SQL function": f"SELECT {schema}.insert_count()",
    }

    for kind, statement in statements.items():
        catalog_reads = _count_catalog_reads(pg_conn, statement)
        assert (
            catalog_reads == 1
        ), f"{kind}: expected one catalog read, got {catalog_reads}"

        for table_name in ("counted", "counted_log"):
            rows = run_query(f"SELECT n FROM {schema}.{table_name}", pg_conn)
            assert [tuple(row) for row in rows] == [(5,)], f"{kind}: {table_name}"

        run_command(f"TRUNCATE {schema}.counted, {schema}.counted_log", pg_conn)
        pg_conn.commit()


# With the extended protocol a statement starts in its Bind message and runs
# in its Execute message, so the scans it starts resolve the catalog in one
# message and the queries its functions run resolve it in the other.  Both
# belong to the one portal the two messages share, so the statement still
# reads one version.  A bound parameter is what makes psycopg send the
# statement that way.
def test_extended_protocol_reads_one_version(
    pg_conn,
    app_user,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_extended_protocol"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    run_command(
        f"CREATE FUNCTION {schema}.rows_after_wait() "
        f"RETURNS TABLE (n bigint) STABLE LANGUAGE plpgsql AS $$ BEGIN "
        f"  {WAIT_FOR_REWRITE} "
        f"  RETURN QUERY SELECT count(*) FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    join_query = (
        f"SELECT count(*), max(f.n) "
        f"FROM {schema}.read_tbl CROSS JOIN {schema}.rows_after_wait() f "
        f"WHERE f.n >= %s"
    )

    notices = []
    with psycopg.connect(default_connection_string(app_user)) as conn:
        conn.add_notice_handler(
            lambda diagnostic: notices.append(diagnostic.message_primary)
        )
        conn.execute("SET client_min_messages TO DEBUG1")
        conn.commit()

        row = conn.execute(join_query, (0,)).fetchone()
        assert tuple(row) == (5, 5)
        catalog_reads = len([n for n in notices if CATALOG_READ_MESSAGE in n])
        assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
        conn.commit()

        # the scan resolved the catalog at Bind, before the rewrite, and the
        # function reads after it at Execute
        row = _rewrite_while_waiting(
            s3,
            key,
            body_late,
            lambda: conn.execute(join_query, (0,)).fetchone(),
        )
        assert tuple(row) == (5, 5)
        conn.commit()

        assert tuple(conn.execute(join_query, (0,)).fetchone()) == (9, 9)
        conn.commit()


# A procedure that calls another one runs both inside one CALL statement, so
# both read the statement's version until one of them commits.  The commit
# ends the transaction, so what either procedure reads next it reads anew.
def test_nested_procedures_read_anew_after_a_commit(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_nested_procedures"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_seen_table(pg_conn, schema)

    def record(label):
        return (
            f"INSERT INTO {schema}.seen "
            f"  SELECT '{label}', count(*) FROM {schema}.read_tbl; "
        )

    run_command(
        f"CREATE PROCEDURE {schema}.inner_read() "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  {record('inner before wait')} "
        f"  {WAIT_FOR_REWRITE} "
        f"  {record('inner after wait')} "
        f"  COMMIT; "
        f"  {record('inner after commit')} "
        f"END; $$",
        pg_conn,
    )
    run_command(
        f"CREATE PROCEDURE {schema}.outer_read() "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  {record('outer before')} "
        f"  CALL {schema}.inner_read(); "
        f"  {record('outer after')} "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    # only a CALL outside a transaction block may commit
    pg_conn.autocommit = True
    try:
        _rewrite_while_waiting(
            s3,
            key,
            body_late,
            lambda: run_command(f"CALL {schema}.outer_read()", pg_conn),
        )
    finally:
        pg_conn.autocommit = False

    assert _seen(pg_conn, schema) == {
        "outer before": 5,
        "inner before wait": 5,
        "inner after wait": 5,
        "inner after commit": 9,
        "outer after": 9,
    }
    pg_conn.commit()


# A catch block of a procedure is a subtransaction.  The block that fails and
# the handler that catches it both read the version of their statement, the
# failure leaves that version in place, and the procedure reads anew only
# after it commits, under either isolation level.
@pytest.mark.parametrize("isolation_level", ["read committed", "repeatable read"])
def test_exception_block_in_a_procedure_keeps_the_version(
    isolation_level,
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_procedure_exception"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_seen_table(pg_conn, schema)

    # what the failing block reads leaves it in a variable, because the
    # block's own writes go with its subtransaction
    run_command(
        f"CREATE PROCEDURE {schema}.read_around_failure() "
        f"LANGUAGE plpgsql AS $$ "
        f"DECLARE in_block bigint; in_handler bigint; BEGIN "
        f"  INSERT INTO {schema}.seen "
        f"    SELECT 'before', count(*) FROM {schema}.read_tbl; "
        f"  {WAIT_FOR_REWRITE} "
        f"  BEGIN "
        f"    SELECT count(*) INTO in_block FROM {schema}.read_tbl; "
        f"    PERFORM 1 / 0; "
        f"  EXCEPTION WHEN division_by_zero THEN "
        f"    SELECT count(*) INTO in_handler FROM {schema}.read_tbl; "
        f"  END; "
        f"  INSERT INTO {schema}.seen "
        f"    VALUES ('in block', in_block), ('in handler', in_handler); "
        f"  INSERT INTO {schema}.seen "
        f"    SELECT 'after block', count(*) FROM {schema}.read_tbl; "
        f"  COMMIT; "
        f"  INSERT INTO {schema}.seen "
        f"    SELECT 'after commit', count(*) FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    pg_conn.autocommit = True
    try:
        run_command(
            f"SET default_transaction_isolation TO '{isolation_level}'", pg_conn
        )
        _rewrite_while_waiting(
            s3,
            key,
            body_late,
            lambda: run_command(f"CALL {schema}.read_around_failure()", pg_conn),
        )
    finally:
        run_command("RESET default_transaction_isolation", pg_conn)
        pg_conn.autocommit = False

    assert _seen(pg_conn, schema) == {
        "before": 5,
        "in block": 5,
        "in handler": 5,
        "after block": 5,
        "after commit": 9,
    }
    pg_conn.commit()


# Another session changing the table sends this session cache invalidations,
# which rebuild what Postgres caches about the table.  The versions a
# transaction holds are not one of those caches, so a REPEATABLE READ
# transaction keeps its version through them and does not read the catalog
# again.
def test_invalidations_keep_the_held_version(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_invalidation"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    run_command(f"CREATE TABLE {schema}.heap_tbl (a int)", pg_conn)
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    count_query = f"SELECT count(*) FROM {schema}.read_tbl"

    run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)
    assert run_query(count_query, pg_conn)[0][0] == 5

    _put_catalog(s3, key, body_late)
    run_command(f"GRANT SELECT ON {schema}.read_tbl TO PUBLIC", superuser_conn)
    superuser_conn.commit()

    # a lock this transaction does not hold yet makes it take the
    # invalidations in before the table is read again
    run_query(f"SELECT count(*) FROM {schema}.heap_tbl", pg_conn)

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, count_query)
    assert rows[0][0] == 5
    assert catalog_reads == 0, f"expected no catalog read, got {catalog_reads}"
    pg_conn.commit()

    assert run_query(count_query, pg_conn)[0][0] == 9
    pg_conn.commit()


# A PL/pgSQL FOR loop over a query opens a cursor, which is a portal of its
# own, and reads the version of the statement that called the function, the
# way the function's other queries do.  So a statement that calls such a
# function for each of its rows reads the catalog once, however the catalog
# moves between two of the calls.
def test_for_loop_reads_its_statements_version(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_for_loop_cursor"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    run_command(
        f"CREATE FUNCTION {schema}.read_around_loop("
        f"OUT before_loop bigint, OUT in_loop bigint, OUT after_loop bigint) "
        f"LANGUAGE plpgsql AS $$ DECLARE r record; BEGIN "
        f"  SELECT count(*) INTO before_loop FROM {schema}.read_tbl; "
        f"  {WAIT_FOR_REWRITE} "
        f"  FOR r IN SELECT count(*) AS n FROM {schema}.read_tbl LOOP "
        f"    in_loop := r.n; "
        f"  END LOOP; "
        f"  SELECT count(*) INTO after_loop FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    run_command(
        f"CREATE FUNCTION {schema}.loop_count_then_wait() RETURNS bigint "
        f"LANGUAGE plpgsql AS $$ DECLARE r record; n bigint; BEGIN "
        f"  FOR r IN SELECT count(*) AS c FROM {schema}.read_tbl LOOP "
        f"    n := r.c; "
        f"  END LOOP; "
        f"  {WAIT_FOR_REWRITE} "
        f"  RETURN n; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    rows = _rewrite_while_waiting(
        s3,
        key,
        body_late,
        lambda: run_query(
            f"SELECT before_loop, in_loop, after_loop "
            f"FROM {schema}.read_around_loop()",
            pg_conn,
        ),
    )
    assert tuple(rows[0]) == (5, 5, 5)
    pg_conn.commit()

    _put_catalog(s3, key, body_early)
    pg_conn.commit()
    calls_query = (
        f"SELECT array_agg({schema}.loop_count_then_wait() ORDER BY g) "
        f"FROM generate_series(1, 3) g"
    )

    rows, catalog_reads = _run_counting_catalog_reads(pg_conn, calls_query)
    assert rows[0][0] == [5, 5, 5]
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"
    pg_conn.commit()

    # the catalog moves between the first call and the second
    rows = _rewrite_while_waiting(
        s3, key, body_late, lambda: run_query(calls_query, pg_conn)
    )
    assert rows[0][0] == [5, 5, 5]
    pg_conn.commit()

    assert run_query(calls_query, pg_conn)[0][0] == [9, 9, 9]
    pg_conn.commit()


# A cursor that a function opens and returns outlives the statement that
# opened it, and keeps reading that statement's version: a later FETCH runs
# the cursor's function calls against the version the cursor's own scan
# started with, not against the catalog as it stands by then.
def test_cursor_opened_by_a_function_keeps_its_statements_version(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_opened_cursor"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_counting_function(pg_conn, schema)
    run_command(
        f"CREATE FUNCTION {schema}.open_counting(c refcursor) RETURNS refcursor "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  OPEN c FOR SELECT (SELECT count(*) FROM {schema}.read_tbl) AS scanned, "
        f"    {schema}.row_count() AS counted; "
        f"  RETURN c; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"
    run_query(f"SELECT {schema}.open_counting('opened_cursor')", pg_conn)

    _put_catalog(s3, key, body_late)
    assert run_query(f"SELECT count(*) FROM {schema}.read_tbl", pg_conn)[0][0] == 9

    rows, catalog_reads = _run_counting_catalog_reads(
        pg_conn, "FETCH ALL FROM opened_cursor"
    )
    assert [tuple(row) for row in rows] == [(5, 5)]
    assert catalog_reads == 0, f"expected no catalog read, got {catalog_reads}"
    pg_conn.commit()


# A cursor whose query calls a function that reads an external table asks the
# catalog at FETCH time, inside its own portal, so every FETCH asks the
# cursor's version scope again.
def _create_counting_function(pg_conn, schema):
    run_command(
        f"CREATE FUNCTION {schema}.row_count() RETURNS bigint VOLATILE "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  RETURN (SELECT count(*) FROM {schema}.read_tbl); "
        f"END; $$",
        pg_conn,
    )


def _declare_counting_cursor(pg_conn, schema, cursor_name):
    run_command(
        f"DECLARE {cursor_name} CURSOR FOR "
        f"SELECT g, {schema}.row_count() FROM generate_series(1, 3) g",
        pg_conn,
    )


# An open cursor and the statements after it are version scopes held side by
# side, each with its own version of the catalog.  A statement that read a
# newer version and ended takes only its own answers with it, so the cursor
# keeps answering with the version it read first.
def test_cursor_keeps_its_version_while_statements_come_and_go(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_cursor_scope"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_counting_function(pg_conn, schema)
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"
    _declare_counting_cursor(pg_conn, schema, "counting_cursor")

    # the first FETCH is the first time the cursor's scope asks
    rows, catalog_reads = _run_counting_catalog_reads(
        pg_conn, "FETCH 1 FROM counting_cursor"
    )
    assert rows[0][1] == 5
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"

    _put_catalog(s3, key, body_late)

    # a statement in between is a scope of its own and sees the rewrite
    rows, catalog_reads = _run_counting_catalog_reads(
        pg_conn, f"SELECT count(*) FROM {schema}.read_tbl"
    )
    assert rows[0][0] == 9
    assert catalog_reads == 1, f"expected one catalog read, got {catalog_reads}"

    # its end left the cursor's answers in place
    rows, catalog_reads = _run_counting_catalog_reads(
        pg_conn, "FETCH 1 FROM counting_cursor"
    )
    assert rows[0][1] == 5
    assert catalog_reads == 0, f"expected no catalog read, got {catalog_reads}"
    pg_conn.commit()


# Two cursors open at once are two version scopes.  Closing one ends its
# scope and drops its answers, and only those: the other cursor keeps reading
# the version it opened with.
def test_closing_a_cursor_drops_only_its_answers(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_two_cursors"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )
    _create_counting_function(pg_conn, schema)
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"

    _declare_counting_cursor(pg_conn, schema, "early_cursor")
    assert run_query("FETCH 1 FROM early_cursor", pg_conn)[0][1] == 5

    _put_catalog(s3, key, body_late)
    _declare_counting_cursor(pg_conn, schema, "late_cursor")
    assert run_query("FETCH 1 FROM late_cursor", pg_conn)[0][1] == 9

    # with the catalog moved back, a cursor that asked again would show it
    _put_catalog(s3, key, body_early)
    run_command("CLOSE early_cursor", pg_conn)

    rows, catalog_reads = _run_counting_catalog_reads(
        pg_conn, "FETCH 1 FROM late_cursor"
    )
    assert rows[0][1] == 9
    assert catalog_reads == 0, f"expected no catalog read, got {catalog_reads}"
    pg_conn.commit()
