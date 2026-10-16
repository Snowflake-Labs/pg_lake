from utils_pytest import *
import json
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
#     level and however many tables and references it has;
#   * READ COMMITTED gives the next statement of the same transaction the
#     catalog as it stands by then;
#   * REPEATABLE READ and SERIALIZABLE hold the whole transaction to the
#     version it started reading with, including for a table it touches for
#     the first time in a later statement;
#   * whatever a transaction pinned is released when that transaction ends,
#     by commit, by abort, or by PREPARE TRANSACTION.
#
# Two limits the implementation accepts, stated here as behaviour rather than
# wished away:
#
#   * the version is pinned when a transaction first reads an external table,
#     not when it took its snapshot;
#   * the statements inside a function body are one statement to the pin, so
#     READ COMMITTED holds one version of the catalog across them.
#
# The tests that race a rewriting thread against a running query are the only
# way to cover two reads inside one statement.  The rest rewrite catalog.json
# between statements, where each isolation level has exactly one right answer
# and no timing is involved.


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

            wait_until_object_store_writable_table_pushed(pg_conn, schema, source_name)
            locations = early_locations if high == early_rows else late_locations
            locations[source_name] = _current_metadata_location(
                pg_conn, schema, source_name
            )

        assert early_locations[source_name] != late_locations[source_name]

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


# Two references to the same table in one query must report the same count.
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

    _create_uncompacted_writable_table(pg_conn, f"{schema}.wrt_tbl", "a INT")

    early_rows, extra_rows = 24, 12
    _insert_in_separate_commits(
        pg_conn, f"{schema}.wrt_tbl", [f"({n})" for n in range(early_rows)]
    )
    wait_until_object_store_writable_table_pushed(pg_conn, schema, "wrt_tbl")
    early_location = _current_metadata_location(pg_conn, schema, "wrt_tbl")

    _insert_in_separate_commits(
        pg_conn,
        f"{schema}.wrt_tbl",
        [f"({n})" for n in range(early_rows, early_rows + extra_rows)],
    )
    wait_until_object_store_writable_table_pushed(pg_conn, schema, "wrt_tbl")
    late_location = _current_metadata_location(pg_conn, schema, "wrt_tbl")
    assert early_location != late_location

    key, body_early, body_late = _reader_catalog_versions(
        pg_conn,
        s3,
        reader_catalog_prefix,
        {"wrt_tbl": early_location},
        {"wrt_tbl": late_location},
    )

    run_command(
        f"CREATE TABLE {schema}.read_tbl(a INT) USING iceberg "
        f"WITH (catalog='object_store', read_only=True, catalog_table_name='wrt_tbl')",
        pg_conn,
    )
    pg_conn.commit()

    # Precondition: both snapshots are independently readable and differ, so a
    # count mismatch below really does mean two snapshots in one query.
    single_query = f"SELECT count(*) FROM {schema}.read_tbl"
    for body, expected in (
        (body_early, early_rows),
        (body_late, early_rows + extra_rows),
    ):
        _put_catalog(s3, key, body)
        observed = run_query(single_query, pg_conn)[0][0]
        pg_conn.commit()
        assert observed == expected, f"expected {expected} rows, got {observed}"

    doubled_query = (
        f"SELECT (SELECT count(*) FROM {schema}.read_tbl) AS first_reference, "
        f"       (SELECT count(*) FROM {schema}.read_tbl) AS second_reference"
    )

    flipper = CatalogFlipper(s3, key, body_early, body_late)
    flipper.start()
    mismatches = []
    attempts = 0
    try:
        for attempts in range(1, 201):
            rows = run_query(doubled_query, pg_conn, raise_error=False)
            if isinstance(rows, str):
                pg_conn.rollback()
                continue
            pg_conn.commit()
            first_reference, second_reference = rows[0][0], rows[0][1]
            if first_reference != second_reference:
                mismatches.append((attempts, first_reference, second_reference))
                break
    finally:
        flipper.stop()

    print(
        f"\ndouble-reference count check: {attempts} queries, "
        f"{flipper.flips} catalog.json rewrites, {len(mismatches)} mismatches"
    )
    for attempt, first_reference, second_reference in mismatches:
        print(
            f"  query #{attempt}: first_reference={first_reference} "
            f"second_reference={second_reference}"
        )

    assert not mismatches, (
        "two references to one read-only object_store table in a single query "
        "returned different row counts, so each resolved its own Iceberg "
        f"snapshot: {mismatches}"
    )


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

    flipper = CatalogFlipper(s3, key, body_early, body_late)
    flipper.start()
    wrong_results = []
    attempts = 0
    try:
        for attempts in range(1, 201):
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
# of that catalog, the same way two references to one table must.
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

    early_rows, extra_rows = 18, 12
    key, body_early, body_late = _two_catalog_versions(
        pg_conn,
        s3,
        schema,
        ["read_one", "read_two"],
        early_rows,
        extra_rows,
        reader_catalog_prefix,
        wide_window=True,
    )

    paired_query = (
        f"SELECT (SELECT count(*) FROM {schema}.read_one) AS first_table, "
        f"       (SELECT count(*) FROM {schema}.read_two) AS second_table"
    )

    # Precondition: both versions are readable on their own, and inside each
    # one the two tables agree, so a disagreement below means the query read
    # two versions.
    for body, expected in (
        (body_early, early_rows),
        (body_late, early_rows + extra_rows),
    ):
        _put_catalog(s3, key, body)
        rows = run_query(paired_query, pg_conn)
        pg_conn.commit()
        assert tuple(rows[0]) == (expected, expected)

    flipper = CatalogFlipper(s3, key, body_early, body_late)
    flipper.start()
    mismatches = []
    attempts = 0
    try:
        for attempts in range(1, 201):
            rows = run_query(paired_query, pg_conn, raise_error=False)
            if isinstance(rows, str):
                pg_conn.rollback()
                continue
            pg_conn.commit()
            first_table, second_table = rows[0][0], rows[0][1]
            if first_table != second_table:
                mismatches.append((attempts, first_table, second_table))
                break
    finally:
        flipper.stop()

    print(
        f"\ntwo-table count check: {attempts} queries, {flipper.flips} "
        f"catalog.json rewrites, {len(mismatches)} mismatches"
    )
    for attempt, first_table, second_table in mismatches:
        print(
            f"  query #{attempt}: first_table={first_table} "
            f"second_table={second_table}"
        )

    assert not mismatches, (
        "two tables of one object_store catalog read in a single query "
        "returned row counts from different versions of that catalog: "
        f"{mismatches}"
    )


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
# leaves it in place.
def test_savepoint_rollback_keeps_pinned_catalog(
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

    run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)

    run_command("SAVEPOINT resolve_here", pg_conn)
    assert run_query(count_query, pg_conn)[0][0] == 5
    run_command("ROLLBACK TO SAVEPOINT resolve_here", pg_conn)

    _put_catalog(s3, key, body_late)

    assert run_query(count_query, pg_conn)[0][0] == 5

    # the same through a subtransaction that aborts on an error rather than
    # one that is rolled back cleanly
    run_command("SAVEPOINT fail_here", pg_conn)
    run_query("SELECT 1 / 0", pg_conn, raise_error=False)
    run_command("ROLLBACK TO SAVEPOINT fail_here", pg_conn)

    assert run_query(count_query, pg_conn)[0][0] == 5
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


# Commands are told apart by their statement start timestamp, which does not
# advance for the statements inside a function body, so READ COMMITTED holds
# one version of the catalog across them.  A loop polling an external table
# for new commits from inside a function never sees one, where the same loop
# over a heap table does.  Holding longer cannot tear a read, so this is the
# direction to err in, and the test states the behaviour.
def test_function_body_holds_one_catalog_version(
    pg_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
    reader_catalog_prefix,
):
    schema = "test_osc_function_body"
    _reset_schema(pg_conn, schema)
    key, body_early, body_late = _two_catalog_versions(
        pg_conn, s3, schema, ["read_tbl"], 5, 4, reader_catalog_prefix
    )

    run_command(
        f"CREATE FUNCTION {schema}.read_twice(sleep_seconds float8, "
        f"OUT first_read bigint, OUT second_read bigint) "
        f"LANGUAGE plpgsql AS $$ BEGIN "
        f"  SELECT count(*) INTO first_read FROM {schema}.read_tbl; "
        f"  PERFORM pg_sleep(sleep_seconds); "
        f"  SELECT count(*) INTO second_read FROM {schema}.read_tbl; "
        f"END; $$",
        pg_conn,
    )
    _put_catalog(s3, key, body_early)
    pg_conn.commit()

    assert run_query("SHOW transaction_isolation", pg_conn)[0][0] == "read committed"

    rewritten_at = []

    def rewrite_catalog():
        _put_catalog(s3, key, body_late)
        rewritten_at.append(time.monotonic())

    sleep_seconds = 4.0
    rewrite_timer = threading.Timer(1.0, rewrite_catalog)
    rewrite_timer.start()
    try:
        rows = run_query(
            f"SELECT first_read, second_read "
            f"FROM {schema}.read_twice({sleep_seconds})",
            pg_conn,
        )
    finally:
        rewrite_timer.join(timeout=30)
    returned_at = time.monotonic()

    # the rewrite landed long before the second read in the function body, so
    # the two reads agreeing below is the pin and not a race the test lost
    assert rewritten_at, "the rewriting thread did not run"
    assert returned_at - rewritten_at[0] > sleep_seconds / 2

    assert rows[0][0] == 5
    assert rows[0][1] == 5

    # the next statement of the same transaction is a command of its own and
    # is entitled to the rewrite
    assert run_query(f"SELECT count(*) FROM {schema}.read_tbl", pg_conn)[0][0] == 9
    pg_conn.commit()
