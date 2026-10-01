import pytest
from utils_pytest import *


def test_recovery(superuser_conn, s3, extension):
    # Currently this only checks whether we can call the function, since it does nothing
    run_command("call lake_table.finish_postgres_recovery()", superuser_conn)
    # superuser_conn is module-scoped and shared with the tests below. Don't
    # leave it in an open transaction, or a later test's `autocommit = True`
    # raises "set_session cannot be used inside a transaction".
    superuser_conn.rollback()


def test_recovery_ignores_spoofed_procedure(s3, extension, superuser_conn):
    """
    lake_table.finish_postgres_recovery() runs in every connectable database,
    including the ones where pg_lake_table was never installed, so it must only
    call lake_table.finish_postgres_recovery_in_db() where that procedure is a
    genuine member of the extension. One that merely has the same schema and
    name has to be left alone.

    This creates such a procedure in a database owned by an unprivileged role and
    checks that recovery does not call it.
    """
    attacker_role = "spoof_recovery_attacker"
    attacker_db = "spoof_recovery_db"

    # An unprivileged role owning its own database, into which pg_lake_table is
    # deliberately NOT installed.
    # Roll back first so autocommit can be toggled even if a previous test left
    # this module-scoped connection inside an open transaction.
    superuser_conn.rollback()
    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE IF EXISTS {attacker_db} WITH (FORCE)", superuser_conn)
    run_command(f"DROP ROLE IF EXISTS {attacker_role}", superuser_conn)
    run_command(f"CREATE ROLE {attacker_role} LOGIN", superuser_conn)
    run_command(f"CREATE DATABASE {attacker_db} OWNER {attacker_role}", superuser_conn)
    superuser_conn.autocommit = False

    attacker_conn = None
    try:
        # Create the look-alike procedure as the database owner, via SET ROLE,
        # to confirm none of it needs an elevated privilege.
        attacker_conn = open_pg_conn_to_db(attacker_db)
        run_command(
            f"""
            SET ROLE {attacker_role};

            CREATE SCHEMA lake_table;

            CREATE TABLE lake_table.spoof_evidence (ran_by text);

            CREATE PROCEDURE lake_table.finish_postgres_recovery_in_db()
            LANGUAGE plpgsql
            AS $spoof$
            BEGIN
                -- Record every execution; this body must never run.
                INSERT INTO lake_table.spoof_evidence VALUES (current_user);

                -- Try something only a superuser can do, so the rolsuper
                -- assertion below is not vacuous. Swallow failures so the row
                -- above is still committed either way.
                BEGIN
                    ALTER ROLE {attacker_role} SUPERUSER;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
            END;
            $spoof$;

            RESET ROLE;
            """,
            attacker_conn,
        )
        attacker_conn.commit()

        # Read side effects with fresh snapshots so the background worker's
        # independently committed writes are visible.
        attacker_conn.autocommit = True

        # Precondition: pg_lake_table must be absent in the attacker database.
        installed = run_query(
            "SELECT 1 FROM pg_extension WHERE extname = 'pg_lake_table'", attacker_conn
        )
        assert installed == [], "pg_lake_table must not be installed in attacker db"

        # Recovery is run from a database where the extension IS installed.
        run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
        superuser_conn.commit()

        # The look-alike procedure must not have run,
        evidence = run_query(
            "SELECT ran_by FROM lake_table.spoof_evidence", attacker_conn
        )
        assert evidence == [], f"the look-alike procedure was called: {evidence!r}"

        # ... and the role must not have gained superuser.
        is_super = run_query(
            f"SELECT rolsuper FROM pg_roles WHERE rolname = '{attacker_role}'",
            superuser_conn,
        )
        escalated = bool(is_super) and is_super[0][0]
        assert not escalated, "recovery granted the role SUPERUSER"
    finally:
        if attacker_conn is not None:
            attacker_conn.close()
        # End any open transaction so autocommit can be toggled, then clean up.
        superuser_conn.rollback()
        superuser_conn.autocommit = True
        run_command(
            f"DROP DATABASE IF EXISTS {attacker_db} WITH (FORCE)", superuser_conn
        )
        run_command(f"DROP ROLE IF EXISTS {attacker_role}", superuser_conn)
        superuser_conn.autocommit = False


def _internal_iceberg_read_only(table, conn):
    """Read the read_only flag lake_table.finish_postgres_recovery_in_db() sets."""
    rows = run_query(
        "SELECT read_only FROM lake_iceberg.tables_internal "
        f"WHERE table_name = '{table}'::regclass",
        conn,
    )
    assert len(rows) == 1, f"expected one catalog row for {table}, got {rows!r}"
    return rows[0][0]


def test_recovery_marks_internal_iceberg_tables_read_only(
    s3, superuser_conn, extension, with_default_location
):
    """
    Positive counterpart to test_recovery_ignores_spoofed_procedure: the genuine
    procedure must still be called.

    Nothing else checks that.  test_recovery() only asserts the call does not
    raise, and the two look-alike tests only assert that a planted procedure was
    skipped.  So if the extension-membership check ever stops matching the real
    lake_table.finish_postgres_recovery_in_db() -- a typo in the extension name,
    a cast that fails in the target database, an install or upgrade path without
    the pg_depend 'e' row -- recovery turns into a cluster-wide no-op with every
    other test still green.  Recovery marks internal Iceberg tables read-only
    after a PostgreSQL restore, so skipping it risks writes to tables whose
    metadata no longer matches the object store.

    finish_postgres_recovery_in_db() sets
    lake_iceberg.tables_internal.read_only, so that flag is the observable proof
    that it ran.
    """
    table = "recovery_positive_marker"

    # The recovery body issues an unqualified
    #     UPDATE lake_iceberg.tables_internal SET read_only = true
    # with no WHERE clause, so it flips *every* internal Iceberg table in this
    # database -- not just ours.  Snapshot the current state and restore it in
    # the finally block so we don't leak a read-only catalog into later tests
    # that share this database.
    already_read_only = [
        row[0]
        for row in run_query(
            "SELECT table_name::text FROM lake_iceberg.tables_internal "
            "WHERE read_only",
            superuser_conn,
        )
    ]
    superuser_conn.commit()

    try:
        run_command(f"CREATE TABLE {table} (x int) USING iceberg", superuser_conn)

        # Commit BEFORE invoking recovery.  finish_postgres_recovery() fans out
        # via extension_base.run_attached(), which starts a background worker
        # that runs in its own transaction and commits independently, while the
        # calling backend blocks until that worker finishes.  So an uncommitted
        # CREATE TABLE would be invisible to the worker, and any lock we still
        # held on lake_iceberg.tables_internal would deadlock the worker against
        # the very backend that is waiting for it.
        superuser_conn.commit()

        assert not _internal_iceberg_read_only(table, superuser_conn), (
            "precondition: a freshly created internal Iceberg table must not "
            "already be read-only"
        )

        run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
        superuser_conn.commit()

        assert _internal_iceberg_read_only(table, superuser_conn), (
            "recovery did not mark the internal Iceberg table read-only, so "
            "lake_table.finish_postgres_recovery_in_db() never ran: the "
            "extension-membership check is rejecting the genuine procedure and "
            "recovery has become a no-op"
        )
    finally:
        superuser_conn.rollback()
        # read_only blocks modifications, so clear it before dropping, then
        # restore exactly the rows that were read-only before this test ran.
        run_command(
            "UPDATE lake_iceberg.tables_internal SET read_only = false",
            superuser_conn,
        )
        run_command(f"DROP TABLE IF EXISTS {table}", superuser_conn)
        if already_read_only:
            restore = ", ".join(
                "'" + name.replace("'", "''") + "'" for name in already_read_only
            )
            run_command(
                "UPDATE lake_iceberg.tables_internal SET read_only = true "
                f"WHERE table_name::text IN ({restore})",
                superuser_conn,
            )
        superuser_conn.commit()


def test_recovery_ignores_forged_extension_membership(s3, extension, superuser_conn):
    """
    Covers the pg_catalog qualification in the recovery command, which the
    membership check depends on as much as it does on the pg_depend join.

    The command is parsed and executed in the target database, and its owner can
    create their own views named pg_proc / pg_depend / pg_extension and set a
    database-level search_path that the attached worker inherits:
        ALTER DATABASE <theirs> SET search_path = evil, pg_catalog, public
    An unqualified membership check then reads those views instead of the real
    catalog, and they can report whatever their owner put in them.

    This builds that setup, first checking that it does fool an unqualified
    version of the check, so the test cannot pass merely because the views were
    wrong, and then that recovery still declines to call the look-alike
    procedure.  It fails if the pg_catalog qualification is ever removed from
    pg_lake_finish_postgres_recovery() as redundant noise.
    """
    attacker_role = "forge_recovery_attacker"
    attacker_db = "forge_recovery_db"

    superuser_conn.rollback()
    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE IF EXISTS {attacker_db} WITH (FORCE)", superuser_conn)
    run_command(f"DROP ROLE IF EXISTS {attacker_role}", superuser_conn)
    run_command(f"CREATE ROLE {attacker_role} LOGIN", superuser_conn)
    run_command(f"CREATE DATABASE {attacker_db} OWNER {attacker_role}", superuser_conn)
    superuser_conn.autocommit = False

    attacker_conn = None
    probe_conn = None
    try:
        attacker_conn = open_pg_conn_to_db(attacker_db)

        # Everything below runs as the unprivileged database owner via SET ROLE,
        # which is all that creating the procedure, the views and the
        # database-level search_path needs.
        run_command(
            f"""
            SET ROLE {attacker_role};

            CREATE SCHEMA lake_table;
            CREATE TABLE lake_table.spoof_evidence (ran_by text);
            CREATE PROCEDURE lake_table.finish_postgres_recovery_in_db()
            LANGUAGE plpgsql
            AS $spoof$
            BEGIN
                -- Record every execution; this body must never run.
                INSERT INTO lake_table.spoof_evidence VALUES (current_user);

                -- Try something only a superuser can do, matching the sibling
                -- test, so the rolsuper assertion below is not vacuous.
                -- Swallow failures so the row above is still committed either
                -- way.
                BEGIN
                    ALTER ROLE {attacker_role} SUPERUSER;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
            END;
            $spoof$;

            CREATE SCHEMA evil;

            -- Stand in for the three catalogs the membership check consults.
            -- The views have to be self-consistent: evil.pg_depend points
            -- classid / refclassid at the evil views, which is what unqualified
            -- 'pg_proc'::regclass and 'pg_extension'::regclass resolve to once
            -- evil precedes pg_catalog on the search_path.  Creation order
            -- matters, since pg_depend references the other two by regclass.
            CREATE VIEW evil.pg_extension AS
                SELECT 99999::oid AS oid, 'pg_lake_table'::text AS extname;

            CREATE VIEW evil.pg_proc AS
                SELECT p.oid, p.proname, p.pronamespace
                FROM pg_catalog.pg_proc p
                WHERE p.pronamespace = 'lake_table'::regnamespace;

            CREATE VIEW evil.pg_depend AS
                SELECT p.oid                          AS objid,
                       'evil.pg_proc'::regclass       AS classid,
                       'evil.pg_extension'::regclass  AS refclassid,
                       99999::oid                     AS refobjid,
                       'e'::"char"                    AS deptype
                FROM pg_catalog.pg_proc p
                WHERE p.pronamespace = 'lake_table'::regnamespace
                  AND p.proname = 'finish_postgres_recovery_in_db';

            -- An always-true equality operator, to catch any comparison left
            -- unqualified.
            CREATE FUNCTION evil.always_eq(anyelement, anyelement) RETURNS boolean
                LANGUAGE sql IMMUTABLE AS $f$ SELECT true $f$;
            CREATE OPERATOR evil.= (
                LEFTARG = anyelement, RIGHTARG = anyelement,
                FUNCTION = evil.always_eq
            );

            ALTER DATABASE {attacker_db}
                SET search_path = evil, pg_catalog, public;

            RESET ROLE;
            """,
            attacker_conn,
        )
        attacker_conn.commit()

        # A database-level search_path only applies to sessions opened after it
        # is set, so check it on a fresh connection, the way the recovery worker
        # connects.
        probe_conn = open_pg_conn_to_db(attacker_db)
        probe_conn.autocommit = True

        inherited = run_query("SELECT current_setting('search_path')", probe_conn)[0][0]
        assert inherited.startswith("evil"), (
            f"test setup failed: new sessions do not inherit the database-level "
            f"search_path (got {inherited!r})"
        )

        # Precondition, deliberately pg_catalog-qualified: an unqualified
        # 'FROM pg_extension' here would read the view above and report
        # pg_lake_table as installed.
        genuinely_installed = run_query(
            "SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_lake_table'",
            probe_conn,
        )
        assert (
            genuinely_installed == []
        ), "pg_lake_table must not really be installed in the attacker database"

        # Check the views are effective rather than inert: the same pg_depend
        # join WITHOUT pg_catalog qualification has to be fooled into returning
        # true.  Without this the test could pass because they were broken.
        fooled = run_query(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_proc p
                JOIN pg_depend d
                  ON d.classid = 'pg_proc'::regclass
                 AND d.objid = p.oid
                 AND d.refclassid = 'pg_extension'::regclass
                 AND d.deptype = 'e'
                JOIN pg_extension e ON e.oid = d.refobjid
                WHERE p.pronamespace::regnamespace::text = 'lake_table'
                  AND p.proname = 'finish_postgres_recovery_in_db'
                  AND e.extname = 'pg_lake_table'
            )
            """,
            probe_conn,
        )[0][0]
        assert fooled, (
            "test setup failed: the views did not fool an unqualified membership "
            "check, so this test would not exercise the pg_catalog qualification"
        )

        # Recovery is run from a database where the extension IS installed.
        run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
        superuser_conn.commit()

        evidence = run_query("SELECT ran_by FROM lake_table.spoof_evidence", probe_conn)
        assert evidence == [], (
            "recovery called the look-alike procedure even though it is not an "
            f"extension member: {evidence!r}"
        )

        is_super = run_query(
            f"SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = '{attacker_role}'",
            superuser_conn,
        )
        assert not (
            bool(is_super) and is_super[0][0]
        ), "recovery granted the role SUPERUSER"
    finally:
        for conn in (attacker_conn, probe_conn):
            if conn is not None:
                conn.close()
        superuser_conn.rollback()
        superuser_conn.autocommit = True
        run_command(
            f"DROP DATABASE IF EXISTS {attacker_db} WITH (FORCE)", superuser_conn
        )
        run_command(f"DROP ROLE IF EXISTS {attacker_role}", superuser_conn)
        superuser_conn.autocommit = False


def test_recovery_clears_deletion_queue(
    s3, superuser_conn, extension, with_default_location
):
    """
    Recovery forgets the queued deletions without deleting the files.

    The rows were queued before the restore, so the files they name belong to
    the instance this one was restored from. Left in place, VACUUM here could
    delete them, for example the rows of a read-only table once that table is
    dropped. This queues a row for a live table and one for a dropped table and
    checks that both rows are gone afterwards while the files are still there.
    """
    table = "recovery_queue_marker"
    prefix = f"s3://{TEST_BUCKET}/test_recovery_clears_deletion_queue"
    paths = [f"{prefix}/live.parquet", f"{prefix}/dropped.parquet"]

    for path in paths:
        bucket, key = parse_s3_path(path)
        s3.put_object(Bucket=bucket, Key=key, Body=b"x")

    already_read_only = [
        row[0]
        for row in run_query(
            "SELECT table_name::text FROM lake_iceberg.tables_internal "
            "WHERE read_only",
            superuser_conn,
        )
    ]
    superuser_conn.commit()

    try:
        run_command(f"CREATE TABLE {table} (x int) USING iceberg", superuser_conn)

        # orphaned in the future, so autovacuum leaves them alone until recovery
        run_command(
            "INSERT INTO lake_engine.deletion_queue (path, table_name, orphaned_at) "
            f"VALUES ('{paths[0]}', '{table}'::regclass, now() + interval '1 day'), "
            f"('{paths[1]}', 0, now() + interval '1 day')",
            superuser_conn,
        )

        # commit first, see test_recovery_marks_internal_iceberg_tables_read_only
        superuser_conn.commit()

        run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
        superuser_conn.commit()

        queued = run_query(
            "SELECT path FROM lake_engine.deletion_queue "
            f"WHERE path LIKE '{prefix}/%'",
            superuser_conn,
        )
        assert queued == [], f"recovery left queued deletions behind: {queued!r}"

        remaining = run_query(
            f"SELECT count(*) FROM lake_file.list('{prefix}/*')", superuser_conn
        )
        assert remaining[0][0] == len(paths), "recovery deleted queued files"
        superuser_conn.commit()
    finally:
        superuser_conn.rollback()
        run_command(
            f"DELETE FROM lake_engine.deletion_queue WHERE path LIKE '{prefix}/%'",
            superuser_conn,
        )
        run_command(
            "UPDATE lake_iceberg.tables_internal SET read_only = false",
            superuser_conn,
        )
        run_command(f"DROP TABLE IF EXISTS {table}", superuser_conn)
        if already_read_only:
            restore = ", ".join(
                "'" + name.replace("'", "''") + "'" for name in already_read_only
            )
            run_command(
                "UPDATE lake_iceberg.tables_internal SET read_only = true "
                f"WHERE table_name::text IN ({restore})",
                superuser_conn,
            )
        superuser_conn.commit()
