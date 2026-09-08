# Architecture

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a scheme is added to `IsSupportedURL`; pgduck_server gains a listener that is not a unix socket, or gains an authentication step; a new extension directory appears at the top level; the set of built-in Iceberg catalog servers changes, or `pg_lake_iceberg.default_catalog` stops defaulting to `postgres`; a lake role starts being checked on a read or write of an existing table; `pg_lake_iceberg.default_location_prefix` changes GUC context; a new background worker or a new path that reaches object storage outside a user query is added; or the two processes stop sharing the PostgreSQL temp-file directory.

pg_lake is a set of PostgreSQL extensions plus one separate process. Reads and
writes of lake tables are planned in PostgreSQL and executed by DuckDB in that
other process, and the two halves are joined by a unix socket and a shared
temp-file directory. Almost every security question about pg_lake is a question
about one of those two joins, or about which URLs a SQL user is allowed to name.

## The extensions

The top-level directories are the extensions: `pg_lake` (an umbrella that
requires the others and exposes `lake.version()`), `pg_lake_table`,
`pg_lake_copy`, `pg_lake_iceberg`, `pg_lake_engine`, `pg_map`,
`pg_lake_spatial`, `pg_lake_benchmark`, `pg_extension_base` and
`pg_extension_updater`. They are versioned together; the current
`default_version` is 3.5 in every control file.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake/pg_lake.control:1-8` @ 031d6f58798d (2026-09-03)

None of them is marked `trusted`, so `CREATE EXTENSION` requires a superuser.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake/pg_lake.control` (no `trusted` in any `.control` file) @ 031d6f58798d (2026-09-03)

`pg_extension_base` is the library that goes in `shared_preload_libraries`. It
refuses to load any other way, and at postmaster start it reads the control
files of installed extensions and loads the libraries named on their
`#!shared_preload_libraries` comment line. `pg_lake.control` carries such a
line, which is how the `pg_lake` library ends up preloaded without being named
in `postgresql.conf`.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_extension_base/src/pg_extension_base.c:53-68`, `pg_extension_base/src/library_preloader.c:20-21,233-236`, `pg_lake/pg_lake.control:8` @ 031d6f58798d (2026-09-03)

That mechanism is a loading decision, not a privilege one: what a preloaded
library may do is decided by the hooks it installs and the role checks it makes,
which is the subject of `trust-boundaries.md`.

## pgduck_server

`pgduck_server` is a standalone multi-threaded process that speaks the
PostgreSQL v3 wire protocol and executes statements against an embedded DuckDB
with the `duckdb_pglake` extension. It listens on a unix socket only, on
`/tmp` and port 5332 by default, with the socket file chmod'ed to 0770 and no
group owner unless one is given.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/command_line/command_line.c:45-50`, `pgduck_server/src/pgserver/pgserver.c:67,116,212,222,278,315` @ 031d6f58798d (2026-09-03)

PostgreSQL backends connect to it with libpq, using the conninfo string in the
`pg_lake_engine.host` GUC. That GUC is `PGC_POSTMASTER` and carries
`GUC_NO_SHOW_ALL`, so it is fixed at postmaster start and does not appear in
`SHOW ALL`.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/init.c:82-90`, `pg_lake_engine/include/pg_lake/pgduck/client.h:25`, `pg_lake_engine/src/pgduck/client.c:57,100-200` @ 031d6f58798d (2026-09-03)

**One server serves the whole cluster.** Every database and every user in the
PostgreSQL instance is served by the same pgduck_server process, so DuckDB's
secrets, settings and file cache are shared state rather than per-session
state. The code says so where it matters: the name of a vended secret includes
`MyDatabaseId` specifically because the namespace is shared.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:53-74` @ 031d6f58798d (2026-09-03)

DuckDB state that is not per-session is the origin of most of `threat-model.md`.

## The two joins between the halves

**The socket.** Statements pg_lake builds as text travel to pgduck_server over
the unix socket. There is no authentication on that channel at all: see
`trust-boundaries.md`, boundary 3.

**The temp-file directory.** Data that cannot stream through the protocol goes
through a file both processes can see. pg_lake generates those paths under the
PostgreSQL temp-file directory, `$PGDATA/base/pgsql_tmp`, and registers a memory
context callback to unlink them.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/storage/local_storage.c:35-80` @ 031d6f58798d (2026-09-03)

In the documented container topology that directory is a shared volume mounted
into both containers, and pgduck_server has full filesystem access to it.

**Basis:** code-verified Snowflake-Labs/pg_lake `docker/docker-compose.yml:8-12,33-38` @ 031d6f58798d (2026-09-03)

## Storage

Lake tables live in object storage. The schemes pg_lake accepts are fixed by
`IsSupportedURL`: `s3://`, `gs://`, the three Azure forms, `http://`,
`https://`, Hugging Face and `r2://`. There is no `file://` scheme.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/copy/copy_format.c:420-442` @ 031d6f58798d (2026-09-03)

**Bounded by:** the absence of `file://` does not mean pg_lake never touches the
local filesystem. `COPY` accepts a bare local path, and the check that gates it
is a replica of a core PostgreSQL check rather than a scheme rule; see
`trust-boundaries.md`, boundary 2.

Iceberg tables are addressed through one of three built-in foreign servers on
the `iceberg_catalog` FDW, created by the extension upgrade script and named
`pg_lake_postgres_catalog` (the catalog is PostgreSQL itself),
`pg_lake_object_store_catalog` (the catalog is a file in the object store) and
`pg_lake_rest_catalog` (an Iceberg REST catalog). `USAGE` on each is granted to
`lake_write`.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/pg_lake_iceberg--3.3--3.4.sql:85-99` @ 031d6f58798d (2026-09-03)

**PostgreSQL is the default catalog.** `pg_lake_iceberg.default_catalog` defaults
to `postgres`, so a `CREATE TABLE ... USING iceberg` that names no catalog gets
the PostgreSQL one. The pointer to each table's current `metadata.json` lives in
`lake_iceberg.tables_internal`, and `pg_catalog.iceberg_tables` presents it in
the shape an Iceberg JDBC catalog client expects. No external catalog service is
in the path, and no credential is needed to reach the catalog, because reaching
it is a local table read.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/init.c:204-214`, `pg_lake_iceberg/pg_lake_iceberg--3.0.sql:24-72` @ 031d6f58798d (2026-09-03)

**Bounded by:** `pg_catalog.iceberg_tables` has `SELECT` granted to `public`, so
any user in the database can read the storage location of every Iceberg table in
it, including tables they hold no privilege on. The path is not the data, but a
user who also holds `lake_read` can then read the files at that path directly and
so bypass the table's own `GRANT`s. That is the reach of `lake_read` rather than a
property of the view, but the view is what makes it easy to aim.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/pg_lake_iceberg--3.0.sql:69-72`, `pg_lake_iceberg/pg_lake_iceberg--3.2--3.3.sql:9-11` @ 031d6f58798d (2026-09-03)

Those three server names are reserved: the extension refuses to let anyone else
create or alter a server under them. The reason is a credential one and is
covered in `data-and-secrets.md`.

## Paths that reach storage outside a user query

Three things touch object storage or the local cache without a user statement
driving them, which makes them worth naming separately.

**The cache manager worker.** A background worker started from `_PG_init`,
connected to shared catalogs only, which calls into the cache-management
function roughly once a minute and trims the file cache to
`pg_lake_engine.max_cache_size`. It restarts after 5 seconds if it dies.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/cache_worker.c:81-95,100-176`, `pg_lake_engine/src/init.c:92-125,258` @ 031d6f58798d (2026-09-03)

**The deletion queue.** Files that a table no longer references are recorded in
`lake_engine.deletion_queue` and deleted later by vacuum, bounded by
`pg_lake_engine.vacuum_file_remove_max_retries`,
`vacuum_file_remove_retry_interval` and `orphaned_file_retention_period`. The
queue is `SELECT`-able by `lake_write`, and `lake_engine.flush_deletion_queue`
is revoked from public and granted to `lake_write`.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/pg_lake_engine--3.0.sql:112-127`, `pg_lake_engine/src/init.c:152-202` @ 031d6f58798d (2026-09-03)

**The deferred-drop resolver.** With `pg_lake_table.defer_drop_file_cleanup`, a
dropped Iceberg table leaves its `metadata.json` on the queue and vacuum later
calls `lake_iceberg.find_all_referenced_files` over SPI to enumerate what to
delete. That function walks an arbitrary object-store path with the server's
credentials, and the upgrade script revokes it from public for exactly that
reason; vacuum reaches it as the extension owner instead.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/pg_lake_iceberg--3.3--3.4.sql:104-113`, `pg_lake_table/src/init.c:353-363` @ 031d6f58798d (2026-09-03)

## The permission model

`pg_lake_engine`'s install script creates three roles: `lake_read`,
`lake_write`, and `lake_read_write`, which holds the other two. They are group
roles created with a bare `CREATE ROLE`, so nobody logs in as them.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/pg_lake_engine--3.0.sql:1-20` @ 031d6f58798d (2026-09-03)

The point that decides most questions about pg_lake's permissions is that those
roles gate **naming a storage location**, not **using a table**. There are two
layers, and they are checked at different times by different code.

### Using a table: ordinary PostgreSQL privileges, no lake role

A user with no lake role at all can `SELECT`, `INSERT`, `UPDATE` and `DELETE` on
a lake table backed by S3 or Iceberg, given only `USAGE` on the schema and the
table-level `GRANT`s. Nothing in the read or write path re-checks for `lake_read`
or `lake_write`, because the role check ran once, at `CREATE`, on the location the
creator named. From then on the table is an ordinary PostgreSQL object whose
privileges are ordinary PostgreSQL privileges, including column-level ones.

A regression test pins the whole sequence: the user is refused all five statements
with schema `USAGE` only, and each one starts working as the matching table-level
`GRANT` arrives, all without any lake role.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/tests/pytests/test_permissions.py:728-847`, `pg_lake_table/tests/pytests/test_permissions.py:354-429` @ 031d6f58798d (2026-09-03)

**Bounded by:** the corollary is that a lake role is much stronger than a table
privilege, not a smaller version of one. `lake_read` is not "may read lake
tables"; it is "may read any object the pgduck_server credentials can reach",
which includes the files behind a table the holder has no `SELECT` on. Grant the
lake roles to the users who define tables, and use table privileges for everyone
who only queries them.

### Naming a location: the lake roles

The role check happens when a statement carries a location. `CheckURLReadAccess`
wants `lake_read` and `CheckURLWriteAccess` wants `lake_write`, and the FDW option
validators call the write one with `NULL` first, for the role check alone, before
any option has been inspected. Because all Iceberg tables are writable, the
`pg_lake_iceberg` validator does that unconditionally: creating an Iceberg table
requires `lake_write` whatever the location is, while `lake_read` is enough to
create a read-only `pg_lake` table over an existing file.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/src/fdw/option.c:95,200,212,225,666,742`, `pg_lake_engine/src/permissions/roles.c:79-103,115-133` @ 031d6f58798d (2026-09-03)

A regression test walks the three states for the same user: with no lake role both
a `pg_lake` and an Iceberg table are refused; with `lake_read` the read-only
`pg_lake` table is created but the Iceberg table and a writable `pg_lake` table
are still refused; with `lake_read_write` both succeed.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/tests/pytests/test_permissions.py:110-198` @ 031d6f58798d (2026-09-03)

### The location is not a free choice either

Holding `lake_write` does not make every location acceptable, and omitting the
location does not let a user pick one implicitly.

- An explicit location goes through the scheme allowlist, the query-parameter
  allowlist and the Azure host allowlist (`trust-boundaries.md`, boundary 1), and
  an Iceberg location additionally may not contain a `?` at all, since the value
  is a prefix that pg_lake appends `/metadata.json` and data-file paths onto.
- With no location, `CREATE TABLE ... USING iceberg` is rewritten to carry a
  placeholder, and after the table exists the placeholder is replaced with
  `pg_lake_iceberg.default_location_prefix` plus
  `database/schema/table/relation_id`. That GUC is `PGC_SUSET`, so a non-superuser
  cannot point it anywhere: the administrator chooses the prefix and the user gets
  a subdirectory of it named after their own table.
- For the object-store catalog the anchor is stricter still. The table is placed
  under the catalog's own root rather than the default prefix, because
  `catalog.json` records absolute metadata locations and a catalog that points
  outside its own storage is unreadable to anyone authorized for just that root.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/src/fdw/option.c:742-756`, `pg_lake_table/src/ddl/create_table.c:1038-1051,1065-1078`, `pg_lake_iceberg/src/init.c:192-202` @ 031d6f58798d (2026-09-03)

**Bounded by:** the default prefix is one prefix for the whole cluster, so
per-table subdirectories separate names, not tenants. Every user who can create a
table writes under the same root, and any of them who holds `lake_read` can read
the whole root. There is no per-role prefix authorization anywhere in pg_lake.

### What is callable without any of this

The SQL surface reachable with no lake role and no table grant is deliberately
small: the `lake` and `lake_struct` schemas, `lake.version()`,
`is_object_created_by_lake`, and the catalog views described above.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake/pg_lake--3.0.sql:4-16`, `pg_lake_table/pg_lake_table--3.0.sql:20-56,435` @ 031d6f58798d (2026-09-03)
