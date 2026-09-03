# Threat model

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a URL gate is added, removed or moved; a new statement type is routed through the `ProcessUtility` hook; pgduck_server gains per-session state, an authentication step or a query timeout; the DuckDB extension posture in `duckdb.c` changes; or a new SQL-callable function reaches object storage or the local filesystem.

The threats below are ordered by how much they cost when they land, not by how
likely they are. Each one names the control that addresses it and what the
control does not reach. The controls themselves are described in
`trust-boundaries.md` and `data-and-secrets.md`; this document is about what
they are for and where the gaps are.

The assumed attacker is a PostgreSQL user who holds `lake_read` or `lake_write`
and nothing more, unless a threat says otherwise. A user without either role
cannot name a URL at all, and a superuser is not an attacker in this model
because a PostgreSQL superuser can run arbitrary code by design.

## T1: a local process connects to pgduck_server directly

**What it gets.** Everything. The socket has no authentication
(`trust-boundaries.md`, boundary 3), so a connection is a DuckDB session with the
server's full privilege: read and write any file the pgduck_server user can
reach, use or dump every secret in the shared secrets manager, and reach every
object those credentials cover. PostgreSQL's role checks are not in the path
because the attacker never went through PostgreSQL.

**Control.** Filesystem permissions on the socket, and nothing else. Default
0770 with an empty group is user-only.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/command_line/command_line.c:40-47`, `pgduck_server/src/pgserver/pgserver.c:212-214,272-300` @ 031d6f58798d (2026-09-03)

**Not covered.** A shared socket directory that other users can write to, a
`--unix_socket_group` or `--unix_socket_permissions` wider than the deployment
needs, and an abstract socket, which has no permissions at all. This threat is
why running pgduck_server as its own user, in its own namespace, or in its own
container is the deployment recommendation rather than an optimisation.

## T2: SSRF into an internal or cloud metadata endpoint

**What it gets.** A URL is not just a name: DuckDB's S3 filesystem takes
connection settings from the query string, and the Azure filesystems take the
endpoint from the host. A `lake_read` user who can steer either one can make the
server issue a request to an address of their choosing inside the network, and
read the response body back as query rows. The classic target is the instance
metadata service, whose response is a set of credentials.

**Controls.** The query-parameter allowlist (two keys), the Azure host-suffix
allowlist with its label-boundary and container-part checks, and the scheme
allowlist applied to manifest-derived paths as well as user-named ones.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/permissions/roles.c:196-269,285-339,374-425,164-172` @ 031d6f58798d (2026-09-03)

Regression tests cover the metadata-endpoint form specifically, on `COPY` in
both directions, on a foreign table, and on the Iceberg metadata functions.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/tests/pytests/test_security.py:198-264,289-337,361-386` @ 031d6f58798d (2026-09-03)

**Not covered.** `http://` and `https://` reads are not gated beyond the
`lake_read` role, because on those schemes the URL is the request target and the
query string is ordinary. Granting `lake_read` therefore includes the ability to
make the server fetch an arbitrary http(s) URL and return the body. If that
matters in a deployment, it has to be handled by network policy around the
pgduck_server process, not by pg_lake. Nothing in this repository restricts
which hosts the DuckDB HTTP client may reach.

## T3: exfiltration through a write target

**What it gets.** A `lake_write` user who can name an outbound target moves any
data they can query out of the deployment in one statement.

**Control.** Writes to `http(s)://` are refused for everyone including
superusers, with exfiltration named as the reason. `pg_lake.stage_location` is
validated the same way when it is set.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/permissions/roles.c:115-155`, `pg_lake_engine/src/init.c:266-300` @ 031d6f58798d (2026-09-03)

**Not covered.** Object-store write targets are only checked for scheme, query
string and (on Azure) host. A `lake_write` user may write to any bucket the
server's credentials can write to, including one that is not theirs, and to a
public bucket belonging to someone else if the credentials permit anonymous
writes. pg_lake has no per-prefix write authorisation.

## T4: arbitrary local file access through the DuckDB process

**What it gets.** DuckDB's `LocalFileSystem` has no restrictions, so a path that
reaches it reads or writes as the pgduck_server user: `~/.pgpass`, key material,
`postgresql.conf`.

**Controls.** There is no `file://` scheme in `IsSupportedURL`, and the one place
that accepts a bare local path, `COPY`, replicates core's
`pg_read_server_files` / `pg_write_server_files` check because pg_lake's
`ProcessUtility` hook runs ahead of core's own check
(`trust-boundaries.md`, boundary 2).

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/copy/copy_format.c:420-442`, `pg_lake_copy/src/copy/copy.c:415-434,823-841` @ 031d6f58798d (2026-09-03)

**Not covered.** The replicated check can drift from the core check it copies,
and it only covers `COPY`. Completeness depends on every other path rejecting a
non-URL argument on the scheme allowlist, which is a property of the current call
sites rather than something enforced centrally. A new entry point that takes a
path and forgets the gate reopens this, which is what the regression suite in
`secure-coding.md` exists to catch.

Two files that are read by design belong here as well: the pgduck_server startup
SQL file, executed as the server's own session at boot, and the DuckDB database
file, which is checked for symlinks and foreign ownership before it is opened.
Both are operator-supplied; see `deployment.md`.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/main.c:74`, `pgduck_server/src/duckdb/duckdb.c:179-219` @ 031d6f58798d (2026-09-03)

## T5: one user reaching another user's credentials or cached data

**What it gets.** One pgduck_server serves every database and every user in the
cluster, so its DuckDB secrets, settings and file cache are cluster-wide state.
A user in database A can potentially use a credential pushed on behalf of a user
in database B, or read a cached file belonging to a query they could not have run.

**Controls.** Vended credentials are pushed under a name that includes the
database OID, the server OID and a hash, and with a URL `SCOPE`, so DuckDB will
only apply a secret to a matching path. They are created as temporary
in-memory secrets, so they do not persist in the database file. Iceberg REST
catalog credentials live on per-user foreign user mappings, and the token cache
is keyed by (server, user mapping).

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:53-74,245-301,355-367`, `pg_lake_iceberg/src/rest_catalog/rest_catalog_auth.c:47-54` @ 031d6f58798d (2026-09-03)

**Not covered.** Scoping is a routing rule, not an access control. Any session on
the socket can enumerate the secrets manager, and a broader long-lived secret
configured by the operator applies to every database in the cluster by design.
The file cache has no tenant dimension at all. Where hard separation between
tenants is required, it needs separate pgduck_server processes, which is a
deployment choice this repository does not make.

## T6: credential disclosure in query text, errors or logs

**What it gets.** Catalog credentials, and with them whatever the catalog
protects.

**Controls.** All `rest_catalog_*` GUCs are `PGC_SUSET` and
`GUC_SUPERUSER_ONLY`, so they are not readable by an ordinary user.
`client_id` and `client_secret` are documented as belonging on a user mapping
rather than a server. Statements that carry those options are blanked out of the
reported query string before any validator can raise an error containing them,
and the redaction hook is deliberately registered after the validator so the
ordering holds. The OAuth failure path does not echo the response body.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/init.c:284-362`, `pg_lake_iceberg/src/rest_catalog/rest_catalog_options.c:273-283`, `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:783-820`, `pg_lake_iceberg/src/rest_catalog/rest_catalog_auth.c:301-322` @ 031d6f58798d (2026-09-03)

**Not covered.** The redaction covers the statement text pg_lake reports; it is
not a claim about every place PostgreSQL might record a statement. And
pgduck_server's `--debug` flag logs full query text, which includes the
`CREATE SECRET` statements pg_lake sends. `--debug` is not a debugging
convenience on a deployment holding real credentials.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/pgsession/pgsession.c:383,557`, `pgduck_server/src/duckdb/duckdb.c:638` @ 031d6f58798d (2026-09-03)

## T7: hostile table metadata

**What it gets.** Iceberg manifests are Avro, data files are Parquet, and both
are parsed by vendored C and C++ code. Anyone who can write to a table's storage
location controls that input, and a storage location is not always as private as
the table's PostgreSQL privileges suggest.

**Control.** Paths taken from metadata go through `ValidateStorageURL`, so a
manifest cannot change the scheme or smuggle a query string into the next read.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/iceberg/read_manifest.c:88,138,261` @ 031d6f58798d (2026-09-03)

**Not covered.** That is a check on the path, not on the bytes. Memory safety in
the Avro and Parquet readers belongs to the vendored dependencies listed in
`security.yaml`, and both carry in-tree patches, so a fix upstream is not
automatically a fix here. Treat write access to a table's storage location as
equivalent to influence over the parsers.

## T8: denial of service against the single process

**What it gets.** pgduck_server is one process for the whole cluster, so a query
that exhausts its memory or a client that occupies it degrades or stops lake
access for every database at once, including administrative paths like the
deletion queue.

**Controls.** `--memory_limit` (default 80 percent of system memory),
`--max_clients` (default 10000), `--cache_on_write_max_size` (default 1GB), and
`--continue_on_oom` to keep serving after an out-of-memory error rather than
stopping. The cache-manager background worker trims the file cache to
`pg_lake_engine.max_cache_size`, and restarts 5 seconds after a crash.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/command_line/command_line.c:48-49,112-114`, `pg_lake_engine/src/pgduck/cache_worker.c:81-95` @ 031d6f58798d (2026-09-03)

**Not covered.** There is no per-user or per-database resource accounting inside
pgduck_server and no statement timeout of its own, so the limits are global.
Availability isolation between tenants needs separate processes, as in T5.

## T9: supply chain through DuckDB extensions

**What it gets.** Code execution inside pgduck_server, which by T1 is
everything.

**Control.** Two coherent postures, described in `deployment.md`. With installs
allowed, unsigned extensions are refused and only known extensions autoinstall.
With `--no_extension_install`, autoinstall is off and
`custom_extension_repository` is set to `disabled` so that manual `INSTALL`
fails too; unsigned extensions are permitted in that mode because the operator
built them.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/duckdb/duckdb.c:248-283` @ 031d6f58798d (2026-09-03)

**Not covered.** `autoload_known_extensions` is on in both postures, so an
extension already present in the extension directory loads on first use without
an explicit `LOAD`. The two postures are also only as separate as the flag: a
deployment that passes `--no_extension_install` and also ships a writable
extension directory has combined "unsigned extensions permitted" with "attacker
can place a file there". Keep the extension directory read-only to the server.
