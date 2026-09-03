# Trust boundaries

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a new call site of `CheckURLReadAccess` / `CheckURLWriteAccess` / `ValidateStorageURL` appears, or an existing one is removed; the query-parameter allowlist in `IsAllowedQueryParamKey` changes; `IsSupportedURL` gains a scheme; the Azure host check changes shape; a second place accepts a bare local path; pgduck_server gains an authentication step or a non-unix listener; or a new SQL-callable function is granted to `lake_read` or `lake_write`.

pg_lake has three boundaries worth naming. Two of them are ordinary
PostgreSQL privilege checks in front of a powerful primitive. The third,
PostgreSQL to pgduck_server, has no authentication and is worth understanding
before deploying pg_lake anywhere the socket is not private.

## Boundary 1: a SQL user naming a URL

Naming a URL is the central privileged act in pg_lake. DuckDB will open it with
whatever credentials pgduck_server holds, and return the bytes as query rows.
Four controls sit in front of that.

### The role check

`CheckURLReadAccess` requires `lake_read` (or superuser); `CheckURLWriteAccess`
requires `lake_write`. When the role does not exist, only a superuser can name a
URL at all.

```c
	if (!superuser() &&
		(readRoleId == InvalidOid || !has_privs_of_role(userId, readRoleId)))
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to read from URL"),
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/permissions/roles.c:79-103,115-133` @ 031d6f58798d (2026-09-03)

The gates are called from every entry point that takes a path from the user:
`COPY` in both directions, `CREATE TABLE ... LOAD FROM`, the FDW option
validator for `path` and `location` and the `writable` option, the Iceberg
metadata functions, the `lake_file` utility functions, and the manifest reader.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/src/copy/copy.c:417,823`, `pg_lake_copy/src/ddl/create_table.c:228`, `pg_lake_table/src/fdw/option.c:95,200,212,225,666,742`, `pg_lake_iceberg/src/iceberg/iceberg_functions.c:55,77,150`, `pg_lake_iceberg/src/iceberg/read_manifest.c:88,138,261`, `pg_lake_table/src/util/s3_file_utils.c:68,100,167,225,276`, `pg_lake_benchmark/src/tpch.c:77,113`, `pg_lake_benchmark/src/tpcds.c:80` @ 031d6f58798d (2026-09-03)

That list is the whole set: at this commit there is no other caller of the three
gate functions anywhere in the tree.

The check is on the statement that names the location, which in practice means
`CREATE`. Querying and modifying an existing lake table afterwards needs only
ordinary PostgreSQL table privileges, and no lake role at all; the permission
model is set out in `architecture.md`.

**Bounded by:** this is a per-URL check, not a per-object one. `lake_read` is
all-or-nothing: a user who holds it may read every object the pgduck_server
credentials can reach, in any bucket, for any tenant those credentials cover.
pg_lake has no notion of which prefixes a given PostgreSQL role may touch. The
blast radius of `lake_read` is therefore exactly the reach of the credentials
configured in pgduck_server, which is an operator decision made outside this
repository. It follows that `lake_read` outranks any table `GRANT`: its holder can
read the files behind a table they cannot `SELECT`, and
`pg_catalog.iceberg_tables` tells them where those files are.

### The scheme allowlist

`IsSupportedURL` is an allowlist, and `ValidateStorageURL` applies it to paths
pg_lake reads out of table data (a manifest entry, a data-file path) where the
role check already ran on the URL the user named.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/copy/copy_format.c:420-442`, `pg_lake_engine/src/permissions/roles.c:164-172` @ 031d6f58798d (2026-09-03)

**Bounded by:** applying the scheme check to manifest-derived paths is what stops
a table's own metadata from redirecting a scan, but it is a scheme and
query-string check, not a location check. A manifest may still point at a
different bucket that the server's credentials happen to reach.

### The query-parameter allowlist

DuckDB's S3 filesystem reads connection settings off the URL query string, so a
URL is a configuration channel and not just a name. The allowlist is two
entries:

```c
	static const char *const allowlist[] = {
		"s3_region=",
		"s3_requester_pays=",
		NULL,
	};
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/permissions/roles.c:196-269` @ 031d6f58798d (2026-09-03)

The vector this closes is named in the code: `s3_endpoint=` lets any `lake_read`
user point the request at an internal host and read the response back as query
rows, and the parameter set DuckDB honours grows over time, so a denylist would
be bypass-prone.

**Bounded by:** `http://` and `https://` URLs are exempt from the check, because
`?` is an ordinary query separator there. On those two schemes the URL is the
request target already, which is why writes to them are refused outright below,
and why reads on them are only as safe as the decision to grant `lake_read`.

### The Azure host allowlist

Azure URLs carry the endpoint in the host rather than the query string, so they
get their own check. `az://<account>.<endpoint>/...` and
`abfss://<container>@<account>.<endpoint>/...` both send the request to
`https://<account>.<endpoint>`, and the host must end in a suffix from
`pg_lake.allowed_azure_host_suffixes`. The default list is the six public and
sovereign-cloud blob and dfs suffixes; an empty list rejects every URL that names
a host.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/permissions/roles.c:285-339,374-425`, `pg_lake_engine/include/pg_lake/extensions/pg_lake_engine.h:33-36`, `pg_lake_engine/src/init.c:227-240` @ 031d6f58798d (2026-09-03)

Three details in that check are each closing a bypass rather than being
defensive in general:

- the suffix match must land on a label boundary and leave at least one
  character of account name, so neither `evilblob.core.windows.net` nor the bare
  suffix itself passes;
- a container part containing `.` or `@` is rejected, because DuckDB's URL parser
  splits the authority elsewhere when it does, which would otherwise reach a host
  this check rejects;
- a host with no dot is allowed, since DuckDB then takes the endpoint from the
  configured secret, but a bracketed IPv6 literal is explicitly not given that
  exit.

**Bounded by:** the setting is `PGC_SUSET`, so widening it is an admin action,
but it is a host allowlist and nothing more. Any account under an allowed suffix
is reachable, including one belonging to someone else, if the server's
credentials or a public container permit it.

### The http(s) write refusal

Writes to `http://` and `https://` are refused for everyone, superusers
included:

```c
	/*
	 * Bare http(s):// URLs have no legitimate write target in pg_lake.  The
	 * only realistic use is exfiltrating data through DuckDB's HTTP client to
	 * an internal service.  Hard reject for everyone, including superusers.
	 */
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/permissions/roles.c:142-152` @ 031d6f58798d (2026-09-03)

`pg_lake.stage_location` is validated the same way at GUC-set time: it must be a
cloud-storage URL, must not be `http(s)://`, and must not contain a query
string.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/init.c:266-300` @ 031d6f58798d (2026-09-03)

## Boundary 2: a SQL user naming a local path

`COPY` accepts a bare local path, and pg_lake routes `COPY` through its
`ProcessUtility` hook, which runs before core's `DoCopy()` and therefore before
core's `pg_read_server_files` check. pg_lake replicates that check on the
local-path branch:

```c
		/*
		 * Local file paths bypass PostgreSQL's standard DoCopy() because
		 * pg_lake routes them through its ProcessUtility hook before the core
		 * pg_read_server_files check fires.  Replicate that check here so a
		 * non-privileged user cannot read arbitrary server files via DuckDB's
		 * unrestricted LocalFileSystem.
		 */
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/src/copy/copy.c:415-434` (read), `pg_lake_copy/src/copy/copy.c:823-841` (write, `pg_write_server_files`) @ 031d6f58798d (2026-09-03)

This is the boundary with the shortest history: the regression tests name the
bypass they were written for, including the glob form and the combination with a
duplicate format option.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/tests/pytests/test_security.py:38-197` @ 031d6f58798d (2026-09-03)

**Bounded by:** a replica of a core check is a copy, and a copy can drift. If
core changes what `pg_read_server_files` means, or pg_lake starts routing
another statement type through its hook ahead of a core check, nothing here
notices. The control is also specific to `COPY`: every other entry point rejects
a non-URL path on the scheme allowlist instead, so the two mechanisms have to
stay complete between them.

## Boundary 3: PostgreSQL to pgduck_server

**There is no authentication on this boundary.** pgduck_server reads a v3
startup packet, refuses SSL and GSSAPI requests (it only listens on a unix
socket, where SSL does not apply) and refuses other protocol versions, and then
proceeds to serve queries. No authentication exchange happens at any point.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/pgsession/pgsession_io.c:199-293` @ 031d6f58798d (2026-09-03)

What holds the boundary is filesystem permissions on the socket: the socket
directory, its group, and the socket mode (0770 by default, group empty unless
`--unix_socket_group` is given).

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/command_line/command_line.c:45-47,107`, `pgduck_server/src/pgserver/pgserver.c:212-214,272-300` @ 031d6f58798d (2026-09-03)

**Bounded by:** those permissions are skipped entirely for an abstract socket.
`--unix_socket_directory` accepts a path starting with `@`, which puts the socket
in the Linux abstract namespace, and both the permission-setting function and the
lock-file function return early on that leading character:

```c
	/* no file system permissions for abstract sockets */
	if (unixSocketPath[0] == '@')
		return STATUS_OK;
```

An abstract socket has no filesystem entry and no mode, so it is reachable by
every process in the network namespace. Since filesystem permissions are the
only thing holding this boundary, an abstract socket removes the boundary. Do not
configure one outside a namespace that contains nothing else.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/pgserver/pgserver.c:241-245,278-283`, `pgduck_server/src/command_line/command_line.c:107,158,191` @ 031d6f58798d (2026-09-03)

Anyone who can connect to that socket has the full DuckDB surface: reading and
writing any local file the pgduck_server user can reach, listing and using every
secret in the shared secrets manager, and running `INSTALL`/`LOAD` subject only
to the extension posture in `deployment.md`. There is no per-database or
per-user separation inside the process, because there is no notion of a
PostgreSQL user there at all.

**Bounded by:** the default socket directory is `/tmp`, which is world-writable
and shared. The 0770 mode with an empty group leaves the socket reachable by the
owning user only, which is the safe end of the range, but the deployment decides
this: `--unix_socket_group postgres` in the documented container setup widens it
to that group, and a wider `--unix_socket_permissions` widens it further. pg_lake
cannot detect a bad choice here, and there is no second control behind it.

One related check does exist for the on-disk database file. Before opening it,
pgduck_server refuses a symlink and refuses a file owned by a different uid, with
the reason stated as `/tmp` squatting.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/duckdb/duckdb.c:179-219` @ 031d6f58798d (2026-09-03)

**Bounded by:** that check covers the database file specifically. The socket
directory, the cache directory and the shared temp-file directory get no
equivalent ownership check, and the temp-file directory is writable by both
processes by design.

## What is not a boundary

**GUCs are admin surface, not user surface.** The security-relevant settings are
`PGC_SUSET`, and the credential-bearing ones additionally
`GUC_SUPERUSER_ONLY`: `pg_lake.allowed_azure_host_suffixes`,
`pg_lake.stage_location`, `pg_lake_table.enable_delete_file_function`,
`pg_lake_table.skip_drop_access_hook`, and the whole
`pg_lake_iceberg.rest_catalog_*` family.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/init.c:216-240`, `pg_lake_table/src/init.c:331-350`, `pg_lake_iceberg/src/init.c:284-362` @ 031d6f58798d (2026-09-03)

A regression test pins the Azure setting as not user-settable.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/tests/pytests/test_security.py:562-585` @ 031d6f58798d (2026-09-03)

**Bounded by:** `pg_lake_table.skip_drop_access_hook` and
`enable_delete_file_function` both turn off a restriction rather than tighten
one, so an admin who sets them in `postgresql.conf` has changed the model for
every user in the cluster. Neither is reported in `SHOW ALL`, so a review of the
running configuration will not show them.
