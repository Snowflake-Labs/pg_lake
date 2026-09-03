# Deployment

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a pgduck_server command-line flag is added or its default changes; the DuckDB configuration in `duckdb_global_init` changes; `install.sh` changes what it writes to `postgresql.conf` or what it downloads; the docker compose topology changes which volumes the two containers share; or the startup umask changes.

pg_lake is two processes that have to be deployed together, and the security of
the result depends more on how they are placed than on anything in the SQL layer.
This document covers what the repository actually configures, and which choices
it leaves to the operator.

## The two processes

PostgreSQL loads `pg_extension_base` from `shared_preload_libraries`, which then
loads the pg_lake libraries named on the installed control files.
`install.sh` writes that line when it initialises a cluster, and prints it as a
manual step when using an existing one.

**Basis:** code-verified Snowflake-Labs/pg_lake `install.sh:566-568,812,825` @ 031d6f58798d (2026-09-03)

pgduck_server runs separately and must be started before pg_lake is usable.
`install.sh` prints the minimal invocation, which uses the default socket
directory:

```
pgduck_server --cache_dir /tmp/pg_lake_cache/
```

**Basis:** code-verified Snowflake-Labs/pg_lake `install.sh:830-831` @ 031d6f58798d (2026-09-03)

**Bounded by:** that default puts both the socket and the cache in `/tmp`, which
is world-writable and shared with every other user on the machine. The socket
itself is protected (0770, no group, so user-only) and the DuckDB database file
is checked for symlinks and foreign ownership before opening, but the cache
directory gets no such check. A single-user development box is the case this
default fits; it is not a deployment posture. Give pgduck_server its own user and
its own directories, per `threat-model.md` T1.

One thing does work in the operator's favour by default: pgduck_server sets a
umask of `0077` before it does anything else, so files it creates are not
group- or world-readable.

```c
	umask(S_IRWXG | S_IRWXO);
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/main.c:60` @ 031d6f58798d (2026-09-03)

## The flags that carry security weight

| Flag | Default | Why it matters |
| --- | --- | --- |
| `--unix_socket_directory` | `/tmp` | The whole of boundary 3. A `@`-prefixed value makes it an abstract socket with no permissions at all. |
| `--unix_socket_group` | empty | Empty means user-only. Naming a group widens access to that group. |
| `--unix_socket_permissions` | `0770` | Widening this widens the only control on the socket. |
| `--no_extension_install` | off | Selects the extension posture below. |
| `--extensions_dir` | unset | Where extensions load from. Must not be writable by anyone but the server. |
| `--init_file_path` | unset | Arbitrary SQL executed at startup as the server's own session. |
| `--duckdb_database_file_path` | `~/.pglake/pgduck_server.db` | Ownership-checked before opening. |
| `--cache_dir` | unset | Local plaintext copies of remote files. Not ownership-checked. |
| `--debug` | off | Logs full query text, including `CREATE SECRET` statements. |
| `--memory_limit` | 80% of system memory | Availability for the whole cluster. |
| `--max_clients` | 10000 | Availability for the whole cluster. |

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/command_line/command_line.c:40-50,107-124,135-172` @ 031d6f58798d (2026-09-03)

## The startup SQL file

`--init_file_path` names a file whose statements are executed on startup, before
any client connects. It runs with the server's full DuckDB privilege, so it is a
trusted input in the strict sense: whoever can write that file can run anything
in the process.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/main.c:67-74`, `pgduck_server/src/command_line/command_line.c:117` @ 031d6f58798d (2026-09-03)

Its intended use is exactly what the shipped example does: create the long-lived
object-store secrets. The example points at a local emulator with dummy
credentials.

**Basis:** code-verified Snowflake-Labs/pg_lake `docker/scripts/init-pgduck-server.sql:1-11` @ 031d6f58798d (2026-09-03)

**Bounded by:** a real deployment puts real credentials in that file or supplies
them another way, and this repository provides nothing for either. Protecting the
file, and keeping the credentials in it as narrow as the deployment allows, is
entirely on the operator. Note the consequence from `data-and-secrets.md`: a
secret created here is cluster-wide, so its reach is the reach of every
`lake_read` holder in every database.

## The DuckDB extension posture

`duckdb_global_init` sets two coherent configurations, selected by whether
extension installation is allowed.

With installs allowed (the default):

- `allow_unsigned_extensions = false`
- `autoinstall_known_extensions = true`

With `--no_extension_install`:

- `autoinstall_known_extensions = false`
- `custom_extension_repository = disabled`, which the comment explains is there to
  make a manual `INSTALL` fail as well
- `allow_unsigned_extensions = true`, permitted in this mode on the reasoning that
  the operator builds the extensions themselves

`autoload_known_extensions` is `true` in both, and `enable_external_file_cache`
is `false` because pg_lake has its own cache.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/duckdb/duckdb.c:248-289`, `pgduck_server/src/main.c:67-74` @ 031d6f58798d (2026-09-03)

**Bounded by:** the second posture trades signature checking for a closed
install path, so it is only sound while the extension directory is not writable
by anyone who could otherwise connect. Combined with a writable
`--extensions_dir` it is strictly worse than the default. And because
`autoload_known_extensions` stays on in both, an extension present in that
directory loads on first use without anyone issuing a `LOAD`.

## The container topology

The shipped compose file runs PostgreSQL and pgduck_server as separate containers
and joins them with two named volumes: one for the socket directory and one for
the PostgreSQL temp-file directory. Both are mounted at the same path in both
containers, and the pgduck_server entrypoint chowns them to `postgres` and
chmods them to 700 before starting the server. pgduck_server listens on a unix
socket only, and the compose comments say so twice.

**Basis:** code-verified Snowflake-Labs/pg_lake `docker/docker-compose.yml:8-12,30-38,66-68`, `docker/scripts/entrypoint-pgduck-server.sh:13-29` @ 031d6f58798d (2026-09-03)

This is the topology the security model assumes: no TCP listener anywhere near
pgduck_server, and the socket reachable only through a shared volume rather than
the network.

**Bounded by:** separate containers give process and filesystem separation, not
isolation of what pgduck_server can do once reached. Both containers also run
with `SYS_PTRACE` added, which is a debugging convenience in this compose file
and should not be carried into a real deployment. The third service is
localstack, an S3 emulator with dummy credentials, which exists for local
development only.

**Basis:** code-verified Snowflake-Labs/pg_lake `docker/docker-compose.yml:21-22,47-48,52-64` @ 031d6f58798d (2026-09-03)

## What install.sh pulls in

`install.sh` installs build and runtime dependencies through the platform package
manager with `sudo`, which is the normal arrangement for a build script but does
mean it is a privileged script.

**Basis:** code-verified Snowflake-Labs/pg_lake `install.sh:228-229,272-275,633-636` @ 031d6f58798d (2026-09-03)

With `--with-test-deps` it also downloads the PostgreSQL JDBC driver over HTTPS
by version number.

**Basis:** code-verified Snowflake-Labs/pg_lake `install.sh:757-762` @ 031d6f58798d (2026-09-03)

**Bounded by:** that download is not checksum-verified or signature-verified in
the script, so it trusts HTTPS and the upstream host. It only happens on the
test-dependency path, so it does not affect a production install, but a CI image
built with `--with-test-deps` inherits the trust.
