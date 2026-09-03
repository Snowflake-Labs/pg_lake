# Data and secrets

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a credential option is added to the `iceberg_catalog` option descriptors, or `IsRedactableUserMappingSecret` changes; the handler registration order in `pg_lake_iceberg/src/init.c` changes; the vended secret name or scope changes; a `rest_catalog_*` GUC changes context; the set of built-in catalog server names changes; or a new statement type can carry a credential in its text.

pg_lake holds two kinds of credential: the object-store credentials that live
inside pgduck_server, and the Iceberg REST catalog credentials that live in the
PostgreSQL catalog. They have different owners, different lifetimes and different
exposure, so they are described separately.

## Object-store credentials live in pgduck_server

DuckDB's secrets manager inside pgduck_server holds the credentials that actually
open an object-store URL. pg_lake in PostgreSQL does not store them and does not
need to see them: a backend sends SQL over the socket and DuckDB applies whatever
secret matches the path.

**Consequence.** Those secrets are process-wide, so they are shared by every
database and every user in the cluster (`architecture.md`). A PostgreSQL user's
reach into object storage is decided by `lake_read` / `lake_write` plus whatever
those secrets cover, and not by anything more granular.

**Bounded by:** how the long-lived secrets get into pgduck_server is a deployment
question this repository does not answer. The container setup here does it with a
startup SQL file against a local emulator; a real deployment substitutes its own
mechanism, and the security of the credentials in it is that mechanism's
responsibility. See `deployment.md`.

## Vended credentials are scoped and temporary

When a catalog vends short-lived credentials for a specific table, pg_lake pushes
them into the shared pgduck_server. Three properties of that push matter.

**The name is namespaced and deterministic.**
`pglake_vended_<dbOid>_<serverOid>_<hash(secretId)>`, where the comment states
the reason for each part: the database OID because one pgduck_server is shared by
all databases in the cluster, and a 64-bit hash so distinct identities do not
collide in the shared namespace. Determinism is what makes
`CREATE OR REPLACE SECRET` idempotent across credential rotation.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:53-74` @ 031d6f58798d (2026-09-03)

**The secret carries a URL `SCOPE`,** so DuckDB only applies it to paths under
that prefix rather than to every S3 request in the process.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:18-25,292-293` @ 031d6f58798d (2026-09-03)

**It is temporary and in-memory,** so it never lands in the DuckDB database file.
It is re-pushed when it is new or nearing expiry, and dropped by name when no
longer needed.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:355-372,383-400` @ 031d6f58798d (2026-09-03)

The credential values are interpolated into the `CREATE SECRET` text, so they go
through `EscapeSingleQuotes` first; that helper and the rest of the rendering
path are in `input-rendering.md`.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:78-102,256-262` @ 031d6f58798d (2026-09-03)

**Bounded by:** `SCOPE` routes a secret, it does not protect it. Any session on
the socket can list the secrets manager and use any secret in it, scope or no
scope (`threat-model.md`, T1 and T5). The naming and scoping keep unrelated
credentials from being applied to the wrong request; they are not a boundary
between tenants.

There is also a deliberate gap in the refresh logic, stated in the code: if
pgduck_server restarts, a backend that believes its pushed secret is still fresh
will not re-push until the next reconcile that has other work to do.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/vended_secrets.c:363-367` @ 031d6f58798d (2026-09-03)

## Catalog credentials belong on a user mapping

For an Iceberg REST catalog, `client_id` and `client_secret` are per-user
credentials. The option descriptors mark them as user-mapping options, and the
validator comment states the rule directly: they "are credentials and therefore
belong on a user mapping".

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_options.c:114-117,273-283,286-338` @ 031d6f58798d (2026-09-03)

The OAuth token obtained with them is cached per (server, user mapping), so one
user's token is not reused for another.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_auth.c:47-54` @ 031d6f58798d (2026-09-03)

There are superuser-only GUC fallbacks for the same values
(`pg_lake_iceberg.rest_catalog_client_id` and `_client_secret`, plus host, OAuth
path, auth type and scope). All are `PGC_SUSET` with `GUC_SUPERUSER_ONLY`,
`GUC_NO_SHOW_ALL` and `GUC_NOT_IN_SAMPLE`, so an ordinary user can neither set
nor read them.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/init.c:284-362` @ 031d6f58798d (2026-09-03)

**Bounded by:** a GUC fallback is one credential for the whole cluster, which is
the opposite of the per-user model above. Setting it in `postgresql.conf` means
every user who can reach the catalog shares one identity there, and
`GUC_NO_SHOW_ALL` means a review of the running configuration will not show that
this happened.

## DDL guards around those credentials

Four refusals protect the credentials already stored, all in the same utility
handler:

- **the endpoint cannot be changed on a server in use.** `ALTER SERVER ... OPTIONS (SET rest_endpoint ...)` or `oauth_endpoint` is refused while the server has user mappings or dependent Iceberg tables, because, as the hint says, it "would redirect catalog credentials of every mapping owner to the new URL". That is a credential-theft primitive available to whoever owns the server.

  **Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:439-503` @ 031d6f58798d (2026-09-03)

- **the three built-in catalog server names are reserved.** `pg_lake_postgres_catalog`, `pg_lake_object_store_catalog` and `pg_lake_rest_catalog` cannot be created, renamed onto, altered, have their owner changed, or have user mappings created on them by anyone. Reserving the names is what lets the other guards trust a name without looking up the FDW behind it.

  **Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:266-302,357-372,391-405,428-437,439-460,504-517` @ 031d6f58798d (2026-09-03)

- **a credential cannot be dropped out from under dependent tables.** `ALTER USER MAPPING ... OPTIONS (DROP client_secret)` is refused while the server has dependent Iceberg tables. `SET` and `ADD` stay allowed, since those are rotation rather than removal.

  **Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:518-573` @ 031d6f58798d (2026-09-03)

- **the built-in servers cannot be detached from the extension.** `ALTER EXTENSION ... DROP SERVER` is refused for them, because detaching the dependency edge would let the object be dropped freely afterwards.

  **Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:574-621` @ 031d6f58798d (2026-09-03)

**Bounded by:** these are utility-statement guards, so they hold for SQL that
goes through `ProcessUtility` in a session where the extension is loaded. They
are not catalog constraints. They also do not fire while `creating_extension` is
set, which is how the extension creates the reserved servers in the first place.

## Keeping credentials out of statement text

A `CREATE USER MAPPING ... OPTIONS (client_secret '...')` statement contains a
credential in its text, and that text is reported in error contexts and in
`pg_stat_statements`. `RedactRestCatalogUserMappingSecrets` scrubs the backing
query-string buffer in place when the statement carries a credential option.

Two design decisions in it are worth repeating:

- **the whole statement is redacted,** not the literal, which the comment
  explains as trading some observability for a much smaller surface than a
  hand-rolled string-literal parser. A user-mapping statement with no credential
  option is left visible.
- **when the server cannot be proven to belong to another FDW, it is treated as
  possibly Iceberg** so the statement is scrubbed anyway.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:657-690,691-719,720-757,758-816,817-859` @ 031d6f58798d (2026-09-03)

The registration order carries the security property. Handlers are prepended, so
the redaction is registered *after* the validator in order to run *before* it,
which is what stops a validator `ereport(ERROR)` from surfacing an unredacted
statement in its error context. The comment at the registration says so.

```c
	/*
	 * Register last so it runs first: RegisterUtilityStatementHandler
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/init.c:379-387`, `pg_lake_iceberg/src/rest_catalog/rest_catalog_ddl.c:800-815` @ 031d6f58798d (2026-09-03)

**Bounded by:** this covers the query string pg_lake's own handler chain sees. It
is not a general claim that the credential appears nowhere: it is still in
`pg_user_mappings` for those entitled to read it, and if the statement was typed
interactively it is in the client's history. Most concretely, pgduck_server's
`--debug` flag logs full query text, which includes the `CREATE SECRET`
statements pg_lake sends with credential values in them.

**Basis:** code-verified Snowflake-Labs/pg_lake `pgduck_server/src/pgsession/pgsession.c:383,557`, `pgduck_server/src/duckdb/duckdb.c:638` @ 031d6f58798d (2026-09-03)

The OAuth failure path is the other place a credential could leak outward: it
reports that the token request failed without echoing the response body.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/src/rest_catalog/rest_catalog_auth.c:301-322` @ 031d6f58798d (2026-09-03)

## Table data

Table data is in object storage, in Parquet, with Iceberg metadata alongside it
when the table is an Iceberg table. pg_lake does not encrypt it: confidentiality
at rest is the object store's, under whatever bucket policy and key the
deployment configures.

Two local copies of data exist and are worth knowing about:

- **the file cache**, a local directory of remote file contents, trimmed by the
  cache-manager worker to `pg_lake_engine.max_cache_size`. It is keyed by path
  and has no tenant dimension, so a cached object is available to any query that
  names the same path.
- **the temp-file directory**, `$PGDATA/base/pgsql_tmp`, used to move data
  between the two processes. pg_lake registers a memory-context callback to
  unlink what it creates.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/cache_worker.c:100-176`, `pg_lake_engine/src/init.c:92-125`, `pg_lake_engine/src/storage/local_storage.c:35-80` @ 031d6f58798d (2026-09-03)

**Bounded by:** both are plaintext on local disk, and both are readable by the
processes that share them. Cleanup of a temp file is tied to the memory context,
so a crash can leave one behind. Neither is a place to rely on for isolation.
