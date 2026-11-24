---
title: External query engines
parent: User guide
nav_order: 7
---

# External query engines
{: .no_toc }

pg_lake runs the queries on its tables in pgduck_server. A foreign table of the
`pg_lake_remote` foreign data wrapper is served by another server instead, an *external query
engine*: any server that speaks the PostgreSQL wire protocol and runs DuckDB SQL, the way
pgduck_server does. PostgreSQL plans the query, pg_lake sends the DuckDB SQL for the engine's
tables to the engine, and reads the result rows back. The tables are read-only.

1. TOC
{:toc}

## Setting up an engine

A superuser creates a server for the engine, user mappings with the credentials to connect,
and a foreign table for each table or view on the engine:

```sql
CREATE SERVER analytics FOREIGN DATA WRAPPER pg_lake_remote
  OPTIONS (conninfo 'host=analytics.internal port=5433 dbname=analytics sslmode=verify-full');

CREATE USER MAPPING FOR app_user SERVER analytics
  OPTIONS (user 'app_user', password 'secret');

CREATE FOREIGN TABLE public.hits (
  id bigint,
  url text,
  event_time timestamptz
)
SERVER analytics
OPTIONS (schema_name 'main', table_name 'hits');

GRANT SELECT ON public.hits TO app_user;
```

| Object | Option | Description |
|:--|:--|:--|
| Server | `conninfo` | Required. A [libpq connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) or URI for the engine. It cannot contain the `user`, secrets such as `password` or `sslpassword`, or libpq's debug options such as `replication`, since every user can read the options of a server. |
| User mapping | `user` | The user to connect as. |
| User mapping | `password`, `sslpassword` | The password, and the passphrase of the client key. pg_lake takes every option that libpq treats as a secret, which in PostgreSQL 18 also includes `oauth_client_secret`. |
| User mapping | `sslcert`, `sslkey` | A client certificate and key for this user, as files on the PostgreSQL server. Only superusers can set them. |
| User mapping | `password_required` | Whether a non-superuser has to connect with a password, `true` by default. Only superusers can set it to `false`, see [passwords](#passwords). |
| Foreign table | `table_name` | Required. The name of the table or view on the engine. Only superusers can set it, see [privileges](#privileges). |
| Foreign table | `schema_name` | The schema of the table on the engine. Without it, the engine's search path finds the table; DuckDB's default schema is `main`. Only superusers can set it. |

The user mapping options are added to the server's `conninfo`. Settings that are the same for
every user, such as `sslmode` and `sslrootcert`, go in the `conninfo`.

The column list can be left out. pg_lake then asks the engine for the columns of the table,
connecting with the user mapping of the user that creates the foreign table, and declares
them with the PostgreSQL types of their DuckDB types, as it does for files:

```sql
CREATE FOREIGN TABLE public.hits ()
SERVER analytics
OPTIONS (schema_name 'main', table_name 'hits');
```

Partitions and tables created with `INHERITS` take their columns from their parent instead.

## Privileges

No role has `USAGE` on the `pg_lake_remote` wrapper, so only superusers create its servers. A
superuser can grant `USAGE` on a server, which lets a role create its own user mapping.

Only superusers can create foreign tables on these servers or change their options, because
`table_name` and `schema_name` decide what the engine reads. DuckDB reads a table name it does
not find in its catalog as a file or URL, such as `'/etc/hosts.csv'` or
`'s3://bucket/data.parquet'`, with the engine's own access to its file system and object
storage, and none of pg_lake's checks, like the `lake_read` role, apply. Grant `SELECT` on the
foreign table to the roles that read it. The owner of a foreign table can rename it, which
does not change the table that it reads.

pg_lake connects with the user mapping of the user that reads the table: the current user, or,
for a table in a view, the owner of the view, unless the view has `security_invoker` set. This
is the user whose privileges PostgreSQL checks, as with other foreign tables. If that user has
no user mapping for the server, pg_lake uses the `PUBLIC` user mapping. Without either, a
superuser connects with just the server's `conninfo`, and a non-superuser gets an error. Since
a query is sent to the engine as one user, a query that reads a table of a view and the same
table directly as another user is not sent to the engine as a whole.

The owner of a server can change its `conninfo`, and so point it at another host, which
receives the credentials of every user mapping of the server. Keep servers owned by superusers.

### Passwords

As with `postgres_fdw`, a non-superuser has to have a password in the user mapping, and the
engine has to ask for it. Otherwise a role could connect with the engine's `trust` or `peer`
authentication, or with the password file of the PostgreSQL server's operating system user,
as any remote user it names.

For an engine that runs next to PostgreSQL and only accepts connections from it, for example
on a Unix socket with `trust` authentication as pgduck_server does, a superuser can lift the
requirement:

```sql
CREATE USER MAPPING FOR PUBLIC SERVER analytics OPTIONS (password_required 'false');
```

Every role that can read a table of the server then queries the engine as the same remote
user, with whatever access the engine gives an unauthenticated connection. Use it only for an
engine that gives all its users the same data, and keep the server and its foreign tables
owned by superusers.

## Querying

When all tables of a query are on the same server and read as the same user, pg_lake sends the
whole query to the engine, so joins, aggregates, sorting and `LIMIT` run there, as they do for
Iceberg tables on pgduck_server. Otherwise each table is scanned on its own and PostgreSQL
computes the rest, for example when a query joins an engine table with a heap table, an Iceberg
table, a table of another server, or a view owned by another user. `EXPLAIN (VERBOSE)` shows why
a query was not sent as a whole, such as `Table on another query engine` or
`Table read as another user`.

`EXPLAIN` shows the server under `Engine: DuckDB`, and the plan that the engine returns for the
query it gets.

A cancel request or `statement_timeout` also cancels the query on the engine, and interrupts a
connection attempt that hangs.

`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY ... FROM` and `TRUNCATE` are refused.
`INSERT ... SELECT` from an engine table into an Iceberg table works, but runs in PostgreSQL,
since pgduck_server, which writes the Iceberg table, cannot read the engine's table.

### Column types

Columns are matched by name, so the engine's table can have its columns in another order, and
more of them. pg_lake casts each column to the DuckDB type of its declared type, so that the
engine computes with the declared type even if its column has another one: an engine
`INTEGER` declared as `text` is compared and sorted as text. DuckDB drops a cast to the type a
column already has, so filters on columns of the declared type still reach the engine's scan.

pg_lake only casts types that have an exact DuckDB type. It does not cast `numeric` without a
precision or with more than 38 digits, geometry, arrays, composite types, maps, and types that
DuckDB does not have. The engine has to return these columns in the declared type.

## What an engine has to provide

- The PostgreSQL wire protocol, version 3. pg_lake uses the extended query protocol, single-row
  mode, and text-format results, rendered in the PostgreSQL text format as pgduck_server renders
  them. Query parameters, as in prepared statements, are sent as text without types, and the
  query casts each one to its type, so an engine binds them as strings. Errors come back with
  their SQLSTATE, and a cancel request cancels the running query.
- DuckDB SQL, including the functions that pg_lake adds to DuckDB, since pg_lake rewrites some
  PostgreSQL functions into them: `acosh_pg`, `atanh_pg`, `initcap_pg`, `substring_pg`,
  `lake_nth_suffix`, `nullify_any_type`, `to_date`, `uuid_extract_timestamp_pg` and
  `uuid_extract_version_pg`, and the macros `lake_regexp_matches`, `generate_series_int`,
  `generate_series_int_step` and `jsonb`. It also uses DuckDB's `json` and `icu` extensions,
  and `spatial` for geospatial queries.
- `explain (format 'json') <query>`, which returns DuckDB's plan in the second column of one
  row, for `EXPLAIN`.
- `DESCRIBE SELECT * FROM <table>`, which returns the name and the DuckDB type of each column
  in its first two columns, for creating a foreign table without a column list.
- Only the engine's own tables and views for a table name. An engine must not read a name that
  it does not know as a file or URL for the user of a user mapping, as DuckDB does.

## Limitations

- Each scan of a query opens its own connection to the engine, and connections are not reused.
  Scans do not share a snapshot, so two scans in one query can see different versions of the
  engine's data, and the engine's data is not part of PostgreSQL transactions.
- `ANALYZE`, `IMPORT FOREIGN SCHEMA` and a `column_name` option are not supported. The column
  names of the foreign table have to match the engine's.
- pg_lake does not check which functions an engine supports. A query that uses a function the
  engine lacks fails on the engine.
