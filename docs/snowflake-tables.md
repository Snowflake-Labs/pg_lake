# Snowflake tables

The `pg_lake_snowflake` extension attaches tables that live inside Snowflake as
PostgreSQL foreign tables, the same way `pg_lake` attaches an external Iceberg
table: you point a foreign table at the remote object, the columns are inferred,
and queries read it in place.

Every scan becomes a `SELECT` that Snowflake executes over its
[SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/index), so one
mechanism covers standard tables, [hybrid
tables](https://docs.snowflake.com/en/user-guide/tables-hybrid), dynamic tables,
Snowflake-managed Iceberg tables and views. Nothing is copied and no data files
are read directly, which is what makes hybrid tables reachable at all: they have
no file layout for a lake reader to open.

Attached tables can be read and written: `INSERT`, `COPY`, `UPDATE`, `DELETE` and
`TRUNCATE` all work, with the limits described under
[Writing](#writing) below.

## Setting up

```sql
CREATE EXTENSION pg_lake_snowflake CASCADE;
```

A server describes one account, and a user mapping carries the credentials:

```sql
CREATE SERVER sf FOREIGN DATA WRAPPER snowflake
  OPTIONS (account 'myorg-myaccount',
           database 'ANALYTICS',
           schema_name 'PUBLIC',
           warehouse 'MY_WH',
           role 'MY_ROLE');

-- a programmatic access token or an OAuth access token
CREATE USER MAPPING FOR CURRENT_USER SERVER sf
  OPTIONS (token 'eyJ...');

-- or key-pair authentication, which is the usual choice for a service account
CREATE USER MAPPING FOR CURRENT_USER SERVER sf
  OPTIONS (user 'MY_USER', private_key_path '/etc/postgresql/snowflake_key.p8');
```

Check that the account, the credentials, the warehouse and the role work together
before attaching anything:

```sql
SELECT lake_snowflake.test_connection('sf');
-- version 10.31.103, account AB12345, warehouse MY_WH, role MY_ROLE, database ANALYTICS, schema PUBLIC
```

### Server options

| Option | Meaning |
| --- | --- |
| `account` | Account identifier, e.g. `myorg-myaccount`. The host is derived from it. |
| `account_url` | Full `https://host` for a regional or private-link host. Give `account` as well when you use key-pair authentication. |
| `database` | Default Snowflake database for tables on this server. |
| `schema_name` | Default Snowflake schema. |
| `warehouse` | Warehouse that runs the statements. Without one, every query fails with *no active warehouse selected*. |
| `role` | Role the statements run as. |
| `statement_timeout` | Seconds Snowflake may spend on one statement. Overrides `pg_lake_snowflake.statement_timeout`. |
| `enable_aggregate_pushdown` | Set to `false` to keep grouping and aggregation local for this server. |
| `batch_size` | Rows one `INSERT` sends. Defaults to `pg_lake_snowflake.batch_size`. |
| `updatable` | Set to `false` to make every table on this server read-only. |

### User mapping options

| Option | Meaning |
| --- | --- |
| `token` | Programmatic access token or OAuth access token, sent as a bearer token. |
| `user` | Snowflake user. Required for key-pair authentication. |
| `private_key` | PEM private key for key-pair authentication. |
| `private_key_path` | Path to a PEM private key, read as the operating-system user that runs PostgreSQL. Superuser only. |
| `private_key_passphrase` | Passphrase of an encrypted private key. |
| `authenticator` | `pat`, `oauth` or `keypair`. Inferred from the other options when omitted. |

A programmatic access token expires and Snowflake refuses it once it has, which
shows up as `Snowflake rejected the credentials of server "sf"`. Key-pair
authentication needs the public key registered on the Snowflake user with
`ALTER USER ... SET RSA_PUBLIC_KEY = '...'`; the JWT is signed locally and cached
for the session.

### Foreign table options

| Option | Meaning |
| --- | --- |
| `database`, `schema_name`, `table_name` | Which Snowflake table this is. |
| `row_estimate` | Rows to assume for planning, instead of analyzing. |
| `batch_size` | Rows one `INSERT` sends, overriding the server. |
| `updatable` | Set to `false` to make this table read-only. |
| `column_name` (per column) | The Snowflake name of a column, when it is not the upper-case of the Postgres one. |

## Attaching tables

Give no column list and the columns are inferred from Snowflake:

```sql
CREATE FOREIGN TABLE orders () SERVER sf OPTIONS (table_name 'ORDERS');

\d orders
--    Column    |     Type      | ...
--  o_orderkey  | bigint        |
--  o_totalprice| numeric(12,2) |
```

A whole schema at once:

```sql
CREATE SCHEMA sf_public;
IMPORT FOREIGN SCHEMA "PUBLIC" FROM SERVER sf INTO sf_public;

-- a schema in another database, and only some of its tables
IMPORT FOREIGN SCHEMA "SALES.PUBLIC" LIMIT TO (orders, lineitem)
  FROM SERVER sf INTO sf_sales;
```

Importing describes each table with one statement, so importing a schema with
many tables costs one round trip per table.

### Names and case

Snowflake folds an unquoted identifier to upper case and PostgreSQL folds it to
lower case, so `orders` here means `ORDERS` there. The rules are:

- A name that comes from an option (`table_name`, `schema_name`, `database`,
  `column_name`) is used exactly as written, because you spelled out the
  Snowflake name.
- A name that comes from the PostgreSQL catalog is upper-cased when it is a plain
  lower-case name, which is exactly reversing what PostgreSQL did to it.

`IMPORT FOREIGN SCHEMA` follows the same rule in reverse: an all-upper-case
Snowflake name becomes a lower-case PostgreSQL name, and any other name is kept
as it is and recorded in a `column_name` or `table_name` option. Note that the
remote schema name in `IMPORT FOREIGN SCHEMA` is parsed by PostgreSQL, so write
`"MixedCase"` in quotes if that is how the schema is spelled in Snowflake.

## Types

| Snowflake | PostgreSQL |
| --- | --- |
| `NUMBER(p,0)`, p ≤ 9 | `integer` |
| `NUMBER(p,0)`, p ≤ 18 | `bigint` |
| `NUMBER(p,s)` otherwise | `numeric(p,s)` |
| `FLOAT`, `DOUBLE`, `REAL` | `double precision` |
| `VARCHAR`, `CHAR`, `STRING` | `text` |
| `BOOLEAN` | `boolean` |
| `DATE` | `date` |
| `TIME` | `time` |
| `TIMESTAMP_NTZ` | `timestamp` |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ` | `timestamptz` |
| `BINARY`, `VARBINARY` | `bytea` |
| `VARIANT`, `OBJECT`, `ARRAY`, `MAP`, `VECTOR` | `jsonb` |
| `GEOGRAPHY`, `GEOMETRY` | `text`, as GeoJSON |

Character types become `text` rather than a length-limited `varchar`, so that a
value can never fail to arrive because of a length that only matters on the
Snowflake side. A `TIMESTAMP_TZ` keeps its instant but not the offset it was
written with, because PostgreSQL does not store one.

You can also declare the columns yourself, and a declared type that differs from
the mapping above is converted through its text representation:

```sql
CREATE FOREIGN TABLE events (
  id     bigint,
  payload text            -- a VARIANT read as its JSON text
) SERVER sf OPTIONS (table_name 'EVENTS');
```

## What runs in Snowflake

`EXPLAIN` shows the statement a scan sends, which is the only way to see what was
pushed down:

```sql
EXPLAIN (VERBOSE, COSTS OFF) SELECT name FROM ht WHERE id = 2;
--  Foreign Scan on ht
--    Snowflake SQL: SELECT "NAME" FROM "DB"."PUBLIC"."HT" WHERE (("ID" = 2))
```

Pushed down:

- only the columns the query reads,
- restriction clauses over `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`,
  `IS NULL`, `IN`, `LIKE` and `+`, `-`, `*`,
- `LIMIT` and `OFFSET`, when nothing between the scan and the limit changes which
  rows it keeps,
- `GROUP BY` with `COUNT`, `SUM`, `MIN`, `MAX`, and `HAVING`,
- parameters of a prepared statement, which are spliced into the statement when
  it runs,
- the whole of an `UPDATE`, a `DELETE` or a `TRUNCATE`, which is the only way
  those run at all (see [Writing](#writing)).

Deliberately not pushed down, because Snowflake would answer differently rather
than fail:

- ordering comparisons (`<`, `>`) and `MIN`/`MAX` over character types, since
  PostgreSQL orders by its collation and Snowflake by code point,
- `/` and `%`, since integer division truncates in PostgreSQL and produces a
  decimal in Snowflake,
- `AVG` over exact numerics, since Snowflake decides the scale of the result by
  its own rules and returns fewer digits,
- anything over a column with a non-default Snowflake collation. `=` on such a
  column may mean something else there; declare the table by hand and leave the
  comparison local if you have one.

Everything else is evaluated by PostgreSQL after the rows arrive, so a query
always returns the same answer with pushdown as without it.

## Writing

```sql
INSERT INTO orders (o_orderkey, o_totalprice) VALUES (1, 9.99), (2, 12.50);
INSERT INTO orders SELECT * FROM local_orders;
COPY orders (o_orderkey, o_totalprice) FROM '/tmp/orders.csv' WITH (format csv);

UPDATE orders SET o_totalprice = o_totalprice * 2 WHERE o_orderkey = 1;
DELETE FROM orders WHERE o_orderkey > 100;
TRUNCATE orders;
```

### Writes are not part of your transaction

Snowflake's SQL API has no session that spans requests, so **each statement
commits on its own**. A `ROLLBACK` does not undo a write that already went out,
and a statement that fails half way through a multi-statement load leaves the
batches before it in place. Writing inside a transaction block warns about this
once per transaction; set
`pg_lake_snowflake.warn_on_write_in_transaction_block` to `off` if you would
rather not hear it.

That is a property of the transport, not a setting: nothing here can make a
Snowflake write participate in a PostgreSQL transaction.

### INSERT and COPY

Rows are sent as batched `INSERT` statements, `pg_lake_snowflake.batch_size` rows
per statement (500 by default, or the `batch_size` option of the table or
server). A batch is also split when its text would grow past what Snowflake
accepts, so a wide table does not need a smaller setting than a narrow one. One
statement is one round trip, so the batch size is what decides how fast a load
runs.

Two details worth knowing:

- A column your `INSERT` does not mention is left out of the statement, so the
  `DEFAULT` or the `AUTOINCREMENT` that Snowflake has for it applies. A column
  that has a PostgreSQL default on the foreign table is sent, because then the
  value in the row *is* that default.
- `COPY` cannot see which columns you listed, so it sends all of them and the
  ones you left out arrive as NULL rather than as the Snowflake default.

`RETURNING` is not supported: Snowflake answers a modification with a row count
rather than with the rows it changed. Neither is `ON CONFLICT`.

### UPDATE and DELETE

An `UPDATE` or a `DELETE` is only ever sent as one statement that Snowflake
evaluates in full:

```sql
EXPLAIN (VERBOSE, COSTS OFF) UPDATE ht SET name = 'x' WHERE id = 2;
--  Update on ht
--    ->  Foreign Update on ht
--          Snowflake SQL: UPDATE "DB"."PUBLIC"."HT" SET "NAME" = 'x' WHERE (("ID" = 2))
```

There is no row-by-row fallback, and that is deliberate. PostgreSQL modifies rows
it has already read, identified by a row identifier the wrapper carries along, and
a Snowflake table exposes nothing that identifies a row: no `ctid`, no rowid, and
a primary key only on a hybrid table. Matching rows by value instead would change
the wrong number of rows as soon as two of them are equal.

So the statement is refused when any part of it would have to be evaluated here:

```sql
UPDATE ht SET name = 'x' WHERE name > 'a';
-- ERROR:  cannot update the Snowflake table "ht" one row at a time
-- DETAIL:  A Snowflake table has no row identifier, so the whole statement has to
--          be one Snowflake can evaluate: its conditions and its assignments must
--          refer only to "ht" and be of a kind that is pushed down.
```

The way out is to make the condition one Snowflake can evaluate — see
[What runs in Snowflake](#what-runs-in-snowflake) for which ones those are. A
condition that names another table, a subquery, or an ordering comparison over
text all keep the statement local and therefore refuse it.

### TRUNCATE

`TRUNCATE` becomes Snowflake's own `TRUNCATE TABLE`, one statement per table.
`RESTART IDENTITY` is refused, because Snowflake does not expose the state of a
column's sequence. `CASCADE` is not passed on: there is nothing in Snowflake for
it to mean, and a hybrid table with dependents refuses the truncation itself.

### Making a table read-only

A credential that is not supposed to write is best described to PostgreSQL, so
that it reports the refusal without a round trip:

```sql
ALTER SERVER sf OPTIONS (ADD updatable 'false');
ALTER FOREIGN TABLE orders OPTIONS (ADD updatable 'false');
-- ERROR:  foreign table "orders" does not allow inserts
```

## Statistics

`ANALYZE` samples the table with Snowflake's own row sampling and counts its rows,
which gives the planner real statistics:

```sql
ANALYZE orders;
```

Until then the planner assumes `pg_lake_snowflake.default_row_estimate` rows, or
the `row_estimate` option of the table if it has one.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `pg_lake_snowflake.default_row_estimate` | `1000000` | Rows assumed for a table that has never been analyzed. |
| `pg_lake_snowflake.statement_timeout` | `300s` | Seconds Snowflake may spend on one statement. |
| `pg_lake_snowflake.enable_aggregate_pushdown` | `on` | Whether grouping and aggregation may run remotely. |
| `pg_lake_snowflake.log_remote_sql` | `off` | Log every statement sent to Snowflake. |
| `pg_lake_snowflake.batch_size` | `500` | Rows one `INSERT` statement sends. |
| `pg_lake_snowflake.warn_on_write_in_transaction_block` | `on` | Warn once per transaction that a write inside a transaction block will not be rolled back. |
| `pg_lake_snowflake.allow_plain_http` | `off` | Allow an `account_url` that is not https. For tests against a local mock of the SQL API. |

## Running statements directly

```sql
SELECT lake_snowflake.execute('sf', 'ALTER WAREHOUSE my_wh RESUME');
```

`lake_snowflake.execute` runs one statement with the credentials of the user
mapping that applies to the caller and returns the first column of the first row,
which is what `SHOW`, `DESCRIBE` and DDL answer with. It needs `USAGE` on the
foreign server.

## Limitations

- A write is not part of your transaction, an `UPDATE` or a `DELETE` has to be one
  Snowflake can evaluate in full, and `RETURNING` and `ON CONFLICT` are not
  supported. See [Writing](#writing).
- Joins are not pushed down: a join between two Snowflake tables reads both and
  joins them in PostgreSQL.
- `ORDER BY` is not pushed down.
- A result set is held in memory while it is read, so a query that returns
  hundreds of millions of rows is better expressed as an aggregate, or as an
  Iceberg table read through `pg_lake` if the data is available that way.
- A statement is cancellable once Snowflake has reported its handle, which happens
  when it has run for longer than the API holds a submission open (about 45
  seconds). Cancelling before that returns control to you immediately but leaves
  the statement running until its own timeout, which is what `statement_timeout`
  bounds.
