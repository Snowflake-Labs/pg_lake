# pg_lake_cortex

A PostgreSQL extension that brings the [Snowflake Cortex AI functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) into Postgres. Every call runs on a Snowflake account through a [`pg_lake_snowflake`](../docs/snowflake-tables.md) foreign server, with the credentials of the caller's user mapping and on the server's warehouse.

```sql
CREATE EXTENSION pg_lake_cortex CASCADE;

CREATE SERVER my_account FOREIGN DATA WRAPPER snowflake
  OPTIONS (account 'myorg-myaccount', warehouse 'AI_WH');
CREATE USER MAPPING FOR CURRENT_USER SERVER my_account
  OPTIONS (token '<programmatic access token>');

SET lake_cortex.default_server TO my_account;

SELECT lake_cortex.complete('llama3.1-8b', 'Write a haiku about Postgres');
SELECT lake_cortex.sentiment('I love this product');                       -- positive
SELECT lake_cortex.classify('My order is late', ARRAY['praise', 'complaint']); -- {complaint}
SELECT lake_cortex.embed('snowflake-arctic-embed-m-v1.5', 'hello');         -- float8[]
```

See [the documentation](../docs/cortex-functions.md) for the full list of functions and things to know (one round trip per call, billing, permissions).

## How it works

The extension contains only SQL and has no shared library. Each function builds one Cortex expression and runs it with `lake_snowflake.execute` as `SELECT TO_JSON(TO_VARIANT(<expression>))`. The JSON answer is then turned into the Postgres return type.

Arguments are inlined as Snowflake string literals rather than bound, because Cortex requires the model (and some other arguments) to be literals. Snowflake strings treat a backslash as an escape, so `lake_cortex.sql_literal` doubles backslashes as well as quotes.

## Building and testing

The extension is part of the pg_lake build (`make install` at the top level), or on its own:

```bash
make -C pg_lake_cortex install
make -C pg_lake_cortex check      # against the mock SQL API from pg_lake_snowflake
make -C pg_lake_cortex check-e2e  # against a real account, see tests/e2e
```

The end-to-end tests use the same `SNOWFLAKE_*` environment variables as the `pg_lake_snowflake` ones. The role needs the `SNOWFLAKE.CORTEX_USER` database role.
