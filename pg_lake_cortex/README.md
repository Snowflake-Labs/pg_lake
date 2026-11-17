# pg_lake_cortex

A PostgreSQL extension that brings [Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api) models into Postgres. Every call is a request to the Cortex REST API of a Snowflake account through a [`pg_lake_snowflake`](../docs/snowflake-tables.md) foreign server, with the credentials of the caller's user mapping. No warehouse is needed.

```sql
CREATE EXTENSION pg_lake_cortex CASCADE;

CREATE SERVER my_account FOREIGN DATA WRAPPER snowflake
  OPTIONS (account 'myorg-myaccount');
CREATE USER MAPPING FOR CURRENT_USER SERVER my_account
  OPTIONS (token '<programmatic access token>');

SET pg_lake_cortex.default_server TO my_account;

SELECT cortex.complete('claude-sonnet-4-5', 'Write a haiku about Postgres');
SELECT cortex.embed('snowflake-arctic-embed-m-v1.5', 'hello');              -- float8[]
SELECT * FROM cortex.embed_batch('snowflake-arctic-embed-m-v1.5', ARRAY['a', 'b']);
```

See [the documentation](../docs/cortex-functions.md) for the full list of functions and things to know (authorization by the default role, rate limits, one request per call).

## How it works

The extension contains only SQL and has no shared library. Each function builds the JSON body of a Cortex REST request and sends it with `lake_snowflake.cortex_request`, which `pg_lake_snowflake` provides: it reuses the account URL, the credentials and the retries of the SQL API client, and only accepts paths below `/api/v2/cortex/`. The JSON answer is then turned into the Postgres return type.

Inputs travel inside a JSON document, so unlike text inlined into SQL they need no escaping.

## Building and testing

The extension is part of the pg_lake build (`make install` at the top level), or on its own:

```bash
make -C pg_lake_cortex install
make -C pg_lake_cortex check      # against the mock of the Snowflake APIs from pg_lake_snowflake
make -C pg_lake_cortex check-e2e  # against a real account, see tests/e2e
```

The end-to-end tests use the same `SNOWFLAKE_*` environment variables as the `pg_lake_snowflake` ones, or `SNOWFLAKE_USER` and `SNOWFLAKE_PRIVATE_KEY_PATH` for key-pair authentication. The default role of the user needs the `SNOWFLAKE.CORTEX_USER` database role.
