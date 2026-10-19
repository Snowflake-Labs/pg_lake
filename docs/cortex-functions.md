# Snowflake Cortex AI functions

`pg_lake_cortex` makes the [Snowflake Cortex AI functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) callable from Postgres. Each call runs on a Snowflake account through a [`pg_lake_snowflake`](./snowflake-tables.md) foreign server, so the models, the billing and the governance are the account's: the call runs with the credentials of the caller's user mapping and on the server's warehouse.

## Setup

```sql
CREATE EXTENSION pg_lake_cortex CASCADE;

CREATE SERVER my_account FOREIGN DATA WRAPPER snowflake
  OPTIONS (account 'myorg-myaccount', warehouse 'AI_WH');
CREATE USER MAPPING FOR CURRENT_USER SERVER my_account
  OPTIONS (token '<programmatic access token>');

-- the server the functions use when no server_name is passed
SET lake_cortex.default_server TO my_account;
```

The Snowflake role needs the `SNOWFLAKE.CORTEX_USER` database role, and the account needs access to the models you call.

## Functions

Every function also takes an optional `server_name` argument that overrides `lake_cortex.default_server`.

| Function | Cortex function | Returns |
|---|---|---|
| `lake_cortex.complete(model, prompt [, model_parameters jsonb])` | `AI_COMPLETE` | `text` |
| `lake_cortex.complete_json(model, prompt, response_format jsonb [, model_parameters jsonb])` | `AI_COMPLETE` with structured output | `jsonb` |
| `lake_cortex.classify(input, categories text[])` | `AI_CLASSIFY` | `text[]` labels |
| `lake_cortex.filter(predicate, input)` | `AI_FILTER` | `boolean` |
| `lake_cortex.sentiment(input)` | `AI_SENTIMENT` | `positive`, `negative`, `neutral` or `mixed` |
| `lake_cortex.translate(input, source_language, target_language)` | `AI_TRANSLATE` | `text` (NULL source language is detected) |
| `lake_cortex.summarize(input)` | `SNOWFLAKE.CORTEX.SUMMARIZE` | `text` |
| `lake_cortex.extract(input, response_format jsonb)` | `AI_EXTRACT` | `jsonb` answers |
| `lake_cortex.redact(input)` | `AI_REDACT` | `text` |
| `lake_cortex.embed(model, input)` | `AI_EMBED` | `float8[]` |
| `lake_cortex.similarity(input1, input2)` | `AI_SIMILARITY` | `float8` |
| `lake_cortex.count_tokens(function_name, model, input)` | `AI_COUNT_TOKENS` | `bigint` |
| `lake_cortex.invoke(expression [, server_name])` | any Snowflake expression | `jsonb` |

```sql
SELECT id, lake_cortex.sentiment(body) FROM reviews WHERE created_at > now() - interval '1 day';

SELECT lake_cortex.complete_json('llama3.1-70b', 'Extract the city from: ' || address,
  '{"type":"json","schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}');
```

A NULL input returns NULL without a round trip.

## Things to know

- **One statement per call.** Each function call is a round trip to the account (a few hundred milliseconds at least), so calling one over a large table is slow. For bulk work over Snowflake tables, run the Cortex function in Snowflake, for example via `lake_snowflake.execute` or a foreign table on a view.
- **Arguments are sent as literals.** Cortex needs the model, and some other arguments, to be literals, so values are inlined as escaped Snowflake string literals. Snowflake strings treat a backslash as an escape, and backslashes are doubled as well as quotes.
- **The functions are `VOLATILE`.** Postgres does not cache their results, and every call is billed by the account.
- Access needs `USAGE` on the foreign server and a user mapping. The functions are granted to `lake_read`.
