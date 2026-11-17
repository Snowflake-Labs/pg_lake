# Snowflake Cortex AI functions

`pg_lake_cortex` makes [Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api) models callable from Postgres. Each call is a request to the Cortex REST API of a Snowflake account, reached through a [`pg_lake_snowflake`](./snowflake-tables.md) foreign server, so the models, the billing and the governance are the account's. A request runs no SQL and needs no warehouse: the account bills the tokens the model processes.

## Setup

```sql
CREATE EXTENSION pg_lake_cortex CASCADE;

-- no warehouse needed
CREATE SERVER my_account FOREIGN DATA WRAPPER snowflake
  OPTIONS (account 'myorg-myaccount');
CREATE USER MAPPING FOR CURRENT_USER SERVER my_account
  OPTIONS (token '<programmatic access token>');

-- the server the functions use when no server_name is passed
SET pg_lake_cortex.default_server TO my_account;
```

Any credential of a `pg_lake_snowflake` user mapping works: a programmatic access token, an OAuth token or key-pair authentication. The Cortex REST API takes no role, so the request is authorized by the **default role** of the Snowflake user, and that role needs the `SNOWFLAKE.CORTEX_USER` (or `SNOWFLAKE.CORTEX_REST_API_USER`) database role. The `role` option of the server does not apply. The account also needs access to the models you call.

## Functions

Every function also takes an optional `server_name` argument that overrides `pg_lake_cortex.default_server`.

| Function | Cortex REST endpoint | Returns |
|---|---|---|
| `cortex.complete(model, prompt [, model_parameters jsonb])` | chat completions | `text` |
| `cortex.complete_json(model, prompt, response_format jsonb [, model_parameters jsonb])` | chat completions with structured output | `jsonb` |
| `cortex.embed(model, input)` | embed | `float8[]` |
| `cortex.embed_batch(model, inputs text[])` | embed, one request for all inputs | `TABLE (index int, embedding float8[])` |
| `cortex.similarity(input1, input2 [, model])` | embed, one request for both | `float8`, cosine similarity |
| `cortex.request(path, body jsonb)` | any endpoint below `/api/v2/cortex/` | `jsonb` |

```sql
SELECT cortex.complete('claude-sonnet-4-5', 'Write a haiku about Postgres');

SELECT cortex.complete_json('llama3.1-70b', 'Extract the city from: ' || address,
  '{"type":"json","schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}');
```

- `model_parameters` takes `temperature`, `top_p` and `max_tokens`, as `AI_COMPLETE` does. Other fields are passed to the chat completions API as they are.
- `response_format` takes the `AI_COMPLETE` form, `{"type": "json", "schema": ...}`, or the `json_schema` form of the chat completions API.
- `cortex.request` is for what the functions do not cover, such as conversations with several messages or the Anthropic-compatible messages endpoint.

A NULL input returns NULL without a request.

### Embeddings with pgvector

An embedding converts to a pgvector `vector` with a cast. `cortex.embed_batch` embeds many texts in one request, which makes filling a column much cheaper than one request per row:

```sql
CREATE EXTENSION vector;
ALTER TABLE reviews ADD COLUMN embedding vector(768);

UPDATE reviews r SET embedding = e.embedding::vector
FROM (SELECT array_agg(id ORDER BY id) AS ids, array_agg(body ORDER BY id) AS bodies FROM reviews) b,
     cortex.embed_batch('snowflake-arctic-embed-m-v1.5', b.bodies) e
WHERE r.id = b.ids[e.index];

-- embed the question once, in a subquery, and let the index do the rest
SELECT id, body FROM reviews
ORDER BY embedding <=> (SELECT cortex.embed('snowflake-arctic-embed-m-v1.5', 'broken on arrival')::vector)
LIMIT 5;
```

Compare vectors from the same model only.

## Things to know

- **One request per call.** `cortex.complete` sends one request for every row it is called on. `cortex.embed_batch` is the way to embed many rows at once.
- **The functions are `VOLATILE`.** Postgres does not cache their results, and evaluates a call once per row even when its arguments are constant. Put a constant call in a scalar subquery, as above, to make it run once.
- **Rate limits.** The account has a per-model limit on tokens and requests per minute. A request over the limit is retried a few times with a backoff and then fails.
- Access needs `USAGE` on the foreign server and a user mapping. The functions are granted to `lake_read`.
