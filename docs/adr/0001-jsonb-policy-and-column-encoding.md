# Separate the JSONB storage policy from each column's encoding

## Decision

`jsonb_storage` is a write policy, not a live interpretation switch:

- `pg_lake_engine.jsonb_storage` is the session default adopted by a newly
  created Iceberg table and the policy for standalone Parquet `COPY TO`.
- An Iceberg table's `jsonb_storage` option is the policy for top-level
  `jsonb` columns created in that table. An absent option means `string`.
- Each column persists the encoding chosen when it is created. Reads and later
  writes follow that encoding, regardless of the current session or table
  policy.

Changing or dropping the table option affects only columns added afterwards;
it never rewrites or reinterprets existing columns. `load_from`,
`definition_from`, CTAS, and `LIKE` create target columns, so the target policy
wins rather than inheriting a source encoding.

Only top-level `jsonb`, including a domain over `jsonb`, is eligible for
`variant`. PostgreSQL `json` must remain string-backed to preserve its input
text, and arrays and composite fields retain their existing nested encodings.
CSV and JSON exports are unaffected.

## Consequences

One table can contain string-backed and variant-backed `jsonb` columns.
Adoption can therefore be gradual and deterministic without depending on
session state. The field mapping is the source of truth for managed tables;
external files are interpreted from their physical schema.

A top-level JSON null cannot be represented distinctly from SQL `NULL` by the
current DuckDB VARIANT path, so variant writes reject the JSON scalar rather
than silently changing its meaning. Nested JSON null remains supported.
