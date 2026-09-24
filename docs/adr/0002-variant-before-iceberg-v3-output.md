# Allow variant encoding before emitting Iceberg v3 tables

## Decision

pg_lake may store a column with the Iceberg `variant` type while continuing to
emit format-version 2 table metadata.

This deliberately violates the Iceberg specification, which introduces
`variant` in version 3. We accept that cost to support parsed JSONB storage
without claiming support for the rest of the v3 write surface, especially
deletion vectors.

## Consequences

Any Iceberg table containing a variant-backed column is pg_lake-only for now.
Other engines are entitled to reject or misread its metadata. The deviation is
documented publicly and covered by a test that pins the emitted format version
to 2, so a future move to compliant v3 output must be an explicit decision
rather than an accidental metadata change.
