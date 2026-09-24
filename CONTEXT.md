# Data Lake Type Representation

This context describes how pg_lake preserves PostgreSQL data semantics while using data-lake physical types.

## Language

**Surface type**:
The PostgreSQL type and semantics visible to SQL users, independent of how the value is physically stored.
_Avoid_: Logical type, declared type

**Storage encoding**:
The physical data-lake type used for one column, which may differ from its surface type.
_Avoid_: Storage type, backing type

**JSONB storage policy**:
The choice applied when creating a new top-level `jsonb` column or exporting `jsonb` without an existing column encoding: `string` or `variant`.
_Avoid_: Variant enablement, variant mode

**Persisted column encoding**:
The storage encoding fixed for a column when that column is created. It remains authoritative for later reads and writes even if the table policy changes.
_Avoid_: Current setting, table encoding

**Variant-backed column**:
A column whose persisted column encoding is `variant` while its surface type is `jsonb`.
_Avoid_: Variant column
