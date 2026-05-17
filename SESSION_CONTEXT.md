# Session context — pg_lake external writes + Polaris stack

This file is committed on `marcoslot/external-writes-create-drop` so it
moves with the work. It is not meant for upstream merge — drop or
.gitignore it before opening the PR for review.

## Branch stack (Snowflake-Labs/pg_lake)

All three branches are pushed.

```
main
└── marcoslot/external-writes              [PR #247, draft]
    └── marcoslot/external-writes-create-drop  [this branch, ready, no PR yet]
└── marcoslot/polaris-extension            [stacked on external-writes,
                                            separate skill area, pushed]
```

The Polaris work sits **off** `marcoslot/external-writes` (the older
branch state), not off this branch. It will need a rebase onto
`external-writes-create-drop` once that's merged or to pick up the new
trigger architecture. See "Polaris status" below.

## What's done on this branch (`marcoslot/external-writes-create-drop`)

PR #247 added UPDATE support to `pg_catalog.iceberg_tables` for the
internal catalog. This branch extends it with INSERT and DELETE so
external clients (Spark, PyIceberg) can fully manage pg_lake-managed
Iceberg tables symmetrically.

Final design after several iterations: **two sibling INSTEAD OF triggers
on `pg_catalog.iceberg_tables`**, one per extension, each handling only
its half of the catalog discriminated by `catalog_name`:

| Trigger                                          | Owner             | Half it handles                                                                                        |
|--------------------------------------------------|-------------------|--------------------------------------------------------------------------------------------------------|
| `lake_iceberg.external_catalog_modification`     | `pg_lake_iceberg` | external (`catalog_name <> current_database()`); forwards to `tables_external` helpers                 |
| `lake_table.internal_catalog_modification`       | `pg_lake_table`   | internal (`catalog_name = current_database()`); UPDATE drives sync, INSERT does CREATE FOREIGN TABLE + sync, DELETE does DROP FOREIGN TABLE |

Each no-ops on the other extension's half. `pg_lake_iceberg`'s trigger
errors clearly if `pg_lake_table` isn't installed and the row is
internal-catalog (otherwise Postgres treats the returned rettuple as
"trigger handled it" and the INSERT silently disappears). Cross-boundary
catalog renames are rejected by `pg_lake_iceberg`'s trigger so the error
is consistent regardless of trigger fire order.

### Why this design (rejected alternatives, in order considered)

1. **Single trigger in pg_lake_iceberg, dispatch to pg_lake_table via
   SQL function lookup** (`LookupFuncName` + `OidFunctionCall`). User
   feedback: "inverts the dependency a bit much."
2. **Function-pointer hooks** (set by pg_lake_table's `_PG_init`).
   User feedback: "those hooks are kind of terrible too."
3. **Single trigger fully in pg_lake_table.** Hits a real constraint:
   `pg_lake_iceberg` is currently usable standalone for cataloging
   external Iceberg tables (PyIceberg writes to iceberg_tables). The
   existing `iceberg_extension` test fixture only installs
   pg_lake_iceberg. Moving the trigger out breaks that.
4. **Reverse the dependency** so pg_lake_iceberg requires pg_lake_table.
   Considered but rejected as too invasive for this change.
5. **Two siblings (chosen).** Cheap (each parses TriggerData, then
   bails on the half it doesn't own), no ordering dependency, no
   callbacks, no hooks.

### Other key design decisions

- **DELETE is no-CASCADE.** Dependent objects (views, MVs) belong to
  Postgres-side users, not external clients. User must drop them first.
- **INSERT is eager.** Sync runs at INSERT time so type-translation or
  unreachable-metadata errors surface immediately.
- **`SkipIcebergDDLProcessing` flag is now load-bearing for INSERT.**
  Set around the synthesized CREATE FOREIGN TABLE so pg_lake_table's
  own DDL path skips `ErrorIfLocationIsNotEmpty` (the external client
  wrote metadata there) and `TrackIcebergMetadataChangesInTx` (would
  clobber the supplied metadata file). Also gates field_id_mapping
  registration in `ApplyDDLChanges`.
- **`SyncSchemaFromMetadata` now skips ADD COLUMN if the column already
  exists** (by name), so the post-CREATE sync can register
  field_id_mappings without column-already-exists errors.
- **`sync_iceberg_metadata_from_external_write` SQL function was
  removed** (was unused after the refactor; static C helper
  `SyncFromExternalMetadata` used internally). Easy to re-add if a
  future extension needs it.

### Files of interest

- `pg_lake_iceberg/src/iceberg/external_metadata_modification.c` — the
  external-half trigger.
- `pg_lake_table/src/sync/sync_external_metadata.c` — the internal-half
  trigger (`internal_catalog_modification`), `SyncFromExternalMetadata`
  static helper, and the original sync logic.
- `pg_lake_table/pg_lake_table--3.3--3.4.sql` — installs the new trigger
  function and `internal_catalog_modifications_trg`.
- `pg_lake_table/src/ddl/create_table.c` line ~919 —
  `ErrorIfLocationIsNotEmpty` is gated by `SkipIcebergDDLProcessing`.
- `pg_lake_table/src/ddl/ddl_changes.c` line ~65, ~137 —
  `SkipIcebergDDLProcessing` gates `TrackIcebergMetadataChangesInTx`
  and `RegisterPostgresColumnMappings` for `DDL_TABLE_CREATE`.
- `pg_lake_table/include/pg_lake/ddl/alter_table.h` —
  `SkipIcebergDDLProcessing` extern declaration (now part of the public
  API across pg_lake extensions).

### Test coverage

22/22 in `pg_lake_table/tests/pytests/test_external_write.py` (added
positive INSERT/DELETE tests + `test_external_write_drop_with_dependent_view_errors`).
4/4 in `pg_lake_iceberg/tests/pytests/test_iceberg_catalog.py`. Broader
124-test pg_lake_iceberg suite passes.

### Open items before opening the PR

1. **Test scaffolding**: `iceberg_extension` fixture only installs
   pg_lake_iceberg, not pg_lake_table. The
   `test_create_in_internal_catalog` test now asserts the new error
   "requires the pg_lake_table extension". Reasonable.
2. **Foreign-table ownership**: synthesized foreign tables are owned by
   the pg_lake_table extension owner because the SPI runs under that
   role. Test queries via `app_user` get `permission denied`. The
   `test_external_write_create_table_via_pyiceberg` test queries via
   `superuser_conn` to work around. Real-world, the trigger should
   probably `ALTER TABLE ... OWNER TO <connected user>`. Not critical
   for v0.1; flag for review.
3. **Branch is not yet a PR.** Push is at `61b729c`. Open PR with
   base = `marcoslot/external-writes` once #247 progresses.

## Polaris status (`marcoslot/polaris-extension` branch)

That branch implements an experimental `pg_lake_polaris` extension — a
two-way sync between pg_lake's iceberg catalog and a co-resident Apache
Polaris metastore, both in the same Postgres database. **Pushed at `051dffd`.**

### Design summary
- Outbound: trigger on `lake_iceberg.tables_internal` mirrors changes
  to `polaris_schema.entities`.
- Inbound: trigger on `polaris_schema.entities` for TABLE_LIKE rows;
  INSERT synthesizes a CREATE FOREIGN TABLE, UPDATE drives the sync,
  DELETE drops the foreign table.
- Loop prevention via a process-global `PgLakePolarisSuppress` flag
  (mirrors `SkipIcebergDDLProcessing`). Self-canceling state (delete
  entity_link first) is the load-bearing mechanism; the flag is
  defense-in-depth.
- Federation alternative (Polaris 1.2+ supports `ICEBERG_REST`
  federation) was considered and rejected: requires a REST shim
  process, undesirable in the managed-service environment.

### Files
```
pg_lake_polaris/
├── pg_lake_polaris.control          # requires pg_lake_iceberg, pg_lake_table; schema = lake_polaris
├── pg_lake_polaris--0.1.sql         # ~700 lines: schema, all triggers, register/unregister/bootstrap
├── Makefile
├── src/init.c                       # _PG_init, GUC, suppress flag (C global)
├── include/pg_lake/pg_lake_polaris.h
└── tests/
    ├── conftest.py                  # polaris_schema fixture (ports h2/schema-v1.sql to Postgres)
    └── pytests/
        ├── test_polaris_outbound.py # 4 dispatch tests
        ├── test_polaris_register.py # 4 register/unregister/bootstrap tests
        ├── test_polaris_inbound.py  # 4 dispatch tests
        └── test_polaris_e2e.py      # 2 outbound e2e tests against real CREATE TABLE USING iceberg
```

14/14 tests pass on that branch.

### Polaris design decisions worth remembering
- Schema is `lake_polaris` (not `pg_lake_polaris` — `pg_` prefix is
  reserved by Postgres).
- Polaris realm_id supplied via GUC `pg_lake_polaris.realm_id`,
  snapshotted into `lake_polaris.catalog_mapping` at register time.
- Polaris entity type codes hardcoded: CATALOG=4, NAMESPACE=6,
  TABLE_LIKE=7, sub_type ICEBERG_TABLE=2 (from Polaris's
  `PolarisEntityType.java`).
- Random ID allocation (`random() * INT64_MAX`) — collides with
  multi-instance Polaris coordinators, documented as v0.1 limitation.
- Inbound INSERT path supports only top-level scalar Iceberg types in
  v0.1 (struct/list/map error). Tested only at the dispatch level; full
  e2e blocked by pg_lake's `deletion_queue_pkey` invariant when two
  iceberg tables share a metadata file (a real pg_lake limitation, not
  a polaris bug — documented in `test_polaris_e2e.py`).

### Polaris next steps (after this branch lands)
1. **Rebase Polaris branch onto `external-writes-create-drop`.** With
   INSERT/DELETE on iceberg_tables now supported, the polaris extension
   can simplify: instead of synthesizing CREATE FOREIGN TABLE itself
   in `inbound_sync_insert`, it can just `INSERT INTO iceberg_tables`
   and let the trigger do the work. Same for inbound DELETE → `DELETE
   FROM iceberg_tables`. That removes the polaris extension's hardest
   code path (~150 lines of plpgsql) and reuses the now-shared trigger
   infrastructure.
2. **Reconsider naming**: `register_catalog(name)` could default to
   `current_database()` per the user's earlier observation that pg_lake
   already keys internal-catalog rows by `current_database()`. The
   parameter becomes an override.
3. Consider whether the polaris branch should target Snowflake-Labs/pg_lake
   at all, or live somewhere snowflake-private. It's "experimental" but
   couples to Polaris's private `polaris_schema.entities` layout.

## Quick resume checklist

When picking this up on a different machine:

```bash
# clone + bootstrap (use the bootstrap-workspace skill if needed)
git clone git@gho:Snowflake-Labs/pg_lake.git ~/lake-external-writes
cd ~/lake-external-writes

# the three branches in this stack
git fetch origin
git checkout marcoslot/external-writes-create-drop
git checkout marcoslot/polaris-extension

# build + test (any branch)
make install-fast
cd pg_lake_table && PYTHONPATH=../test_common pipenv run pytest tests/pytests/test_external_write.py
cd ../pg_lake_iceberg && PYTHONPATH=../test_common pipenv run pytest tests/pytests/test_iceberg_catalog.py

# polaris-specific
cd ../pg_lake_polaris && PYTHONPATH=../test_common pipenv run pytest tests/pytests/  # only on the polaris branch
```

Use the `review-inbox` skill to get current PR status. The
`address-reviews` skill handles applying review feedback as new commits.
