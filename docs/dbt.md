---
title: dbt
parent: User guide
nav_order: 6
---

# dbt

[dbt](https://www.getdbt.com/) works with pg_lake through the standard `dbt-postgres` adapter, so you can build and incrementally update both regular PostgreSQL tables and Iceberg tables from dbt models. You need `dbt-core` 1.9.2 or later and `dbt-postgres` 1.9.0 or later.

## Connection

dbt connects to PostgreSQL like any other client. The user needs the `lake_read_write` role to create Iceberg tables (see [roles and permissions](configuration.md#roles-and-permissions)).

<!-- The raw tags stop the docs site from evaluating dbt's Jinja. {% raw %} -->
```yaml
my_dbt_project:
  outputs:
    dev:
      type: postgres
      host: pglake.example.com
      user: postgres
      password: "{{ env_var('DBT_PASSWORD') }}"
      port: 5432
      dbname: postgres
      schema: public
      threads: 4
  target: dev
```

## Storage location

Iceberg tables are stored under `pg_lake_iceberg.default_location_prefix`. If it is not already set for the database or user, pass it to dbt through an environment variable:

```bash
export ICEBERG_LOCATION_PREFIX=<S3 location>
```

To see the current setting, run:

```bash
psql <connection-string> -c 'show pg_lake_iceberg.default_location_prefix'
```

Changing `pg_lake_iceberg.default_location_prefix` requires superuser, so the `pre_hook` below
only works when dbt connects as a superuser. Otherwise, have a superuser set it once for the
database, and leave it out of the hook:

```sql
ALTER DATABASE postgres SET pg_lake_iceberg.default_location_prefix TO 's3://mybucket/iceberg';
```

## Model configuration

The model configuration controls how the transformation process behaves. 

- `materialized='incremental'`: This tells dbt to perform incremental updates instead of fully rebuilding the table each time.
- `incremental_strategy='delete+insert'`: Iceberg tables do not support `MERGE`, so new versions of existing rows are applied by deleting and inserting them.
- `unique_key='created_at'`: This specifies the unique identifier for each record, used to detect new records.
- `pre_hook` and `post_hook`: These hooks are executed before and after the model runs. In this case, the `pre_hook` sets the default access method to `iceberg` and configures the location prefix for storing Iceberg tables in S3. The `post_hook` resets these settings after the model has completed.

```jinja
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='created_at',
    pre_hook="SET default_table_access_method TO 'iceberg'; SET pg_lake_iceberg.default_location_prefix = '{{ env_var('ICEBERG_LOCATION_PREFIX', '') }}';",
    post_hook="RESET default_table_access_method; RESET pg_lake_iceberg.default_location_prefix;"
) }}
```
<!-- {% endraw %} -->

## Regular PostgreSQL tables

With dbt you can run the full range of features in the `dbt-postgres` adaptor for loading data and performing SQL operations in Postgres.

- Create and manage Postgres tables with pg_lake
- Load data from outside sources to Postgres
- Utilize incremental processing for new or updated data instead of full table refreshes, including the `merge` strategy

## Iceberg tables

dbt can be especially helpful for adding data to Iceberg for fast analytics with `pg_lake`. With any data source, dbt can be configured to create and populate Iceberg table data. Included in the support for dbt with Iceberg is:

- Creating and managing Iceberg tables inside `pg_lake`
- Data loading from outside sources to Iceberg
- Incrementally processing and transforming data in Iceberg, with the `append` or `delete+insert` strategies. `MERGE` is not supported on Iceberg tables, so use `incremental_strategy='delete+insert'` for models with a `unique_key`.
