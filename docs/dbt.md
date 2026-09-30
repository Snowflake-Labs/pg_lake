---
title: dbt
parent: User guide
nav_order: 5
---

# dbt

`pg_lake` integrates with Data Build Tool (dbt) to update, insert, or transform data. Installing both the `dbt-core` (version 1.9.2+) and the database adapter `dbt-postgres`(version 1.9.0+) are required.

### Connection

dbt runs externally over a Postgres connection. Superuser access might be needed depending on the number of changes expected.

```yaml
my_dbt_project:
  outputs:
    dev:
      type: postgres
      host: atx5231hr.pgtest.us-west-2.aws.postgres.app
      user: postgres
      password: ....
      port: 5432
      dbname: postgres
      schema: public
      threads: 4
  target: dev
```

**S3 file location**

While configuring dbt, you will need to create an environment variable to point dbt at S3, like this:

```bash
export ICEBERG_LOCATION_PREFIX=<S3 location>
```

If you’re using the built-in Iceberg appliance for pg_lake, your S3 bucket location can be accessed from psql with a show command, like this:

```bash
psql <connection-string> -c 'show pg_lake_iceberg.default_location_prefix'
```

### Models configuration

The model configuration controls how the transformation process behaves. 

- `materialized='incremental'`: This tells dbt to perform incremental updates instead of fully rebuilding the table each time.
- `unique_key='created_at'`: This specifies the unique identifier for each record, used to detect new records.
- `pre_hook` and `post_hook`: These hooks are executed before and after the model runs. In this case, the `pre_hook` sets the default access method to `iceberg` and configures the location prefix for storing Iceberg tables in S3. The `post_hook` resets these settings after the model has completed.

<!-- The raw tags stop the docs site from evaluating the dbt Jinja. {% raw %} -->
```jinja
{{ config(
    materialized='incremental',
    unique_key='created_at',
    pre_hook="SET default_table_access_method TO 'iceberg'; SET pg_lake_iceberg.default_location_prefix = '{{ env_var('ICEBERG_LOCATION_PREFIX', '') }}';",
    post_hook="RESET default_table_access_method; RESET pg_lake_iceberg.default_location_prefix;"
) }}
```
<!-- {% endraw %} -->

## DBT for Postgres and pg_lake

With dbt you can run the full range of features in the `dbt-postgres` adaptor for loading data and performing SQL operations in Postgres.

- Create and manage Postgres tables with pg_lake
- Load data from outside sources to Postgres
- Utilize incremental processing for new or updated data instead of full table refreshes. `MERGE` actions supported

## DBT for Iceberg and pg_lake

dbt can be especially helpful for adding data to Iceberg for fast analytics with `pg_lake`. With any data source, dbt can be configured to create and populate Iceberg table data. Included in the support for dbt with Iceberg is:

- Creating and managing Iceberg tables inside `pg_lake`
- Data loading from outside sources to Iceberg
- Incrementally processing and transforming data in Iceberg. This is done with a `delete`+`insert` strategy. Merge is not currently supported on Iceberg tables, but it can be used directly with the Postgres instance.
