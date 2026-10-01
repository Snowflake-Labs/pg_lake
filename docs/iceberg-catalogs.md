---
title: Catalogs and interoperability
parent: Iceberg tables
grand_parent: User guide
nav_order: 3
---

# Catalogs and interoperability
{: .no_toc }

An Iceberg *catalog* keeps track of which metadata file is the current version of each table.
Engines find tables through a catalog, and commit changes by swapping the pointer to a new
metadata file. pg_lake can use PostgreSQL itself as the catalog, or register tables in an
external catalog, and it can read Iceberg tables written by other systems.

1. TOC
{:toc}

## Choosing a catalog

Every Iceberg table records its catalog in the `catalog` option when it is created. The choice
cannot be changed afterwards.

| Catalog | `catalog` value | What it does |
|:--|:--|:--|
| PostgreSQL (default) | `postgres` | pg_lake is the catalog. Commits are part of the PostgreSQL transaction, and other engines read tables through the `iceberg_tables` view. |
| REST | `rest`, or the name of a [catalog server](#external-catalogs-with-create-server) | Tables are registered in an [Iceberg REST catalog](https://iceberg.apache.org/rest-catalog-spec/) such as Apache Polaris, where every engine using that catalog can find them. |
| Object store | `object_store` | pg_lake publishes tables to a catalog file in object storage. |

To change the default for new tables, set `pg_lake_iceberg.default_catalog`:

```sql
SET pg_lake_iceberg.default_catalog TO 'rest';
```

## The PostgreSQL catalog

By default, pg_lake acts as its own Iceberg catalog. When a transaction modifies an Iceberg
table, pg_lake writes the new metadata file and updates its catalog in the same transaction,
so the table and the catalog never disagree.

The catalog is exposed as the `iceberg_tables` view:

```sql
SELECT catalog_name, table_namespace, table_name, metadata_location
FROM iceberg_tables;

 catalog_name | table_namespace |  table_name  |                              metadata_location
--------------+-----------------+--------------+------------------------------------------------------------------------------
 postgres     | public          | measurements | s3://testbucket/iceberg/postgres/public/measurements/metadata/00003-6403833e-0766-4496-ad47-ec9641ee965f.metadata.json
```

For tables created through PostgreSQL, `catalog_name` is the database name, and
`table_namespace` is the schema. If the database is renamed, `catalog_name` changes with it.

The view has the layout that the Iceberg SQL catalog implementations expect: the
[Iceberg JDBC catalog](https://iceberg.apache.org/docs/latest/jdbc/#configurations) (used by
Spark, Flink and others), the
[pyiceberg SQL catalog](https://py.iceberg.apache.org/reference/pyiceberg/catalog/sql/) and
[iceberg-rust](https://rust.iceberg.apache.org/api/iceberg_catalog_sql/struct.SqlCatalog.html).
Those tools can connect to PostgreSQL and always read the latest committed version of each
table. They cannot write to tables created by pg_lake; if they create tables of their own
under a different catalog name, those tables have no corresponding PostgreSQL table.

### Reading tables from Spark

Connect Spark's Iceberg JDBC catalog to PostgreSQL. The catalog name must match the database
name, `postgres` in this example:

```bash
export PGHOST="db host"
export PGDATABASE="postgres"
export PGUSER="user name"
export PGPASSWORD="your password"
export AWS_REGION="us-east-1"
export JDBC_CONN_STR="jdbc:postgresql://${PGHOST}/${PGDATABASE}?user=${PGUSER}&password=${PGPASSWORD}"

spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.postgres=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.postgres.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
  --conf spark.sql.catalog.postgres.uri=$JDBC_CONN_STR \
  --conf spark.sql.catalog.postgres.warehouse=s3:// \
  --conf spark.sql.catalog.postgres.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.postgres.s3.endpoint=https://s3.${AWS_REGION}.amazonaws.com
```

A table created in PostgreSQL:

```sql
CREATE TABLE public.pg_lake_iceberg_table
USING iceberg
AS SELECT id FROM generate_series(0, 1000) id;
```

can then be queried from Spark:

```sql
spark-sql (default)> SELECT avg(id) FROM postgres.public.pg_lake_iceberg_table;
500.0
```

### Reading tables from Python

pyiceberg's SQL catalog works the same way:

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "postgres",  # must match the database name
    uri="postgresql+psycopg2://user:password@dbhost:5432/postgres",
    warehouse="s3://mybucket/iceberg",
)

table = catalog.load_table("public.measurements")
df = table.scan(row_filter="measurement > 20").to_pandas()
```

### Reading tables from any Iceberg engine

Any engine that can open an Iceberg table from a metadata file can read a snapshot of a
pg_lake table by its `metadata_location`. The location changes on every commit, so this gives
a point-in-time view; use a catalog to always see the latest version. The
[sync use case](use-case-iceberg-sync.md#duckdb) reads a table this way from DuckDB.

## REST catalogs

With a REST catalog, pg_lake creates and commits tables in an external catalog service instead
of its own catalog, and other engines using that catalog see them immediately. pg_lake can also
attach tables that other engines created there. It speaks the
[Iceberg REST catalog protocol](https://iceberg.apache.org/rest-catalog-spec/) with OAuth2
client credentials, and has been tested with [Apache Polaris](https://polaris.apache.org/).

{: .note }
Writing to an external catalog is still experimental. See
[Create tables in an external catalog](#create-tables-in-an-external-catalog).

There are two ways to connect to a REST catalog:

- The built-in `rest` catalog is configured once by a superuser, and every database user shares
  its credentials.
- Catalog servers created with `CREATE SERVER` can point at any number of catalogs, each user
  can have their own credentials, and they do not need a superuser. See
  [External catalogs with CREATE SERVER](#external-catalogs-with-create-server).

### Configure the built-in `rest` catalog

The built-in `rest` catalog is configured by a superuser, for example in `postgresql.conf` or
with `ALTER SYSTEM`:

```sql
ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'https://polaris.example.com/api/catalog';
ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO '<client id>';
ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO '<client secret>';
SELECT pg_reload_conf();
```

| Setting | Description |
|:--|:--|
| `pg_lake_iceberg.rest_catalog_host` | Base URL of the REST catalog API. |
| `pg_lake_iceberg.rest_catalog_client_id` | OAuth2 client ID. |
| `pg_lake_iceberg.rest_catalog_client_secret` | OAuth2 client secret. |
| `pg_lake_iceberg.rest_catalog_oauth_host_path` | Token endpoint URL, if the catalog does not serve it at the default path. |
| `pg_lake_iceberg.rest_catalog_scope` | OAuth2 scope. Default `PRINCIPAL_ROLE:ALL`. |
| `pg_lake_iceberg.rest_catalog_enable_vended_credentials` | Ask the catalog for temporary storage credentials instead of using pgduck_server's own. Default `off`. |

Tables then use `catalog = 'rest'`, and are created and attached the same way as with a
catalog server.

## External catalogs with CREATE SERVER

A catalog server describes one external Iceberg REST catalog: where it is and how to log in.
You create it with `CREATE SERVER` and the `iceberg_catalog` foreign data wrapper, give database
users credentials with user mappings, and then name the server in the `catalog` option of a
table. A database can have several catalog servers, for example one per team or per cloud
account, and a single query can combine tables from all of them with tables in the
PostgreSQL catalog and regular PostgreSQL tables.

### Define a catalog server

Any member of `lake_write` can create a catalog server. Catalog servers always have
`TYPE 'rest'`, and the names `postgres`, `object_store` and `rest` are reserved for the built-in
catalogs:

```sql
CREATE SERVER polaris TYPE 'rest'
  FOREIGN DATA WRAPPER iceberg_catalog
  OPTIONS (rest_endpoint 'https://polaris.example.com/api/catalog',
           location_prefix 's3://lake-bucket');
```

| Option | Where | Description |
|:--|:--|:--|
| `rest_endpoint` | server | Base URL of the REST catalog API. |
| `location_prefix` | server | Storage location for tables pg_lake creates in this catalog. pg_lake adds the database, schema and table name to it. |
| `catalog_name` | server | Catalog (warehouse) to attach existing tables from. Without it, pg_lake uses the prefix the catalog advertises, or the database name. A server with `catalog_name` can only be used to attach tables, see [below](#create-tables-in-an-external-catalog). |
| `oauth_endpoint` | server | OAuth2 token endpoint URL, if the catalog does not serve it at `<rest_endpoint>/v1/oauth/tokens`. |
| `enable_vended_credentials` | server | Ask the catalog for temporary storage credentials instead of using pgduck_server's own. |
| `scope` | server or user mapping | OAuth2 scope. Default `PRINCIPAL_ROLE:ALL`. The user mapping wins if both set it. |
| `client_id`, `client_secret` | user mapping | OAuth2 client credentials. |

### Credentials and permissions

Each database user logs in to the catalog with the credentials in their user mapping, so the
catalog's own access control decides what that user can see and change. A catalog server never
falls back to the `pg_lake_iceberg.rest_catalog_client_*` settings of the built-in catalog: a
user without a user mapping gets the error `no credentials found for REST catalog`.

```sql
-- credentials for one user
CREATE USER MAPPING FOR data_eng SERVER polaris
  OPTIONS (client_id '<client id>', client_secret '<client secret>');

-- credentials for everyone else
CREATE USER MAPPING FOR PUBLIC SERVER polaris
  OPTIONS (client_id '<reader client id>', client_secret '<reader client secret>');
```

A user mapping for a specific user takes precedence over the `PUBLIC` one. As with any foreign
server, PostgreSQL only shows the options of a user mapping to that user, the server owner and
superusers.

Inside PostgreSQL, the usual privileges apply on top:

- Creating a table that uses a catalog server needs `USAGE` on the server, which its owner has.
  Use `GRANT USAGE ON FOREIGN SERVER polaris TO <role>` to let others create tables in it.
  Creating Iceberg tables also needs the `lake_read_write` role.
- Querying a table needs `SELECT` on it, and a user mapping for the user or for `PUBLIC`.

### Query tables from an external catalog

A table that another engine created in the catalog is attached with `read_only`, naming it as
the catalog knows it. The columns come from the catalog:

```sql
CREATE TABLE orders () USING iceberg
WITH (catalog = 'polaris', read_only = true,
      catalog_name = 'sales', catalog_namespace = 'analytics',
      catalog_table_name = 'orders');

SELECT region, sum(amount) FROM orders GROUP BY region;
```

| Option | Description |
|:--|:--|
| `catalog_name` | Catalog (warehouse) that holds the table. Defaults to the server's `catalog_name`, then to the prefix the catalog advertises, then to the database name. |
| `catalog_namespace` | Namespace of the table. Defaults to the PostgreSQL schema name. |
| `catalog_table_name` | Name of the table in the catalog. Defaults to the PostgreSQL table name. |
| `lowercase_column_names` | Fold column and struct field names to lowercase. Valid for read-only REST and `object_store` catalog tables; defaults to `false`. |

An attached table always reads the catalog's current version, so queries see new commits from
other engines without any changes in PostgreSQL. Dropping it only removes it from PostgreSQL.

To attach many tables from one catalog, set `catalog_name` on the server and mirror the
namespaces as schemas. The other catalog options then follow from the names you use:

```sql
CREATE SERVER sales_catalog TYPE 'rest'
  FOREIGN DATA WRAPPER iceberg_catalog
  OPTIONS (rest_endpoint 'https://polaris.example.com/api/catalog',
           catalog_name 'sales');

CREATE USER MAPPING FOR PUBLIC SERVER sales_catalog
  OPTIONS (client_id '<client id>', client_secret '<client secret>');

CREATE SCHEMA analytics;
CREATE TABLE analytics.orders () USING iceberg
WITH (catalog = 'sales_catalog', read_only = true);
CREATE TABLE analytics.customers () USING iceberg
WITH (catalog = 'sales_catalog', read_only = true);
```

Attached tables cannot be written to. Writing would mean taking over the table's metadata,
field IDs and file inventory from whatever produced them, which pg_lake does not do: it writes
only to tables it created. To move existing data under pg_lake, create a new table and copy
into it.

#### Lowercase column names from external engines

Some engines write Iceberg column names in uppercase. PostgreSQL folds unquoted identifiers to
lowercase, so a column named `"ID"` normally has to be quoted. Set
`lowercase_column_names = true` to expose it as `id`; nested struct field names are folded too.
The option changes column and struct field names only; catalog, namespace and table names must
still match the catalog. For example, the catalog identifiers below remain uppercase:

```sql
CREATE TABLE sales () USING iceberg
WITH (catalog = 'polaris', read_only = true,
      catalog_name = 'sales', catalog_namespace = 'PUBLIC',
      catalog_table_name = 'SALES', lowercase_column_names = true);

SELECT id, (address).city FROM sales;
```

The option cannot be changed after the table is created. Creation or query fails if two names
in the same table or struct differ only in case.

### Create tables in an external catalog

{: .note }
Writing to an external catalog is still experimental, both with catalog servers and with the
built-in `rest` catalog.

Without `read_only`, pg_lake creates the table in the catalog and owns it: it writes the data
and metadata, and other engines can read it through the catalog.

```sql
CREATE TABLE order_summary USING iceberg
WITH (catalog = 'polaris')
AS SELECT region, sum(amount) AS total FROM orders GROUP BY region;
```

pg_lake names the table in the catalog after the database, schema and table you used. Here
that is the catalog (warehouse) named after the current database, namespace `public` and table
`order_summary`, so the REST catalog needs a catalog with the same name as the database, and it
must allow writes under the server's `location_prefix`. pg_lake creates the namespace if it
does not exist. The catalog options of attached tables are not accepted, and neither is a
server with `catalog_name`, so that the location of a table in the catalog cannot change after
it was created. For the same reason, these tables cannot be renamed or moved to another schema.

{: .warning }
pg_lake sends its changes to the catalog after the PostgreSQL transaction commits. If the
catalog rejects a change, for example because the location is not allowed, the transaction is
still committed in PostgreSQL and you only get a `WARNING`. Check for warnings when you start
using a new catalog server.

### Change or remove a catalog server

Most server options can be changed with `ALTER SERVER ... OPTIONS`, with two exceptions that
protect the tables and credentials that use the server:

- `rest_endpoint` cannot change while the server has user mappings or tables, since that would
  send their credentials to a different URL.
- A catalog server cannot be renamed. Drop it and create a new one instead.

`DROP SERVER` fails while tables or user mappings depend on the server. `DROP SERVER ... CASCADE`
drops them too, and, like `DROP TABLE`, deletes the tables pg_lake created from the external
catalog. Attached tables are only removed from PostgreSQL.

## The object store catalog

With `catalog = 'object_store'`, pg_lake publishes the current metadata location of each table
to a catalog file under `pg_lake_iceberg.object_store_catalog_location_prefix`. The file is
rewritten when tables change, and at least every `pg_lake_iceberg.object_store_catalog_max_age`
seconds. Tables that another system publishes under the same prefix can be attached with
`read_only = true` and `catalog_table_name`, the same way as for REST catalogs. This catalog
is intended for managed integrations that exchange tables through object storage.

## External Iceberg tables from metadata files

You can query any Iceberg table, whoever wrote it, by creating a `pg_lake` foreign table that
points at one of its metadata files. If the file has uppercase column names, set
`lowercase_column_names` to fold column and nested struct field names to lowercase:

```sql
CREATE FOREIGN TABLE external_iceberg ()
SERVER pg_lake
OPTIONS (path 's3://mybucket/table/metadata/v14.metadata.json', lowercase_column_names 'true');
```

The table is a fixed snapshot: later changes by the writer are not visible until you point it
at a newer metadata file, which keeps dependent views and grants in place:

```sql
ALTER FOREIGN TABLE external_iceberg
OPTIONS (SET path 's3://mybucket/table/metadata/v15.metadata.json');
```

When the table is registered in a REST catalog, attaching it with `read_only = true` (above)
avoids this manual step. The same `lowercase_column_names` option is accepted by `CREATE TABLE`
with `load_from` or `definition_from`, and by `COPY ... FROM` an Iceberg metadata file. See the
[file formats reference](file-formats-reference.md#external-iceberg-format) for those forms.

## Snowflake

Snowflake can query pg_lake's Iceberg tables in place, without copying the data:

- **Snowflake Postgres** instances come with pg_lake, and a Snowflake catalog integration
  with `CATALOG_SOURCE = SNOWFLAKE_POSTGRES` reads their Iceberg tables directly. See the
  [Snowflake documentation](https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-pg_lake).
- **Self-managed pg_lake** tables can be read through an object storage catalog integration
  from the table's metadata file, or through a REST catalog that both systems use.

For tables that Snowflake reads, create them with `compatibility_mode = 'snowflake'` (or set
`pg_lake_iceberg.default_compatibility_mode`). This stores `uuid` values nested inside arrays
and composite types as strings, which Snowflake requires, while keeping the column type
`uuid` in PostgreSQL.

Conversely, when pg_lake reads tables written by Snowflake, column and nested field names may
be uppercase. Set `lowercase_column_names = true` when attaching the table through a read-only
catalog or a metadata file; catalog identifiers themselves are unchanged. See
[lowercase column names from external engines](#lowercase-column-names-from-external-engines)
and [external metadata files](#external-iceberg-tables-from-metadata-files).

The [sync use case](use-case-iceberg-sync.md#snowflake) walks through both setups.
