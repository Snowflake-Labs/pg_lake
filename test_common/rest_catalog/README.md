# Polaris server for testing

PgLake supports iceberg tables with REST catalog. In this test module, we provide the infrastructure for doing in the regression tests as well as running locally as tests.

## Prerequisites

- You need to have JAVA 21 (or higher) installed to run tests with Polaris catalog


## How to run locally

You need to set some of the enviroment variables to run the Polaris server. Note that Polaris server requires up and running Postgres. To reset the state, you can connect to the Postgres server and do `DROP SCHEMA polaris_schema CASCADE`. 

There are also two additional log files created in ` /tmp/polaris.log` and `/tmp/polaris_startup.log` that `rest_catalog_server.sh` generates.


```bash
export JAVA_HOME="/opt/homebrew/Cellar/openjdk@21/21.0.7/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

export PGHOST=localhost
export PGPORT=5432
export PGUSER=okalaci
export PGDATABASE=postgres
export PGPASSOWRD=
export CLIENT_ID=arbitrary_string
export CLIENT_SECRET=arbitrary_string
export AWS_ROLE_ARN="arn:aws:iam::000000000000:role/FakeRoleForTest"
export STORAGE_LOCATION=s3://testbucketcdw
export POLARIS_HOSTNAME=localhost
export POLARIS_PORT=5433
export POLARIS_PRINCIPAL_CREDS_FILE=/tmp/polaris-creds.txt
export POLARIS_PYICEBERG_SAMPLE=/tmp/polaris-pyiceberg.sample
./rest_catalog_server.sh                                                                   
Client id is: arbitrary_string
Bootstrapping metadata database.
Realm 'postgres' successfully bootstrapped.
Bootstrap completed successfully.
Starting Polaris server...
Waiting for Polaris to be healthy...
✅ Polaris is healthy
Getting OAuth token...
Ensuring catalog postgres_catalog exists...
✅ Created catalog postgres_catalog
Ensuring principal snowflake exists...
✅ Created principal
Ensuring principal role snowflake_role exists...
✅ Created principal role
Assigning role to principal...
✅ Assigned role
Ensuring catalog role postgres_catalog_role exists...
✅ Created catalog role postgres_catalog_role
Binding catalog role to principal role...
✅ Bound catalog role
Granting catalog privileges...
✅ Granted CATALOG_MANAGE_CONTENT
🎉 Polaris setup complete.
```

# Lakekeeper server for testing

[Lakekeeper](https://github.com/lakekeeper/lakekeeper) is a second REST catalog the tests run against, so behaviour that differs from Polaris is caught in CI: its prefix is a warehouse ID that `/v1/config` only returns when given the warehouse, and it keeps the staged metadata of a staged create. `make install-rest_catalog` installs a pinned release binary as `$(pg_config --bindir)/lakekeeper`; to upgrade it, change `LAKEKEEPER_VERSION` and the SHA-256 values in the `Makefile` together.

Tests that do `from helpers.lakekeeper import *` start it on port 8282 with authentication disabled and create a warehouse named `pglake` on the moto S3 server (`s3://testbucketcdw/lakekeeper`). pg_lake reaches it through a server with `rest_auth_type 'none'`, see `test_lakekeeper_catalog.py`.

## How to run locally

Lakekeeper keeps its metadata in its own database (`lakekeeper` by default) on the Postgres server given by the `PG*` variables, and needs an S3 endpoint for the warehouse. `--purge` drops that database first; without it, an existing bootstrap and warehouse are reused. Logs go to `/tmp/lakekeeper.log`, and the fixture's output to `/tmp/lakekeeper_startup.log`.

```bash
export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD= PGDATABASE=postgres
export S3_ENDPOINT=http://localhost:5999 S3_BUCKET=testbucketcdw S3_REGION=us-west-1
export S3_ACCESS_KEY_ID=testing S3_SECRET_ACCESS_KEY=testing
bash ./lakekeeper_server.sh --purge
✅ Lakekeeper is healthy
✅ Bootstrapped Lakekeeper
✅ Created warehouse pglake
🎉 Lakekeeper setup complete (warehouse 01a17c31-642e-7873-9d2c-5f7e2211adfc).
```