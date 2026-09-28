#!/usr/bin/env bash

set -euo pipefail

PG_LIBDIR=$(pg_config --libdir)
PG_BINDIR=$(pg_config --bindir)
PG_SHAREDIR=$(pg_config --sharedir)

POLARIS_ADMIN_JAR="$PG_BINDIR/polaris-admin.jar"
POLARIS_SERVER_JAR="$PG_BINDIR/polaris-server.jar"

POLARIS_HOST="http://$POLARIS_HOSTNAME:$POLARIS_PORT"
PRINCIPAL_NAME="snowflake"
PRINCIPAL_ROLE="snowflake_role"
REALM_NAME=$PGDATABASE
CATALOG_NAME="${REALM_NAME}"
CATALOG_ROLE="${REALM_NAME}_role"
STORAGE_TYPE=${STORAGE_TYPE:-S3}

export PGHOST=${PGHOST:-localhost}
export PGPORT=${PGPORT:-5432}
export PGUSER=${PGUSER:-$USER}
export PGDATABASE=${PGDATABASE:-postgres}
export PGPASSWORD=${PGPASSWORD:-}

export QUARKUS_HTTP_PORT=$POLARIS_PORT
export POLARIS_REALM_CONTEXT_REALMS=$PGDATABASE
export POLARIS_PERSISTENCE_TYPE=relational-jdbc
export QUARKUS_DATASOURCE_DB_KIND=pgsql

# Would want to use {realm} here to enable Polaris for different databases,
# but seems that is currently only supported via deprecated EclipseLink path
export QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$PGDATABASE"
export QUARKUS_DATASOURCE_USERNAME=$PGUSER
export QUARKUS_DATASOURCE_PASSWORD=$PGPASSWORD

# Ensure CLIENT_ID and CLIENT_SECRET are set
if [[ -z "${CLIENT_ID:-}" || -z "${CLIENT_SECRET:-}" ]]; then
  echo "❌ CLIENT_ID and CLIENT_SECRET must be set in the environment."
  exit 1
fi

echo "Client id is: $CLIENT_ID"

if [[ "$STORAGE_TYPE" == "S3" ]]; then
  if [[ -z "${AWS_ROLE_ARN:-}" ]]; then
    echo "❌ AWS_ROLE_ARN must be set for storage type $STORAGE_TYPE"
    exit 1
  fi
elif [[ "$STORAGE_TYPE" == "AZURE" ]]; then
  if [[ -z "${AZURE_TENANT_ID:-}" ]]; then
    echo "❌ AZURE_TENANT_ID must be set for storage type $STORAGE_TYPE"
    exit 1
  fi
else
  echo "❌ $STORAGE_TYPE is not yet supported, only: S3, AZURE"
  exit 1
fi

if [[ -z "${STORAGE_LOCATION:-}"  ]]; then
  echo "❌ STORAGE_LOCATION must point to a $STORAGE_TYPE path."
  exit 1
fi

PURGE=false
NOWAIT=false
for arg in "$@"; do
  if [[ "$arg" == "--purge" ]]; then
    PURGE=true
  elif [[ "$arg" == "--no-wait" ]]; then
    NOWAIT=true
  fi
done

# purge and exit if --purge is passed
if [[ "$PURGE" == "true" ]]; then
  echo "Purging metadata database."
  java -jar "$POLARIS_ADMIN_JAR" purge --realm=$REALM_NAME || true
  psql -tAX -c "drop schema polaris_schema cascade" || true
fi

# The metadata schema alone no longer means the realm is bootstrapped: we create
# the schema ourselves below, so use the schema-version table the bootstrap
# writes into it as the marker instead.
BOOTSTRAPPED=$(psql -tAX -c "select count(*) > 0 from pg_tables where schemaname = 'polaris_schema' and tablename = 'version'")

if [[ "$BOOTSTRAPPED" != "t" ]]; then
  # The relational-jdbc backend does not issue CREATE SCHEMA, so the schema its
  # connections resolve to (the currentSchema JDBC property, POLARIS_SCHEMA by
  # default) has to exist before the admin tool can bootstrap into it.
  echo "Creating metadata schema."
  psql -tAX -c "create schema if not exists polaris_schema"

  echo "Bootstrapping metadata database."
  java -jar "$POLARIS_ADMIN_JAR" bootstrap --realm=$REALM_NAME --credential=$REALM_NAME,$CLIENT_ID,$CLIENT_SECRET
fi

AUTH_HEADER=""
CONTENT_HEADER="Content-Type: application/json"
POLARIS_TOKEN=""

### --- Curl Wrappers --- ###

api_get() {
  curl -sf -H "$AUTH_HEADER" "$POLARIS_HOST$1"
}

api_post() {
  local path="$1"
  local data="$2"
  curl -sf -X POST -H "$AUTH_HEADER" -H "$CONTENT_HEADER" \
    -d "$data" "$POLARIS_HOST$path"
}

api_put() {
  local path="$1"
  local data="$2"
  curl -sf -X PUT -H "$AUTH_HEADER" -H "$CONTENT_HEADER" \
    -d "$data" "$POLARIS_HOST$path" > /dev/null
}

### --- Polaris Setup Steps --- ###

wait_for_polaris() {
  echo "Waiting for Polaris to be healthy..."
  for i in {1..30}; do
    if curl -s "$POLARIS_HOST/api/catalog/v1/config" > /dev/null; then
      echo "✅ Polaris is healthy"
      return
    fi
    sleep 2
  done
  echo "❌ Polaris did not become healthy in time"
  exit 1
}

get_token() {
  echo "Getting OAuth token..."
  POLARIS_TOKEN=$(curl -sf -u "$CLIENT_ID:$CLIENT_SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=PRINCIPAL_ROLE:ALL" \
    "$POLARIS_HOST/api/catalog/v1/oauth/tokens" | jq -r .access_token)

  if [[ -z "$POLARIS_TOKEN" || "$POLARIS_TOKEN" == "null" ]]; then
    echo "❌ Failed to get token"
    exit 1
  fi

  AUTH_HEADER="Authorization: Bearer $POLARIS_TOKEN"
}

create_principal() {
  echo "Ensuring principal $PRINCIPAL_NAME exists..."
  if ! api_get "/api/management/v1/principals/$PRINCIPAL_NAME" > /dev/null; then
    api_post "/api/management/v1/principals" "{\"principal\":{\"name\":\"$PRINCIPAL_NAME\"}}" > $POLARIS_PRINCIPAL_CREDS_FILE
    echo "✅ Created principal"
  else
    api_get "/api/management/v1/principals/$PRINCIPAL_NAME" > $POLARIS_PRINCIPAL_CREDS_FILE
    echo "ℹ️  Principal already exists"
  fi

  # create sample pyiceberg file 
  PRINCIPAL_CLIENT_ID=$(jq -r '.credentials.clientId' < $POLARIS_PRINCIPAL_CREDS_FILE)
  PRINCIPAL_CLIENT_SECRET=$(jq -r '.credentials.clientSecret' < $POLARIS_PRINCIPAL_CREDS_FILE)

  echo '
catalog:
default:
  uri: '$POLARIS_HOST'/api/catalog
  warehouse: '$CATALOG_NAME'
  oauth2-server-url: '$POLARIS_HOST'/api/catalog/v1/oauth/tokens
  scope: PRINCIPAL_ROLE:ALL
  credential: '$PRINCIPAL_CLIENT_ID':'$PRINCIPAL_CLIENT_SECRET'
' > $POLARIS_PYICEBERG_SAMPLE
}

create_principal_role() {
  echo "Ensuring principal role $PRINCIPAL_ROLE exists..."
  if ! api_get "/api/management/v1/principal-roles/$PRINCIPAL_ROLE" > /dev/null; then
    api_post "/api/management/v1/principal-roles" "{\"principalRole\":{\"name\":\"$PRINCIPAL_ROLE\"}}"
    echo "✅ Created principal role"

    echo "Assigning role to principal..."
    api_put "/api/management/v1/principals/$PRINCIPAL_NAME/principal-roles" \
      "{\"principalRole\":{\"name\":\"$PRINCIPAL_ROLE\"}}"
    echo "✅ Assigned role"
  else
    echo "ℹ️  Principal role already exists"
  fi

}

create_catalog() {
  echo "Ensuring catalog $CATALOG_NAME exists..."

  if ! api_get "/api/management/v1/catalogs/$CATALOG_NAME" > /dev/null; then
    STORAGE_CONFIG_INFO="{\"storageType\": \"$STORAGE_TYPE\", \"allowedLocations\": [\"$STORAGE_LOCATION\"]}"

    if [[ "$STORAGE_TYPE" == "S3" ]]; then
      STORAGE_CONFIG_INFO=$(echo "$STORAGE_CONFIG_INFO" | jq --arg roleArn "$AWS_ROLE_ARN" '. + {roleArn: $roleArn}')
    elif [[ "$STORAGE_TYPE" == "AZURE" ]]; then
      STORAGE_CONFIG_INFO=$(echo "$STORAGE_CONFIG_INFO" | jq --arg tenantId "$AZURE_TENANT_ID" '. + {tenantId: $tenantId}')
    fi

    PAYLOAD='{
      "catalog": {
      "name": "'$CATALOG_NAME'",
      "type": "INTERNAL",
      "readOnly": false,
      "properties": {
        "default-base-location": "'$STORAGE_LOCATION'"
      },
      "storageConfigInfo": '$STORAGE_CONFIG_INFO'
      }
    }'

    api_post "/api/management/v1/catalogs" "$PAYLOAD"

    echo "✅ Created catalog $CATALOG_NAME"
  else
    echo "ℹ️  Catalog $CATALOG_NAME already exists"
  fi
}

create_catalog_role() {
  echo "Ensuring catalog role $CATALOG_ROLE exists..."
  if ! api_get "/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles/$CATALOG_ROLE" > /dev/null; then
    api_post "/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles" \
      "{\"catalogRole\":{\"name\":\"$CATALOG_ROLE\"}}"

    echo "✅ Created catalog role $CATALOG_ROLE"

    bind_catalog_role
    grant_privileges
  else
    echo "ℹ️  Catalog role $CATALOG_ROLE already exists"
  fi
}

bind_catalog_role() {
  echo "Binding catalog role to principal role..."
  api_put "/api/management/v1/principal-roles/$PRINCIPAL_ROLE/catalog-roles/$CATALOG_NAME" \
    "{\"catalogRole\":{\"name\":\"$CATALOG_ROLE\"}}"
  echo "✅ Bound catalog role"
}

grant_privileges() {
  echo "Granting catalog privileges..."
  api_put "/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles/$CATALOG_ROLE/grants" \
    '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
  echo "✅ Granted CATALOG_MANAGE_CONTENT"
}

main() {
  
  echo "Starting Polaris server..."

  # Moto does not support certain checksums, so
  # instruct AWS SDK not to use
  export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
  
  java \
   -jar "$POLARIS_SERVER_JAR" "$@" \
    > /tmp/polaris.log 2>&1 &
  POLARIS_PID=$!

  wait_for_polaris
  get_token
  create_catalog
  create_principal
  create_principal_role
  create_catalog_role
  echo "🎉 Polaris setup complete."

  echo "$POLARIS_PID" > "$POLARIS_PID_FILE"

  if [[ "$NOWAIT" == "false" ]]; then
    # Forward TERM/INT from Python to the child
    trap 'echo "Stopping Polaris..."; kill "$POLARIS_PID"; wait "$POLARIS_PID"' TERM INT

    wait "$POLARIS_PID"            # shell stays alive until Polaris exits
  fi
}

main