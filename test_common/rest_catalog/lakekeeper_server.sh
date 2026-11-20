#!/usr/bin/env bash
#
# Starts Lakekeeper, a second REST catalog for tests (#739), and creates a
# warehouse on the moto S3 server. Authentication is left disabled, so pg_lake
# connects with rest_auth_type 'none'.

set -euo pipefail

PG_BINDIR=$(pg_config --bindir)
LAKEKEEPER_BIN="$PG_BINDIR/lakekeeper"

export PGHOST=${PGHOST:-localhost}
export PGPORT=${PGPORT:-5432}
export PGUSER=${PGUSER:-$USER}
export PGDATABASE=${PGDATABASE:-postgres}
export PGPASSWORD=${PGPASSWORD:-}

LAKEKEEPER_PORT=${LAKEKEEPER_PORT:-8282}
LAKEKEEPER_HOST="http://localhost:$LAKEKEEPER_PORT"
LAKEKEEPER_DATABASE=${LAKEKEEPER_DATABASE:-lakekeeper}
LAKEKEEPER_WAREHOUSE=${LAKEKEEPER_WAREHOUSE:-pglake}
LAKEKEEPER_PID_FILE=${LAKEKEEPER_PID_FILE:-/tmp/regress-lakekeeper.pid}
LAKEKEEPER_WAREHOUSE_FILE=${LAKEKEEPER_WAREHOUSE_FILE:-/tmp/regress-lakekeeper-warehouse.txt}
LAKEKEEPER_LOG=${LAKEKEEPER_LOG:-/tmp/lakekeeper.log}

for var in S3_ENDPOINT S3_BUCKET S3_REGION S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY; do
  if [[ -z "${!var:-}" ]]; then
    echo "❌ $var must be set in the environment."
    exit 1
  fi
done

PURGE=false
NOWAIT=false
for arg in "$@"; do
  if [[ "$arg" == "--purge" ]]; then
    PURGE=true
  elif [[ "$arg" == "--no-wait" ]]; then
    NOWAIT=true
  fi
done

# Lakekeeper keeps its metadata in its own database, so its schema never shows
# up next to the objects the tests create.
if [[ "$PURGE" == "true" ]]; then
  echo "Dropping Lakekeeper database."
  psql -tAX -c "drop database if exists $LAKEKEEPER_DATABASE with (force)"
fi

if [[ "$(psql -tAX -c "select count(*) from pg_database where datname = '$LAKEKEEPER_DATABASE'")" == "0" ]]; then
  psql -tAX -c "create database $LAKEKEEPER_DATABASE"
fi

LAKEKEEPER_DB_URL="postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$LAKEKEEPER_DATABASE"
export LAKEKEEPER__PG_DATABASE_URL_READ=$LAKEKEEPER_DB_URL
export LAKEKEEPER__PG_DATABASE_URL_WRITE=$LAKEKEEPER_DB_URL
# Encrypts the warehouse's storage credential in the database; test-only.
export LAKEKEEPER__PG_ENCRYPTION_KEY=pg-lake-tests-not-a-secret
export LAKEKEEPER__LISTEN_PORT=$LAKEKEEPER_PORT
export LAKEKEEPER__BIND_IP=127.0.0.1

CONTENT_HEADER="Content-Type: application/json"

api_get() {
  curl -sf "$LAKEKEEPER_HOST$1"
}

api_post() {
  curl -sf -X POST -H "$CONTENT_HEADER" -d "$2" "$LAKEKEEPER_HOST$1"
}

wait_for_lakekeeper() {
  echo "Waiting for Lakekeeper to be healthy..."
  for i in {1..30}; do
    if ! kill -0 "$LAKEKEEPER_PID" 2> /dev/null; then
      echo "❌ Lakekeeper exited during startup, see $LAKEKEEPER_LOG"
      exit 1
    fi
    if curl -sf "$LAKEKEEPER_HOST/health" > /dev/null; then
      echo "✅ Lakekeeper is healthy"
      return
    fi
    sleep 2
  done
  echo "❌ Lakekeeper did not become healthy in time, see $LAKEKEEPER_LOG"
  exit 1
}

# Bootstrapping is allowed only once per database.
bootstrap() {
  if [[ "$(api_get /management/v1/info | jq -r .bootstrapped)" == "true" ]]; then
    echo "ℹ️  Lakekeeper is already bootstrapped"
    return
  fi
  api_post /management/v1/bootstrap '{"accept-terms-of-use": true}' > /dev/null
  echo "✅ Bootstrapped Lakekeeper"
}

warehouse_id() {
  api_get /management/v1/warehouse |
    jq -r --arg name "$LAKEKEEPER_WAREHOUSE" \
      '.warehouses[] | select(.name == $name) | (.id // ."warehouse-id")'
}

# Writable REST tables do not use vended credentials, so the warehouse offers
# neither STS nor remote signing; pgduck_server reaches moto with its own secret.
create_warehouse() {
  if [[ -n "$(warehouse_id)" ]]; then
    echo "ℹ️  Warehouse $LAKEKEEPER_WAREHOUSE already exists"
    return
  fi

  PAYLOAD=$(jq -n \
    --arg name "$LAKEKEEPER_WAREHOUSE" \
    --arg bucket "$S3_BUCKET" \
    --arg region "$S3_REGION" \
    --arg endpoint "$S3_ENDPOINT" \
    --arg key "$S3_ACCESS_KEY_ID" \
    --arg secret "$S3_SECRET_ACCESS_KEY" \
    '{
      "warehouse-name": $name,
      "storage-profile": {
        "type": "s3",
        "bucket": $bucket,
        "key-prefix": "lakekeeper",
        "region": $region,
        "endpoint": $endpoint,
        "path-style-access": true,
        "flavor": "s3-compat",
        "sts-enabled": false,
        "remote-signing-enabled": false
      },
      "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": $key,
        "aws-secret-access-key": $secret
      },
      "delete-profile": {"type": "hard"}
    }')

  api_post /management/v1/warehouse "$PAYLOAD" > /dev/null
  echo "✅ Created warehouse $LAKEKEEPER_WAREHOUSE"
}

main() {
  # Otherwise the health check below would pass against that other server.
  if curl -sf "$LAKEKEEPER_HOST/health" > /dev/null; then
    echo "❌ Something already serves $LAKEKEEPER_HOST; stop it first"
    exit 1
  fi

  echo "Migrating Lakekeeper database..."
  "$LAKEKEEPER_BIN" migrate > "$LAKEKEEPER_LOG" 2>&1

  echo "Starting Lakekeeper server..."

  # Moto does not support certain checksums, so instruct the AWS SDK not to
  # use them (as for Polaris).
  export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
  export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required

  "$LAKEKEEPER_BIN" serve >> "$LAKEKEEPER_LOG" 2>&1 &
  LAKEKEEPER_PID=$!
  echo "$LAKEKEEPER_PID" > "$LAKEKEEPER_PID_FILE"

  wait_for_lakekeeper
  bootstrap
  create_warehouse
  WAREHOUSE_ID=$(warehouse_id)
  if [[ -z "$WAREHOUSE_ID" ]]; then
    echo "❌ Could not find the id of warehouse $LAKEKEEPER_WAREHOUSE"
    exit 1
  fi
  echo "$WAREHOUSE_ID" > "$LAKEKEEPER_WAREHOUSE_FILE"
  echo "🎉 Lakekeeper setup complete (warehouse $WAREHOUSE_ID)."

  if [[ "$NOWAIT" == "false" ]]; then
    trap 'echo "Stopping Lakekeeper..."; kill "$LAKEKEEPER_PID"; wait "$LAKEKEEPER_PID"' TERM INT
    wait "$LAKEKEEPER_PID"
  fi
}

main
