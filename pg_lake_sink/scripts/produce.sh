#!/usr/bin/env bash
# Create the topic (if needed) and produce sample JSON messages into it.
# Usage: scripts/produce.sh [topic] [count]
set -euo pipefail

TOPIC="${1:-events}"
COUNT="${2:-10}"
CONTAINER="${CONTAINER:-pg_lake_sink_redpanda}"

echo "Ensuring topic '$TOPIC' exists..."
docker exec "$CONTAINER" rpk topic create "$TOPIC" >/dev/null 2>&1 || true

echo "Producing $COUNT message(s) to '$TOPIC'..."
for i in $(seq 1 "$COUNT"); do
  key="user-$((RANDOM % 5))"
  value="{\"id\": $i, \"event\": \"click\", \"user\": \"$key\", \"ts\": \"$(date -u +%FT%TZ)\"}"
  printf '%s\t%s\n' "$key" "$value"
done | docker exec -i "$CONTAINER" rpk topic produce "$TOPIC" --format '%k\t%v\n'

echo "Done. Recent messages:"
docker exec "$CONTAINER" rpk topic consume "$TOPIC" --num "$COUNT" --offset start --format '%v\n' 2>/dev/null | tail -n "$COUNT" || true
