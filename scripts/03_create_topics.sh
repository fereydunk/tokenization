#!/usr/bin/env bash
# Create the demo topic in Confluent Cloud (idempotent — `--if-not-exists`).
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh first" >&2; exit 1; }
source "$ENV_FILE"

[[ -n "${ENV_ID:-}"     ]] || { echo "ERROR: ENV_ID not set"     >&2; exit 1; }
[[ -n "${CLUSTER_ID:-}" ]] || { echo "ERROR: CLUSTER_ID not set" >&2; exit 1; }
: "${TOPIC:=mortgage-tokenized}"
: "${PLAINTEXT_TOPIC:=${TOPIC}-plaintext}"

create_topic() {
  local t="$1"
  echo "→ ${t} ..."
  if confluent kafka topic create "$t" \
       --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
       --partitions 3 --if-not-exists; then
    echo "  ✓ ready"
  else
    echo "  ✗ topic create failed"
    exit 1
  fi
}

create_topic "$TOPIC"
create_topic "$PLAINTEXT_TOPIC"

echo ""
echo "Setup complete!"
echo "  Produce:  make produce"
echo "  Consume:  make consume-authorized   /   make consume-unauthorized"
echo "  Web UI:   bash startup.sh   →   http://localhost:8894"
