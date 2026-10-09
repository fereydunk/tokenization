#!/usr/bin/env bash
# Provision the local token vault for the demo:
#   1. create the two token spaces (pii-ssn, pci-cc)
#   2. mint an AUTHORIZED key   (can_tokenize + can_detokenize)
#   3. mint an UNAUTHORIZED key (can_tokenize only — never sees real values back)
#
# These are the vault-side analog of the per-principal KEK RBAC the CSFLE demo
# sets up in Confluent. The AUTHORIZED key is used by the producer and the
# authorized consumer; the UNAUTHORIZED key by the consumer that only ever sees
# tokens.
#
# Idempotent: token-space creation is idempotent; keys are minted fresh only if
# the corresponding .env value is empty.
#
# Prereq: the mock vault must be running (python3 mock-vault/server.py).
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh first" >&2; exit 1; }
source "$ENV_FILE"

: "${VAULT_URL:=http://localhost:9099}"
: "${PII_TOKEN_SPACE:=pii-ssn}"
: "${PCI_TOKEN_SPACE:=pci-cc}"

upsert() {
  local k="$1" v="$2"
  if grep -qE "^${k}=" "$ENV_FILE"; then
    sed -i '' "s|^${k}=.*|${k}=${v}|" "$ENV_FILE"
  else
    echo "${k}=${v}" >> "$ENV_FILE"
  fi
}

# fail fast if the vault isn't up
if ! curl -sf "${VAULT_URL}/health" >/dev/null; then
  echo "ERROR: token vault not reachable at ${VAULT_URL}" >&2
  echo "       start it with:  python3 mock-vault/server.py" >&2
  exit 1
fi

create_space() {
  local name="$1"
  echo "→ token space ${name} ..."
  curl -s -X POST "${VAULT_URL}/token-spaces" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${name}\"}" >/dev/null
  echo "  ✓ ready"
}

mint_key() {
  # mint_key LABEL CAN_TOKENIZE CAN_DETOKENIZE  -> echoes api_key
  local label="$1" tok="$2" detok="$3"
  curl -s -X POST "${VAULT_URL}/keys" \
    -H "Content-Type: application/json" \
    -d "{\"label\":\"${label}\",\"can_tokenize\":${tok},\"can_detokenize\":${detok}}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['api_key'])"
}

create_space "$PII_TOKEN_SPACE"
create_space "$PCI_TOKEN_SPACE"

if [[ -z "${VAULT_AUTHORIZED_KEY:-}" ]]; then
  echo "→ minting AUTHORIZED key (tokenize + detokenize) ..."
  VAULT_AUTHORIZED_KEY=$(mint_key "authorized" true true)
  upsert VAULT_AUTHORIZED_KEY "$VAULT_AUTHORIZED_KEY"
  echo "  ✓ ${VAULT_AUTHORIZED_KEY}"
else
  echo "✓ AUTHORIZED key already set"
fi

if [[ -z "${VAULT_UNAUTHORIZED_KEY:-}" ]]; then
  echo "→ minting UNAUTHORIZED key (tokenize only) ..."
  VAULT_UNAUTHORIZED_KEY=$(mint_key "unauthorized" true false)
  upsert VAULT_UNAUTHORIZED_KEY "$VAULT_UNAUTHORIZED_KEY"
  echo "  ✓ ${VAULT_UNAUTHORIZED_KEY}"
else
  echo "✓ UNAUTHORIZED key already set"
fi

echo ""
echo "Vault ready. Next: bash scripts/02_register_schemas.sh"
