#!/usr/bin/env bash
# Register the MortgageApplication JSON Schema with a TOKENIZE rule set:
#
#   ${TOPIC}-value → ruleSet.domainRules → TWO TOKENIZE rules:
#       tokenizePII  (tag PII, token space pii-ssn, deterministic)
#       tokenizePCI  (tag PCI, token space pci-cc, non-deterministic)
#
# Also registers under the canonical (un-suffixed) subject so both the
# user-visible and TopicNameStrategy serializer subjects resolve.
#
# This is the exact same mechanism CSFLE uses — a TRANSFORM domain rule, tag-
# scoped, mode WRITEREAD, onFailure ERROR,NONE — only the rule TYPE is TOKENIZE
# (resolved client-side by our executor) and the params are tokenize.* instead
# of encrypt.*.
#
# Two domainRules on one schema require validateRules=false at the SR /config
# endpoint (the same Limited-Availability multi-rule path the CSFLE2 add-on uses).
# We GET-merge-PUT so other config fields are preserved.
#
# Idempotent: SR returns the same schema id on re-POST of identical content.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"
[[ -f "$ENV_FILE" ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh first" >&2; exit 1; }
source "$ENV_FILE"

[[ -n "${SR_URL:-}"     ]] || { echo "ERROR: SR_URL not set"     >&2; exit 1; }
[[ -n "${SR_API_KEY:-}" ]] || { echo "ERROR: SR_API_KEY not set" >&2; exit 1; }
: "${VAULT_URL:=http://localhost:9099}"
: "${PII_TOKEN_SPACE:=pii-ssn}"
: "${PCI_TOKEN_SPACE:=pci-cc}"
: "${TOKENIZE_VAULT_TYPE:=mock-vault}"
: "${TOPIC:=mortgage-tokenized}"
: "${PLAINTEXT_TOPIC:=${TOPIC}-plaintext}"

SR_AUTH="${SR_API_KEY}:${SR_API_SECRET}"
SCHEMA_FILE="${REPO_DIR}/schemas/mortgage_application_tokenized.json"
SCHEMA_STR=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$SCHEMA_FILE")
# Plaintext baseline uses a DIFFERENT schema where ssn/credit_card_number are
# dashed strings, so clear-text is visually obvious vs. the tokenized integers.
PLAINTEXT_SCHEMA_FILE="${REPO_DIR}/schemas/mortgage_application_plaintext.json"
PLAINTEXT_SCHEMA_STR=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$PLAINTEXT_SCHEMA_FILE")

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

# ── Unlock multi-rule: GET-merge-PUT validateRules=false on the subject config ─
enable_multi_rule() {
  local subject="$1"
  curl -s -u "$SR_AUTH" "${SR_URL}/config/${subject}" -o "$TMP/cfg.json" || true
  python3 - "$TMP/cfg.json" "$TMP/cfg_new.json" <<'PY'
import json, sys
try:
    cfg = json.load(open(sys.argv[1]))
    if not isinstance(cfg, dict):
        cfg = {}
except Exception:
    cfg = {}
cfg["validateRules"] = False
json.dump(cfg, open(sys.argv[2], "w"))
PY
  curl -s -u "$SR_AUTH" -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -X PUT "${SR_URL}/config/${subject}" -d @"$TMP/cfg_new.json" >/dev/null || true
}

build_plaintext_payload() {
  # Dashed-string ssn/cc, NO ruleSet — the "without tokenization" baseline.
  cat <<EOF
{
  "schemaType": "JSON",
  "schema": ${PLAINTEXT_SCHEMA_STR},
  "metadata": {
    "properties": {
      "version":     "1.0.0",
      "owner":       "tokenization-demo",
      "description": "MortgageApplication — PLAINTEXT baseline: no ruleSet, ssn/credit_card_number in the clear (dashed)"
    }
  }
}
EOF
}

# Allow the plaintext subject's field types to change (integer -> dashed string)
# without a BACKWARD-compatibility rejection.
set_compat_none() {
  local subject="$1"
  curl -s -u "$SR_AUTH" -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -X PUT "${SR_URL}/config/${subject}" -d '{"compatibility":"NONE"}' >/dev/null || true
}

build_payload() {
  cat <<EOF
{
  "schemaType": "JSON",
  "schema": ${SCHEMA_STR},
  "metadata": {
    "properties": {
      "version":     "1.0.0",
      "owner":       "tokenization-demo",
      "description": "MortgageApplication — CSFLT: ssn + credit_card_number tokenized (integer, type/length preserving)",
      "pii_fields":  "ssn",
      "pci_fields":  "credit_card_number"
    }
  },
  "ruleSet": {
    "domainRules": [
      {
        "name": "tokenizePII",
        "kind": "TRANSFORM",
        "type": "TOKENIZE",
        "mode": "WRITEREAD",
        "tags": ["PII"],
        "params": {
          "tokenize.vault.type":    "${TOKENIZE_VAULT_TYPE}",
          "tokenize.vault.url":     "${VAULT_URL}",
          "tokenize.token.space":   "${PII_TOKEN_SPACE}",
          "tokenize.deterministic": "true"
        },
        "onFailure": "ERROR,NONE"
      },
      {
        "name": "tokenizePCI",
        "kind": "TRANSFORM",
        "type": "TOKENIZE",
        "mode": "WRITEREAD",
        "tags": ["PCI"],
        "params": {
          "tokenize.vault.type":    "${TOKENIZE_VAULT_TYPE}",
          "tokenize.vault.url":     "${VAULT_URL}",
          "tokenize.token.space":   "${PCI_TOKEN_SPACE}",
          "tokenize.deterministic": "false"
        },
        "onFailure": "ERROR,NONE"
      }
    ]
  }
}
EOF
}

register() {
  local subject="$1" payload="$2" status sid
  status=$(curl -s -o "$TMP/resp.json" -w "%{http_code}" \
            -u "$SR_AUTH" \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            -X POST "${SR_URL}/subjects/${subject}/versions" \
            -d "$payload")
  if [[ "$status" == "200" ]]; then
    sid=$(python3 -c "import json; print(json.load(open('$TMP/resp.json'))['id'])")
    echo "  ✓ ${subject} → schema id ${sid}"
  else
    echo "  ✗ ${subject} → HTTP ${status}"
    cat "$TMP/resp.json"; echo
    exit 1
  fi
}

echo "── enabling multi-rule (validateRules=false) ──────────────────────────"
enable_multi_rule "${TOPIC}-value"
enable_multi_rule "${TOPIC}"
echo "  ✓ done"

echo ""
echo "── CSFLT schema (two TOKENIZE domain rules: PII + PCI) ────────────────"
payload=$(build_payload)
register "${TOPIC}"        "$payload"
register "${TOPIC}-value"  "$payload"

echo ""
echo "── plaintext baseline schema (no ruleSet, dashed ssn/cc) ──────────────"
set_compat_none "${PLAINTEXT_TOPIC}-value"
set_compat_none "${PLAINTEXT_TOPIC}"
pt_payload=$(build_plaintext_payload)
register "${PLAINTEXT_TOPIC}"        "$pt_payload"
register "${PLAINTEXT_TOPIC}-value"  "$pt_payload"

echo ""
echo "Schemas registered. Next: bash scripts/03_create_topics.sh"
