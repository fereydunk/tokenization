#!/usr/bin/env bash
# Preflight the CSFLT demo, then launch the web wizard.
#
# Checks: confluent CLI (logged in), CP 8.3.2, Java 21, Maven, python3.
# Builds the executor JAR if missing, starts the local mock vault if not already
# running, then serves the wizard on http://localhost:8894.
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"
[[ -f .env ]] || cp .env.example .env
set -a; source .env; set +a

CP_HOME="${CP_HOME:-$HOME/confluent-8.3.2}"
JAVA_HOME_DIR="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
PORT=8894
JAR="$REPO_DIR/executor/target/field-tokenization-executor-0.1.0.jar"
: "${VAULT_URL:=http://localhost:9099}"
: "${VAULT_PORT:=9099}"
: "${VAULT_DB_PATH:=/tmp/csflt-mock-vault/vault.db}"

fail() { echo "ERROR: $*" >&2; exit 1; }

# ── preflight ────────────────────────────────────────────────────────────────
command -v confluent >/dev/null || fail "confluent CLI not installed"
confluent kafka cluster list >/dev/null 2>&1 || fail "not logged in — run: confluent login --save"
[[ -x "$CP_HOME/bin/kafka-json-schema-console-producer" ]] \
  || fail "CP 8.3.2 not at $CP_HOME — download confluent-8.3.2.tar.gz and extract there"
[[ -x "$JAVA_HOME_DIR/bin/java" ]] || fail "Java 21 not at $JAVA_HOME_DIR (brew install openjdk@21)"
command -v mvn >/dev/null || fail "maven not installed (brew install maven)"
command -v python3 >/dev/null || fail "python3 not installed"
echo "✓ preflight ok"

# ── build the executor JAR if missing ────────────────────────────────────────
if [[ ! -f "$JAR" ]]; then
  echo "→ building executor JAR (mvn package) ..."
  (cd executor && JAVA_HOME="$JAVA_HOME_DIR" mvn -q -B package) || fail "executor build failed"
fi
echo "✓ executor JAR present"

# ── start the mock vault if not already up ───────────────────────────────────
if curl -sf "${VAULT_URL}/health" >/dev/null 2>&1; then
  echo "✓ token vault already running at ${VAULT_URL}"
else
  echo "→ starting mock token vault ..."
  mkdir -p scripts/logs
  VAULT_PORT="$VAULT_PORT" VAULT_DB_PATH="$VAULT_DB_PATH" \
    nohup python3 mock-vault/server.py > scripts/logs/vault.log 2>&1 &
  for _ in $(seq 1 20); do
    curl -sf "${VAULT_URL}/health" >/dev/null 2>&1 && break
    sleep 0.25
  done
  curl -sf "${VAULT_URL}/health" >/dev/null 2>&1 \
    && echo "✓ token vault up at ${VAULT_URL}" \
    || fail "token vault failed to start (see scripts/logs/vault.log)"
fi

# ── free the web port, then launch the wizard ────────────────────────────────
lsof -ti ":$PORT" | xargs kill -9 2>/dev/null || true

echo ""
echo "tokenization (CSFLT) → http://localhost:$PORT"
echo ""

# Open the browser once the server is up (2s head start). `open` is macOS;
# fall back to xdg-open on Linux. Backgrounded so it doesn't block the server.
( sleep 2 && { command -v open >/dev/null && open "http://localhost:$PORT" \
             || { command -v xdg-open >/dev/null && xdg-open "http://localhost:$PORT"; }; } ) >/dev/null 2>&1 &

CP_HOME="$CP_HOME" JAVA_HOME="$JAVA_HOME_DIR" EXECUTOR_JAR="$JAR" WEB_PORT="$PORT" \
  exec python3 web/server.py
