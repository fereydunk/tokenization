#!/usr/bin/env bash
# End-to-end test for the CSFLT demo — drives the full produce → consume flow
# against the real Confluent Cloud cluster + CP 8.3.2 console tools + our Java
# TOKENIZE executor, and asserts the behaviors from the README's
# "what you should observe" table.
#
# Test matrix (the CSFLT analog of a CSFLE verification):
#   TC1  producer tokenizes on write          (vault gains mappings)
#   TC2  authorized consumer detokenizes       (sees real ssn / credit_card_number)
#   TC3  unauthorized consumer sees tokens      (values differ from real)
#   TC4  type/length preserved                  (ssn 9 digits, cc 16 digits, integers)
#   TC5  deterministic PII                       (same ssn -> same token)
#   TC6  non-deterministic PCI                   (same cc -> different tokens)
#   TC7  untagged field passes through           (credit_score identical everywhere)
#   TC8  raw broker holds tokens, not plaintext  (real values absent on the wire)
#   TC9  plaintext topic (no rule) shows real values to ANY reader (no vault key)
#   TC10 raw plaintext wire CONTAINS the real values (the contrast to TC8)
#
# Usage:  bash tests/e2e_test.sh            (runs unit suites first, then live e2e)
#         bash tests/e2e_test.sh --no-unit  (skip unit suites)
set -uo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"
[[ -f .env ]] || { echo "ERROR: .env missing — run scripts/00_discover_env.sh"; exit 1; }
set -a; source .env; set +a

CP_HOME="${CP_HOME:-$HOME/confluent-8.3.2}"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
JAR="$REPO_DIR/executor/target/field-tokenization-executor-0.1.0.jar"
export CLASSPATH="$JAR"
PRODUCER="$CP_HOME/bin/kafka-json-schema-console-producer"
CONSUMER="$CP_HOME/bin/kafka-json-schema-console-consumer"
EXECUTOR="io.confluent.csflt.FieldTokenizationExecutor"
: "${VAULT_URL:=http://localhost:9099}"
: "${PLAINTEXT_TOPIC:=${TOPIC:-}-plaintext}"

PASS=0; FAIL=0
ok()   { echo "  ✓ PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "  ✗ FAIL: $1"; FAIL=$((FAIL+1)); }
note() { echo "— $1"; }

# Known, fixed values so assertions are deterministic.
REAL_SSN=123456789            # 9 digits
REAL_CC=4111111111111111      # 16 digits
SCORE=750
RUN="e2e-$$-$RANDOM"          # unique marker in loan_id to isolate this run's records

# ── preflight ────────────────────────────────────────────────────────────────
note "preflight"
[[ -f "$JAR" ]] || { echo "building executor JAR…"; (cd executor && mvn -q -B package) || { echo "build failed"; exit 1; }; }
if ! curl -sf "$VAULT_URL/health" >/dev/null 2>&1; then
  echo "starting mock vault…"
  mkdir -p scripts/logs
  VAULT_PORT="${VAULT_PORT:-9099}" VAULT_DB_PATH="${VAULT_DB_PATH:-/tmp/csflt-mock-vault/vault.db}" \
    nohup python3 mock-vault/server.py > scripts/logs/vault.log 2>&1 &
  for _ in $(seq 1 20); do curl -sf "$VAULT_URL/health" >/dev/null 2>&1 && break; sleep 0.25; done
fi
curl -sf "$VAULT_URL/health" >/dev/null 2>&1 || { echo "ERROR: vault not reachable at $VAULT_URL"; exit 1; }
[[ -n "${VAULT_AUTHORIZED_KEY:-}" && -n "${VAULT_UNAUTHORIZED_KEY:-}" ]] \
  || { echo "ERROR: vault keys not set — run scripts/01_setup_vault.sh"; exit 1; }
[[ -n "${TOPIC:-}" ]] || { echo "ERROR: TOPIC not set — run setup"; exit 1; }
SID=$(curl -sf -u "$SR_API_KEY:$SR_API_SECRET" "$SR_URL/subjects/${TOPIC}-value/versions/latest" \
      | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])") \
  || { echo "ERROR: schema for ${TOPIC}-value not registered — run scripts/02_register_schemas.sh"; exit 1; }
echo "  vault up · topic=$TOPIC · schema id=$SID · run marker=$RUN"

# ── optional unit suites ─────────────────────────────────────────────────────
if [[ "${1:-}" != "--no-unit" ]]; then
  note "unit suites"
  if [[ -x .venv/bin/python ]]; then
    .venv/bin/python -m pytest tests/test_mock_vault.py -q >/tmp/e2e_py.txt 2>&1 \
      && ok "mock-vault pytest" || { bad "mock-vault pytest"; tail -5 /tmp/e2e_py.txt; }
  else
    echo "  (skip pytest — no .venv; create with: python3 -m venv .venv && .venv/bin/pip install pytest)"
  fi
  (cd executor && mvn -q test >/tmp/e2e_java.txt 2>&1) \
    && ok "executor mvn test" || { bad "executor mvn test"; tail -8 /tmp/e2e_java.txt; }
fi

# ── produce two known records (same ssn + cc, distinct loan_id) ──────────────
note "produce 2 records (ssn=$REAL_SSN cc=$REAL_CC, marker $RUN)"
rec () { # rec N
  printf '{"loan_id":"%s-%s","applicant_name":"E2E Tester","ssn":%s,"credit_card_number":%s,"annual_income_usd":120000,"loan_amount_usd":450000,"credit_score":%s,"property_address":"1 Test St","loan_purpose":"PURCHASE"}\n' \
    "$RUN" "$1" "$REAL_SSN" "$REAL_CC" "$SCORE"
}
{ rec 1; rec 2; } | VAULT_API_KEY="$VAULT_AUTHORIZED_KEY" "$PRODUCER" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$TOPIC" \
  --producer.config <(envsubst < config/producer-tokenize.properties) \
  --property schema.registry.url="$SR_URL" \
  --property basic.auth.credentials.source=USER_INFO \
  --property basic.auth.user.info="$SR_API_KEY:$SR_API_SECRET" \
  --property value.schema.id="$SID" \
  --property auto.register.schemas=false \
  --property "rule.executors=_default_" \
  --property "rule.executors._default_.class=$EXECUTOR" \
  >/tmp/e2e_prod.txt 2>&1
if grep -q "Registering rule object _default_ for TOKENIZE" /tmp/e2e_prod.txt; then
  ok "TC1 producer loaded the TOKENIZE executor and produced"
else
  bad "TC1 producer did not load executor"; tail -5 /tmp/e2e_prod.txt
fi

# ── consume helper: capture records matching our RUN marker ──────────────────
consume_run () {  # consume_run KEY CONFIG OUTFILE
  local key="$1" cfg="$2" out="$3"
  VAULT_API_KEY="$key" "$CONSUMER" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$TOPIC" --from-beginning \
    --timeout-ms 10000 --group "csflt-e2e-$RANDOM" \
    --consumer.config <(envsubst < "config/$cfg") \
    --formatter-property schema.registry.url="$SR_URL" \
    --formatter-property basic.auth.credentials.source=USER_INFO \
    --formatter-property basic.auth.user.info="$SR_API_KEY:$SR_API_SECRET" \
    --formatter-property "rule.executors=_default_" \
    --formatter-property "rule.executors._default_.class=$EXECUTOR" \
    2>/dev/null \
  | python3 -c "
import sys,json
run='$RUN'
rows=[]
for line in sys.stdin:
    line=line.strip()
    if not line or line[0] != '{': continue
    try: d=json.loads(line)
    except: continue
    if str(d.get('loan_id','')).startswith(run):
        rows.append(d)
# emit: ssn|cc|credit_score|ssn_type|cc_type  (dedup by loan_id, keep order)
seen=set();
for d in rows:
    lid=d['loan_id']
    if lid in seen: continue
    seen.add(lid)
    print('%s|%s|%s|%s|%s|%s' % (lid, d.get('ssn'), d.get('credit_card_number'),
          d.get('credit_score'), type(d.get('ssn')).__name__, type(d.get('credit_card_number')).__name__))
" > "$out"
}

note "consume AUTHORIZED"
consume_run "$VAULT_AUTHORIZED_KEY" consumer-tokenize-authorized.properties /tmp/e2e_auth.txt
note "consume UNAUTHORIZED"
consume_run "$VAULT_UNAUTHORIZED_KEY" consumer-tokenize-unauthorized.properties /tmp/e2e_unauth.txt

AUTH_N=$(wc -l < /tmp/e2e_auth.txt | tr -d ' ')
UNAUTH_N=$(wc -l < /tmp/e2e_unauth.txt | tr -d ' ')
echo "  authorized records: $AUTH_N ; unauthorized records: $UNAUTH_N"

# ── TC2: authorized sees REAL values ─────────────────────────────────────────
if [[ "$AUTH_N" -eq 2 ]] && awk -F'|' -v s="$REAL_SSN" -v c="$REAL_CC" \
      '{if($2!=s||$3!=c) bad=1} END{exit bad?1:0}' /tmp/e2e_auth.txt; then
  ok "TC2 authorized consumer detokenizes to real ssn=$REAL_SSN cc=$REAL_CC"
else
  bad "TC2 authorized values wrong"; cat /tmp/e2e_auth.txt
fi

# ── TC3: unauthorized sees TOKENS (differ from real) ─────────────────────────
if [[ "$UNAUTH_N" -eq 2 ]] && awk -F'|' -v s="$REAL_SSN" -v c="$REAL_CC" \
      '{if($2==s||$3==c) leaked=1} END{exit leaked?1:0}' /tmp/e2e_unauth.txt; then
  ok "TC3 unauthorized consumer sees tokens, never the real values"
else
  bad "TC3 unauthorized leaked a real value or wrong count"; cat /tmp/e2e_unauth.txt
fi

# ── TC4: type/length preserved (ssn 9-digit int, cc 16-digit int) ────────────
if awk -F'|' '{if(length($2)!=9||length($3)!=16||$5!="int"||($6!="int"&&$6!="long")) bad=1} END{exit bad?1:0}' /tmp/e2e_unauth.txt; then
  ok "TC4 tokens preserve type/length (ssn=9-digit int, cc=16-digit int)"
else
  bad "TC4 token type/length not preserved"; cat /tmp/e2e_unauth.txt
fi

# ── TC5: deterministic PII — the two ssn tokens are EQUAL ─────────────────────
SSN_TOKENS=$(awk -F'|' '{print $2}' /tmp/e2e_unauth.txt | sort -u | wc -l | tr -d ' ')
if [[ "$SSN_TOKENS" -eq 1 ]]; then
  ok "TC5 deterministic PII — same ssn tokenizes to the same token"
else
  bad "TC5 deterministic PII — expected 1 distinct ssn token, got $SSN_TOKENS"; awk -F'|' '{print $2}' /tmp/e2e_unauth.txt
fi

# ── TC6: non-deterministic PCI — the two cc tokens DIFFER ─────────────────────
CC_TOKENS=$(awk -F'|' '{print $3}' /tmp/e2e_unauth.txt | sort -u | wc -l | tr -d ' ')
if [[ "$CC_TOKENS" -eq 2 ]]; then
  ok "TC6 non-deterministic PCI — same cc tokenizes to different tokens"
else
  bad "TC6 non-deterministic PCI — expected 2 distinct cc tokens, got $CC_TOKENS"; awk -F'|' '{print $3}' /tmp/e2e_unauth.txt
fi

# ── TC7: untagged credit_score unchanged everywhere ──────────────────────────
if awk -F'|' -v v="$SCORE" '{if($4!=v) bad=1} END{exit bad?1:0}' /tmp/e2e_auth.txt \
   && awk -F'|' -v v="$SCORE" '{if($4!=v) bad=1} END{exit bad?1:0}' /tmp/e2e_unauth.txt; then
  ok "TC7 untagged credit_score=$SCORE passes through unchanged for both consumers"
else
  bad "TC7 untagged field changed"; echo "auth:"; cat /tmp/e2e_auth.txt; echo "unauth:"; cat /tmp/e2e_unauth.txt
fi

# ── TC8: raw broker bytes contain tokens, not plaintext ──────────────────────
note "raw consume (no executor) — verifying plaintext is absent on the wire"
cat > /tmp/e2e_wire.properties <<EOF
bootstrap.servers=${BOOTSTRAP_SERVERS}
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_API_KEY}" password="${KAFKA_API_SECRET}";
EOF
# NOTE: use the PLAIN console consumer here (not the json-schema one) so we see
# the stored bytes without SR deserialization — that's the whole point of a raw read.
"$CP_HOME/bin/kafka-console-consumer" --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$TOPIC" --from-beginning \
  --timeout-ms 12000 --group "csflt-e2e-raw-$RANDOM" \
  --consumer.config /tmp/e2e_wire.properties 2>/dev/null \
  | LC_ALL=C tr -c '[:print:]\n' ' ' | grep "$RUN" > /tmp/e2e_raw.txt || true
RAW_N=$(wc -l < /tmp/e2e_raw.txt | tr -d ' ')
if [[ "$RAW_N" -ge 2 ]] && ! grep -q "$REAL_SSN" /tmp/e2e_raw.txt && ! grep -q "$REAL_CC" /tmp/e2e_raw.txt; then
  ok "TC8 broker stores tokens — real ssn/cc absent from the raw wire bytes ($RAW_N marked records)"
else
  bad "TC8 plaintext found on the wire, or marker records missing (raw=$RAW_N)"
  grep -o "$REAL_SSN\|$REAL_CC" /tmp/e2e_raw.txt | sort -u
fi

# ── plaintext contrast (produce WITHOUT tokenization) ────────────────────────
note "produce 1 record to the PLAINTEXT topic ($PLAINTEXT_TOPIC) — no tokenization"
PT_SID=$(curl -sf -u "$SR_API_KEY:$SR_API_SECRET" "$SR_URL/subjects/${PLAINTEXT_TOPIC}-value/versions/latest" \
         | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null || true)
# The plaintext schema stores ssn/credit_card_number as dashed strings.
PT_SSN="123-45-6789"; PT_CC="4111-1111-1111-1111"
if [[ -z "$PT_SID" ]]; then
  bad "TC9/TC10 plaintext schema not registered — run scripts/02_register_schemas.sh"
else
  printf '{"loan_id":"%s-PT","applicant_name":"E2E Tester","ssn":"%s","credit_card_number":"%s","annual_income_usd":120000,"loan_amount_usd":450000,"credit_score":%s,"property_address":"1 Test St","loan_purpose":"PURCHASE"}\n' \
    "$RUN" "$PT_SSN" "$PT_CC" "$SCORE" \
  | "$PRODUCER" --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$PLAINTEXT_TOPIC" \
      --producer.config <(envsubst < config/producer-plaintext.properties) \
      --property schema.registry.url="$SR_URL" \
      --property basic.auth.credentials.source=USER_INFO \
      --property basic.auth.user.info="$SR_API_KEY:$SR_API_SECRET" \
      --property value.schema.id="$PT_SID" \
      --property auto.register.schemas=false >/tmp/e2e_pt_prod.txt 2>&1

  # TC9: a consumer with NO vault key sees the real (dashed) values on the plaintext topic
  VAULT_API_KEY="" "$CONSUMER" --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$PLAINTEXT_TOPIC" \
    --from-beginning --timeout-ms 10000 --group "csflt-e2e-pt-$RANDOM" \
    --consumer.config <(envsubst < config/consumer-plaintext.properties) \
    --formatter-property schema.registry.url="$SR_URL" \
    --formatter-property basic.auth.credentials.source=USER_INFO \
    --formatter-property basic.auth.user.info="$SR_API_KEY:$SR_API_SECRET" \
    2>/dev/null | grep "$RUN" > /tmp/e2e_pt.txt || true
  if grep -qF "\"ssn\":\"$PT_SSN\"" /tmp/e2e_pt.txt && grep -qF "\"credit_card_number\":\"$PT_CC\"" /tmp/e2e_pt.txt; then
    ok "TC9 plaintext topic shows real dashed ssn/cc to a reader with NO vault key (no protection)"
  else
    bad "TC9 plaintext consumer did not show real values"; cat /tmp/e2e_pt.txt
  fi

  # TC10: raw plaintext wire CONTAINS the real values (contrast to TC8)
  "$CP_HOME/bin/kafka-console-consumer" --bootstrap-server "$BOOTSTRAP_SERVERS" --topic "$PLAINTEXT_TOPIC" \
    --from-beginning --timeout-ms 10000 --group "csflt-e2e-ptraw-$RANDOM" \
    --consumer.config /tmp/e2e_wire.properties 2>/dev/null \
    | LC_ALL=C tr -c '[:print:]\n' ' ' | grep "$RUN" > /tmp/e2e_ptraw.txt || true
  if grep -qF "$PT_SSN" /tmp/e2e_ptraw.txt && grep -qF "$PT_CC" /tmp/e2e_ptraw.txt; then
    ok "TC10 without tokenization, real ssn/cc ARE on the raw wire (the risk CSFLT removes)"
  else
    bad "TC10 expected plaintext values on the wire but did not find them"; cat /tmp/e2e_ptraw.txt
  fi
fi

# ── summary ──────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════"
echo "  CSFLT end-to-end:  $PASS passed, $FAIL failed"
echo "════════════════════════════════════════════"
[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
