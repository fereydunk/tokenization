# CSFLT demo — CLI path (mirrors the web wizard, minus the guided UX).
#
# Prereqs: confluent CLI logged in; CP 8.3.2 at ~/confluent-8.3.2; Java 21; mvn;
# the mock vault running (make vault-start) ; .env populated (make discover).

SHELL := /bin/bash

-include .env
export

CP_HOME   ?= $(HOME)/confluent-8.3.2
JAVA_HOME ?= /opt/homebrew/opt/openjdk@21
export JAVA_HOME

PRODUCER := $(CP_HOME)/bin/kafka-json-schema-console-producer
CONSUMER := $(CP_HOME)/bin/kafka-json-schema-console-consumer
EXECUTOR := io.confluent.csflt.FieldTokenizationExecutor
JAR      := $(CURDIR)/executor/target/field-tokenization-executor-0.1.0.jar
PLAINTEXT_TOPIC ?= $(TOPIC)-plaintext

# our executor JAR must be on the console tools' classpath
export CLASSPATH := $(JAR)

.PHONY: help build vault-start vault-stop discover vault schemas topics setup \
        produce produce-plaintext consume-authorized consume-unauthorized \
        consume-plaintext web kill clean test e2e

help:
	@echo "Setup:    make build        — compile the Java TOKENIZE executor JAR"
	@echo "          make vault-start   — start the local mock vault"
	@echo "          make discover      — discover CC env/cluster/SR, mint API keys"
	@echo "          make setup         — vault + schemas + topics (after discover)"
	@echo "With tokenization:    make produce             COUNT=20"
	@echo "                      make consume-authorized   (sees real ssn / credit_card_number)"
	@echo "                      make consume-unauthorized (sees opaque integer tokens)"
	@echo "Without tokenization: make produce-plaintext   COUNT=20"
	@echo "                      make consume-plaintext    (ANY reader sees real values)"
	@echo "Web UI:   bash startup.sh    → http://localhost:8894"

build:
	cd executor && mvn -q -B package

vault-start:
	@mkdir -p scripts/logs
	@VAULT_PORT=$(or $(VAULT_PORT),9099) VAULT_DB_PATH=$(or $(VAULT_DB_PATH),/tmp/csflt-mock-vault/vault.db) \
	  nohup python3 mock-vault/server.py > scripts/logs/vault.log 2>&1 & \
	  sleep 1 ; curl -sf $(or $(VAULT_URL),http://localhost:9099)/health && echo " vault up" || echo " vault failed to start"

vault-stop:
	@pkill -f "mock-vault/server.py" && echo "vault stopped" || echo "no vault running"

discover:
	bash scripts/00_discover_env.sh

vault:
	bash scripts/01_setup_vault.sh

schemas:
	bash scripts/02_register_schemas.sh

topics:
	bash scripts/03_create_topics.sh

setup: vault schemas topics

# ── produce (tokenizes PII/PCI integer fields before they hit Kafka) ─────────
produce:
	@test -n "$(TOPIC)" || (echo "ERROR: run 'make discover' first"; exit 1)
	@echo "→ producing $(or $(COUNT),20) records to $(TOPIC) (ssn + credit_card_number tokenized) ..."
	@SID=$$(curl -sf -u "$(SR_API_KEY):$(SR_API_SECRET)" "$(SR_URL)/subjects/$(TOPIC)-value/versions/latest" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])") ; \
	VAULT_API_KEY="$(VAULT_AUTHORIZED_KEY)" $(PRODUCER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(TOPIC) \
	  --producer.config <(envsubst < config/producer-tokenize.properties) \
	  --property schema.registry.url=$(SR_URL) \
	  --property basic.auth.credentials.source=USER_INFO \
	  --property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --property value.schema.id=$$SID \
	  --property auto.register.schemas=false \
	  --property "rule.executors=_default_" \
	  --property "rule.executors._default_.class=$(EXECUTOR)" \
	  < <(python3 web/datagen.py $(or $(COUNT),20))

# ── produce WITHOUT tokenization (plaintext baseline, no rule) ───────────────
produce-plaintext:
	@test -n "$(PLAINTEXT_TOPIC)" || (echo "ERROR: run 'make discover' first"; exit 1)
	@echo "→ producing $(or $(COUNT),20) PLAINTEXT records to $(PLAINTEXT_TOPIC) (no tokenization) ..."
	@SID=$$(curl -sf -u "$(SR_API_KEY):$(SR_API_SECRET)" "$(SR_URL)/subjects/$(PLAINTEXT_TOPIC)-value/versions/latest" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])") ; \
	TOPIC="$(PLAINTEXT_TOPIC)" $(PRODUCER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(PLAINTEXT_TOPIC) \
	  --producer.config <(envsubst < config/producer-plaintext.properties) \
	  --property schema.registry.url=$(SR_URL) \
	  --property basic.auth.credentials.source=USER_INFO \
	  --property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --property value.schema.id=$$SID \
	  --property auto.register.schemas=false \
	  < <(TOPIC="$(PLAINTEXT_TOPIC)" python3 web/datagen.py $(or $(COUNT),20))

# ── consume (authorized: real values ; unauthorized: tokens) ─────────────────
# Note: macOS has no `timeout`; the consumer streams until Ctrl-C.
# A fresh --group each run guarantees --from-beginning actually replays.
# group.id is intentionally absent from the config files so --group is the sole
# source (the console consumer aborts if the two disagree).
consume-authorized:
	@echo "[AUTHORIZED]  $(TOPIC) — ssn / credit_card_number DETOKENIZED  (Ctrl-C to stop)"
	VAULT_API_KEY="$(VAULT_AUTHORIZED_KEY)" $(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(TOPIC) --from-beginning --group csflt-auth-$$RANDOM \
	  --consumer.config <(envsubst < config/consumer-tokenize-authorized.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --formatter-property "rule.executors=_default_" \
	  --formatter-property "rule.executors._default_.class=$(EXECUTOR)"

consume-unauthorized:
	@echo "[UNAUTHORIZED]  $(TOPIC) — ssn / credit_card_number stay TOKENS  (Ctrl-C to stop)"
	VAULT_API_KEY="$(VAULT_UNAUTHORIZED_KEY)" $(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(TOPIC) --from-beginning --group csflt-unauth-$$RANDOM \
	  --consumer.config <(envsubst < config/consumer-tokenize-unauthorized.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --formatter-property "rule.executors=_default_" \
	  --formatter-property "rule.executors._default_.class=$(EXECUTOR)"

consume-plaintext:
	@echo "[PLAINTEXT]  $(PLAINTEXT_TOPIC) — ssn / credit_card_number in the CLEAR (any reader)  (Ctrl-C to stop)"
	$(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(PLAINTEXT_TOPIC) --from-beginning --group csflt-plain-$$RANDOM \
	  --consumer.config <(envsubst < config/consumer-plaintext.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET)

# ── tests ────────────────────────────────────────────────────────────────────
# test : unit suites only (mock vault + executor), no Confluent Cloud needed.
# e2e  : full produce→consume verification against CC (needs setup done).
test:
	@test -x .venv/bin/python && .venv/bin/python -m pytest tests/test_mock_vault.py -q \
	  || echo "(skip pytest — create venv: python3 -m venv .venv && .venv/bin/pip install pytest)"
	cd executor && mvn -q test

e2e:
	bash tests/e2e_test.sh

web:
	bash startup.sh

kill:
	@lsof -ti :8894 | xargs kill -9 2>/dev/null || true

clean:
	cd executor && mvn -q clean || true
	@$(MAKE) vault-stop
