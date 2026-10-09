# Troubleshooting

## `confluent kafka cluster list` fails / "not logged in"
Run `confluent login --save`. The scripts and wizard rely on an active CLI session.

## `CP 8.3.2 not at ~/confluent-8.3.2`
```bash
curl -O https://packages.confluent.io/archive/8.3/confluent-8.3.2.tar.gz
tar -xzf confluent-8.3.2.tar.gz -C ~
```

## `Java 21 not at /opt/homebrew/opt/openjdk@21`
`brew install openjdk@21`. If Java lives elsewhere, set `JAVA_HOME` in `.env` or export it.

## Executor JAR missing / rebuild
`make build` (or the wizard's "Build executor JAR"). Produces
`executor/target/field-tokenization-executor-0.1.0.jar`. It must be on the console tool classpath —
the Makefile and wizard set `CLASSPATH` to it automatically.

## Producer error: `Connection refused ... localhost:8081`
The serde config isn't reaching the serializer — pass `schema.registry.url` (and `basic.auth.*`,
`value.schema.id`) as `--property` on the **producer**, not only in `--producer.config`. The Makefile
and wizard already do this.

## Consumer prints its usage/help and exits
Two causes, both already handled by the Makefile/wizard:
- The consumer uses **`--formatter-property`**, not `--property`, for serde config.
- `--group` must **not** conflict with a `group.id` in the `--consumer.config` file. The consumer
  config files intentionally have **no** `group.id`; `--group` is the sole source.

## Consumer reads 0 records even though produce succeeded
A reused consumer group whose committed offsets are already at the end makes `--from-beginning` a
no-op. Use a **fresh** `--group` (the Makefile appends `$RANDOM`; the wizard randomizes per read).
Verify messages exist with:
```bash
~/confluent-8.3.2/bin/kafka-get-offsets --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --topic "$TOPIC" --command-config <(envsubst < config/consumer-tokenize-authorized.properties)
```

## There is no `timeout` command on macOS
Don't wrap the console tools in `timeout` — it isn't installed by default. Use the consumer's own
`--timeout-ms` flag (the wizard uses it; the Makefile streams until Ctrl-C).

## Authorized consumer shows tokens instead of real values
- The vault isn't reachable or `VAULT_API_KEY` is wrong/empty — check `VAULT_AUTHORIZED_KEY` in
  `.env` and `curl $VAULT_URL/health`.
- The key lacks `can_detokenize` — re-mint with `scripts/01_setup_vault.sh` (clear
  `VAULT_AUTHORIZED_KEY` in `.env` first to force a fresh mint).

## Unauthorized consumer shows real values (boundary not holding)
Confirm it's launched with `VAULT_UNAUTHORIZED_KEY` (not the authorized key) and that that key was
minted with `can_detokenize=false`.

## Schema registration fails with a rule-validation error
Two `TOKENIZE` domain rules on one schema need `validateRules=false` on the subject config.
`scripts/02_register_schemas.sh` sets it (GET-merge-PUT). If a stale/incompatible schema is already
registered under the subject, delete it:
```bash
curl -X DELETE -u "$SR_API_KEY:$SR_API_SECRET" "$SR_URL/subjects/$TOPIC-value"
curl -X DELETE -u "$SR_API_KEY:$SR_API_SECRET" "$SR_URL/subjects/$TOPIC-value?permanent=true"
```

## "feature requires Advanced" on schema/rule registration
The environment needs **Stream Governance Advanced** (Cloud UI → Environment → Stream Governance).
Same requirement as CSFLE/CSPE.

## Wizard "Save & mint keys" fails, or the env/cluster dropdowns are empty
The picker uses your `confluent` CLI session. If the dropdowns show "login needed", run
`confluent login --save` and re-check. "Schema Registry not enabled for this environment" means the
chosen env has no SR — enable Stream Governance for it in the Cloud UI first. Switching to a
different cluster/SR mints fresh API keys and deletes the old ones.

## Plaintext produce fails schema validation
The plaintext topic's schema stores `ssn`/`credit_card_number` as **dashed strings**, not integers.
`web/datagen.py` is type-aware and produces the right shape per topic automatically; if you hand-craft
a record, quote those fields (`"ssn":"123-45-6789"`). The plaintext subject's compatibility is set to
`NONE` by `scripts/02` so its field types may differ from the tokenized schema.

## The browser didn't open on `startup.sh`
`startup.sh` opens the wizard with `open` (macOS) / `xdg-open` (Linux) after a 2-second head start.
If neither is available, just visit http://localhost:8894 manually.

## Vault isn't running
`make vault-start`, or `python3 mock-vault/server.py`. Health: `curl http://localhost:9099/health`.
Logs: `scripts/logs/vault.log`.

## Port 8894 already in use
`make kill` (or `lsof -ti :8894 | xargs kill -9`), then re-run `bash startup.sh`.

## Running the test suites
```bash
make test     # unit suites: mock-vault pytest + executor mvn test (no Cloud needed)
make e2e      # full end-to-end against Confluent Cloud (10 live cases + the 2 unit suites)
```
`make test` needs a venv for pytest: `python3 -m venv .venv && .venv/bin/pip install pytest`.
`make e2e` needs `make setup` to have run (vault keys, schemas, topics).
