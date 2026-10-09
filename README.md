# Client-Side Field Level Tokenization (CSFLT) — demo

A working prototype of **tokenization as a Schema Registry data-contract rule**, the
tokenization counterpart to Confluent's shipped Client-Side Field Level Encryption (CSFLE).

Where CSFLE **encrypts** a tagged field (changing its length, so it can't protect fixed-length
types like integers), CSFLT **replaces** the field value with a vault-issued **token of the same
type and length**. Integer SSNs and credit-card numbers tokenize to other integers of the same
digit length — the headline capability CSFLE cannot provide.

> **This is a prototype of a not-yet-shipped capability.** We implement a real Schema Registry
> `TOKENIZE` rule executor (Java) and run it through Confluent Platform's real console tools against
> a real Confluent Cloud cluster. The only mocked piece is the token **vault** (a tiny local service
> standing in for Protegrity / HashiCorp Vault). Token generation is demo-grade, **not** cryptographic
> format-preserving encryption.

## CSFLE vs CSFLT

| | CSFLE (shipped) | CSFLT (this demo) |
|---|---|---|
| Rule type | `ENCRYPT` | `TOKENIZE` |
| What it does | encrypts the field (ciphertext) | swaps the value for a vault token |
| Reversible via | a key (KMS/KEK/DEK) — local crypto | a live lookup in an external vault |
| Confluent holds | the KEK (shared for managed services) | nothing — only tokens ever |
| Preserves field length/type | **no** (int/fixed unsupported) | **yes** (int → same-length int) |
| Who resolves the secret | Confluent client, using the key | the customer's vault (Protegrity/Vault) |
| What an unauthorized reader sees | ciphertext | opaque same-length token |

Both are `TRANSFORM` domain rules in the same data-contract `ruleSet`, tag-scoped, `mode: WRITEREAD`,
`onFailure: ERROR,NONE`, resolved client-side by a registered executor. CSFLT adds no new
serialization path or SR concept — just a new rule *type*.

## Prerequisites

- **Confluent Platform 8.3.2** at `~/confluent-8.3.2` (the demo subprocesses its
  `kafka-json-schema-console-{producer,consumer}` tools). Download:
  `curl -O https://packages.confluent.io/archive/8.3/confluent-8.3.2.tar.gz && tar -xzf confluent-8.3.2.tar.gz -C ~`
- **Java 21** at `/opt/homebrew/opt/openjdk@21` (`brew install openjdk@21`)
- **Maven** (`brew install maven`) — builds the executor JAR
- **`confluent` CLI**, logged in (`confluent login --save`)
- **Python 3** (stdlib only — runs the mock vault and the wizard)
- A Confluent Cloud environment with **Stream Governance Advanced** (authorizes rule execution)

## Quick start (web wizard)

```bash
bash startup.sh     # preflight, build the JAR, start the vault, open the wizard
# → http://localhost:8894
```

The wizard has three sections, top to bottom:

1. **Connect & set up token services** — vault + Confluent Cloud status; **pick your environment,
   cluster, and topic names** from the dropdowns and *Save & mint keys* (describes the cluster + Schema
   Registry and mints API keys into `.env`); then *Run all*: build the executor → mint vault keys +
   token spaces → register the `TOKENIZE` schema and a plaintext baseline schema → create both topics.
   (Prefer the CLI? `make discover` auto-selects when there's a single env/cluster.)
2. **Produce — two ways** — side by side: *with tokenization* (→ tokenized topic; `ssn`/`credit_card_number`
   become same-length integer tokens before leaving the client) and *without tokenization* (→ plaintext
   topic; same data, no rule, values in the clear). Each box streams the content of every message as it's
   produced, so you can see exactly what the client sends.
3. **Consume** — the key payoff. *Read & compare* reads the tokenized topic **both ways** and shows, for
   each record (joined by `loan_id`), the token that's actually on the broker (what an unauthorized reader
   sees) **next to** its detokenized real value (what an authorized reader sees) — so the difference is
   undeniable. Below it, the *plaintext topic* panel shows the no-protection baseline, where any reader
   sees the real dashed values.

## What you should observe

**Tokenized topic** (produced *with* tokenization):

| Field | On the broker (raw) | Authorized consumer | Unauthorized consumer |
|---|---|---|---|
| `ssn` (integer, PII) | 9-digit **token** | real 9-digit value | 9-digit **token** |
| `credit_card_number` (integer, PCI) | 16-digit **token** | real 16-digit value | 16-digit **token** |
| `credit_score` (integer, untagged) | real value | real value | real value |

The authorized and unauthorized consumers differ **only** in which vault API key they run with
(`VAULT_AUTHORIZED_KEY` can detokenize; `VAULT_UNAUTHORIZED_KEY` cannot) — same schema, same rule,
same cluster. This mirrors the CSFLE demo's with-KEK vs. no-KEK boundary.

In the wizard, the **Read & compare** consumer shows both of the first two columns at once — the
token on the broker next to its detokenized real value, joined per record by `loan_id` — so "the
broker never holds the real value" is undeniable at a glance.

**Plaintext topic** (produced *without* tokenization — the contrast):

| Field | On the broker (raw) | Any consumer (vault key or not) |
|---|---|---|
| `ssn`, `credit_card_number` | **real dashed values, in the clear** (`123-45-6789`) | **real values** |

The plaintext topic uses a separate, ruleSet-free schema where `ssn`/`credit_card_number` are
**dashed strings** — so clear-text is obvious at a glance versus the tokenized topic's bare-integer
tokens. With no rule there is no protection: the sensitive values sit on the broker and every reader
sees them. That gap is exactly what CSFLT closes.

## CLI path (without the web UI)

```bash
make build              # compile the Java TOKENIZE executor JAR
make vault-start        # start the local mock vault
confluent login --save
make discover           # discover CC env/cluster/SR, mint API keys
make setup              # vault keys + spaces, register both schemas, create both topics

# with tokenization
make produce COUNT=20         # tokenizes ssn + credit_card_number
make consume-authorized       # sees real values
make consume-unauthorized     # sees opaque integer tokens

# without tokenization (the contrast)
make produce-plaintext COUNT=20   # same data, no rule — stored in the clear
make consume-plaintext            # ANY reader sees the real values
```

## How it works

```
Producer (CP console tool + our TOKENIZE executor JAR on the classpath)
  │  for each PII/PCI-tagged field: POST /tokenize → vault returns a same-length token
  ▼
Confluent Cloud Kafka  ── stores only tokens; raw ssn/cc never reach the broker
  │
  ├─► Authorized consumer   (VAULT_API_KEY can_detokenize) → POST /detokenize → real values
  └─► Unauthorized consumer (VAULT_API_KEY cannot)         → 403 → onFailure=NONE → token stays
```

- The **`TOKENIZE` rule** lives in the schema's `ruleSet.domainRules` in Confluent Cloud Schema
  Registry — registered by `scripts/02_register_schemas.sh`. Confluent Cloud accepts the custom
  rule type and stores it; `validateRules=false` on the subject config unlocks the two-rule schema.
- The **executor** (`executor/`, Java) is discovered by the console tools via
  `rule.executors._default_.class=io.confluent.csflt.FieldTokenizationExecutor` and dispatched for
  every `TOKENIZE` rule. It calls the vault and reconstructs the token into the field's original
  Java type.
- The **vault** (`mock-vault/server.py`) owns all token generation/storage — Confluent never does.

## Repository layout

```
executor/        Maven project — the Java TOKENIZE FieldRuleExecutor (the engineering deliverable)
  src/main/java/io/confluent/csflt/
    FieldTokenizationExecutor.java   the TOKENIZE rule executor (extends FieldRuleExecutor)
    VaultClient.java                 pluggability SPI (bring-your-own vault)
    MockVaultClient.java             HTTP client to the demo vault (java.net.http)
mock-vault/      stdlib Python token vault (stands in for Protegrity / HashiCorp Vault)
schemas/
  mortgage_application_tokenized.json   integer ssn/cc, PII/PCI tags, TOKENIZE rules
  mortgage_application_plaintext.json   dashed-string ssn/cc, NO ruleSet (clear-text baseline)
scripts/         00 discover env · 01 set up vault · 02 register schemas · 03 create topics
config/          producer-tokenize · consumer-tokenize-{authorized,unauthorized}
                 producer-plaintext · consumer-plaintext  (.properties templates)
web/             server.py (wizard: env/cluster picker, produce, Read & compare) + datagen.py
tests/           test_mock_vault.py (pytest) · e2e_test.sh (full produce→consume assertions)
startup.sh       preflight + build + start vault + open the wizard
Makefile         CLI path (build, setup, produce[-plaintext], consume-*, test, e2e)
```

## Testing

```bash
make test     # unit suites only — mock-vault pytest + executor mvn test (no Cloud needed)
make e2e      # full end-to-end against Confluent Cloud (after `make setup`)
```

`make e2e` (`tests/e2e_test.sh`) drives produce → consume and asserts the "what you should
observe" table as 10 live test cases, plus the 2 unit suites (12 total):

| | Test case |
|---|---|
| TC1 | producer loads the `TOKENIZE` executor and tokenizes on write |
| TC2 | authorized consumer detokenizes to the real `ssn` / `credit_card_number` |
| TC3 | unauthorized consumer sees only tokens — never the real values |
| TC4 | tokens preserve type/length (9-digit int ssn, 16-digit int cc) |
| TC5 | deterministic PII — the same ssn always tokenizes to the same token |
| TC6 | non-deterministic PCI — the same cc tokenizes to different tokens |
| TC7 | untagged `credit_score` passes through unchanged for every reader |
| TC8 | the tokenized broker stores tokens — real values are absent from the raw wire bytes |
| TC9 | the plaintext topic shows real values to a reader with **no** vault key (no protection) |
| TC10 | without tokenization, the real values **are** on the raw wire — the risk CSFLT removes |

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — trust boundaries, data flow, the data-contract rule model
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — common failure modes and fixes

## Scope boundaries (v1)

- **Java executor only** (matches the PRD's "Java first"); a `confluent-kafka-python` executor is a
  documented future addition.
- **Mock vault only** — the `VaultClient` interface is the extension point for real Protegrity /
  HashiCorp Vault drivers.
- **No fully-managed Flink/Connect detokenization** — unlike CSFLE's shared-KEK model, there is no
  key to hand Confluent; a managed service would have to call the customer's vault at runtime. This
  is the PRD's open item and is out of scope here (the demo shows local clients only).
- Token generation is **demo-grade**, not cryptographic FPE.
