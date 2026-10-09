# Architecture

CSFLT spans three independently-administered domains, exactly like the CSFLE demo — only the
"secret keeper" changes from a KMS to a token vault:

- **The token vault** (Protegrity / HashiCorp Vault in production; a local mock here) — generates,
  stores, and resolves tokens. Holds the only mapping from token back to real value.
- **Confluent Cloud** — the Kafka cluster (stores only tokens) and Schema Registry (stores the
  `TOKENIZE` rule). Confluent never sees or stores the token mapping.
- **The client** — a CP console producer/consumer running locally with our executor JAR, which
  calls the vault to tokenize on write and detokenize on read.

```
            ┌────────────────────────────────────────────────────────┐
            │  Laptop / client host                                    │
            │                                                          │
   stdin ──▶│  CP console producer ──┐                                 │
            │   + TOKENIZE executor  │  POST /tokenize   ┌───────────┐ │
            │                        └──────────────────▶│  Token    │ │
            │  CP console consumer ───  POST /detokenize ▶│  Vault    │ │
            │   + TOKENIZE executor    (authorized only)  │ (mapping) │ │
            └───────────┬──────────────────────────────  └───────────┘ │
                        │ tokenized records only                        │
            ────────────┼───────────────────────────────────────────────
                        ▼
            ┌───────────────────────────┐     ┌──────────────────────────┐
            │ Confluent Cloud — Kafka    │     │ Confluent Cloud — Schema  │
            │ topic holds TOKENS only    │     │ Registry: TOKENIZE ruleSet│
            └───────────────────────────┘     └──────────────────────────┘
```

## The data-contract rule model

A Confluent **data contract** is `schema + metadata + ruleSet`. Rules have a **kind**
(`TRANSFORM` / `CONDITION`) and a **type**. CSFLE is a `TRANSFORM` domain rule of type `ENCRYPT`.
**CSFLT is a `TRANSFORM` domain rule of type `TOKENIZE`** — same slot in the contract, same
tag-scoping, same client-side executor mechanism.

The rule as registered (`scripts/02_register_schemas.sh`):

```json
{
  "name": "tokenizePII", "kind": "TRANSFORM", "type": "TOKENIZE",
  "mode": "WRITEREAD", "tags": ["PII"],
  "params": {
    "tokenize.vault.type":    "mock-vault",
    "tokenize.vault.url":     "http://localhost:9099",
    "tokenize.token.space":   "pii-ssn",
    "tokenize.deterministic": "true"
  },
  "onFailure": "ERROR,NONE"
}
```

A second rule (`tokenizePCI`, tag `PCI`, space `pci-cc`, non-deterministic) sits alongside it. Two
domain rules on one schema require `validateRules=false` on the subject config — the same
Limited-Availability multi-rule path CSFLE2 uses.

## Two topics: tokenized vs. plaintext baseline

The demo provisions **two** topics from the same domain, to make the value of tokenization visible:

| | Tokenized topic (`$TOPIC`) | Plaintext topic (`$PLAINTEXT_TOPIC`) |
|---|---|---|
| Schema | `ssn`/`credit_card_number` as **integers**, PII/PCI tags | `ssn`/`credit_card_number` as **dashed strings**, no tags |
| ruleSet | two `TOKENIZE` domain rules | **none** |
| On the broker | same-length integer **tokens** | real **dashed** values, in the clear |
| Who can read the real value | only a consumer whose vault key can detokenize | **anyone** |

The plaintext topic is the "a team that hasn't adopted CSFLT" baseline. Its dashed string format
(`123-45-6789`, `4111-1111-1111-1111`) is deliberately chosen so clear-text is unmistakable at a
glance next to the tokenized topic's bare-integer tokens. It has its own compatibility set to `NONE`
so its field types can differ from the tokenized schema.

## Component responsibilities

### The executor (`executor/`, Java)
`FieldTokenizationExecutor extends io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor`,
`type() == "TOKENIZE"`. The base class walks the record and invokes our per-field transform only on
fields whose tags match the rule. Per field:
- **WRITE**: read `tokenize.*` params, `POST /tokenize`, reconstruct the token into the field's
  original Java type (so an `INT` stays an `INT`, same digit length).
- **READ**: `POST /detokenize`; on a vault `403` throw, which the framework maps through
  `onFailure=NONE` (read) — leaving the token in place.

It holds **no secret**: token generation/storage is entirely the vault's. The vault API key is read
from the `VAULT_API_KEY` environment variable (not the schema, not the Kafka config), mirroring how
CSFLE resolves KMS credentials from the environment.

### The vault (`mock-vault/server.py`)
Owns token generation + the token→value table (SQLite). **Capability-based API keys**
(`can_tokenize` / `can_detokenize`) gate the two operations and apply across all token spaces — the
demo mints one `VAULT_AUTHORIZED_KEY` (both capabilities, used by the producer and the authorized
consumer) and one `VAULT_UNAUTHORIZED_KEY` (tokenize only). Token generation preserves type/length:
a 9-digit integer → a random 9-digit integer; a 16-digit integer → a random 16-digit integer. Token
spaces (`pii-ssn`, `pci-cc`) are namespaces that keep each tag's token↔value mappings separate.

> Per-tag key scoping (a key that can detokenize PII but not PCI) is the CSFLE2 analog and a
> documented future extension — out of scope for v1.

### The wizard (`web/server.py`)
A stdlib HTTP server (no framework) that drives the same scripts and CP console tools the Makefile
uses, so the UI and CLI paths stay in sync. Three sections: (1) **Connect & set up** — vault/Cloud
status, an environment → cluster → topic **picker** (`/cc-envs`, `/cc-clusters`, `/cc-pick` describe
the cluster + SR and mint API keys into `.env`), and the provisioning steps; (2) **Produce** — streams
each message's content as it's written, with and without tokenization; (3) **Read & compare** — reads
the tokenized topic both ways (raw = tokens, authorized = real) and joins by `loan_id` so each
record's token and real value sit side by side.

### Confluent Cloud
Kafka stores only tokenized records. Schema Registry stores the `TOKENIZE` ruleSet and (via Stream
Governance Advanced) authorizes rule execution. **Confluent never sees a raw value and never sees
the token mapping.**

## Data flow

**Producer (WRITE)**
1. Console producer reads a JSON record; the JSON-schema serializer runs the schema's domain rules.
2. For each `PII`/`PCI`-tagged field, our executor `POST /tokenize` with the real value + field type.
3. The vault returns a same-length token; the executor puts it back into the record as the same type.
4. The serialized record — tokens only — is produced to Kafka. The raw value never left the client
   except to the customer's own vault.

**Authorized consumer (READ, `can_detokenize=true`)**
1. Deserializer runs the rules in reverse; our executor `POST /detokenize` for each tagged field.
2. The vault returns the real value; the consumer sees `ssn=123456789`, `credit_card_number=4111…`.

**Unauthorized consumer (READ, `can_detokenize=false`)**
1. `POST /detokenize` returns `403`; the executor throws.
2. `onFailure=NONE` (read) leaves the stored token in place; the consumer sees only the opaque
   same-length integer token.

## Trust boundaries

**Confluent can see**: tokenized records on the broker; the rule definition and its params
(vault URL, token-space names) in Schema Registry.

**Confluent can NOT see**: any real `ssn` / `credit_card_number` value; the token→value mapping;
the vault API keys (those live in the client's environment and the vault).

**Two enforcement layers** (defense in depth, like CSFLE's Confluent-RBAC + AWS-IAM):
- a consumer needs a vault API key with `can_detokenize` to recover values; and
- it must be able to reach the vault at all. Remove either and it sees only tokens.

## Why integers work here but not in CSFLE

CSFLE's executor reduces every field to bytes and AES-GCM-encrypts it, producing a longer,
base64 value — so it only supports `STRING`/`BYTES` and rejects `INT`. Our executor never reduces to
bytes: it asks the vault for a surrogate of the same type/length and reconstructs the field into its
original Java class. Nothing in the Schema Registry rules framework forbids this — the limitation was
always CSFLE-executor-local, not framework-wide.
