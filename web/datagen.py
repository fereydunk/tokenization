"""Schema-driven sample-record generator for the CSFLT demo.

Derives records from the live Schema Registry schema for `<TOPIC>-value`, so
adding a field never desynchronizes the data. ssn and credit_card_number are
generated as INTEGERS (matching the tokenized schema) — this is deliberate: the
whole point of CSFLT is protecting fixed-length integer fields that CSFLE's
length-changing encryption cannot.

Entry points:
  - import:  generate(env, count) -> Iterator[str]   (used by web/server.py)
  - CLI:     python3 web/datagen.py <count>           (used by the Makefile)
"""

from __future__ import annotations

import json
import os
import random
import sys
import urllib.request
import uuid
from pathlib import Path
from typing import Callable, Iterator

REPO_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_DIR / ".env"


def _read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _fetch_schema(env: dict[str, str], subject: str) -> dict:
    import base64
    auth = "Basic " + base64.b64encode(
        f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()
    ).decode()
    req = urllib.request.Request(
        f"{env['SR_URL']}/subjects/{subject}/versions/latest",
        headers={"Authorization": auth},
    )
    payload = json.loads(urllib.request.urlopen(req, timeout=10).read())
    return json.loads(payload["schema"])


_FIRST = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry",
          "Iris", "Jack", "Kate", "Liam", "Maya", "Noah", "Olivia", "Paul"]
_LAST = ["Johnson", "Martinez", "Smith", "Chen", "Patel", "Garcia", "Brown",
         "Davis", "Miller", "Wilson", "Khan", "Nguyen", "Singh", "Lee"]
_STREETS = ["Maple Ave", "Oak Blvd", "Pine St", "Cedar Ln", "Elm Dr", "Birch Way"]
_CITIES = [("Austin", "TX", "78701"), ("Denver", "CO", "80203"),
           ("Seattle", "WA", "98101"), ("Boston", "MA", "02108"),
           ("Miami", "FL", "33130"), ("Chicago", "IL", "60601")]


def _fake_name() -> str:
    return f"{random.choice(_FIRST)} {random.choice(_LAST)}"


def _fake_ssn_int() -> int:
    # 9-digit integer, leading digit non-zero so it renders as 9 digits.
    return random.randint(100_000_000, 899_999_999)


def _fake_cc_int() -> int:
    # 16-digit integer starting with 4 (Visa-ish), so the token's 16-digit
    # length is visually meaningful in the demo.
    return int("4" + "".join(str(random.randint(0, 9)) for _ in range(15)))


def _fake_ssn_str() -> str:
    # Dashed SSN (NNN-NN-NNNN) — used on the plaintext topic so clear-text is
    # obvious at a glance vs. the tokenized topic's bare-integer tokens.
    return f"{random.randint(100, 899):03d}-{random.randint(10, 99):02d}-{random.randint(0, 9999):04d}"


def _fake_cc_str() -> str:
    # Dashed 16-digit card (NNNN-NNNN-NNNN-NNNN), Visa-ish leading 4.
    return "4%03d-%04d-%04d-%04d" % (random.randint(0, 999), random.randint(0, 9999),
                                     random.randint(0, 9999), random.randint(0, 9999))


def _fake_address() -> str:
    city, state, zipc = random.choice(_CITIES)
    return f"{random.randint(100, 9999)} {random.choice(_STREETS)}, {city} {state} {zipc}"


_BY_NAME: dict[str, Callable[[], object]] = {
    "loan_id":            lambda: f"L-{random.randint(10000, 99999)}",
    "applicant_name":     _fake_name,
    # ssn / credit_card_number are handled in generate_record (type-aware); see there.
    "annual_income_usd":  lambda: round(random.uniform(45000, 280000), 2),
    "loan_amount_usd":    lambda: round(random.uniform(150000, 850000), 2),
    "credit_score":       lambda: random.randint(620, 820),
    "property_address":   _fake_address,
}


def _by_type(prop: dict) -> object:
    if "enum" in prop:
        return random.choice(prop["enum"])
    t = prop.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), t[0])
    if t == "string":
        return f"sample-{uuid.uuid4().hex[:8]}"
    if t == "integer":
        return random.randint(0, 1000)
    if t == "number":
        return round(random.uniform(0, 1000), 2)
    if t == "boolean":
        return random.choice([True, False])
    return None


def generate_record(schema: dict) -> dict:
    out: dict[str, object] = {}
    for name, prop in schema.get("properties", {}).items():
        t = prop.get("type")
        # ssn / credit_card_number adapt to the field's declared type: dashed
        # strings on the plaintext topic (string), bare integers on the
        # tokenized topic (integer). This is what makes clear-text vs. tokenized
        # obvious in the message viewer.
        if name == "ssn":
            out[name] = _fake_ssn_str() if t == "string" else _fake_ssn_int()
        elif name == "credit_card_number":
            out[name] = _fake_cc_str() if t == "string" else _fake_cc_int()
        else:
            gen = _BY_NAME.get(name)
            out[name] = gen() if gen else _by_type(prop)
    return out


def generate(env: dict[str, str], count: int) -> Iterator[str]:
    topic = env["TOPIC"]
    schema = _fetch_schema(env, f"{topic}-value")
    for _ in range(count):
        yield json.dumps(generate_record(schema))


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    env = {**_read_env_file(ENV_FILE), **os.environ}
    for line in generate(env, count):
        print(line, flush=True)


if __name__ == "__main__":
    main()
