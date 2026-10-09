#!/usr/bin/env python3
"""Mock tokenization vault — stands in for Protegrity / HashiCorp Vault.

This is the ONE deliberately-fake component of the CSFLT demo. In production,
token generation, storage, and resolution are owned entirely by the customer's
third-party vault; Confluent never sees the token mapping. This service models
that external vault with the smallest possible HTTP + SQLite surface so the
Java TOKENIZE executor has something real to call.

It is language-agnostic on purpose: the executor talks to it only over HTTP.

Endpoints
    POST /token-spaces                      {name}                -> {admin_key}
    POST /keys                              {label,can_tokenize,can_detokenize} -> {api_key}
    POST /tokenize    (X-Vault-Key header)  {token_space,value,field_type,deterministic} -> {token}
    POST /detokenize  (X-Vault-Key header)  {token_space,token}   -> {value,field_type}
    GET  /health                                                  -> {ok:true}

Security model (mirrors how Confluent RBAC gates CSFLE KEK access):
    Token spaces are namespaces for the token->value mappings (one per tag, e.g.
    pii-ssn, pci-cc). API keys carry capability flags — can_tokenize /
    can_detokenize — and apply across all spaces. /tokenize and /detokenize
    require a key with the matching capability, else 403. This is what makes the
    "unauthorized consumer only sees tokens" story demonstrable: the producer and
    authorized consumer use a key with can_detokenize=true; the unauthorized
    consumer's key has can_detokenize=false, so its read-path detokenize calls
    403 and (via the rule's onFailure=NONE) the token is left in place.

    (Per-tag access — a key that can detokenize PII but not PCI — is the CSFLE2
    analog and a documented future extension, intentionally out of scope for v1.)

Token generation is DEMO-GRADE (random, same length/format), explicitly NOT
cryptographic format-preserving encryption. Never present it as production FPE.
"""

import json
import os
import secrets
import sqlite3
import threading
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DB_PATH = os.environ.get("VAULT_DB_PATH", "/tmp/csflt-mock-vault/vault.db")
PORT = int(os.environ.get("VAULT_PORT", "9099"))

_db_lock = threading.Lock()


def _connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_db(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS token_spaces (
            name       TEXT PRIMARY KEY,
            admin_key  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS api_keys (
            api_key        TEXT PRIMARY KEY,
            label          TEXT,
            can_tokenize   INTEGER NOT NULL DEFAULT 0,
            can_detokenize INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS tokens (
            token_space TEXT NOT NULL,
            token       TEXT NOT NULL,
            value       TEXT NOT NULL,
            field_type  TEXT NOT NULL,
            value_hash  TEXT NOT NULL,
            PRIMARY KEY (token_space, token)
        );
        CREATE INDEX IF NOT EXISTS idx_tokens_hash
            ON tokens (token_space, value_hash);
        """
    )
    conn.commit()


# ── token generation (demo-grade; NOT cryptographic FPE) ─────────────────────

def _generate_token(value: str, field_type: str) -> str:
    """Return a surrogate that preserves the original type/length.

    This is the whole point of tokenization vs. CSFLE: a 9-digit integer SSN
    tokenizes to another 9-digit integer, not to a longer base64 ciphertext.
    """
    ft = (field_type or "").upper()
    if ft in ("INT", "LONG"):
        # Preserve digit length; keep leading digit non-zero so the token has
        # the same number of digits as the original when rendered as a number.
        n = len(value)
        if n == 1:
            return str(secrets.randbelow(10))
        first = str(1 + secrets.randbelow(9))
        rest = "".join(str(secrets.randbelow(10)) for _ in range(n - 1))
        return first + rest
    # STRING (and anything else): preserve length and per-char class — digits
    # map to digits, letters to letters, everything else passes through, so an
    # SSN-shaped "123-45-6789" stays SSN-shaped.
    out = []
    for ch in value:
        if ch.isdigit():
            out.append(str(secrets.randbelow(10)))
        elif ch.isalpha():
            alpha = "abcdefghijklmnopqrstuvwxyz"
            c = alpha[secrets.randbelow(26)]
            out.append(c.upper() if ch.isupper() else c)
        else:
            out.append(ch)
    return "".join(out)


# ── request handler ──────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    server_version = "MockVault/1.0"

    def log_message(self, fmt, *args):  # quieter logs
        pass

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode())

    # --- GET -----------------------------------------------------------------
    def do_GET(self):
        if self.path == "/health":
            return self._send(200, {"ok": True})
        self._send(404, {"error": "not found"})

    # --- POST ----------------------------------------------------------------
    def do_POST(self):
        try:
            if self.path == "/token-spaces":
                return self._create_space()
            if self.path == "/keys":
                return self._mint_key()
            if self.path == "/tokenize":
                return self._tokenize()
            if self.path == "/detokenize":
                return self._detokenize()
            self._send(404, {"error": "not found"})
        except Exception as e:  # keep the demo server alive, report the error
            self._send(500, {"error": str(e)})

    def _create_space(self):
        body = self._read_json()
        name = body.get("name")
        if not name:
            return self._send(400, {"error": "name required"})
        admin_key = "adm_" + secrets.token_hex(16)
        with _db_lock:
            exists = DB.execute(
                "SELECT admin_key FROM token_spaces WHERE name=?", (name,)
            ).fetchone()
            if exists:
                # idempotent: return the existing admin key
                return self._send(200, {"token_space": name, "admin_key": exists[0],
                                        "created": False})
            DB.execute("INSERT INTO token_spaces(name, admin_key) VALUES(?,?)",
                       (name, admin_key))
            DB.commit()
        self._send(200, {"token_space": name, "admin_key": admin_key, "created": True})

    def _mint_key(self):
        body = self._read_json()
        api_key = "tk_" + secrets.token_hex(16)
        with _db_lock:
            DB.execute(
                "INSERT INTO api_keys(api_key, label, can_tokenize, can_detokenize)"
                " VALUES(?,?,?,?)",
                (api_key, body.get("label", ""),
                 1 if body.get("can_tokenize") else 0,
                 1 if body.get("can_detokenize") else 0),
            )
            DB.commit()
        self._send(200, {
            "api_key": api_key,
            "can_tokenize": bool(body.get("can_tokenize")),
            "can_detokenize": bool(body.get("can_detokenize")),
        })

    def _authz(self, capability):
        """Return (ok, err_response_args). capability is 'can_tokenize'|'can_detokenize'."""
        api_key = self.headers.get("X-Vault-Key", "")
        row = DB.execute(
            "SELECT can_tokenize, can_detokenize FROM api_keys WHERE api_key=?",
            (api_key,),
        ).fetchone()
        if not row:
            return False, (401, {"error": "missing or unknown vault API key"})
        can_tok, can_detok = row
        allowed = can_tok if capability == "can_tokenize" else can_detok
        if not allowed:
            return False, (403, {"error": f"key lacks {capability}"})
        return True, None

    def _tokenize(self):
        body = self._read_json()
        space = body.get("token_space")
        value = body.get("value")
        field_type = body.get("field_type", "STRING")
        deterministic = bool(body.get("deterministic"))
        if space is None or value is None:
            return self._send(400, {"error": "token_space and value required"})
        value = str(value)
        ok, err = self._authz("can_tokenize")
        if not ok:
            return self._send(*err)
        vhash = sha256(f"{field_type}:{value}".encode()).hexdigest()
        with _db_lock:
            if deterministic:
                existing = DB.execute(
                    "SELECT token FROM tokens WHERE token_space=? AND value_hash=?",
                    (space, vhash),
                ).fetchone()
                if existing:
                    return self._send(200, {"token": existing[0], "reused": True})
            # mint a fresh unique token
            for _ in range(10):
                token = _generate_token(value, field_type)
                clash = DB.execute(
                    "SELECT 1 FROM tokens WHERE token_space=? AND token=?",
                    (space, token),
                ).fetchone()
                if not clash:
                    break
            DB.execute(
                "INSERT INTO tokens(token_space, token, value, field_type, value_hash)"
                " VALUES(?,?,?,?,?)",
                (space, token, value, field_type, vhash),
            )
            DB.commit()
        self._send(200, {"token": token, "reused": False})

    def _detokenize(self):
        body = self._read_json()
        space = body.get("token_space")
        token = body.get("token")
        if space is None or token is None:
            return self._send(400, {"error": "token_space and token required"})
        token = str(token)
        ok, err = self._authz("can_detokenize")
        if not ok:
            return self._send(*err)
        row = DB.execute(
            "SELECT value, field_type FROM tokens WHERE token_space=? AND token=?",
            (space, token),
        ).fetchone()
        if not row:
            return self._send(404, {"error": "token not found"})
        self._send(200, {"value": row[0], "field_type": row[1]})


DB = _connect()
_init_db(DB)


def main():
    httpd = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"mock-vault listening on http://127.0.0.1:{PORT}  (db: {DB_PATH})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
