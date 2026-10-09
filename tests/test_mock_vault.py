"""Tests for the mock tokenization vault.

Starts the server in-process on an ephemeral DB and exercises the HTTP contract
the Java TOKENIZE executor relies on: token-space creation, scoped key minting,
deterministic vs non-deterministic tokenization, type/length preservation, and
the authorization boundary (a key without can_detokenize gets 403).

Run:  python3 -m pytest tests/test_mock_vault.py -v
"""

import importlib.util
import json
import os
import tempfile
import threading
import time
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer

import pytest

HERE = os.path.dirname(__file__)
SERVER_PY = os.path.join(HERE, "..", "mock-vault", "server.py")


@pytest.fixture(scope="module")
def vault():
    # Load server.py as a module against a throwaway DB on an ephemeral port.
    tmpdir = tempfile.mkdtemp(prefix="csflt-vault-test-")
    os.environ["VAULT_DB_PATH"] = os.path.join(tmpdir, "vault.db")
    os.environ["VAULT_PORT"] = "0"  # unused; we bind manually below

    spec = importlib.util.spec_from_file_location("mock_vault_server", SERVER_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), mod.Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    # wait for health
    base = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            urllib.request.urlopen(base + "/health", timeout=1)
            break
        except Exception:
            time.sleep(0.05)
    yield base
    httpd.shutdown()


def _post(base, path, body, key=None):
    data = json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("X-Vault-Key", key)
    try:
        resp = urllib.request.urlopen(req, timeout=2)
        return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def test_health(vault):
    resp = urllib.request.urlopen(vault + "/health", timeout=2)
    assert json.loads(resp.read().decode())["ok"] is True


def test_create_space_and_mint_keys(vault):
    code, body = _post(vault, "/token-spaces", {"name": "pii-ssn"})
    assert code == 200 and body["admin_key"].startswith("adm_")

    code, body = _post(vault, "/keys",
                       {"label": "demo", "can_tokenize": True, "can_detokenize": True})
    assert code == 200 and body["api_key"].startswith("tk_")


def test_create_space_idempotent(vault):
    code1, b1 = _post(vault, "/token-spaces", {"name": "dup-space"})
    code2, b2 = _post(vault, "/token-spaces", {"name": "dup-space"})
    assert code1 == 200 and code2 == 200
    assert b1["admin_key"] == b2["admin_key"]
    assert b1["created"] is True and b2["created"] is False


def test_int_tokenization_preserves_digit_length(vault):
    _post(vault, "/token-spaces", {"name": "pci-cc"})
    _, k = _post(vault, "/keys",
                 {"can_tokenize": True, "can_detokenize": True})
    key = k["api_key"]
    code, body = _post(vault, "/tokenize",
                       {"token_space": "pci-cc", "value": "4111111111111111",
                        "field_type": "LONG", "deterministic": False}, key=key)
    assert code == 200
    token = body["token"]
    assert token.isdigit()
    assert len(token) == len("4111111111111111")  # same digit length
    assert token != "4111111111111111"            # actually replaced


def test_string_tokenization_preserves_format(vault):
    _post(vault, "/token-spaces", {"name": "pii-str"})
    _, k = _post(vault, "/keys",
                 {"can_tokenize": True, "can_detokenize": True})
    key = k["api_key"]
    code, body = _post(vault, "/tokenize",
                       {"token_space": "pii-str", "value": "123-45-6789",
                        "field_type": "STRING", "deterministic": True}, key=key)
    token = body["token"]
    # same length, dashes preserved in the same positions
    assert len(token) == len("123-45-6789")
    assert token[3] == "-" and token[6] == "-"


def test_deterministic_reuses_token(vault):
    _post(vault, "/token-spaces", {"name": "det-space"})
    _, k = _post(vault, "/keys",
                 {"can_tokenize": True, "can_detokenize": True})
    key = k["api_key"]
    _, b1 = _post(vault, "/tokenize",
                  {"token_space": "det-space", "value": "999", "field_type": "INT",
                   "deterministic": True}, key=key)
    _, b2 = _post(vault, "/tokenize",
                  {"token_space": "det-space", "value": "999", "field_type": "INT",
                   "deterministic": True}, key=key)
    assert b1["token"] == b2["token"]
    assert b2["reused"] is True


def test_nondeterministic_fresh_each_time(vault):
    _post(vault, "/token-spaces", {"name": "nondet-space"})
    _, k = _post(vault, "/keys",
                 {"can_tokenize": True, "can_detokenize": True})
    key = k["api_key"]
    tokens = set()
    for _ in range(5):
        _, b = _post(vault, "/tokenize",
                     {"token_space": "nondet-space", "value": "42", "field_type": "INT",
                      "deterministic": False}, key=key)
        tokens.add(b["token"])
    assert len(tokens) >= 2  # overwhelmingly likely to differ for 2-digit space


def test_roundtrip_detokenize(vault):
    _post(vault, "/token-spaces", {"name": "rt-space"})
    _, k = _post(vault, "/keys",
                 {"can_tokenize": True, "can_detokenize": True})
    key = k["api_key"]
    _, tok = _post(vault, "/tokenize",
                   {"token_space": "rt-space", "value": "123456789", "field_type": "INT",
                    "deterministic": True}, key=key)
    code, body = _post(vault, "/detokenize",
                       {"token_space": "rt-space", "token": tok["token"]}, key=key)
    assert code == 200
    assert body["value"] == "123456789"
    assert body["field_type"] == "INT"


def test_unauthorized_detokenize_is_403(vault):
    """The headline security boundary: a tokenize-only key cannot detokenize."""
    _post(vault, "/token-spaces", {"name": "authz-space"})
    _, full = _post(vault, "/keys",
                    {"can_tokenize": True, "can_detokenize": True})
    _, tokonly = _post(vault, "/keys",
                       {"can_tokenize": True, "can_detokenize": False})
    _, tok = _post(vault, "/tokenize",
                   {"token_space": "authz-space", "value": "555", "field_type": "INT",
                    "deterministic": True}, key=full["api_key"])
    code, body = _post(vault, "/detokenize",
                       {"token_space": "authz-space", "token": tok["token"]},
                       key=tokonly["api_key"])
    assert code == 403
    assert "can_detokenize" in body["error"]


def test_unknown_key_is_401(vault):
    code, body = _post(vault, "/tokenize",
                       {"token_space": "pii-ssn", "value": "1", "field_type": "INT"},
                       key="tk_does_not_exist")
    assert code == 401
