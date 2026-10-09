#!/usr/bin/env python3
"""CSFLT demo wizard — a small stdlib HTTP server (no framework, no JS build).

Mirrors demo-csfle-cspe-cloud's wizard pattern (cards + SSE log boxes) but for
tokenization: start the local vault, run the setup steps, produce tokenized
records, and watch an authorized vs. unauthorized consumer side by side.

The heavy lifting is delegated to the same bash scripts and CP console tools the
Makefile uses, so the wizard and CLI paths stay in sync.
"""

import json
import os
import subprocess
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

REPO_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_DIR / ".env"
PORT = int(os.environ.get("WEB_PORT", "8894"))
CP_HOME = os.environ.get("CP_HOME", str(Path.home() / "confluent-8.3.2"))
JAVA_HOME = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@21")
EXECUTOR_JAR = os.environ.get("EXECUTOR_JAR",
                              str(REPO_DIR / "executor/target/field-tokenization-executor-0.1.0.jar"))
EXECUTOR = "io.confluent.csflt.FieldTokenizationExecutor"
PRODUCER = f"{CP_HOME}/bin/kafka-json-schema-console-producer"
CONSUMER = f"{CP_HOME}/bin/kafka-json-schema-console-consumer"
SECRET_KEYS = {"KAFKA_API_SECRET", "SR_API_SECRET", "VAULT_ADMIN_KEY",
               "VAULT_AUTHORIZED_KEY", "VAULT_UNAUTHORIZED_KEY"}


def load_env() -> dict:
    env = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def _redact(k, v):
    if k in SECRET_KEYS and v:
        return v[:6] + "…" if len(v) > 6 else "set"
    return v


def _plaintext_topic(env):
    return env.get("PLAINTEXT_TOPIC") or (env.get("TOPIC", "") + "-plaintext")


_NOISE_SUBSTR = ("SLF4J", "] INFO ", "] ERROR ", "] WARN ", "TimeoutException",
                 "ConsoleConsumer", "ConsoleProducer", "terminating consumer",
                 "Processed a total of", "SerializationException")
_NOISE_PREFIX = ("Warning: --", "at ", "org.apache.", "io.confluent.", "Caused by",
                 "java.", "...")


def _is_noise(line: str) -> bool:
    """Drop CP console-tool log/stack-trace chatter so the UI shows only signal."""
    if any(s in line for s in _NOISE_SUBSTR):
        return True
    s = line.lstrip()
    return s.startswith(_NOISE_PREFIX)


# ── Confluent Cloud env/cluster/SR helpers (for the target picker) ───────────

def _run_confluent(args, timeout=20):
    p = subprocess.run(["confluent"] + args, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def _cc_envs():
    rc, out, err = _run_confluent(["environment", "list", "-o", "json"], 15)
    if rc != 0:
        raise RuntimeError(err.strip() or "environment list failed")
    return json.loads(out)


def _cc_clusters(env_id):
    rc, out, err = _run_confluent(["kafka", "cluster", "list", "--environment", env_id, "-o", "json"], 15)
    if rc != 0:
        raise RuntimeError(err.strip() or "cluster list failed")
    return json.loads(out)


def _cc_describe_cluster(env_id, cluster_id):
    rc, out, err = _run_confluent(
        ["kafka", "cluster", "describe", cluster_id, "--environment", env_id, "-o", "json"], 15)
    if rc != 0:
        raise RuntimeError(err.strip() or "cluster describe failed")
    return json.loads(out)


def _cc_describe_sr(env_id):
    rc, out, err = _run_confluent(
        ["schema-registry", "cluster", "describe", "--environment", env_id, "-o", "json"], 15)
    if rc != 0:
        return {}
    d = json.loads(out)
    return {"id": d.get("cluster") or d.get("id") or "",
            "url": d.get("endpoint_url") or d.get("endpoint") or ""}


def _cc_create_api_key(resource_id, env_id, desc):
    rc, out, err = _run_confluent(
        ["api-key", "create", "--resource", resource_id, "--environment", env_id,
         "--description", desc, "-o", "json"], 30)
    if rc != 0:
        raise RuntimeError(err.strip() or "api-key create failed")
    return json.loads(out)


def _cc_delete_api_key_quiet(key):
    if key:
        _run_confluent(["api-key", "delete", key, "--force"], 20)


def _upsert_env(updates):
    """Replace-or-append KEY=VALUE lines in .env (runtime-only write)."""
    lines = ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else []
    out, seen = [], set()
    for line in lines:
        k = line.split("=", 1)[0].strip() if "=" in line else None
        if k in updates:
            out.append(f"{k}={updates[k]}"); seen.add(k)
        else:
            out.append(line)
    for k, v in updates.items():
        if k not in seen:
            out.append(f"{k}={v}")
    ENV_FILE.write_text("\n".join(out) + "\n")


# ── subprocess → SSE helpers ─────────────────────────────────────────────────

def _base_subprocess_env(extra=None):
    e = dict(os.environ)
    e["JAVA_HOME"] = JAVA_HOME
    e["CLASSPATH"] = EXECUTOR_JAR
    e["PATH"] = f"{JAVA_HOME}/bin:" + e.get("PATH", "")
    if extra:
        e.update(extra)
    return e


def _envsubst(path: Path, env: dict) -> str:
    """Minimal envsubst for ${VAR} in a properties template."""
    text = path.read_text()
    for k, v in env.items():
        text = text.replace("${" + k + "}", v)
    return text


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    # ---- plumbing -----------------------------------------------------------
    def _html(self, body):
        b = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _json(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _sse_start(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

    def _sse_line(self, line):
        try:
            self.wfile.write(f"data: {json.dumps({'line': line})}\n\n".encode())
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            raise

    def _sse_done(self, ok, rc=0):
        try:
            self.wfile.write(f"event: done\ndata: {json.dumps({'ok': ok, 'rc': rc})}\n\n".encode())
            self.wfile.flush()
        except Exception:
            pass

    def _stream_cmd(self, cmd, env=None, stdin_cmd=None):
        """Run cmd, streaming stdout+stderr as SSE lines. `mvn` runs in executor/."""
        cwd = REPO_DIR / "executor" if cmd and cmd[0] == "mvn" else REPO_DIR
        self._sse_start()
        try:
            if stdin_cmd:
                src = subprocess.Popen(stdin_cmd, stdout=subprocess.PIPE,
                                       env=env or _base_subprocess_env(), cwd=REPO_DIR)
                proc = subprocess.Popen(cmd, stdin=src.stdout, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, env=env or _base_subprocess_env(),
                                        cwd=cwd, text=True)
                src.stdout.close()
            else:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        env=env or _base_subprocess_env(), cwd=cwd, text=True)
            for line in proc.stdout:
                line = line.rstrip("\n")
                if not line or _is_noise(line):
                    continue
                self._sse_line(line)
            rc = proc.wait()
            self._sse_done(rc == 0, rc)
        except Exception as e:
            try:
                self._sse_line(f"ERROR: {e}")
                self._sse_done(False, 1)
            except Exception:
                pass

    # ---- routes -------------------------------------------------------------
    def do_GET(self):
        u = urlparse(self.path)
        p, q = u.path, parse_qs(u.query)
        if p == "/":
            return self._html(PAGE)
        if p == "/env":
            env = load_env()
            return self._json({k: _redact(k, v) for k, v in env.items()})
        if p == "/vault-health":
            env = load_env()
            url = env.get("VAULT_URL", "http://localhost:9099")
            try:
                urllib.request.urlopen(url + "/health", timeout=2)
                return self._json({"up": True, "url": url})
            except Exception:
                return self._json({"up": False, "url": url})
        if p == "/cc-status":
            try:
                subprocess.run(["confluent", "kafka", "cluster", "list"],
                               capture_output=True, timeout=15, check=True)
                return self._json({"ok": True})
            except Exception:
                return self._json({"ok": False})
        if p == "/cc-envs":
            try:
                return self._json(_cc_envs())
            except Exception as e:
                return self._json({"error": str(e)}, 500)
        if p == "/cc-clusters":
            try:
                return self._json(_cc_clusters(q.get("env", [""])[0]))
            except Exception as e:
                return self._json({"error": str(e)}, 500)
        if p == "/schema":
            return self._schema()
        if p.startswith("/run/"):
            return self._run_step(p[len("/run/"):])
        if p == "/produce":
            return self._produce(int(q.get("count", ["10"])[0]),
                                 q.get("mode", ["tokenized"])[0])
        if p == "/compare":
            return self._compare()
        if p.startswith("/consume/"):
            return self._consume(p[len("/consume/"):])
        self._json({"error": "not found"}, 404)

    def do_POST(self):
        if urlparse(self.path).path == "/cc-pick":
            return self._cc_pick()
        self._json({"error": "not found"}, 404)

    def _cc_pick(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length)) if length else {}
            env_id = body["env_id"]
            cluster_id = body["cluster_id"]
            topic = (body.get("topic") or "mortgage-tokenized").strip()
            ptopic = (body.get("plaintext_topic") or (topic + "-plaintext")).strip()
            cluster = _cc_describe_cluster(env_id, cluster_id)
            bootstrap = (cluster.get("endpoint") or "").replace("SASL_SSL://", "")
            sr = _cc_describe_sr(env_id)
            if not sr.get("id"):
                return self._json({"ok": False, "msg": "Schema Registry not enabled for this "
                                   "environment — enable Stream Governance in the Cloud UI first."}, 400)
            current = load_env()
            updates = {"ENV_ID": env_id, "ENV_NAME": body.get("env_name", ""),
                       "CLUSTER_ID": cluster_id, "CLUSTER_NAME": body.get("cluster_name", ""),
                       "BOOTSTRAP_SERVERS": bootstrap, "SR_ID": sr["id"], "SR_URL": sr["url"],
                       "TOPIC": topic, "PLAINTEXT_TOPIC": ptopic}
            cluster_changed = current.get("CLUSTER_ID") and current.get("CLUSTER_ID") != cluster_id
            sr_changed = current.get("SR_ID") and current.get("SR_ID") != sr["id"]
            if not current.get("KAFKA_API_KEY") or cluster_changed:
                if cluster_changed and current.get("KAFKA_API_KEY"):
                    _cc_delete_api_key_quiet(current["KAFKA_API_KEY"])
                k = _cc_create_api_key(cluster_id, env_id, "tokenization-demo")
                updates["KAFKA_API_KEY"] = k["api_key"]; updates["KAFKA_API_SECRET"] = k["api_secret"]
            if not current.get("SR_API_KEY") or sr_changed:
                if sr_changed and current.get("SR_API_KEY"):
                    _cc_delete_api_key_quiet(current["SR_API_KEY"])
                s = _cc_create_api_key(sr["id"], env_id, "tokenization-demo-sr")
                updates["SR_API_KEY"] = s["api_key"]; updates["SR_API_SECRET"] = s["api_secret"]
            _upsert_env(updates)
            self._json({"ok": True, "bootstrap": bootstrap, "sr": sr["id"], "topic": topic})
        except KeyError as e:
            self._json({"ok": False, "msg": f"missing field {e}"}, 400)
        except Exception as e:
            self._json({"ok": False, "msg": str(e)}, 500)

    # ---- step runners -------------------------------------------------------
    def _run_step(self, step):
        scripts = {
            "discover": ["bash", "scripts/00_discover_env.sh"],
            "vault":    ["bash", "scripts/01_setup_vault.sh"],
            "schemas":  ["bash", "scripts/02_register_schemas.sh"],
            "topics":   ["bash", "scripts/03_create_topics.sh"],
        }
        if step == "build":
            return self._stream_cmd(["mvn", "-q", "-B", "package"])
        if step in scripts:
            return self._stream_cmd(scripts[step])
        self._json({"error": f"unknown step {step}"}, 404)

    def _schema(self):
        env = load_env()
        topic = env.get("TOPIC", "")
        try:
            import base64
            auth = "Basic " + base64.b64encode(
                f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()).decode()
            req = urllib.request.Request(
                f"{env['SR_URL']}/subjects/{topic}-value/versions/latest",
                headers={"Authorization": auth})
            data = json.loads(urllib.request.urlopen(req, timeout=10).read())
            return self._json({"schema": json.loads(data["schema"]),
                               "ruleSet": data.get("ruleSet"), "id": data.get("id")})
        except Exception as e:
            return self._json({"error": str(e)}, 500)

    def _produce(self, count, mode):
        env = load_env()
        tokenized = (mode != "plaintext")
        topic = env["TOPIC"] if tokenized else _plaintext_topic(env)
        try:
            sid = self._schema_id(env, topic)
        except Exception as e:
            self._sse_start(); self._sse_line(f"ERROR fetching schema id for {topic}: {e}")
            return self._sse_done(False, 1)
        cfg = "config/producer-tokenize.properties" if tokenized else "config/producer-plaintext.properties"
        props = self._write_temp_props(cfg, env)
        cmd = [PRODUCER,
               "--bootstrap-server", env["BOOTSTRAP_SERVERS"],
               "--topic", topic,
               "--producer.config", props,
               "--property", f"schema.registry.url={env['SR_URL']}",
               "--property", "basic.auth.credentials.source=USER_INFO",
               "--property", f"basic.auth.user.info={env['SR_API_KEY']}:{env['SR_API_SECRET']}",
               "--property", f"value.schema.id={sid}",
               "--property", "auto.register.schemas=false"]
        if tokenized:
            cmd += ["--property", "rule.executors=_default_",
                    "--property", f"rule.executors._default_.class={EXECUTOR}"]
        extra = {"TOPIC": topic}  # datagen fetches the schema for this topic
        if tokenized:
            extra["VAULT_API_KEY"] = env.get("VAULT_AUTHORIZED_KEY", "")
        penv = _base_subprocess_env(extra)
        datagen = ["python3", "web/datagen.py", str(count)]
        self._tee_produce(cmd, penv, datagen)

    def _tee_produce(self, cmd, penv, datagen_cmd):
        """Generate records, echo each one into the log as it's produced, and feed
        it to the console producer's stdin — so the user sees the message content."""
        self._sse_start()
        try:
            src = subprocess.Popen(datagen_cmd, stdout=subprocess.PIPE,
                                   env=penv, cwd=REPO_DIR, text=True)
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, env=penv, cwd=REPO_DIR, text=True)
            n = 0
            for rec in src.stdout:
                rec = rec.rstrip("\n")
                if not rec:
                    continue
                n += 1
                self._sse_line(f"→ #{n}  {rec}")       # show the message content
                try:
                    proc.stdin.write(rec + "\n"); proc.stdin.flush()
                except (BrokenPipeError, ValueError):
                    break
            src.wait()
            try:
                proc.stdin.close()
            except Exception:
                pass
            for line in proc.stdout:                    # drain producer output (filtered)
                line = line.rstrip("\n")
                if line and not _is_noise(line):
                    self._sse_line(line)
            rc = proc.wait()
            self._sse_line(f"✓ produced {n} record{'s' if n != 1 else ''}")
            self._sse_done(rc == 0, rc)
        except Exception as e:
            try:
                self._sse_line(f"ERROR: {e}"); self._sse_done(False, 1)
            except Exception:
                pass

    def _consume(self, role):
        import random
        env = load_env()
        use_executor = True
        key = ""
        if role == "authorized":
            key = env.get("VAULT_AUTHORIZED_KEY", ""); cfg = "config/consumer-tokenize-authorized.properties"
            topic = env["TOPIC"]
        elif role == "unauthorized":
            key = env.get("VAULT_UNAUTHORIZED_KEY", ""); cfg = "config/consumer-tokenize-unauthorized.properties"
            topic = env["TOPIC"]
        else:  # plaintext baseline — no executor, no vault
            cfg = "config/consumer-plaintext.properties"
            topic = _plaintext_topic(env)
            use_executor = False
        props = self._write_temp_props(cfg, env)
        cmd = [CONSUMER,
               "--bootstrap-server", env["BOOTSTRAP_SERVERS"],
               "--topic", topic, "--from-beginning",
               "--timeout-ms", "10000",
               "--group", f"csflt-{role}-{random.randint(1000,999999)}",
               "--consumer.config", props,
               "--formatter-property", f"schema.registry.url={env['SR_URL']}",
               "--formatter-property", "basic.auth.credentials.source=USER_INFO",
               "--formatter-property", f"basic.auth.user.info={env['SR_API_KEY']}:{env['SR_API_SECRET']}"]
        if use_executor:
            cmd += ["--formatter-property", "rule.executors=_default_",
                    "--formatter-property", f"rule.executors._default_.class={EXECUTOR}"]
        penv = _base_subprocess_env({"VAULT_API_KEY": key} if use_executor else None)
        self._stream_cmd(cmd, env=penv)

    def _read_topic(self, env, topic, mode):
        """Read a topic to completion, return {loan_id: record}. mode 'raw' uses
        the plain consumer (stored bytes = tokens); 'auth' uses the json-schema
        consumer + executor + authorized key (detokenized = real values)."""
        import random
        grp = f"cmp-{mode}-{random.randint(1, 10**6)}"
        if mode == "raw":
            props = self._write_temp_props("config/consumer-plaintext.properties", env)
            cmd = [f"{CP_HOME}/bin/kafka-console-consumer",
                   "--bootstrap-server", env["BOOTSTRAP_SERVERS"], "--topic", topic,
                   "--from-beginning", "--timeout-ms", "7000", "--group", grp,
                   "--consumer.config", props]
            penv = _base_subprocess_env()
        else:
            props = self._write_temp_props("config/consumer-tokenize-authorized.properties", env)
            cmd = [CONSUMER, "--bootstrap-server", env["BOOTSTRAP_SERVERS"], "--topic", topic,
                   "--from-beginning", "--timeout-ms", "7000", "--group", grp,
                   "--consumer.config", props,
                   "--formatter-property", f"schema.registry.url={env['SR_URL']}",
                   "--formatter-property", "basic.auth.credentials.source=USER_INFO",
                   "--formatter-property", f"basic.auth.user.info={env['SR_API_KEY']}:{env['SR_API_SECRET']}",
                   "--formatter-property", "rule.executors=_default_",
                   "--formatter-property", f"rule.executors._default_.class={EXECUTOR}"]
            penv = _base_subprocess_env({"VAULT_API_KEY": env.get("VAULT_AUTHORIZED_KEY", "")})
        try:
            p = subprocess.run(cmd, capture_output=True, env=penv, cwd=REPO_DIR, timeout=30)
        except Exception:
            return {}
        out = {}
        for line in p.stdout.decode("utf-8", "replace").splitlines():
            i = line.find("{")
            if i < 0:
                continue
            try:
                d = json.loads(line[i:])
            except Exception:
                continue
            lid = d.get("loan_id")
            if lid:
                out[lid] = d
        return out

    def _compare(self):
        """Stream per-record rows joining the stored token (what's on the broker /
        what an unauthorized reader sees) with the detokenized real value (what an
        authorized reader sees) — proving they differ for the SAME record."""
        import threading
        env = load_env()
        topic = env.get("TOPIC")
        res = {}
        def worker(mode):
            res[mode] = self._read_topic(env, topic, mode)
        ts = [threading.Thread(target=worker, args=(m,)) for m in ("raw", "auth")]
        for t in ts: t.start()
        for t in ts: t.join()
        raw, real = res.get("raw", {}), res.get("auth", {})
        self._sse_start()
        try:
            for lid, r in real.items():
                t = raw.get(lid, {})
                self._sse_line(json.dumps({
                    "loan_id": lid,
                    "ssn_real": r.get("ssn"), "ssn_tok": t.get("ssn"),
                    "cc_real": r.get("credit_card_number"), "cc_tok": t.get("credit_card_number"),
                    "score": r.get("credit_score")}))
            self._sse_done(True, 0)
        except Exception as e:
            self._sse_line(f"ERROR: {e}"); self._sse_done(False, 1)

    # ---- helpers ------------------------------------------------------------
    def _schema_id(self, env, topic=None):
        import base64
        topic = topic or env["TOPIC"]
        auth = "Basic " + base64.b64encode(
            f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}".encode()).decode()
        req = urllib.request.Request(
            f"{env['SR_URL']}/subjects/{topic}-value/versions/latest",
            headers={"Authorization": auth})
        return json.loads(urllib.request.urlopen(req, timeout=10).read())["id"]

    def _write_temp_props(self, rel, env):
        import tempfile
        text = _envsubst(REPO_DIR / rel, env)
        fd, path = tempfile.mkstemp(suffix=".properties")
        with os.fdopen(fd, "w") as f:
            f.write(text)
        return path


PAGE = """<!doctype html><html><head><meta charset=utf-8>
<title>CSFLT demo</title><style>
:root{--bg:#0d1117;--card:#161b22;--panel:#0d1117;--border:#30363d;--fg:#c9d1d9;
--mut:#8b949e;--blue:#58a6ff;--grn:#3fb950;--red:#f85149;--amber:#d29922}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.5 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif}
header{padding:16px 24px;border-bottom:1px solid var(--border)}
h1{margin:0;font-size:17px}h1 .s{color:var(--mut);font-weight:400;font-size:13px}
main{max-width:1060px;margin:0 auto;padding:22px 20px 60px;display:flex;flex-direction:column;gap:18px}
.section{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:18px 20px}
.section>h2{margin:0;font-size:15px}
.section>h2 .n{color:var(--blue)}
.desc{color:var(--mut);font-size:12.5px;margin:6px 0 14px}
.statusrow{display:flex;gap:10px;flex-wrap:wrap;margin:0 0 14px}
.chip{display:flex;align-items:center;gap:8px;background:var(--panel);border:1px solid var(--border);
border-radius:8px;padding:7px 11px;font-size:12.5px}
.chip .lbl{color:var(--mut)}.chip a{color:var(--blue);cursor:pointer;font-size:11px;text-decoration:none}
button{background:#21262d;color:var(--fg);border:1px solid var(--border);border-radius:6px;
padding:7px 12px;font-size:13px;cursor:pointer}
button:hover{border-color:var(--blue)}button:disabled{opacity:.5;cursor:default}
button.pri{background:#1f6feb;border-color:#1f6feb;color:#fff}
button.pri:hover{background:#388bfd}
.btns{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
input.num{width:60px;background:var(--panel);color:var(--fg);border:1px solid var(--border);
border-radius:6px;padding:6px 8px;font-size:13px}
.pick{display:grid;grid-template-columns:auto 1fr;gap:8px 12px;align-items:center;
background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:12px 14px;margin:0 0 14px;max-width:640px}
.pick label{color:var(--mut);font-size:12px}
.pick select,.pick input{width:100%;background:#010409;color:var(--fg);border:1px solid var(--border);
border-radius:6px;padding:6px 8px;font-size:12.5px;font-family:ui-monospace,Menlo,monospace}
.pick .act{grid-column:1/3;display:flex;gap:10px;align-items:center;margin-top:2px}
.cols{display:grid;gap:14px}.cols.c2{grid-template-columns:1fr 1fr}.cols.c3{grid-template-columns:1fr 1fr 1fr}
@media(max-width:820px){.cols.c2,.cols.c3{grid-template-columns:1fr}}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:13px}
.panel h3{margin:0 0 3px;font-size:13.5px}
.panel .ptopic{color:var(--mut);font-size:11px;font-family:ui-monospace,Menlo,monospace;
word-break:break-all;margin:3px 0 10px}
.panel .pnote{color:var(--mut);font-size:12px;margin:0 0 10px;min-height:32px}
.log{background:#010409;border:1px solid var(--border);border-radius:6px;padding:9px 10px;
font:11.5px/1.5 ui-monospace,Menlo,monospace;height:150px;overflow:auto;white-space:pre-wrap;color:#9fb3c8}
.recs{background:#010409;border:1px solid var(--border);border-radius:6px;height:230px;overflow:auto;margin-top:10px}
.recs .ph{color:var(--mut);font-size:12px;padding:12px}
.rec{display:flex;gap:14px;padding:6px 10px;border-bottom:1px solid #161b22;
font:12px ui-monospace,Menlo,monospace;white-space:nowrap}
.rec .f{color:var(--mut)}.rec .v{color:var(--fg)}
.rec.real .v.sens{color:var(--grn)}.rec.tok .v.sens{color:var(--amber)}
.foot{color:var(--mut);font-size:11.5px;padding:7px 10px}
.foot.ok{color:var(--grn)}.foot.bad{color:var(--red)}
.crow{display:grid;grid-template-columns:90px 1fr 1fr;gap:12px;padding:7px 10px;
border-bottom:1px solid #161b22;font:12px ui-monospace,Menlo,monospace;align-items:center}
.crow.hdr{color:var(--mut);background:#0b0f14;position:sticky;top:0}
.crow .lid{color:var(--mut)}
.crow .pair{display:flex;align-items:center;gap:7px;white-space:nowrap;overflow-x:auto}
.crow .arr{color:var(--mut)}
.crow .v.tok{color:var(--amber)}.crow .v.real{color:var(--grn)}
.pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;vertical-align:middle}
.pill.ok{background:#193b2a;color:var(--grn)}.pill.bad{background:#3b1a1a;color:var(--red)}
.pill.wait{background:#3a2f12;color:var(--amber)}
.real{color:var(--grn)}.tok{color:var(--amber)}code{color:var(--blue);font-size:12px}
</style></head><body>
<header><h1>Client-Side Field Level Tokenization
 <span class=s>— integer ssn &amp; credit_card_number, tokenized before they reach Kafka</span></h1></header>
<main>

 <section class=section>
  <h2><span class=n>1</span> · Connect &amp; set up token services</h2>
  <div class=desc>Confirm the vault and Confluent Cloud, pick your environment / cluster / topic names
   (Save &amp; mint keys), then provision everything: build the executor, mint vault keys + token
   spaces, register the schemas, and create the topics.</div>
  <div class=statusrow>
   <div class=chip><span class=lbl>Token vault</span><span id=vaultPill class="pill wait">checking…</span><a onclick=checkVault()>re-check</a></div>
   <div class=chip><span class=lbl>Confluent Cloud</span><span id=ccPill class="pill wait">checking…</span><a onclick=checkCC()>re-check</a></div>
  </div>
  <div class=pick>
   <label>Environment</label><select id=envSel onchange=loadClusters()><option>…</option></select>
   <label>Cluster</label><select id=clusSel><option>…</option></select>
   <label>Tokenized topic</label><input id=topicIn placeholder=mortgage-tokenized>
   <label>Plaintext topic</label><input id=ptopicIn placeholder=mortgage-tokenized-plaintext>
   <div class=act><button class=pri onclick=ccPick()>Save &amp; mint keys</button>
    <span id=pickMsg class=desc style="margin:0"></span></div>
  </div>
  <div class=btns>
   <button onclick="run('build')">Build executor</button>
   <button onclick="run('discover')">Discover CC</button>
   <button onclick="run('vault')">Vault keys &amp; spaces</button>
   <button onclick="run('schemas')">Register schemas</button>
   <button onclick="run('topics')">Create topics</button>
   <button class=pri onclick="runAll()">Run all</button>
  </div>
  <div class=log id=setupLog></div>
 </section>

 <section class=section>
  <h2><span class=n>2</span> · Produce — send the same records two ways</h2>
  <div class=desc>Records are generated from the schema. <b>With</b> tokenization, ssn &amp; credit_card_number
   are replaced by same-length integer tokens before leaving the client; <b>without</b>, they go to a
   separate topic in the clear.</div>
  <div class=btns><label class=desc style="margin:0">records&nbsp;</label>
   <input id=count class=num type=number value=10 min=1></div>
  <div class="cols c2">
   <div class=panel>
    <h3 class=real>With tokenization</h3>
    <div class=ptopic id=tkTopic>…</div>
    <div class=pnote>ssn &amp; credit_card_number become same-length integer tokens; the broker never sees the real values.</div>
    <button class=pri onclick="produce('tokenized')">Produce tokenized</button>
    <div class=log id=prodTkLog style="margin-top:10px"></div>
   </div>
   <div class=panel>
    <h3 class=tok>Without tokenization</h3>
    <div class=ptopic id=ptTopic>…</div>
    <div class=pnote>Same data, no rule — the raw ssn &amp; credit_card_number land on the broker in the clear.</div>
    <button onclick="produce('plaintext')">Produce plaintext</button>
    <div class=log id=prodPtLog style="margin-top:10px"></div>
   </div>
  </div>
 </section>

 <section class=section>
  <h2><span class=n>3</span> · Consume — read the records back from the topic</h2>
  <div class=desc>The headline: for the <b>same record</b>, see the token that's actually on the broker
   (what an <span class=tok>unauthorized</span> reader gets) next to its detokenized real value
   (what an <span class=real>authorized</span> reader gets). Then contrast with the plaintext topic.</div>

  <div class=panel style="margin-bottom:14px">
   <h3>Tokenized topic — the same record, two ways</h3>
   <div class=ptopic id=cmpTopic>…</div>
   <div class=pnote>Each row is one record, joined by <code>loan_id</code>. The
    <span class=tok>token</span> is what Kafka stores and what an unauthorized consumer sees; the
    <span class=real>real</span> value is what the vault returns to an authorized consumer.</div>
   <button class=pri onclick=compare()>Read &amp; compare</button>
   <div class=recs id=cmpLog><div class=ph>Click to read the topic both ways.</div></div>
  </div>

  <div class=panel>
   <h3>Plaintext topic <span class="pill bad">no protection</span></h3>
   <div class=ptopic id=plainTopic>…</div>
   <div class=pnote>No rule, no vault — <b>any</b> reader sees the <span class=real>real (dashed) values</span>.
    There is no token; the sensitive data sits on the broker in the clear.</div>
   <button class=pri onclick="consume('plaintext')">Read</button>
   <div class=recs id=plainLog><div class=ph>Click Read to consume.</div></div>
  </div>
 </section>

</main>
<script>
const $=id=>document.getElementById(id)
const T={tk:'',pt:''}
function pill(el,ok,txt){el.className='pill '+(ok?'ok':'bad');el.textContent=txt}
async function checkVault(){const el=$('vaultPill');el.className='pill wait';el.textContent='checking…'
 try{const r=await(await fetch('/vault-health')).json();pill(el,r.up,r.up?'up':'down')}catch(e){pill(el,false,'down')}}
async function checkCC(){const el=$('ccPill');el.className='pill wait';el.textContent='checking…'
 try{const r=await(await fetch('/cc-status')).json();pill(el,r.ok,r.ok?'logged in':'login needed')}catch(e){pill(el,false,'error')}}

// text-log SSE (setup + produce): shows cleaned lines then a done marker
function sse(url,box){return new Promise(res=>{
 const log=$(box);log.textContent='';const es=new EventSource(url)
 es.onmessage=e=>{const l=JSON.parse(e.data).line;log.textContent+=l+'\\n';log.scrollTop=log.scrollHeight}
 es.addEventListener('done',e=>{const d=JSON.parse(e.data)
  log.textContent+=(d.ok?'✓ done':'✗ exit '+d.rc)+'\\n';log.scrollTop=log.scrollHeight;es.close();res(d.ok)})
 es.onerror=()=>{es.close();res(false)}})}
const run=s=>sse('/run/'+s,'setupLog')
async function runAll(){for(const s of ['build','discover','vault','schemas','topics']){if(!await run(s))break}
 loadTopics()}
const prodBox={tokenized:'prodTkLog',plaintext:'prodPtLog'}
const produce=mode=>sse('/produce?mode='+mode+'&count='+$('count').value,prodBox[mode])

// side-by-side compare: token on broker → detokenized real value, per record
function compare(){const log=$('cmpLog');log.innerHTML='';let n=0
 const es=new EventSource('/compare')
 es.onmessage=e=>{const line=(JSON.parse(e.data).line||'').trim()
  if(line[0]!=='{')return;let d;try{d=JSON.parse(line)}catch(_){return}
  if(d.ssn_real===undefined)return;n++
  if(n===1){const h=document.createElement('div');h.className='crow hdr'
   h.innerHTML='<span class=lid>loan_id</span>'
    +'<span class=pair>ssn:&nbsp;<span class=tok>on&nbsp;broker&nbsp;(token)</span> → <span class=real>real</span></span>'
    +'<span class=pair>credit_card:&nbsp;<span class=tok>on&nbsp;broker&nbsp;(token)</span> → <span class=real>real</span></span>'
   log.appendChild(h)}
  const div=document.createElement('div');div.className='crow'
  div.innerHTML='<span class=lid>'+d.loan_id+'</span>'
   +'<span class=pair><span class="v tok">'+d.ssn_tok+'</span><span class=arr>→</span><span class="v real">'+d.ssn_real+'</span></span>'
   +'<span class=pair><span class="v tok">'+d.cc_tok+'</span><span class=arr>→</span><span class="v real">'+d.cc_real+'</span></span>'
  log.appendChild(div);log.scrollTop=log.scrollHeight}
 es.addEventListener('done',e=>{es.close()
  const f=document.createElement('div');f.className='foot '+(n?'ok':'bad')
  f.textContent=(n?'✓ compared '+n+' record'+(n>1?'s':''):'no records read')+' on '+T.tk
  log.appendChild(f);log.scrollTop=log.scrollHeight})
 es.onerror=()=>es.close()}

// record SSE (consume): renders ONLY parsed records + a footer count — never raw logs
const cMeta={plaintext:['plainLog','real',()=>T.pt]}
function consume(role){const [box,cls,topicFn]=cMeta[role];const log=$(box)
 log.innerHTML='';let n=0;const es=new EventSource('/consume/'+role)
 es.onmessage=e=>{const line=(JSON.parse(e.data).line||'').trim()
  if(line[0]!=='{')return;let d;try{d=JSON.parse(line)}catch(_){return}
  if(d.ssn===undefined)return;n++
  const div=document.createElement('div');div.className='rec '+cls
  div.innerHTML='<span class=f>ssn</span><span class="v sens">'+d.ssn+'</span>'
   +'<span class=f>cc</span><span class="v sens">'+d.credit_card_number+'</span>'
   +'<span class=f>score</span><span class=v>'+d.credit_score+'</span>'
  log.appendChild(div);log.scrollTop=log.scrollHeight}
 es.addEventListener('done',e=>{es.close()
  const f=document.createElement('div');f.className='foot '+(n?'ok':'bad')
  f.textContent=(n?'✓ read '+n+' record'+(n>1?'s':''):'no records read')+' from '+topicFn()
  log.appendChild(f);log.scrollTop=log.scrollHeight})
 es.onerror=()=>es.close()}

async function loadTopics(){try{const e=await(await fetch('/env')).json()
 T.tk=e.TOPIC||'(run setup)';T.pt=e.PLAINTEXT_TOPIC||((e.TOPIC||'')+'-plaintext')
 $('tkTopic').textContent='→ '+T.tk;$('ptTopic').textContent='→ '+T.pt
 $('cmpTopic').textContent='topic: '+T.tk
 $('plainTopic').textContent='topic: '+T.pt
 if(e.TOPIC&&!$('topicIn').value)$('topicIn').value=e.TOPIC
 if(e.PLAINTEXT_TOPIC&&!$('ptopicIn').value)$('ptopicIn').value=e.PLAINTEXT_TOPIC}catch(e){}}

// ── target picker: environment → cluster → topics ──
const opt=(v,name,id)=>`<option value="${v}" data-name="${name}">${name} (${id})</option>`
async function loadEnvs(){const el=$('envSel')
 try{const envs=await(await fetch('/cc-envs')).json()
  if(envs.error||!Array.isArray(envs)){el.innerHTML='<option>login needed</option>';return}
  el.innerHTML=envs.map(e=>opt(e.id,e.name,e.id)).join('')
  const cur=(await(await fetch('/env')).json()).ENV_ID;if(cur)el.value=cur
  loadClusters()}catch(_){el.innerHTML='<option>error</option>'}}
async function loadClusters(){const el=$('clusSel');el.innerHTML='<option>…</option>'
 try{const cs=await(await fetch('/cc-clusters?env='+encodeURIComponent($('envSel').value))).json()
  if(cs.error||!Array.isArray(cs)){el.innerHTML='<option>'+((cs.error||'none').slice(0,38))+'</option>';return}
  el.innerHTML=cs.map(c=>opt(c.id,c.name,c.id)).join('')
  const cur=(await(await fetch('/env')).json()).CLUSTER_ID;if(cur)el.value=cur}catch(_){el.innerHTML='<option>error</option>'}}
async function ccPick(){const es=$('envSel'),cs=$('clusSel'),m=$('pickMsg')
 if(!es.value||!cs.value){m.textContent='pick an environment and cluster';return}
 m.textContent='saving & minting keys…'
 const body={env_id:es.value,env_name:es.selectedOptions[0]?.dataset.name||'',
  cluster_id:cs.value,cluster_name:cs.selectedOptions[0]?.dataset.name||'',
  topic:$('topicIn').value.trim(),plaintext_topic:$('ptopicIn').value.trim()}
 try{const r=await(await fetch('/cc-pick',{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify(body)})).json()
  m.textContent=r.ok?('✓ saved — bootstrap '+r.bootstrap):('✗ '+(r.msg||'error'));if(r.ok)loadTopics()}
 catch(e){m.textContent='✗ '+e}}
checkVault();checkCC();loadTopics();loadEnvs()
</script></body></html>"""


def main():
    httpd = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
