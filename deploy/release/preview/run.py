#!/usr/bin/env python3
"""Run the released Iceberg trial inside the preview runtime container."""
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time


def wait_for(label, process, ready, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and process.poll() is None:
        if ready():
            return
        time.sleep(0.25)
    raise RuntimeError(f"{label} was not ready within {timeout}s (exit={process.poll()})")


def health(path):
    connection = http.client.HTTPConnection("127.0.0.1", 8080, timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    except (OSError, ValueError, http.client.HTTPException):
        return 0, {}
    finally:
        connection.close()


def fresh_runtime():
    status, body = health("/v1/health")
    checks = body.get("checks", {})
    return (status == 503 and checks.get("http") is True
            and checks.get("transport") is True and checks.get("hasWorkers") is True
            and checks.get("hasCatalogs") is False)


def stop(process):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=45)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def coordinator_live():
    status, body = health("/v1/health/live")
    return status == 200 and body.get("live") is True


def main():
    root = Path(__file__).resolve().parents[1]
    classpath = subprocess.check_output(
        ["bash", "-c", 'source "$PANGROVA_HOME/bin/pangrova-classpath.sh"; pangrova_runtime_classpath standalone'],
        env={**os.environ, "PANGROVA_HOME": str(root)}, text=True).strip()
    with tempfile.TemporaryDirectory(prefix="pangrova-preview-") as temporary:
        work = Path(temporary)
        authority = subprocess.Popen(
            ["java", "-cp", classpath, str(root / "preview/PreviewAuthority.java"), str(work)],
            stdin=subprocess.PIPE)
        try:
            issuer = work / "issuer"
            wait_for("Preview issuer", authority, issuer.is_file, 30)
            host = socket.gethostbyname(socket.gethostname())
            key = secrets.token_hex(32)
            (work / "spill").mkdir()
            config = work / "standalone.yml"
            config.write_text(json.dumps({
                "coordinator": {"id": "preview-coordinator", "port": 8080, "host": "127.0.0.1",
                                "transport": {"advertised-host": "127.0.0.1"},
                                "internal-auth-secret": key},
                "worker": {"id": "preview-worker", "port": 8081, "host": host,
                           "advertised-host": host, "coordinator-url": "http://127.0.0.1:8080",
                           "internal-auth-secret": key,
                           "shuffle": {"service": {"spill-directory": str(work / "spill")}}},
                "security": {"enabled": True, "auth": {"type": "OAUTH2", "properties": {
                    "issuer-uri": issuer.read_text(), "audience": "pangrova-preview"}}},
                "metadata": {"enabled": True, "jdbc-url": "jdbc:postgresql://metadata:5432/pangrova_meta",
                             "jdbc-username": "pangrova",
                             "jdbc-password": os.environ["PANGROVA_PREVIEW_DB_PASSWORD"]},
                "tenant": {"enabled": True}, "indexing": {"enabled": False}, "observability": {"enabled": False},
            }), encoding="utf-8")
            env = {**os.environ, "PANGROVA_HOME": str(root), "PANGROVA_LOG_DIR": str(work / "logs"),
                   "PANGROVA_JAVA_OPTS": "-Xms256m -Xmx2g -XX:MaxDirectMemorySize=1g "
                   "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"}
            topology = os.environ["PANGROVA_PREVIEW_TOPOLOGY"]
            roles = ("standalone",) if topology == "standalone" else ("coordinator", "worker")
            processes = []
            try:
                for role in roles:
                    runtime = subprocess.Popen(
                        [str(root / f"bin/pangrova-{role}.sh"), "--config", str(config)], env=env)
                    processes.append(runtime)
                    if role == "coordinator":
                        wait_for("Coordinator liveness", runtime, coordinator_live, 120)
                wait_for("Preview runtime", processes[-1], fresh_runtime, 180)
                authority.stdin.write(b"G")
                authority.stdin.flush()
                token = work / "token"
                wait_for("Preview privileges", authority, token.is_file, 30)
                with tempfile.TemporaryDirectory(dir=work) as smoke:
                    subprocess.run(
                        [sys.executable, str(root / "examples/iceberg-smoke.py"), "--local-standalone-port", "8080",
                         "--token-env", "PANGROVA_TRIAL_TOKEN"],
                        env={**env, "PANGROVA_TRIAL_TOKEN": token.read_text(), "TMPDIR": smoke},
                        check=True, timeout=300)
                    if any(Path(smoke).iterdir()) or not fresh_runtime():
                        raise RuntimeError("Preview catalog or warehouse cleanup was not verified")
                print("Preview catalog and warehouse cleanup verified.", flush=True)
            finally:
                for process in reversed(processes):
                    stop(process)
        finally:
            authority.stdin.close()
            try:
                authority.wait(timeout=10)
            except subprocess.TimeoutExpired:
                stop(authority)


if __name__ == "__main__":
    main()
