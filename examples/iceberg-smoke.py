#!/usr/bin/env python3
"""Verify Pangrova Iceberg SQL on an already-started, disposable local standalone."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parent.parent
HOST = "127.0.0.1"
EXPECTED = [("event_type", "cnt"), ("click", "2"), ("purchase", "1")]
CREATE_PREFIXES = ("CREATE CATALOG ", "CREATE SCHEMA ", "CREATE TABLE ")


def require_clean_local_coordinator(port):
    # Direct loopback only: no proxies, environment URLs or redirects.
    conn = http.client.HTTPConnection(HOST, port, timeout=5)
    try:
        conn.request("GET", "/v1/health")
        response = conn.getresponse()
        # A healthy Coordinator without a catalog reports 503 before this demo.
        if response.status != 503:
            raise RuntimeError("Expected HTTP 503 from a fresh Coordinator with no catalog")
        health = json.loads(response.read(65536))
    finally:
        conn.close()
    checks = health.get("checks", {}) if isinstance(health, dict) else {}
    if (not isinstance(health, dict) or health.get("nodeType") != "COORDINATOR"
            or health.get("ready") is not False or not isinstance(checks, dict)
            or checks.get("http") is not True
            or checks.get("transport") is not True or checks.get("hasWorkers") is not True
            or checks.get("hasCatalogs") is not False):
        raise RuntimeError("Expected a live local Coordinator with an active Worker and no catalog")


def fixture_statements(root, work, catalog):
    fixture = (root / "examples/iceberg-smoke.sql").read_text(encoding="utf-8")
    warehouse = "file:///tmp/iceberg-warehouse"
    if fixture.count(warehouse) != 1:
        raise RuntimeError("Iceberg fixture must have exactly one replaceable warehouse path")
    fixture = fixture.replace(warehouse, (work / "warehouse").as_uri())
    fixture = re.sub(r"\blocal_iceberg\b", catalog, fixture)
    pending, statements = [], []
    for line in fixture.splitlines():
        if not line.strip() or line.lstrip().startswith("--"):
            continue
        pending.append(line)
        # Match the checked-in fixture's CLI line-terminated SQL statements.
        if line.rstrip().endswith(";"):
            statements.append("\n".join(pending).rstrip()[:-1] + "\n")
            pending = []
    prefixes = (*CREATE_PREFIXES, "INSERT INTO ", "SELECT ")
    if (pending or len(statements) != len(prefixes)
            or any(not statement.startswith(prefix) or catalog not in statement
                   for statement, prefix in zip(statements, prefixes))):
        raise RuntimeError("Unexpected SQL in the Iceberg fixture")
    return statements


def execute(root, work, port, step, statement, token):
    sql = work / f"{step}.sql"
    output = work / f"{step}.out"
    receipt = work / f"{step}.json"
    sql.write_text(statement, encoding="utf-8")
    env = {key: os.environ[key] for key in ("PATH", "HOME", "JAVA_HOME", "LANG", "LC_ALL")
           if key in os.environ}
    # The launcher must not source user-wide connection or credential overrides.
    env.update(PANGROVA_HOME=str(root), PANGROVA_CONF_DIR=str(work))
    command = [str(root / "bin/pangrova-cli.sh"), "--host", HOST, "--port", str(port),
               "--query-timeout", "30s", "--file", str(sql), "--output", str(output),
               "--execution-receipt", str(receipt)]
    if token is not None:
        env["PANGROVA_ICEBERG_TRIAL_TOKEN"] = token
        command.extend(("--token-env", "PANGROVA_ICEBERG_TRIAL_TOKEN"))
    completed = subprocess.run(
        command,
        cwd=work, env=env, capture_output=True, text=True, timeout=45,
    )
    if completed.returncode != 0:
        diagnostic = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"{step} failed: {diagnostic}")
    proof = json.loads(receipt.read_text(encoding="utf-8"))
    if (not isinstance(proof, dict) or not proof.get("queryId") or proof.get("state") != "FINISHED"
            or proof.get("resultConsumed") is not True
            or proof.get("sqlSha256") != hashlib.sha256(sql.read_bytes()).hexdigest()):
        raise RuntimeError(f"Invalid execution receipt for {step}")
    return output.read_text(encoding="utf-8")


def verify_result(output):
    # The batch CLI can write polling dots before an ASCII table header.
    rows = [tuple(field.strip() for field in line.split("|")[1:-1])
            for line in output.splitlines() if line.rstrip().endswith("|")]
    if rows != EXPECTED:
        raise RuntimeError(f"Unexpected Iceberg result: {rows!r}")


def run(root, port, token):
    require_clean_local_coordinator(port)
    work = Path(tempfile.mkdtemp(prefix="pangrova-iceberg-")).resolve()
    catalog = f"iceberg_trial_{uuid.uuid4().hex}"
    confirmed_creates = 0
    ambiguous_create = False
    can_remove_work = True
    try:
        try:
            for index, statement in enumerate(fixture_statements(root, work, catalog)):
                ambiguous_create = index < len(CREATE_PREFIXES)
                output = execute(root, work, port, f"statement-{index + 1}", statement, token)
                if ambiguous_create:
                    confirmed_creates += 1
                    ambiguous_create = False
            verify_result(output)
        finally:
            # A failed or interrupted CREATE may already have committed. Keep the
            # scoped evidence instead of guessing and issuing destructive cleanup.
            trial_failure = sys.exc_info()[1]
            can_remove_work = False
            if not ambiguous_create:
                cleanup_steps = ((3, f"DROP TABLE {catalog}.demo.events"),
                                 (1, f"DROP CATALOG {catalog}"))
                try:
                    for step, statement in cleanup_steps:
                        if confirmed_creates >= step:
                            execute(root, work, port, f"cleanup-{step}", f"{statement}\n", token)
                except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
                    if trial_failure is not None:
                        print(f"Trial SQL failed before cleanup: {trial_failure}", file=sys.stderr)
                    raise
                can_remove_work = True
    finally:
        if can_remove_work:
            shutil.rmtree(work)
        else:
            print(f"Cleanup not verified. Check catalog {catalog}; files retained at {work}",
                  file=sys.stderr)
    print("Iceberg trial passed: click=2, purchase=1")


def local_port(value):
    port = int(value)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be in 1..65535")
    return port


def token_from_environment(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise argparse.ArgumentTypeError("invalid bearer token environment variable name")
    token = os.environ.get(name)
    if not token or not token.strip():
        raise argparse.ArgumentTypeError("bearer token environment variable is unset or blank")
    return token


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-standalone-port", type=local_port, required=True,
                        help="confirm the selected port belongs to a disposable local standalone")
    parser.add_argument("--token-env", type=token_from_environment, metavar="NAME",
                        help="read the trial principal's bearer token from the named environment variable")
    args = parser.parse_args()
    try:
        run(ROOT, args.local_standalone_port, args.token_env)
    except (OSError, RuntimeError, ValueError, http.client.HTTPException, subprocess.SubprocessError) as exc:
        sys.exit(f"Iceberg trial failed: {exc}")
    except KeyboardInterrupt:
        sys.exit("Iceberg trial interrupted")
