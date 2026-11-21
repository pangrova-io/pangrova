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
TOKEN_ENV = "PANGROVA_ICEBERG_TRIAL_TOKEN"
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
    auth_args = []
    if token:
        env[TOKEN_ENV] = token
        auth_args = ["--token-env", TOKEN_ENV]
    completed = subprocess.run(
        [str(root / "bin/pangrova-cli.sh"), "--host", HOST, "--port", str(port),
         "--query-timeout", "30s", "--file", str(sql), "--output", str(output),
         "--execution-receipt", str(receipt), *auth_args],
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


def run(root, port, catalog):
    token = os.environ.get(TOKEN_ENV)
    if token is not None and not token.strip():
        raise ValueError(f"{TOKEN_ENV} is blank")
    require_clean_local_coordinator(port)
    work = Path(tempfile.mkdtemp(prefix="pangrova-iceberg-")).resolve()
    catalog = catalog or f"iceberg_trial_{uuid.uuid4().hex}"
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
            can_remove_work = False
            if not ambiguous_create:
                if confirmed_creates == len(CREATE_PREFIXES):
                    execute(root, work, port, "drop-table", f"DROP TABLE {catalog}.demo.events\n", token)
                if confirmed_creates:
                    execute(root, work, port, "drop-catalog", f"DROP CATALOG {catalog}\n", token)
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


def trial_catalog(value):
    if not re.fullmatch(r"iceberg_trial_[a-z0-9_]{1,49}", value):
        raise argparse.ArgumentTypeError("catalog must start with iceberg_trial_ and use lowercase letters, digits or underscores")
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-standalone-port", type=local_port, required=True,
                        help="confirm the selected port belongs to a disposable local standalone")
    parser.add_argument("--catalog", type=trial_catalog, help="use a catalog name with pre-provisioned trial grants")
    args = parser.parse_args()
    try:
        run(ROOT, args.local_standalone_port, args.catalog)
    except (OSError, RuntimeError, ValueError, http.client.HTTPException, subprocess.SubprocessError) as exc:
        sys.exit(f"Iceberg trial failed: {exc}")
    except KeyboardInterrupt:
        sys.exit("Iceberg trial interrupted")
