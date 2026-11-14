#!/usr/bin/env python3
"""Run CTest with a fresh, process-owned real PostgreSQL/Lance authority."""

import argparse
from contextlib import contextmanager
import json
import os
import re
from pathlib import Path
import secrets
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time


MODULE = "pangrova-connector/pangrova-connector-lance"
BROKER = "org.pangrova.connector.lance.LanceManagedSourceFixtureBroker"


def stop_process_group(process, timeout):
    if process.poll() is not None:
        return True
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait()
        return True
    try:
        process.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        return False


def receive_exact(connection, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = connection.recv(size - len(chunks))
        if not chunk:
            raise OSError("Fixture closed its readiness response")
        chunks.extend(chunk)
    return bytes(chunks)


def check_ready(endpoint, nonce):
    """Verify the live authority, without printing its credentials or nonce."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(1)
        connection.connect(str(endpoint))
        request = json.dumps({"operation": "catalog", "nonce": nonce}).encode()
        connection.sendall(struct.pack("!I", len(request)) + request)
        size = struct.unpack("!I", receive_exact(connection, 4))[0]
        if not 0 < size <= 32 * 1024 * 1024:
            raise RuntimeError("Invalid fixture readiness frame")
        response = json.loads(receive_exact(connection, size))
        if response.get("success") is not True or not response.get("catalog"):
            raise RuntimeError("Real PostgreSQL/Lance authority rejected readiness")


@contextmanager
def fixture(command, workdir, log_file, timeout=180, stop_timeout=30):
    # A short Unix socket path also fits macOS's sockaddr_un limit.
    with tempfile.TemporaryDirectory(prefix="pgf-", dir="/tmp") as temporary:
        directory = Path(temporary)
        directory.chmod(0o700)
        endpoint, ready = directory / "s", directory / "ready.json"
        nonce = secrets.token_hex(32)
        environment = dict(os.environ, PANGROVA_LANCE_FIXTURE_SOCKET=str(endpoint),
                           PANGROVA_LANCE_FIXTURE_NONCE=nonce)
        with log_file.open("wb") as output:
            process = subprocess.Popen(
                [*command, str(endpoint), str(ready)], cwd=workdir,
                env=environment, stdin=subprocess.DEVNULL,
                stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + timeout
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"Lance fixture exited {process.returncode}; see {log_file}")
                    if endpoint.is_socket() and ready.is_file():
                        check_ready(endpoint, nonce)
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"Lance fixture readiness timed out; see {log_file}")
                    time.sleep(0.1)
                yield environment
                if process.poll() is not None:
                    raise RuntimeError(f"Lance fixture exited during CTest; see {log_file}")
            finally:
                if not stop_process_group(process, stop_timeout):
                    raise RuntimeError(f"Lance fixture did not shut down cleanly; see {log_file}")


def prepare_java(root, logs):
    classpath = logs / "classpath.txt"
    commands = [
        ["mvn", "-q", "-pl", MODULE, "-am", "-DskipTests",
         f"-Dpangrova.function-contract.python={sys.executable}", "install"],
        ["mvn", "-q", "-pl", MODULE, "-DincludeScope=test",
         f"-Dmdep.outputFile={classpath}", "dependency:build-classpath"],
    ]
    with (logs / "java-build.log").open("wb") as output:
        for command in commands:
            subprocess.run(command, cwd=root, stdout=output,
                           stderr=subprocess.STDOUT, check=True,
                           env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    entries = [root / MODULE / "target/test-classes", root / MODULE / "target/classes"]
    return os.pathsep.join([*(str(entry) for entry in entries), classpath.read_text().strip()])


def retain_ctest_log(build, logs, started_ns):
    """Keep this invocation's passing-test output before discovery can replace it."""
    source = build / "Testing/Temporary/LastTest.log"
    if not source.is_file() or source.stat().st_mtime_ns < started_ns:
        raise RuntimeError("CTest did not produce a fresh per-test log for this invocation")
    destination = logs / "ctest.log"
    with source.open("rb") as incoming, destination.open("xb") as retained:
        shutil.copyfileobj(incoming, retained)


SANITIZER_DIAGNOSTIC = re.compile(
    rb"^(?:(?:==[0-9]+==)?(?:ERROR|SUMMARY): (?:Address|Leak|UndefinedBehavior|Thread|Memory)Sanitizer"
    rb"|.+:[0-9]+(?::[0-9]+)?: runtime error: )")


def require_clean_sanitizers(log):
    """CTest can succeed after recoverable UBSan findings. Check the retained test output."""
    with log.open("rb") as stream:
        for line in stream:
            if SANITIZER_DIAGNOSTIC.match(line):
                detail = line.decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"Native sanitizer finding: {detail}. Full per-test output: {log}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    if sys.version_info < (3, 10):
        parser.error(
            f"Python 3.10 or later is required (running {sys.version.split()[0]}); "
            "the nested Maven fixture build reuses this interpreter for function contracts")
    native = Path(__file__).resolve().parents[1]
    parser.add_argument("--build-dir", type=Path, default=native / "build")
    parser.add_argument("ctest_args", nargs=argparse.REMAINDER,
                        help="CTest options after --, e.g. -- -R IcebergManifestEntryReaderTest")
    arguments = parser.parse_args()
    def terminate(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    build = arguments.build_dir.resolve()
    if not (build / "CTestTestfile.cmake").is_file():
        parser.error(f"Configure and build Native tests first: {build}")
    java = Path(os.environ["JAVA_HOME"]) / "bin/java" if os.environ.get("JAVA_HOME") else shutil.which("java")
    if not java or not shutil.which("mvn") or not shutil.which("ctest"):
        parser.error("JDK 17, Maven and CTest must be available")
    log_root = build / "Testing/LanceFixture"
    log_root.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=log_root))
    print(f"Native fixture logs: {logs}", flush=True)
    try:
        classpath = prepare_java(native.parent, logs)
        command = [str(java), "--add-opens=java.base/java.nio=ALL-UNNAMED",
                   "-cp", classpath, BROKER]
        # Honor Docker/Testcontainers settings and ASAN_OPTIONS from the caller.
        # JAVA_TOOL_OPTIONS is the standard way to supply a required Docker API override.
        with fixture(command, native.parent, logs / "broker.log") as environment:
            options = arguments.ctest_args
            if options[:1] == ["--"]:
                options = options[1:]
            started_ns = time.time_ns()
            process = subprocess.Popen(["ctest", "--test-dir", str(build),
                                        "--output-on-failure", *options],
                                       cwd=native, env=environment, start_new_session=True)
            try:
                code = process.wait()
                try:
                    retain_ctest_log(build, logs, started_ns)
                except (OSError, RuntimeError) as failure:
                    if code == 0:
                        raise
                    # Preserve CTest's failure status even if it could not write evidence.
                    print(f"CTest evidence was not retained: {failure}", file=sys.stderr)
                if code == 0:
                    require_clean_sanitizers(logs / "ctest.log")
                return code if code >= 0 else 128 - code
            finally:
                if not stop_process_group(process, 30):
                    print("CTest process group required forced cleanup", file=sys.stderr)
    except subprocess.CalledProcessError as failure:
        print(f"Java fixture build failed ({failure.returncode}); see {logs / 'java-build.log'}", file=sys.stderr)
        return failure.returncode
    except (OSError, RuntimeError) as failure:
        print(str(failure), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
