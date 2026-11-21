"""Measure a fixed remote reader baseline and the current reader on the same committed partition."""

import argparse
import csv
import os
import platform
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

BASELINE = "5d28b406a8b72522fec2025df5d2196dfb49141b"
CLIENTS = Path("pangrova-shuffle/pangrova-shuffle-pangrova/src/main/java/org/pangrova/shuffle/pangrova/client")
TYPES = ("PangrovaPartitionRead", "RemoteShuffleExchangeSource", "StreamingBatchChunkValidator")
FIELDS = "fork pair order reader duration_ns payload_bytes read_response_protobuf_bytes response_frames".split()


def compile_readers(root, temporary, classpath, javac):
    # Rename only these type identifiers, keeping both implementations in their original
    # Java package. Both borrow the same client pool and negotiated network connection.
    identifiers = re.compile(r"\b(" + "|".join(TYPES) + r")\b")
    sources = []
    for name in TYPES:
        path = CLIENTS / (name + ".java")
        original = subprocess.check_output(["git", "show", f"{BASELINE}:{path}"], cwd=root, text=True)
        if "RemoteShuffleClientException" in original:
            original = original.replace("package org.pangrova.shuffle.pangrova.client;",
                    "package org.pangrova.shuffle.pangrova.client;\n\n"
                    "import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;")
        renamed = temporary / f"Baseline{name}.java"
        renamed.write_text(identifiers.sub(lambda m: "Baseline" + m.group(0), original))
        sources.append(renamed)
    sources.append(Path(__file__).resolve().parent / "java/ShuffleReadComparison.java")
    subprocess.run([javac, "--release", "17", "-encoding", "UTF-8", "-cp", classpath,
                    "-d", temporary, *sources], cwd=root, check=True)


def run_fork(java, temporary, root, classpath, fork, log, elapsed):
    with tempfile.TemporaryDirectory(prefix="pgr-shuffle-") as fixture:
        command = [java, "-Xms512m", "-Xmx1024m", "-XX:+UseG1GC", "-Dio.netty.maxDirectMemory=0"]
        for package in ("java.nio", "java.lang", "java.util", "java.util.concurrent", "sun.nio.ch"):
            command.append(f"--add-opens=java.base/{package}=ALL-UNNAMED")
        command += ["-cp", os.pathsep.join([str(temporary), classpath]),
                    "org.pangrova.testing.integration.e2e.shuffle.ShuffleReadComparison", fixture, "6", "32"]
        if elapsed:
            command = ["/usr/bin/time", "-l", *command]
        with log.open("x") as output:
            subprocess.run(command, cwd=root, stdout=output, stderr=subprocess.STDOUT, check=True)
    samples = [row[1:] for row in csv.reader(line for line in log.read_text().splitlines() if line.startswith("SAMPLE,"))]
    if len(samples) != 64:
        raise RuntimeError(f"Fork {fork} produced {len(samples)} samples, expected 64")
    for pair in range(32):
        ordered = samples[2 * pair:2 * pair + 2]
        names = [row[2] for row in ordered]
        expected = ["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]
        if names != expected or any(int(row[0]) != pair for row in ordered):
            raise RuntimeError(f"Fork {fork} has an invalid pair {pair}")
    return [[fork, *row] for row in samples]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath-file", required=True, type=Path,
                        help="Current integration-test classpath, including compiled main/test classes and resources")
    parser.add_argument("--output", required=True, type=Path, help="New directory for CSV and per-fork logs")
    args = parser.parse_args()
    classpath = args.classpath_file.read_text().strip()
    if not classpath:
        parser.error("The integration-test classpath is empty")
    root = Path(__file__).resolve().parents[3]
    args.output.mkdir(parents=True, exist_ok=False)
    home = os.environ.get("JAVA_HOME")
    java = str(Path(home) / "bin/java") if home else shutil.which("java")
    javac = str(Path(home) / "bin/javac") if home else shutil.which("javac")
    if not java or not javac:
        parser.error("JDK 17 is required")
    timed = platform.system() == "Darwin"
    with tempfile.TemporaryDirectory(prefix="pgr-shuffle-classes-") as classes:
        temporary = Path(classes)
        compile_readers(root, temporary, classpath, javac)
        rows = []
        for fork in (1, 2, 3):
            rows.extend(run_fork(java, temporary, root, classpath, fork, args.output / f"fork-{fork}.log", timed))
        with (args.output / "shuffle-read-comparison.csv").open("x", newline="") as output:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(FIELDS)
            writer.writerows(rows)


if __name__ == "__main__":
    main()
