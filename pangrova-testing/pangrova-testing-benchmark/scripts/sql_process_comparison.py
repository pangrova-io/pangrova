#!/usr/bin/env python3
"""Run one process-isolated SQL benchmark fork against an installed checkout."""
import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--test-classpath", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=12)
    args = parser.parse_args()
    if args.warmup < 0 or args.iterations < 1:
        parser.error("Warmup must be nonnegative and iterations must be positive")
    if not os.environ.get("JAVA_HOME"):
        parser.error("Set JAVA_HOME to a JDK 17 installation")
    root = args.project.resolve(strict=True)
    stage = args.runtime.resolve(strict=True)
    native = args.native.resolve(strict=True)
    jdk = Path(os.environ["JAVA_HOME"]) / "bin"
    module = root / "pangrova-testing/pangrova-testing-integration-tests"
    package = "org.pangrova.testing.integration.e2e.speculation"
    helper = module / "src/test/java" / package.replace(".", "/") / "IndependentLocalProcesses.java"
    # Compile against the version that owns the processes and its result protocol.
    protocol, = re.findall(r"^import (org\.pangrova\.[\w.]+)\.ResultProtocol;$",
                           helper.read_text(), re.MULTILINE)
    driver = Path(__file__).with_name("SqlProcessComparison.java.in").read_text()
    cp = [module / "target/test-classes"]
    if (module / "target/classes").is_dir():
        cp.append(module / "target/classes")
    cp.extend(Path(p) for p in args.test_classpath.read_text().strip().split(os.pathsep))
    classpath = os.pathsep.join(str(p.resolve(strict=True)) for p in cp)
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        parser.error(f"Output already exists: {output}")
    with tempfile.TemporaryDirectory(prefix="pangrova-sql-benchmark-") as tmp:
        source = Path(tmp) / "SqlProcessComparison.java"
        source.write_text(driver.replace("PROTOCOL_PACKAGE", protocol))
        subprocess.run([jdk / "javac", "--release", "17", "-cp", classpath, "-d", tmp, source],
                       cwd=root, check=True)
        options = ["-Xms256m", "-Xmx768m", "-XX:MaxDirectMemorySize=512m",
                   "-Dio.netty.maxDirectMemory=0", "-DHADOOP_USER_NAME=pangrova",
                   "--patch-module", "java.base=" + str(module / "src/test/resources/patch-java-base"),
                   "-Dpangrova.e2e.local.lib-dir=" + str(stage / "lib"),
                   "-Dpangrova.e2e.local.jar-manifest=" + str(stage / "runtime.sha256"),
                   "-Dpangrova.e2e.local.native-lib=" + str(native)]
        for name in ("java.nio", "javax.security.auth", "java.lang", "java.lang.reflect",
                     "java.util", "java.util.concurrent", "sun.security.action", "sun.nio.ch"):
            options.extend(["--add-opens", "java.base/" + name + "=ALL-UNNAMED"])
        subprocess.run([jdk / "java", *options, "-cp", tmp + os.pathsep + classpath,
                        package + ".SqlProcessComparison", str(args.warmup),
                        str(args.iterations), output], cwd=root, check=True)


if __name__ == "__main__":
    main()
