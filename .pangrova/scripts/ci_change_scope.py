#!/usr/bin/env python3
"""Select PR validation; pushes and release calls retain full correctness checks."""

import argparse
import json
from pathlib import PurePosixPath
import subprocess
import sys


# Single platform inventory for PR, protected-branch and release validation.
NATIVE_PLATFORMS = [
    {
        "name": "linux-x86_64",
        "runner": "ubuntu-24.04",
        "os": "linux",
        "arch": "x86_64",
        "lib-ext": "so",
        "classifier": "linux-x86_64"
    },
    {
        "name": "linux-aarch_64",
        "runner": "ubuntu-24.04-arm",
        "os": "linux",
        "arch": "aarch64",
        "lib-ext": "so",
        "classifier": "linux-aarch_64"
    },
    {
        "name": "osx-x86_64",
        "runner": "macos-15-intel",
        "os": "osx",
        "arch": "x86_64",
        "lib-ext": "dylib",
        "classifier": "osx-x86_64"
    },
    {
        "name": "osx-aarch_64",
        "runner": "macos-15",
        "os": "osx",
        "arch": "aarch64",
        "lib-ext": "dylib",
        "classifier": "osx-aarch_64"
    }
]


def is_documentation(name):
    # A runtime resource or source package named docs is still a build input.
    if "/src/main/" in f"/{name}" or "/src/test/" in f"/{name}":
        return False
    return (PurePosixPath(name).suffix.lower() in {".md", ".rst", ".adoc"}
            or name.startswith(("docs/", "website/", ".github/ISSUE_TEMPLATE/"))
            or name in {".github/CODEOWNERS", ".github/dependabot.yml", ".gitignore"})


def needs_runtime(paths):
    for name in paths:
        if is_documentation(name):
            continue
        # These tools have their own checks in the lightweight lint job.
        if (name == ".pangrova/setup.sh" or name.startswith(".pangrova/scripts/")) and not name.startswith((
                ".pangrova/scripts/ci_change_scope.py",
                ".pangrova/scripts/stage-production-regression-native.sh",
                ".pangrova/scripts/build-iceberg-sdk-candidate.sh",
                ".pangrova/scripts/proto-descriptors/")):
            continue
        if name.startswith("config/checkstyle/") or name.startswith("config/spotbugs/"):
            return True
        if name in {".github/workflows/deploy-website.yml",
                    ".github/workflows/nightly-operator-fuzzer.yml"}:
            continue
        # Tests, deleted sources, dependencies and unknown inputs all retain
        # the full correctness chain. Do not infer native ABI safety from names.
        return True
    return False


def needs_native_checks(paths):
    """Select portability and standalone native sanitizer tests by their inputs."""
    for name in paths:
        if not needs_runtime([name]):
            continue
        if name.startswith(('pangrova-worker/', 'pangrova-worker-native/',
                            'pangrova-native-bundle/', 'pangrova-protocol/')):
            return True
        # Other Java leaves do not own JNI. Production-image E2E still verifies
        # their behavior against fresh Linux JNI; unchanged standalone C++ tests
        # need not rerun on three other architectures and two sanitizers.
        if name.startswith('pangrova-') and any(part in name for part in (
                '/src/main/java/', '/src/test/java/', '/src/test/resources/')):
            continue
        return True  # Build inputs, runtime resources and unknown paths.
    return False


def needs_performance(paths):
    for name in paths:
        path = PurePosixPath(name)
        # Runtime resources can be Markdown, and a source package may be named docs.
        if "/src/main/" in f"/{name}" or path.name == "pom.xml":
            return True
        if is_documentation(name):
            continue
        if name.startswith(("pangrova-testing/pangrova-testing-benchmark/",
                            "pangrova-testing/pangrova-testing-integration-tests/src/test/java/"
                            "org/pangrova/testing/integration/e2e/performance/")):
            return True
        if name in {".pangrova/scripts/ci_change_scope.py",
                    ".pangrova/scripts/build-iceberg-sdk-candidate.sh"}:
            return True
        if name == ".pangrova/setup.sh":
            continue
        if name in {".github/workflows/deploy-website.yml",
                    ".github/workflows/nightly-operator-fuzzer.yml"}:
            continue
        if name.startswith(("pangrova-worker-native/", ".github/",
                            "deploy/", "config/", "etc/", "bin/")):
            return True
        if "src/test" in name or name.startswith(".pangrova/scripts/tests/"):
            continue
        if name.startswith(".pangrova/scripts/"):
            continue
        # Unknown build/runtime inputs are treated conservatively.
        return True
    return False


def emit_scope(*, runtime, native, performance):
    print(f"runtime={str(runtime).lower()}")
    print(f"native={str(native).lower()}")
    print(f"performance={str(performance).lower()}")
    platforms = NATIVE_PLATFORMS if native else NATIVE_PLATFORMS[:1]
    print('native_matrix=' + json.dumps(platforms, separators=(',', ':')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref")
    parser.add_argument("--full", action="store_true",
                        help="Select the complete correctness chain for pushes/releases")
    parser.add_argument("--head-ref", default="HEAD")
    args = parser.parse_args()
    if args.full:
        emit_scope(runtime=True, native=True, performance=False)
        return 0
    if not args.base_ref:
        parser.error("--base-ref is required unless --full is used")
    try:
        for ref in (args.base_ref, args.head_ref):
            subprocess.run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        result = subprocess.run(
            ["git", "diff", "--no-renames", "--name-only", "-z",
             f"{args.base_ref}...{args.head_ref}"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        paths = [name for name in result.stdout.decode("utf-8").split("\0") if name]
    except (subprocess.CalledProcessError, OSError, UnicodeError) as error:
        print(f"Cannot determine CI change scope: {error}", file=sys.stderr)
        return 1
    emit_scope(runtime=needs_runtime(paths), native=needs_native_checks(paths),
               performance=needs_performance(paths))
    return 0


if __name__ == "__main__":
    sys.exit(main())
