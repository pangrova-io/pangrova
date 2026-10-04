#!/usr/bin/env python3
"""Select extra performance jobs; correctness jobs are selected independently."""

import argparse
from pathlib import PurePosixPath
import subprocess
import sys


def needs_performance(paths):
    for name in paths:
        path = PurePosixPath(name)
        # Runtime resources can be Markdown, and a source package may be named docs.
        if "/src/main/" in f"/{name}" or path.name == "pom.xml":
            return True
        if path.suffix.lower() in {".md", ".rst", ".adoc"} or name.startswith("docs/"):
            continue
        if name.startswith(("pangrova-testing/pangrova-test-benchmark/",
                            "pangrova-testing/pangrova-test-e2e/src/test/java/"
                            "org/pangrova/testing/e2e/performance/")):
            return True
        if name == ".pangrova/scripts/ci_change_scope.py":
            return True
        if name.startswith(("pangrova-worker-native/", ".github/workflows/",
                            "deploy/", "config/", "etc/", "bin/")):
            return True
        if "src/test" in name or name.startswith(".pangrova/scripts/tests/"):
            continue
        if name.startswith(".pangrova/scripts/"):
            continue
        # Unknown build/runtime inputs are treated conservatively.
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--head-ref", default="HEAD")
    args = parser.parse_args()
    try:
        for ref in (args.base_ref, args.head_ref):
            subprocess.run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        result = subprocess.run(
            ["git", "diff", "--name-only", "-z", f"{args.base_ref}...{args.head_ref}"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        paths = result.stdout.decode("utf-8").split("\0")
    except (subprocess.CalledProcessError, OSError, UnicodeError) as error:
        print(f"Cannot determine CI change scope: {error}", file=sys.stderr)
        return 1
    required = needs_performance(name for name in paths if name)
    print(f"performance={str(required).lower()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
