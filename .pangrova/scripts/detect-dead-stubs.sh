#!/usr/bin/env bash
# Check concrete Java method bodies in production, including default methods.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
git ls-files --cached --others --exclude-standard -z -- ':(glob)**/src/main/java/**/*.java' |
    java .pangrova/scripts/DeadStubCheck.java
