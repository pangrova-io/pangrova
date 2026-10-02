#!/usr/bin/env bash
# Measure committed, staged, unstaged and untracked changed executable lines.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/diff_coverage.py" "$@"
