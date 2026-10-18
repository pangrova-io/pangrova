#!/usr/bin/env bash
# Pinned TPC-derived generation; all children and data completeness are validated.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${PYTHON:-python3}" "${SCRIPT_DIR}/generate_tpc_data.py" "$@"
