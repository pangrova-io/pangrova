#!/usr/bin/env bash
# Run complete SQL files through the batch CLI and retain measured latency evidence.
# See --help for options. Few-sample runs do not emit P99 and cannot pass regression gates.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/run_tpc_benchmark.py" "$@"
