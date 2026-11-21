#!/usr/bin/env bash
#
# Run the reference SQL generator self-test locally.
# Generates expression and plan trees and executes DuckDB only.
# This does not validate Pangrova engine execution or differential correctness.
#
# Usage:
#   ./run-operator-fuzzer.sh                    # defaults: 10000 iterations, operator mode
#   ./run-operator-fuzzer.sh --iterations 5000  # custom iteration count
#   ./run-operator-fuzzer.sh --mode all         # run all fuzzer types
#   ./run-operator-fuzzer.sh --seed 12345       # reproducible run
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUZZER_DIR="${SCRIPT_DIR}/../src/main/cpp"
BUILD_DIR="${FUZZER_DIR}/build"
RUNNER="${BUILD_DIR}/pangrova_fuzzer_runner"

# Default configuration (matches the manual workflow)
ITERATIONS="${FUZZER_ITERATIONS:-10000}"
MODE="${FUZZER_MODE:-operator}"
SEED="${FUZZER_SEED:-0}"
BATCH_SIZE="${FUZZER_BATCH_SIZE:-100}"
OUTPUT="${FUZZER_OUTPUT:-}"

# Parse command-line arguments (override env vars)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --mode)       MODE="$2"; shift 2 ;;
        --seed)       SEED="$2"; shift 2 ;;
        --batch-size) BATCH_SIZE="$2"; shift 2 ;;
        --output)     OUTPUT="$2"; shift 2 ;;
        --build-only) BUILD_ONLY=1; shift ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --iterations <N>    Number of plans per run (default: 10000)"
            echo "  --mode <mode>       operator|expression|plan|all (default: operator)"
            echo "  --seed <N>          Random seed, 0 for time-based (default: 0)"
            echo "  --batch-size <N>    Iterations per batch (default: 100)"
            echo "  --output <path>     Write JSON results to file"
            echo "  --build-only        Build without running"
            echo ""
            echo "Environment variables:"
            echo "  FUZZER_ITERATIONS   Same as --iterations"
            echo "  FUZZER_MODE         Same as --mode"
            echo "  FUZZER_SEED         Same as --seed"
            echo "  FUZZER_BATCH_SIZE   Same as --batch-size"
            echo "  FUZZER_OUTPUT       Same as --output"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Build if needed
if [[ ! -x "${RUNNER}" ]] || [[ "${BUILD_ONLY:-}" == "1" ]]; then
    echo "Building fuzzer..."
    cmake -B "${BUILD_DIR}" -S "${FUZZER_DIR}" -G Ninja -DCMAKE_BUILD_TYPE=Release
    cmake --build "${BUILD_DIR}"
    echo "Build complete: ${RUNNER}"
fi

if [[ "${BUILD_ONLY:-}" == "1" ]]; then
    exit 0
fi

# Determine seed
if [[ "${SEED}" == "0" ]]; then
    SEED="$(date +%s)"
fi

# Build runner arguments
ARGS=(
    --mode "${MODE}"
    --iterations "${ITERATIONS}"
    --seed "${SEED}"
    --batch-size "${BATCH_SIZE}"
)

if [[ -n "${OUTPUT}" ]]; then
    ARGS+=(--output "${OUTPUT}")
fi

echo "=== Pangrova Operator Fuzzer ==="
echo "Iterations: ${ITERATIONS} | Mode: ${MODE} | Seed: ${SEED} | Batch: ${BATCH_SIZE}"
echo ""

exec "${RUNNER}" "${ARGS[@]}"
