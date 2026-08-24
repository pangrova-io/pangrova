#!/usr/bin/env bash
# TPC-H / TPC-DS Benchmark Runner for Pangrova
#
# Executes all TPC-H (22) or TPC-DS (99) queries against a running Pangrova cluster,
# records execution times, and outputs results in JSON format for regression comparison.
#
# Prerequisites:
#   - Running Pangrova cluster (coordinator + workers)
#   - TPC data loaded at the specified scale factor
#   - pangrova-cli available on PATH (or specify --cli-path)
#
# Usage: ./run-tpc-benchmark.sh [options]
#
# Options:
#   --benchmark BENCH    Which benchmark: tpch or tpcds (required)
#   --scale-factor SF    Scale factor for result labeling (default: 100)
#   --host HOST          Coordinator host (default: localhost)
#   --port PORT          Coordinator port (default: 8080)
#   --output FILE        Output JSON file (default: results/<date>.json)
#   --queries Q1,Q2,...  Specific queries to run (default: all)
#   --warmup N           Number of warmup iterations (default: 1)
#   --iterations N       Number of measured iterations (default: 3)
#   --timeout SECS       Per-query timeout in seconds (default: 600)
#   --catalog CATALOG    Catalog name (default: tpc)
#   --schema SCHEMA      Schema name (default: sf100)
#   --cli-path PATH      Path to pangrova-cli binary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_ROOT="${SCRIPT_DIR}/.."
QUERIES_DIR="${BENCHMARK_ROOT}/queries"

# Defaults
BENCHMARK=""
SCALE_FACTOR=100
HOST="localhost"
PORT=8080
OUTPUT=""
QUERIES=""
WARMUP=1
ITERATIONS=3
TIMEOUT=600
CATALOG="tpc"
SCHEMA="sf100"
CLI_PATH="pangrova-cli"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --benchmark) BENCHMARK="$2"; shift 2 ;;
        --scale-factor) SCALE_FACTOR="$2"; shift 2 ;;
        --host) HOST="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        --queries) QUERIES="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --catalog) CATALOG="$2"; shift 2 ;;
        --schema) SCHEMA="$2"; shift 2 ;;
        --cli-path) CLI_PATH="$2"; shift 2 ;;
        -h|--help) head -28 "$0" | tail -26; exit 0 ;;
        *) echo "Unknown option: $1"; exit 2 ;;
    esac
done

if [[ -z "$BENCHMARK" ]]; then
    echo "ERROR: --benchmark is required (tpch or tpcds)"
    exit 2
fi

# Determine query directory and query list
QUERY_DIR="${QUERIES_DIR}/${BENCHMARK}"
if [[ ! -d "$QUERY_DIR" ]]; then
    echo "ERROR: Query directory not found: $QUERY_DIR"
    exit 2
fi

# Build query list
if [[ -n "$QUERIES" ]]; then
    IFS=',' read -ra QUERY_LIST <<< "$QUERIES"
else
    case "$BENCHMARK" in
        tpch)
            QUERY_LIST=()
            for i in $(seq -w 1 22); do
                QUERY_LIST+=("q${i}")
            done
            ;;
        tpcds)
            QUERY_LIST=()
            for i in $(seq -w 1 99); do
                QUERY_LIST+=("q${i}")
            done
            ;;
        *) echo "ERROR: Unknown benchmark: $BENCHMARK"; exit 2 ;;
    esac
fi

# Set output file
if [[ -z "$OUTPUT" ]]; then
    OUTPUT="${BENCHMARK_ROOT}/results/${BENCHMARK}-sf${SCALE_FACTOR}-$(date +%Y-%m-%d).json"
fi
mkdir -p "$(dirname "$OUTPUT")"

echo "============================================"
echo "  TPC Benchmark Runner"
echo "============================================"
echo "  Benchmark:   ${BENCHMARK^^}"
echo "  Scale:       SF=${SCALE_FACTOR}"
echo "  Cluster:     ${HOST}:${PORT}"
echo "  Catalog:     ${CATALOG}.${SCHEMA}"
echo "  Queries:     ${#QUERY_LIST[@]}"
echo "  Warmup:      ${WARMUP} iterations"
echo "  Measured:    ${ITERATIONS} iterations"
echo "  Timeout:     ${TIMEOUT}s per query"
echo "  Output:      ${OUTPUT}"
echo "============================================"
echo ""

# Initialize results
RESULTS="{"
RESULTS+="\"benchmark\":\"${BENCHMARK}\","
RESULTS+="\"scaleFactor\":${SCALE_FACTOR},"
RESULTS+="\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
RESULTS+="\"environment\":{\"host\":\"${HOST}\",\"port\":${PORT},\"catalog\":\"${CATALOG}\",\"schema\":\"${SCHEMA}\"},"
RESULTS+="\"config\":{\"warmup\":${WARMUP},\"iterations\":${ITERATIONS},\"timeout\":${TIMEOUT}},"
RESULTS+="\"queries\":{"

PASSED=0
FAILED=0
FIRST=true

for QUERY_NAME in "${QUERY_LIST[@]}"; do
    QUERY_FILE="${QUERY_DIR}/${QUERY_NAME}.sql"
    if [[ ! -f "$QUERY_FILE" ]]; then
        echo "  ⚠ ${QUERY_NAME}: query file not found, skipping"
        continue
    fi

    printf "  %-6s " "${QUERY_NAME}:"

    # Warmup runs (discard results)
    for _ in $(seq 1 "$WARMUP"); do
        timeout "$TIMEOUT" "$CLI_PATH" \
            --host "$HOST" --port "$PORT" \
            --catalog "$CATALOG" --schema "$SCHEMA" \
            --file "$QUERY_FILE" \
            --output /dev/null 2>/dev/null || true
    done

    # Measured runs
    TIMES=()
    QUERY_FAILED=false
    for _ in $(seq 1 "$ITERATIONS"); do
        START_MS=$(($(date +%s%N) / 1000000))
        if timeout "$TIMEOUT" "$CLI_PATH" \
            --host "$HOST" --port "$PORT" \
            --catalog "$CATALOG" --schema "$SCHEMA" \
            --file "$QUERY_FILE" \
            --output /dev/null 2>/dev/null; then
            END_MS=$(($(date +%s%N) / 1000000))
            ELAPSED=$((END_MS - START_MS))
            TIMES+=("$ELAPSED")
        else
            QUERY_FAILED=true
            break
        fi
    done

    if [[ "$QUERY_FAILED" == "true" ]]; then
        echo "FAILED"
        FAILED=$((FAILED + 1))
        if [[ "$FIRST" == "true" ]]; then FIRST=false; else RESULTS+=","; fi
        RESULTS+="\"${QUERY_NAME}\":{\"status\":\"failed\"}"
    else
        # Calculate p50 and p99 (sort times, pick median and max)
        IFS=$'\n' SORTED=($(sort -n <<<"${TIMES[*]}")); unset IFS
        COUNT=${#SORTED[@]}
        P50_IDX=$(( (COUNT - 1) / 2 ))
        P99_IDX=$(( COUNT - 1 ))
        P50="${SORTED[$P50_IDX]}"
        P99="${SORTED[$P99_IDX]}"

        echo "${P50}ms (p50) / ${P99}ms (p99)"
        PASSED=$((PASSED + 1))
        if [[ "$FIRST" == "true" ]]; then FIRST=false; else RESULTS+=","; fi
        RESULTS+="\"${QUERY_NAME}\":{\"status\":\"passed\",\"p50Ms\":${P50},\"p99Ms\":${P99},\"samples\":[$(IFS=,; echo "${TIMES[*]}")]}"
    fi
done

RESULTS+="},"
RESULTS+="\"summary\":{\"total\":$((PASSED + FAILED)),\"passed\":${PASSED},\"failed\":${FAILED}}"
RESULTS+="}"

# Write results
echo "$RESULTS" | python3 -m json.tool > "$OUTPUT" 2>/dev/null || echo "$RESULTS" > "$OUTPUT"

echo ""
echo "============================================"
echo "  Results: ${PASSED} passed, ${FAILED} failed"
echo "  Output:  ${OUTPUT}"
echo "============================================"

if [[ $FAILED -gt 0 ]]; then
    exit 1
fi
