#!/usr/bin/env bash
# TPC-H and TPC-DS Data Generation for SF=100 (100GB)
#
# Downloads and builds the official TPC data generators (dbgen/dsdgen),
# then generates SF=100 datasets in Parquet format for ingestion into Pangrova.
#
# Prerequisites:
#   - GCC/G++ (for building dbgen/dsdgen)
#   - Python 3.8+ with pyarrow (for CSV-to-Parquet conversion)
#   - ~200GB free disk space (CSV + Parquet)
#
# Usage: ./generate-tpc-data.sh [options]
#
# Options:
#   --scale-factor SF    Scale factor (default: 100)
#   --output-dir DIR     Output directory (default: /data/tpc)
#   --benchmark BENCH    Which benchmark: tpch, tpcds, or both (default: both)
#   --format FORMAT      Output format: parquet or csv (default: parquet)
#   --parallel N         Number of parallel chunks for generation (default: 8)
#   --skip-build         Skip building dbgen/dsdgen (use existing binaries)
#
# Environment:
#   TPCH_DBGEN_DIR       Path to pre-built TPC-H dbgen (skips download)
#   TPCDS_DSDGEN_DIR     Path to pre-built TPC-DS dsdgen (skips download)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
SCALE_FACTOR=100
OUTPUT_DIR="/data/tpc"
BENCHMARK="both"
FORMAT="parquet"
PARALLEL=8
SKIP_BUILD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --scale-factor) SCALE_FACTOR="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --benchmark) BENCHMARK="$2"; shift 2 ;;
        --format) FORMAT="$2"; shift 2 ;;
        --parallel) PARALLEL="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        -h|--help) head -25 "$0" | tail -23; exit 0 ;;
        *) echo "Unknown option: $1"; exit 2 ;;
    esac
done

echo "============================================"
echo "  TPC Data Generation"
echo "============================================"
echo "  Scale Factor: SF=${SCALE_FACTOR} (~${SCALE_FACTOR}GB)"
echo "  Output:       ${OUTPUT_DIR}"
echo "  Benchmark:    ${BENCHMARK}"
echo "  Format:       ${FORMAT}"
echo "  Parallel:     ${PARALLEL} chunks"
echo "============================================"
echo ""

BUILD_DIR="${OUTPUT_DIR}/.build"
mkdir -p "${BUILD_DIR}"

# ─── TPC-H Data Generation ───────────────────────────────────────────────────

generate_tpch() {
    local TPCH_DIR="${OUTPUT_DIR}/tpch/sf${SCALE_FACTOR}"
    local TPCH_CSV_DIR="${TPCH_DIR}/csv"
    local TPCH_PARQUET_DIR="${TPCH_DIR}/parquet"

    echo ">>> Generating TPC-H SF=${SCALE_FACTOR} data..."

    # Build dbgen if needed
    local DBGEN_BIN="${TPCH_DBGEN_DIR:-${BUILD_DIR}/tpch-dbgen}/dbgen"
    if [[ ! -x "$DBGEN_BIN" ]] && [[ "$SKIP_BUILD" == "false" ]]; then
        echo "  Building TPC-H dbgen..."
        local TPCH_SRC="${BUILD_DIR}/tpch-dbgen"
        if [[ ! -d "$TPCH_SRC" ]]; then
            git clone --depth 1 https://github.com/electrum/tpch-dbgen.git "$TPCH_SRC"
        fi
        make -C "$TPCH_SRC" -j"$(nproc)" MACHINE=LINUX DATABASE=ORACLE
        DBGEN_BIN="${TPCH_SRC}/dbgen"
    fi

    if [[ ! -x "$DBGEN_BIN" ]]; then
        echo "ERROR: dbgen binary not found at: $DBGEN_BIN"
        echo "  Set TPCH_DBGEN_DIR or remove --skip-build"
        exit 2
    fi

    # Generate CSV data in parallel chunks
    mkdir -p "$TPCH_CSV_DIR"
    echo "  Generating CSV data (${PARALLEL} chunks)..."
    local DBGEN_DIR
    DBGEN_DIR="$(dirname "$DBGEN_BIN")"

    for i in $(seq 1 "$PARALLEL"); do
        (
            cd "$DBGEN_DIR"
            ./dbgen -s "$SCALE_FACTOR" -C "$PARALLEL" -S "$i" -f -T L -O "$TPCH_CSV_DIR" 2>/dev/null
            ./dbgen -s "$SCALE_FACTOR" -C "$PARALLEL" -S "$i" -f -T O -O "$TPCH_CSV_DIR" 2>/dev/null
        ) &
    done
    wait

    # Generate dimension tables (single chunk)
    (
        cd "$DBGEN_DIR"
        for table in c s n r P S; do
            ./dbgen -s "$SCALE_FACTOR" -f -T "$table" -O "$TPCH_CSV_DIR" 2>/dev/null
        done
    )

    echo "  CSV generation complete: ${TPCH_CSV_DIR}"

    # Convert to Parquet if requested
    if [[ "$FORMAT" == "parquet" ]]; then
        mkdir -p "$TPCH_PARQUET_DIR"
        echo "  Converting to Parquet..."
        python3 "${SCRIPT_DIR}/csv-to-parquet.py" \
            --benchmark tpch \
            --input-dir "$TPCH_CSV_DIR" \
            --output-dir "$TPCH_PARQUET_DIR" \
            --parallel "$PARALLEL"
        echo "  Parquet conversion complete: ${TPCH_PARQUET_DIR}"
    fi

    echo ">>> TPC-H SF=${SCALE_FACTOR} data generation complete."
    echo ""
}

# ─── TPC-DS Data Generation ──────────────────────────────────────────────────

generate_tpcds() {
    local TPCDS_DIR="${OUTPUT_DIR}/tpcds/sf${SCALE_FACTOR}"
    local TPCDS_CSV_DIR="${TPCDS_DIR}/csv"
    local TPCDS_PARQUET_DIR="${TPCDS_DIR}/parquet"

    echo ">>> Generating TPC-DS SF=${SCALE_FACTOR} data..."

    # Build dsdgen if needed
    local DSDGEN_BIN="${TPCDS_DSDGEN_DIR:-${BUILD_DIR}/tpcds-dsdgen}/tools/dsdgen"
    if [[ ! -x "$DSDGEN_BIN" ]] && [[ "$SKIP_BUILD" == "false" ]]; then
        echo "  Building TPC-DS dsdgen..."
        local TPCDS_SRC="${BUILD_DIR}/tpcds-dsdgen"
        if [[ ! -d "$TPCDS_SRC" ]]; then
            git clone --depth 1 https://github.com/gregrahn/tpcds-kit.git "$TPCDS_SRC"
        fi
        make -C "${TPCDS_SRC}/tools" -j"$(nproc)" OS=LINUX
        DSDGEN_BIN="${TPCDS_SRC}/tools/dsdgen"
    fi

    if [[ ! -x "$DSDGEN_BIN" ]]; then
        echo "ERROR: dsdgen binary not found at: $DSDGEN_BIN"
        echo "  Set TPCDS_DSDGEN_DIR or remove --skip-build"
        exit 2
    fi

    # Generate CSV data in parallel chunks
    mkdir -p "$TPCDS_CSV_DIR"
    echo "  Generating CSV data (${PARALLEL} chunks)..."
    local DSDGEN_DIR
    DSDGEN_DIR="$(dirname "$DSDGEN_BIN")"

    for i in $(seq 1 "$PARALLEL"); do
        (
            cd "$DSDGEN_DIR"
            ./dsdgen -SCALE "$SCALE_FACTOR" -PARALLEL "$PARALLEL" -CHILD "$i" \
                -DIR "$TPCDS_CSV_DIR" -FORCE -QUIET
        ) &
    done
    wait

    echo "  CSV generation complete: ${TPCDS_CSV_DIR}"

    # Convert to Parquet if requested
    if [[ "$FORMAT" == "parquet" ]]; then
        mkdir -p "$TPCDS_PARQUET_DIR"
        echo "  Converting to Parquet..."
        python3 "${SCRIPT_DIR}/csv-to-parquet.py" \
            --benchmark tpcds \
            --input-dir "$TPCDS_CSV_DIR" \
            --output-dir "$TPCDS_PARQUET_DIR" \
            --parallel "$PARALLEL"
        echo "  Parquet conversion complete: ${TPCDS_PARQUET_DIR}"
    fi

    echo ">>> TPC-DS SF=${SCALE_FACTOR} data generation complete."
    echo ""
}

# ─── Main ────────────────────────────────────────────────────────────────────

case "$BENCHMARK" in
    tpch)  generate_tpch ;;
    tpcds) generate_tpcds ;;
    both)  generate_tpch; generate_tpcds ;;
    *)     echo "ERROR: Unknown benchmark: $BENCHMARK (use: tpch, tpcds, both)"; exit 2 ;;
esac

echo "============================================"
echo "  Data generation complete!"
echo "  Output: ${OUTPUT_DIR}"
echo "============================================"
