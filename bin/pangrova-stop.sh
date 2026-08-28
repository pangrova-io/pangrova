#!/usr/bin/env bash
# ==============================================================================
# pangrova-stop.sh — Gracefully stop Pangrova processes
#
# Usage:
#   bin/pangrova-stop.sh --coordinator
#   bin/pangrova-stop.sh --worker
#   bin/pangrova-stop.sh --standalone
#   bin/pangrova-stop.sh --all
#   bin/pangrova-stop.sh --help
#
# Reads PID files from $PANGROVA_LOG_DIR, sends SIGTERM, waits up to 30s
# for graceful shutdown, then sends SIGKILL if the process is still alive.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"
# shellcheck source=/dev/null
[[ -f "$PANGROVA_CONF_DIR/pangrova-env.sh" ]] && source "$PANGROVA_CONF_DIR/pangrova-env.sh"
PANGROVA_LOG_DIR="${PANGROVA_LOG_DIR:-$PANGROVA_HOME/logs}"
GRACEFUL_TIMEOUT=30

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Gracefully stop Pangrova processes.

Options:
  --coordinator   Stop the Coordinator process
  --worker        Stop the Worker process
  --standalone    Stop the Standalone process
  --all           Stop all Pangrova processes (coordinator, worker, standalone)
  --help          Show this help message
EOF
    exit 0
}

stop_process() {
    local role="$1"
    local pid_file="$PANGROVA_LOG_DIR/${role}.pid"

    if [[ ! -f "$pid_file" ]]; then
        echo "WARN: No PID file for ${role} at ${pid_file}, skipping." >&2
        return 0
    fi

    local pid
    pid=$(cat "$pid_file")

    if ! kill -0 "$pid" 2>/dev/null; then
        echo "WARN: ${role} process (PID ${pid}) not running, cleaning up PID file."
        rm -f "$pid_file"
        return 0
    fi

    echo "Stopping Pangrova ${role} (PID ${pid})..."
    kill -TERM "$pid"

    local elapsed=0
    while kill -0 "$pid" 2>/dev/null; do
        if [[ $elapsed -ge $GRACEFUL_TIMEOUT ]]; then
            echo "WARN: ${role} (PID ${pid}) did not stop within ${GRACEFUL_TIMEOUT}s, sending SIGKILL."
            kill -KILL "$pid" 2>/dev/null || true
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    rm -f "$pid_file"
    echo "Stopped Pangrova ${role} (PID ${pid})."
}

if [[ $# -eq 0 ]]; then
    echo "ERROR: No option specified. Use --coordinator, --worker, --standalone, or --all." >&2
    exit 1
fi

ROLES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)        usage ;;
        --coordinator) ROLES+=("coordinator"); shift ;;
        --worker)      ROLES+=("worker"); shift ;;
        --standalone)  ROLES+=("standalone"); shift ;;
        --all)         ROLES=("coordinator" "worker" "standalone"); shift ;;
        *)             echo "ERROR: Unknown option: $1" >&2; exit 1 ;;
    esac
done

for role in "${ROLES[@]}"; do
    stop_process "$role"
done
