#!/usr/bin/env bash
# ==============================================================================
# pangrova-cluster-status.sh — Display Pangrova cluster status
#
# Usage:
#   bin/pangrova-cluster-status.sh [--coordinator-url <url>] [--token <token>]
#                                [--insecure] [--help]
#
# Queries the Coordinator's cluster status API (GET /v1/cluster/status) and
# displays node roles, states, versions, and resource usage.
# Falls back to local PID file inspection when the Coordinator is unreachable.
#
# NOTE: The cluster status API (GET /v1/cluster/status) is not yet implemented.
#       This script is ready and will work once the API is available.
# ==============================================================================
set -euo pipefail

CLUSTER_STATUS_ENDPOINT="/v1/cluster/status"
TIMEOUT=10
DEFAULT_COORDINATOR_URL="http://localhost:8080"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"
# shellcheck source=/dev/null
[[ -f "$PANGROVA_CONF_DIR/pangrova-env.sh" ]] && source "$PANGROVA_CONF_DIR/pangrova-env.sh"
PANGROVA_LOG_DIR="${PANGROVA_LOG_DIR:-$PANGROVA_HOME/logs}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Display the status of all nodes in the Pangrova cluster.

Options:
  --coordinator-url <url>   Coordinator base URL (default: ${DEFAULT_COORDINATOR_URL})
  --token <token>           Bearer token for authenticated Coordinators
  --insecure                Skip TLS certificate verification (for self-signed certs)
  --help                    Show this help message

Output:
  Table of all cluster nodes with role, state, version, host, CPU, and memory usage.
  When the Coordinator API is unreachable, falls back to local PID file inspection.

NOTE:
  This script depends on GET /v1/cluster/status, which is not yet implemented.

Examples:
  $(basename "$0")
  $(basename "$0") --coordinator-url https://coordinator.example.com:8080
  $(basename "$0") --coordinator-url https://coordinator:8080 --token \$PANGROVA_TOKEN
  $(basename "$0") --insecure --coordinator-url https://coordinator:8080
EOF
    exit 0
}

# --- Parse arguments ---
COORDINATOR_URL="$DEFAULT_COORDINATOR_URL"
TOKEN=""
INSECURE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            usage
            ;;
        --coordinator-url)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --coordinator-url requires a value." >&2; exit 1
            fi
            COORDINATOR_URL="$2"; shift 2
            ;;
        --token)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --token requires a value." >&2; exit 1
            fi
            TOKEN="$2"; shift 2
            ;;
        --insecure)
            INSECURE=true; shift
            ;;
        *)
            echo "ERROR: Unknown option: $1" >&2
            echo "Usage: $(basename "$0") [--coordinator-url <url>] [--token <token>] [--insecure]" >&2
            exit 1
            ;;
    esac
done

# --- Check curl availability ---
if ! command -v curl &>/dev/null; then
    echo "ERROR: curl is required but not found in PATH." >&2
    exit 1
fi

# --- Build curl flags ---
CURL_FLAGS=(-s --connect-timeout "$TIMEOUT" --max-time "$TIMEOUT")
[[ "$INSECURE" == "true" ]] && CURL_FLAGS+=(-k)
[[ -n "$TOKEN" ]] && CURL_FLAGS+=(-H "Authorization: Bearer ${TOKEN}")

# --- Local PID fallback function ---
show_local_status() {
    echo ""
    echo "=== Local Process Status (Coordinator API unreachable) ==="
    local found=false
    for role in coordinator worker standalone; do
        local pid_file="$PANGROVA_LOG_DIR/${role}.pid"
        if [[ -f "$pid_file" ]]; then
            found=true
            local pid
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  ${role}: RUNNING (PID ${pid})"
            else
                echo "  ${role}: DEAD (stale PID file: ${pid_file})"
            fi
        fi
    done
    if [[ "$found" == "false" ]]; then
        echo "  No PID files found in ${PANGROVA_LOG_DIR}/ — no local Pangrova processes tracked."
    fi
}

# --- Format JSON output ---
format_status_json() {
    local body="$1"

    if command -v jq &>/dev/null; then
        echo ""
        echo "$body" | jq -r '
            (["NODE ID", "ROLE", "STATE", "VERSION", "HOST", "CPU%", "MEM"] | @tsv),
            ("--------\t------\t------\t-------\t----\t----\t---"),
            (.nodes[]? |
                [.id // "-", .role // "-", .state // "-", .version // "-",
                 .host // "-",
                 (if .cpu then (.cpu | tostring) + "%" else "-" end),
                 .memory // "-"] | @tsv)
        ' 2>/dev/null | column -t -s $'\t' || {
            echo "Cluster Status (raw JSON):"
            echo "$body"
        }
    elif command -v python3 &>/dev/null; then
        echo ""
        echo "$body" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    nodes = data.get('nodes', [])
    fmt = '{:<12} {:<12} {:<10} {:<12} {:<25} {:<6} {}'
    print(fmt.format('NODE ID', 'ROLE', 'STATE', 'VERSION', 'HOST', 'CPU%', 'MEM'))
    print('-' * 90)
    for n in nodes:
        print(fmt.format(
            n.get('id', '-')[:11], n.get('role', '-')[:11], n.get('state', '-')[:9],
            n.get('version', '-')[:11], n.get('host', '-')[:24],
            str(n.get('cpu', '-')), str(n.get('memory', '-'))
        ))
    print('')
    print('Total nodes: {}'.format(len(nodes)))
except Exception as e:
    print('Failed to parse JSON:', e, file=sys.stderr)
    print(sys.stdin.read())
" 2>/dev/null || {
            echo "Cluster Status (raw JSON):"
            echo "$body"
        }
    else
        echo ""
        echo "Cluster Status (install jq or python3 for formatted output):"
        echo "$body"
    fi
}

# --- Query cluster status ---
URL="${COORDINATOR_URL}${CLUSTER_STATUS_ENDPOINT}"
echo "Querying cluster status from ${URL} ..."

RESPONSE=$(curl "${CURL_FLAGS[@]}" -w "\n%{http_code}" "$URL" 2>/dev/null) || {
    echo "ERROR: Unable to connect to Coordinator at ${COORDINATOR_URL}." >&2
    show_local_status
    exit 1
}

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [[ "$HTTP_CODE" != "200" ]]; then
    echo "ERROR: Coordinator returned HTTP ${HTTP_CODE}." >&2
    if [[ -n "$BODY" ]]; then
        echo "$BODY" >&2
    fi
    show_local_status
    exit 1
fi

format_status_json "$BODY"
