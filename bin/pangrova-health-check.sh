#!/usr/bin/env bash
# ==============================================================================
# pangrova-health-check.sh — Check the health of an Pangrova node
#
# Usage:
#   bin/pangrova-health-check.sh [OPTIONS] <target>
#
# Target formats:
#   host:port              (scheme defaults to http)
#   https://host:port      (scheme inferred from prefix)
#   http://host:port       (scheme inferred from prefix)
#
# Output:
#   healthy     — HTTP 200 from /v1/health
#   unhealthy   — HTTP non-200 response
#   unreachable — Connection failed or timed out
#
# Exit codes:
#   0 = healthy
#   1 = unhealthy
#   2 = unreachable
# ==============================================================================
set -euo pipefail

HEALTH_ENDPOINT="/v1/health"
DEFAULT_TIMEOUT=5
DEFAULT_SCHEME="http"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] <target>

Check the health of an Pangrova node by querying its health endpoint.

Arguments:
  target        Node address: host:port, https://host:port, or http://host:port

Options:
  --scheme <http|https>  Protocol scheme (default: ${DEFAULT_SCHEME}; inferred from target prefix)
  --insecure             Skip TLS certificate verification (for self-signed certs)
  --verbose              Print the full JSON health response body
  --timeout <seconds>    Connection and read timeout in seconds (default: ${DEFAULT_TIMEOUT})
  --help                 Show this help message

Output:
  healthy       HTTP 200 from ${HEALTH_ENDPOINT}
  unhealthy     HTTP non-200 response
  unreachable   Connection failed or timed out

Exit Codes:
  0 = healthy
  1 = unhealthy
  2 = unreachable

Examples:
  $(basename "$0") localhost:8080
  $(basename "$0") --scheme https coordinator.example.com:8080
  $(basename "$0") https://coordinator.example.com:8080
  $(basename "$0") --insecure --verbose https://coordinator:8080
  $(basename "$0") --timeout 10 coordinator:8080
EOF
    exit 0
}

# --- Parse arguments ---
SCHEME="$DEFAULT_SCHEME"
INSECURE=false
VERBOSE=false
TIMEOUT="$DEFAULT_TIMEOUT"
TARGET=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            usage
            ;;
        --scheme)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --scheme requires a value (http or https)." >&2; exit 2
            fi
            SCHEME="$2"; shift 2
            ;;
        --insecure)
            INSECURE=true; shift
            ;;
        --verbose)
            VERBOSE=true; shift
            ;;
        --timeout)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --timeout requires a value in seconds." >&2; exit 2
            fi
            TIMEOUT="$2"; shift 2
            ;;
        -*)
            echo "ERROR: Unknown option: $1" >&2
            echo "Usage: $(basename "$0") [OPTIONS] <target>" >&2
            exit 2
            ;;
        *)
            if [[ -n "$TARGET" ]]; then
                echo "ERROR: Multiple targets specified. Provide exactly one target." >&2; exit 2
            fi
            TARGET="$1"; shift
            ;;
    esac
done

if [[ -z "$TARGET" ]]; then
    echo "ERROR: Missing required argument <target>." >&2
    echo "Usage: $(basename "$0") [OPTIONS] <target>" >&2
    exit 2
fi

# Infer scheme from URL prefix if present (https://host:port or http://host:port)
if [[ "$TARGET" =~ ^https?:// ]]; then
    SCHEME="${TARGET%%://*}"
    TARGET="${TARGET#*://}"
fi

URL="${SCHEME}://${TARGET}${HEALTH_ENDPOINT}"

# --- Check curl availability ---
if ! command -v curl &>/dev/null; then
    echo "ERROR: curl is required but not found in PATH." >&2
    exit 2
fi

# --- Build curl flags ---
CURL_FLAGS=(-s --connect-timeout "$TIMEOUT" --max-time "$TIMEOUT")
[[ "$INSECURE" == "true" ]] && CURL_FLAGS+=(-k)

# --- Perform health check ---
if [[ "$VERBOSE" == "true" ]]; then
    RESPONSE=$(curl "${CURL_FLAGS[@]}" -w "\n%{http_code}" "$URL" 2>/dev/null) || {
        echo "unreachable"
        exit 2
    }
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | sed '$d')
else
    HTTP_CODE=$(curl "${CURL_FLAGS[@]}" -o /dev/null -w "%{http_code}" "$URL" 2>/dev/null) || {
        echo "unreachable"
        exit 2
    }
    BODY=""
fi

if [[ "$HTTP_CODE" == "200" ]]; then
    echo "healthy"
    [[ "$VERBOSE" == "true" && -n "$BODY" ]] && echo "$BODY"
    exit 0
else
    echo "unhealthy (HTTP $HTTP_CODE)"
    [[ "$VERBOSE" == "true" && -n "$BODY" ]] && echo "$BODY" >&2
    exit 1
fi
