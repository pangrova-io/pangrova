#!/usr/bin/env bash
# ==============================================================================
# pangrova-catalog.sh — Pangrova Catalog management utility
#
# Usage:
#   bin/pangrova-catalog.sh list
#   bin/pangrova-catalog.sh show <catalog-name>
#   bin/pangrova-catalog.sh test <catalog-name> [--coordinator-url <url>] [--token <tok>]
#   bin/pangrova-catalog.sh --help
#
# Subcommands:
#   list    — List all configured catalogs from $PANGROVA_CONF_DIR/catalog/
#   show    — Print the full YAML content of a catalog configuration
#   test    — Test a catalog connection via Coordinator API
#             (requires curl; API endpoint not yet implemented)
#
# Environment:
#   PANGROVA_HOME        — Project root (auto-detected from script location)
#   PANGROVA_CONF_DIR    — Config directory (default: $PANGROVA_HOME/docs/develop/configuration)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/docs/develop/configuration}"
CATALOG_DIR="$PANGROVA_CONF_DIR/catalog"
DEFAULT_COORDINATOR_URL="http://localhost:8080"

usage() {
    cat <<EOF
Usage: $(basename "$0") <subcommand> [options...]

Pangrova Catalog management utility.

Subcommands:
  list                            List all configured catalogs
  show <catalog-name>             Print the full YAML content of a catalog
  test <catalog-name> [opts]      Test a catalog connection via Coordinator API

Options for 'test':
  --coordinator-url <url>         Coordinator base URL (default: ${DEFAULT_COORDINATOR_URL})
  --token <token>                 Bearer token for authenticated Coordinators
  --insecure                      Skip TLS certificate verification

Environment Variables:
  PANGROVA_HOME       Project root directory (auto-detected)
  PANGROVA_CONF_DIR   Config directory (default: \$PANGROVA_HOME/docs/develop/configuration)

Catalog files are stored in: \$PANGROVA_CONF_DIR/catalog/*.yml

NOTE:
  The 'test' subcommand depends on POST /v1/catalogs/{name}/test,
  which is not yet implemented in the Coordinator.

Examples:
  $(basename "$0") list
  $(basename "$0") show my-iceberg
  $(basename "$0") test my-iceberg
  $(basename "$0") test my-iceberg --coordinator-url https://coordinator:8080 --token \$PANGROVA_TOKEN
EOF
    exit 0
}

# --- list subcommand ---
cmd_list() {
    if [[ ! -d "$CATALOG_DIR" ]]; then
        echo "No catalog directory found at ${CATALOG_DIR}." >&2
        echo "Create catalog YAML files in ${CATALOG_DIR}/ to configure catalogs." >&2
        exit 1
    fi

    local found=false
    local catalogs=()
    for yml_file in "$CATALOG_DIR"/*.yml; do
        [[ -f "$yml_file" ]] || continue
        found=true
        catalogs+=("$yml_file")
    done

    if [[ "$found" == "false" ]]; then
        echo "No catalog configurations found in ${CATALOG_DIR}/."
        echo "Add .yml files to configure catalogs."
        exit 0
    fi

    printf "%-30s %-20s %s\n" "CATALOG" "CONNECTOR" "FILE"
    printf "%-30s %-20s %s\n" "-------" "---------" "----"

    for yml_file in "${catalogs[@]}"; do
        local name
        name="$(basename "$yml_file" .yml)"

        local connector="-"
        if grep -q "^connector:" "$yml_file" 2>/dev/null; then
            connector=$(grep "^connector:" "$yml_file" | head -1 | sed 's/^connector:[[:space:]]*//' | sed 's/[[:space:]]*$//')
        fi

        printf "%-30s %-20s %s\n" "$name" "$connector" "$yml_file"
    done
}

# --- show subcommand ---
cmd_show() {
    if [[ $# -eq 0 ]]; then
        echo "ERROR: 'show' requires a catalog name." >&2
        echo "Usage: $(basename "$0") show <catalog-name>" >&2
        exit 1
    fi

    local catalog_name="$1"
    local catalog_file="$CATALOG_DIR/${catalog_name}.yml"

    if [[ ! -f "$catalog_file" ]]; then
        echo "ERROR: Catalog configuration not found: ${catalog_file}" >&2
        echo "Available catalogs:" >&2
        cmd_list >&2 || true
        exit 1
    fi

    echo "# Catalog: ${catalog_name}"
    echo "# File: ${catalog_file}"
    echo ""
    cat "$catalog_file"
}

# --- test subcommand ---
cmd_test() {
    if [[ $# -eq 0 ]]; then
        echo "ERROR: 'test' requires a catalog name." >&2
        echo "Usage: $(basename "$0") test <catalog-name> [--coordinator-url <url>] [--token <tok>]" >&2
        exit 1
    fi

    local catalog_name="$1"
    shift

    local coordinator_url="$DEFAULT_COORDINATOR_URL"
    local token=""
    local insecure=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --coordinator-url)
                coordinator_url="$2"; shift 2
                ;;
            --token)
                token="$2"; shift 2
                ;;
            --insecure)
                insecure=true; shift
                ;;
            *)
                echo "ERROR: Unknown option: $1" >&2
                exit 1
                ;;
        esac
    done

    local catalog_file="$CATALOG_DIR/${catalog_name}.yml"
    if [[ ! -f "$catalog_file" ]]; then
        echo "ERROR: Catalog configuration not found: ${catalog_file}" >&2
        exit 1
    fi

    if ! command -v curl &>/dev/null; then
        echo "ERROR: curl is required but not found in PATH." >&2
        exit 1
    fi

    local curl_args=(-s -w "\n%{http_code}" --connect-timeout 10 --max-time 30
                     -X POST -H "Content-Type: application/yaml" -d @"$catalog_file")
    [[ "$insecure" == "true" ]] && curl_args+=(-k)
    [[ -n "$token" ]] && curl_args+=(-H "Authorization: Bearer ${token}")

    local url="${coordinator_url}/v1/catalogs/${catalog_name}/test"
    echo "Testing catalog '${catalog_name}' via ${url} ..."

    local response
    response=$(curl "${curl_args[@]}" "$url" 2>/dev/null) || {
        echo "FAILED: Unable to connect to Coordinator at ${coordinator_url}." >&2
        exit 1
    }

    local http_code
    http_code=$(echo "$response" | tail -1)
    local body
    body=$(echo "$response" | sed '$d')

    if [[ "$http_code" == "200" ]]; then
        echo "SUCCESS: Catalog '${catalog_name}' connection test passed."
        [[ -n "$body" ]] && echo "$body"
    else
        echo "FAILED: Catalog '${catalog_name}' connection test failed (HTTP ${http_code})." >&2
        [[ -n "$body" ]] && echo "$body" >&2
        exit 1
    fi
}

# --- Main ---
if [[ $# -eq 0 ]]; then
    echo "ERROR: No subcommand specified." >&2
    echo "Usage: $(basename "$0") {list|show|test} [options...]" >&2
    exit 1
fi

SUBCOMMAND="$1"
shift

case "$SUBCOMMAND" in
    list)
        cmd_list
        ;;
    show)
        cmd_show "$@"
        ;;
    test)
        cmd_test "$@"
        ;;
    --help)
        usage
        ;;
    *)
        echo "ERROR: Unknown subcommand '${SUBCOMMAND}'." >&2
        echo "Usage: $(basename "$0") {list|show|test} [options...]" >&2
        exit 1
        ;;
esac
