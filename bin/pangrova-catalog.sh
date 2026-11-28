#!/usr/bin/env bash
# ==============================================================================
# pangrova-catalog.sh — Pangrova Catalog management utility
#
# Usage:
#   bin/pangrova-catalog.sh list
#   bin/pangrova-catalog.sh show <catalog-name>
#   bin/pangrova-catalog.sh test <catalog-name> [--coordinator-url <url>] [--token-env <name>]
#   bin/pangrova-catalog.sh --help
#
# Subcommands:
#   list    — List all configured catalogs from $PANGROVA_CONF_DIR/catalog/
#   show    — Print the full YAML content of a catalog configuration
#   test    — List schemas in a registered catalog through the SQL CLI
#
# Environment:
#   PANGROVA_HOME        — Project root (auto-detected from script location)
#   PANGROVA_CONF_DIR    — Config directory (default: $PANGROVA_HOME/etc)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"
CATALOG_DIR="$PANGROVA_CONF_DIR/catalog"
DEFAULT_COORDINATOR_URL="http://localhost:8080"

usage() {
    cat <<EOF
Usage: $(basename "$0") <subcommand> [options...]

Pangrova Catalog management utility.

Subcommands:
  list                            List all configured catalogs
  show <catalog-name>             Print the full YAML content of a catalog
  test <catalog-name> [opts]      List schemas in a registered catalog

Options for 'test':
  --coordinator-url <url>         Coordinator base URL (default: ${DEFAULT_COORDINATOR_URL})
  --token <token>                 Bearer token for authenticated Coordinators
  --token-env <name>              Read Bearer token from an environment variable

Environment Variables:
  PANGROVA_HOME       Project root directory (auto-detected)
  PANGROVA_CONF_DIR   Config directory (default: \$PANGROVA_HOME/etc)

Catalog files are stored in: \$PANGROVA_CONF_DIR/catalog/*.yml

Examples:
  $(basename "$0") list
  $(basename "$0") show my-iceberg
  $(basename "$0") test my-iceberg
  $(basename "$0") test my-iceberg --coordinator-url https://coordinator:8080 --token-env PANGROVA_TOKEN
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
cmd_test() (
    if [[ $# -eq 0 ]]; then
        echo "ERROR: 'test' requires a catalog name." >&2
        echo "Usage: $(basename "$0") test <catalog-name> [--coordinator-url <url>] [--token-env <name>]" >&2
        exit 1
    fi

    local catalog_name="$1"
    shift

    local coordinator_url="$DEFAULT_COORDINATOR_URL"
    local auth_args=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --coordinator-url)
                [[ $# -ge 2 ]] || { echo "ERROR: $1 requires a value." >&2; exit 1; }
                coordinator_url="$2"
                ;;
            --token|--token-env)
                [[ $# -ge 2 ]] || { echo "ERROR: $1 requires a value." >&2; exit 1; }
                auth_args+=("$1" "$2")
                ;;
            *)
                echo "ERROR: Unknown option: $1" >&2
                exit 1
                ;;
        esac
        shift 2
    done

    local url_pattern='^(https?)://(\[[0-9a-fA-F:]+\]|[^:/?#[:space:]@]+)(:([0-9]+))?/?$'
    if [[ ! "$coordinator_url" =~ $url_pattern ]]; then
        echo "ERROR: Expected an HTTP or HTTPS Coordinator base URL." >&2
        exit 1
    fi
    local host="${BASH_REMATCH[2]}" port="${BASH_REMATCH[4]}" tls=false
    if [[ "${BASH_REMATCH[1]}" == https ]]; then
        tls=true
        port="${port:-443}"
    fi
    port="${port:-80}"

    local query_file
    query_file="$(mktemp "${TMPDIR:-/tmp}/pangrova-catalog-test.XXXXXXXX")"
    trap 'rm -f -- "$query_file"' EXIT
    printf 'SHOW SCHEMAS IN "%s"\n' "${catalog_name//\"/\"\"}" > "$query_file"

    if ! "$SCRIPT_DIR/pangrova-cli.sh" --host "$host" --port "$port" --ssl "$tls" \
            --query-timeout 30s --file "$query_file" ${auth_args[@]+"${auth_args[@]}"}; then
        echo "FAILED: Catalog '$catalog_name' connection test failed." >&2
        exit 1
    fi
    echo "SUCCESS: Catalog '$catalog_name' connection test passed."
)

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
