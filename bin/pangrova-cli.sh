#!/usr/bin/env bash
# ==============================================================================
# pangrova-cli.sh — Pangrova interactive and batch SQL CLI client
#
# Usage:
#   bin/pangrova-cli.sh [--host <host>] [--port <port>] [--catalog <name>] [options...]
#   bin/pangrova-cli.sh --help
#
# Environment:
#   PANGROVA_HOME              — Project root (auto-detected from script location)
#   PANGROVA_CONF_DIR          — Config directory (default: $PANGROVA_HOME/etc)
#   PANGROVA_COORDINATOR_URL   — Coordinator URL, e.g. http://host:8080 (parsed into --host/--port
#                              if --host and --port are not explicitly provided)
#   JAVA_HOME                — JDK 17+ path
#
# All arguments are transparently passed through to PangrovaCli.
# Default connection values (host=localhost, port=8080) are handled internally.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"

# --- Help ---
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Pangrova interactive and batch SQL CLI client.

Connection Options:
  --host <host>              Coordinator host (default: localhost)
  --port <port>              Coordinator port (default: 8080)
  --catalog <name>           Default catalog to use for queries
  --schema <name>            Default schema within the catalog
  --query-timeout <duration> Query timeout, e.g. 30s, 5m or bare seconds
  --token-env <name>       Read Bearer token from an existing environment variable
  --file <path>             Execute one UTF-8 SQL statement; failures exit nonzero
  --output <path>           New batch result output file (default: stdout)
  --execution-receipt <path> New JSON receipt after FINISHED and result consumption

All options are passed through to PangrovaCli.

Environment Variables:
  PANGROVA_HOME              Project root directory (auto-detected)
  PANGROVA_CONF_DIR          Config directory (default: \$PANGROVA_HOME/etc)
  PANGROVA_COORDINATOR_URL   Coordinator URL (e.g. https://coordinator:8080); --host/--port
                           are extracted from it when not explicitly provided on the command line
  NO_COLOR                   Disable ANSI color in interactive output
  COLUMNS                    Optional terminal width override; below 80 uses the compact wordmark

Examples:
  $(basename "$0")
  $(basename "$0") --host coordinator.example.com --port 8080
  $(basename "$0") --host localhost --port 8080 --catalog my_iceberg
  $(basename "$0") --catalog my_iceberg --schema main --query-timeout 30m
  $(basename "$0") --file q01.sql --output results.txt --execution-receipt execution.json
  PANGROVA_COORDINATOR_URL=http://coordinator:8080 $(basename "$0") --catalog my_iceberg
EOF
    exit 0
}

# Check for --help before Java setup (fast path)
for arg in "$@"; do
    if [[ "$arg" == "--help" ]]; then
        usage
    fi
done

# --- Source environment overrides ---
# shellcheck source=/dev/null
[[ -f "$PANGROVA_CONF_DIR/pangrova-env.sh" ]] && source "$PANGROVA_CONF_DIR/pangrova-env.sh"

# --- Resolve Java ---
if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="$(command -v java 2>/dev/null || true)"
fi

if [[ -z "$JAVA" || ! -x "$JAVA" ]]; then
    echo "ERROR: Java not found. Set JAVA_HOME or add java to PATH." >&2
    exit 1
fi

JAVA_VERSION=$("$JAVA" -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/')
if [[ "$JAVA_VERSION" -lt 17 ]]; then
    echo "ERROR: Java 17+ required, found Java $JAVA_VERSION" >&2
    exit 1
fi

# --- Resolve the isolated executable; never mix embedded dependencies into lib/*.jar. ---
CLI_DIRECTORY="$PANGROVA_HOME/cli"
if [[ ! -d "$CLI_DIRECTORY" ]]; then
    CLI_DIRECTORY="$PANGROVA_HOME/pangrova-client/pangrova-client-cli/target"
fi
CLI_JARS=()
for jar in "$CLI_DIRECTORY"/pangrova-client-cli-*-exec.jar; do
    [[ -f "$jar" ]] && CLI_JARS+=("$jar")
done
if [[ "${#CLI_JARS[@]}" -ne 1 ]]; then
    echo "ERROR: Expected one CLI executable in $CLI_DIRECTORY; found ${#CLI_JARS[@]}. Build the CLI package." >&2
    exit 1
fi

# --- JVM options (lightweight for CLI) ---
JAVA_OPTS="-Xms256m -Xmx512m --add-opens=java.base/java.nio=ALL-UNNAMED"

# --- PANGROVA_COORDINATOR_URL: extract host and port if not in args ---
URL_ARGS=()
if [[ -n "${PANGROVA_COORDINATOR_URL:-}" ]]; then
    HAS_HOST=false
    HAS_PORT=false
    for arg in "$@"; do
        [[ "$arg" == "--host" ]] && HAS_HOST=true
        [[ "$arg" == "--port" ]] && HAS_PORT=true
    done

    if [[ "$HAS_HOST" == "false" || "$HAS_PORT" == "false" ]]; then
        # Strip scheme: http:// or https://
        URL_WITHOUT_SCHEME="${PANGROVA_COORDINATOR_URL#*://}"
        # Strip any path suffix
        URL_HOST_PORT="${URL_WITHOUT_SCHEME%%/*}"
        URL_HOST="${URL_HOST_PORT%%:*}"
        URL_PORT="${URL_HOST_PORT##*:}"
        # If no port separator found, port equals host (i.e., no port in URL)
        [[ "$URL_PORT" == "$URL_HOST" ]] && URL_PORT=""

        [[ "$HAS_HOST" == "false" && -n "$URL_HOST" ]] && URL_ARGS+=("--host" "$URL_HOST")
        [[ "$HAS_PORT" == "false" && -n "$URL_PORT" ]] && URL_ARGS+=("--port" "$URL_PORT")
    fi
fi

# The launcher can inspect the real file descriptors regardless of JDK console implementation.
CLI_TERMINAL=false
CLI_WIDTH="${COLUMNS:-}"
if [[ -t 0 && -t 1 ]]; then
    CLI_TERMINAL=true
    if [[ ! "$CLI_WIDTH" =~ ^[1-9][0-9]*$ ]]; then
        # stty size is supported by both Linux and macOS; do not consume query input.
        TERMINAL_SIZE="$(stty size 2>/dev/null || true)"
        CLI_WIDTH="${TERMINAL_SIZE##* }"
    fi
fi
if [[ ! "$CLI_WIDTH" =~ ^[1-9][0-9]*$ ]]; then
    CLI_WIDTH=60
fi

# --- Start CLI (foreground, transparent passthrough) ---
exec "$JAVA" $JAVA_OPTS "-Dpangrova.cli.terminal=$CLI_TERMINAL" "-Dpangrova.cli.columns=$CLI_WIDTH" -jar "${CLI_JARS[0]}" "${URL_ARGS[@]+"${URL_ARGS[@]}"}" "$@"
