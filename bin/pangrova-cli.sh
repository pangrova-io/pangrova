#!/usr/bin/env bash
# ==============================================================================
# pangrova-cli.sh — Pangrova interactive SQL CLI client
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

MAIN_CLASS="org.pangrova.client.cli.PangrovaCli"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"

# --- Help ---
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Pangrova interactive SQL CLI client.

Connection Options:
  --host <host>              Coordinator host (default: localhost)
  --port <port>              Coordinator port (default: 8080)
  --catalog <name>           Default catalog to use for queries
  --schema <name>            Default schema within the catalog
  --query-timeout <ms>       Query timeout in milliseconds

All options are passed through to PangrovaCli.

Environment Variables:
  PANGROVA_HOME              Project root directory (auto-detected)
  PANGROVA_CONF_DIR          Config directory (default: \$PANGROVA_HOME/etc)
  PANGROVA_COORDINATOR_URL   Coordinator URL (e.g. https://coordinator:8080); --host/--port
                           are extracted from it when not explicitly provided on the command line

Examples:
  $(basename "$0")
  $(basename "$0") --host coordinator.example.com --port 8080
  $(basename "$0") --host localhost --port 8080 --catalog my_iceberg
  $(basename "$0") --catalog my_iceberg --schema main --query-timeout 60000
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

# --- Build classpath ---
CLASSPATH=""
if [[ -d "$PANGROVA_HOME/lib" ]]; then
    for jar in "$PANGROVA_HOME"/lib/*.jar; do
        [[ -f "$jar" ]] && CLASSPATH="${CLASSPATH:+$CLASSPATH:}$jar"
    done
fi

if [[ -z "$CLASSPATH" ]]; then
    echo "WARN: No lib/ directory, using Maven target/ (dev mode)" >&2
    for module_dir in "$PANGROVA_HOME"/pangrova-*/target; do
        if [[ -d "$module_dir" ]]; then
            for jar in "$module_dir"/*.jar; do
                [[ -f "$jar" ]] && CLASSPATH="${CLASSPATH:+$CLASSPATH:}$jar"
            done
        fi
    done
fi

if [[ -z "$CLASSPATH" ]]; then
    echo "ERROR: No JARs found. Run 'mvn clean package -DskipTests' first." >&2
    exit 1
fi

# --- JVM options (lightweight for CLI) ---
JAVA_OPTS="-Xms256m -Xmx512m"

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

# --- Start CLI (foreground, transparent passthrough) ---
exec "$JAVA" $JAVA_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" "${URL_ARGS[@]+"${URL_ARGS[@]}"}" "$@"
