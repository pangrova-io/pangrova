#!/usr/bin/env bash
# ==============================================================================
# pangrova-worker.sh — Start the Pangrova Worker process
#
# Usage:
#   bin/pangrova-worker.sh [--config <path>] [--daemon] [--help]
#
# Environment:
#   PANGROVA_HOME        — Project root (auto-detected from script location)
#   PANGROVA_CONF_DIR    — Config directory (default: $PANGROVA_HOME/etc)
#   PANGROVA_LOG_DIR     — Log directory (default: $PANGROVA_HOME/logs)
#   JAVA_HOME          — JDK 17+ path
#   PANGROVA_JAVA_OPTS   — JVM options override (highest priority)
#
# JVM options priority (highest → lowest):
#   1. PANGROVA_JAVA_OPTS environment variable
#   2. $PANGROVA_CONF_DIR/jvm-worker.config file (one option per line)
#   3. Built-in defaults (includes -XX:MaxDirectMemorySize=4g for Arrow off-heap)
# ==============================================================================
set -euo pipefail

ROLE="worker"
MAIN_CLASS="org.pangrova.worker.bootstrap.PangrovaWorkerMain"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"
PANGROVA_LOG_DIR="${PANGROVA_LOG_DIR:-$PANGROVA_HOME/logs}"

# --- Help ---
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Start the Pangrova Worker process.

Options:
  --config <path>   Path to YAML config file (default: \$PANGROVA_CONF_DIR/worker.yml)
  --daemon          Run in background, write PID to \$PANGROVA_LOG_DIR/worker.pid
  --help            Show this help message

Environment Variables:
  PANGROVA_HOME       Project root directory (auto-detected)
  PANGROVA_CONF_DIR   Config directory (default: \$PANGROVA_HOME/etc)
  PANGROVA_LOG_DIR    Log directory (default: \$PANGROVA_HOME/logs)
  JAVA_HOME         JDK installation path
  PANGROVA_JAVA_OPTS  JVM options (overrides jvm-worker.config)
EOF
    exit 0
}

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

# --- Load JVM options (three-level priority) ---
DEFAULT_JAVA_OPTS="-Xms2g -Xmx16g -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+ExitOnOutOfMemoryError -XX:MaxDirectMemorySize=4g -Djava.net.preferIPv4Stack=true"
JVM_CONFIG_FILE="$PANGROVA_CONF_DIR/jvm-${ROLE}.config"

if [[ -n "${PANGROVA_JAVA_OPTS:-}" ]]; then
    JAVA_OPTS="$PANGROVA_JAVA_OPTS"
elif [[ -f "$JVM_CONFIG_FILE" ]]; then
    JAVA_OPTS=""
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%%#*}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -n "$line" ]] && JAVA_OPTS="${JAVA_OPTS:+$JAVA_OPTS }$line"
    done < <(sed \
        -e "s|\${PANGROVA_LOG_DIR:-logs}|${PANGROVA_LOG_DIR}|g" \
        -e "s|\${PANGROVA_LOG_DIR}|${PANGROVA_LOG_DIR}|g" \
        "$JVM_CONFIG_FILE")
else
    JAVA_OPTS="$DEFAULT_JAVA_OPTS"
fi

# Arrow's direct-buffer view and Netty's native channel access are required
# even when a deployment overrides memory sizing through PANGROVA_JAVA_OPTS.
JAVA_OPTS="${JAVA_OPTS} --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# logback.xml uses ${PANGROVA_ROLE:-pangrova} and ${PANGROVA_LOG_DIR:-logs} as Logback
# property substitutions — they are resolved only when passed as Java system properties.
JAVA_OPTS="${JAVA_OPTS} -DPANGROVA_LOG_DIR=${PANGROVA_LOG_DIR} -DPANGROVA_ROLE=${ROLE}"
if [[ -f "${PANGROVA_CONF_DIR}/logback.xml" ]]; then
    JAVA_OPTS="-Dlogback.configurationFile=${PANGROVA_CONF_DIR}/logback.xml ${JAVA_OPTS}"
fi

# --- Parse arguments ---
CONFIG_FILE="${PANGROVA_CONF_DIR}/worker.yml"
CONFIG_ARGS=""
[[ -f "$CONFIG_FILE" ]] && CONFIG_ARGS="--config $CONFIG_FILE"

DAEMON=false
EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)   usage ;;
        --daemon) DAEMON=true; shift ;;
        --config) CONFIG_ARGS="--config $2"; shift 2 ;;
        *)        EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# --- Pre-flight checks ---
mkdir -p "$PANGROVA_LOG_DIR"

PID_FILE="$PANGROVA_LOG_DIR/${ROLE}.pid"
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Worker already running (PID $OLD_PID). Stop it first or remove $PID_FILE." >&2
        exit 1
    fi
    rm -f "$PID_FILE"
fi

# --- Start ---
echo "Starting Pangrova Worker..."
echo "  PANGROVA_HOME:     $PANGROVA_HOME"
echo "  PANGROVA_CONF_DIR: $PANGROVA_CONF_DIR"
echo "  PANGROVA_LOG_DIR:  $PANGROVA_LOG_DIR"
echo "  JAVA:            $JAVA (version $JAVA_VERSION)"
echo "  JVM_CONFIG:      ${JVM_CONFIG_FILE}$([ -f "$JVM_CONFIG_FILE" ] && echo ' (loaded)' || echo ' (not found, using defaults)')"

if [[ "$DAEMON" == "true" ]]; then
    nohup "$JAVA" $JAVA_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" $CONFIG_ARGS "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}" \
        > "$PANGROVA_LOG_DIR/${ROLE}.out" 2>&1 &
    echo $! > "$PID_FILE"
    echo "  PID:             $(cat "$PID_FILE") (daemon)"
    echo "  Log:             $PANGROVA_LOG_DIR/${ROLE}.out"
else
    exec "$JAVA" $JAVA_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" $CONFIG_ARGS "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"
fi
