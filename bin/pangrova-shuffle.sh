#!/usr/bin/env bash
# ==============================================================================
# pangrova-shuffle.sh — Start the Pangrova durable shuffle service
#
# Usage:
#   bin/pangrova-shuffle.sh [--config <path>] [--daemon] [--help]
#
# Environment:
#   PANGROVA_HOME       — Project root (auto-detected from script location)
#   PANGROVA_CONF_DIR   — Config directory (default: $PANGROVA_HOME/etc)
#   PANGROVA_LOG_DIR    — Log directory (default: $PANGROVA_HOME/logs)
#   JAVA_HOME           — JDK 17+ path
#   PANGROVA_JAVA_OPTS  — JVM options override (highest priority)
# ==============================================================================
set -euo pipefail

ROLE="shuffle"
MAIN_CLASS="org.pangrova.shuffle.deployment.PangrovaShuffleServer"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
PANGROVA_CONF_DIR="${PANGROVA_CONF_DIR:-$PANGROVA_HOME/etc}"
PANGROVA_LOG_DIR="${PANGROVA_LOG_DIR:-$PANGROVA_HOME/logs}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Start the Pangrova durable shuffle service.

Options:
  --config <path>   Path to YAML config file (default: \$PANGROVA_CONF_DIR/shuffle.yml)
  --daemon          Run in background, write PID to \$PANGROVA_LOG_DIR/shuffle.pid
  --help            Show this help message

Environment Variables:
  PANGROVA_HOME       Project root directory (auto-detected)
  PANGROVA_CONF_DIR   Config directory (default: \$PANGROVA_HOME/etc)
  PANGROVA_LOG_DIR    Log directory (default: \$PANGROVA_HOME/logs)
  JAVA_HOME           JDK installation path
  PANGROVA_JAVA_OPTS  JVM options override
EOF
    exit 0
}

# shellcheck source=/dev/null
[[ -f "$PANGROVA_CONF_DIR/pangrova-env.sh" ]] && source "$PANGROVA_CONF_DIR/pangrova-env.sh"

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

# The service ships as a shaded executable JAR; fall back to the role runtime
# classpath when a lib/shuffle distribution is installed instead.
MODULE="pangrova-shuffle/pangrova-shuffle-service"
TARGET="$PANGROVA_HOME/$MODULE/target"
PROPERTIES="$TARGET/maven-archiver/pom.properties"
if [[ -d "$PANGROVA_HOME/lib/shuffle" ]]; then
    CLASSPATH="$PANGROVA_HOME/lib/shuffle/*"
elif [[ -f "$PROPERTIES" ]]; then
    VERSION="$(sed -n 's/^version=//p' "$PROPERTIES")"
    EXEC_JAR="$TARGET/pangrova-shuffle-service-${VERSION}-exec.jar"
    if [[ -z "$VERSION" || ! -f "$EXEC_JAR" ]]; then
        echo "ERROR: Missing shuffle executable JAR. Run 'mvn -pl $MODULE -am package -DskipTests'." >&2
        exit 1
    fi
    CLASSPATH="$EXEC_JAR"
else
    echo "ERROR: No packaged shuffle runtime. Run 'mvn -pl $MODULE -am package -DskipTests'." >&2
    exit 1
fi

DEFAULT_JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"
JAVA_OPTS="${PANGROVA_JAVA_OPTS:-$DEFAULT_JAVA_OPTS}"
JAVA_OPTS="${JAVA_OPTS} -DPANGROVA_LOG_DIR=${PANGROVA_LOG_DIR} -DPANGROVA_ROLE=${ROLE}"
if [[ -f "${PANGROVA_CONF_DIR}/logback.xml" ]]; then
    JAVA_OPTS="-Dlogback.configurationFile=${PANGROVA_CONF_DIR}/logback.xml ${JAVA_OPTS}"
fi

CONFIG_FILE="${PANGROVA_CONF_DIR}/shuffle.yml"
DAEMON=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)   usage ;;
        --daemon) DAEMON=true; shift ;;
        --config) CONFIG_FILE="$2"; shift 2 ;;
        *) echo "ERROR: Unknown argument: $1" >&2; usage ;;
    esac
done
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Configuration file not found: $CONFIG_FILE" >&2
    exit 1
fi

mkdir -p "$PANGROVA_LOG_DIR"
PID_FILE="$PANGROVA_LOG_DIR/${ROLE}.pid"
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Shuffle service already running (PID $OLD_PID)." >&2
        exit 1
    fi
    rm -f "$PID_FILE"
fi

echo "Starting Pangrova Shuffle Service..."
echo "  CONFIG: $CONFIG_FILE"
if [[ "$DAEMON" == "true" ]]; then
    nohup "$JAVA" $JAVA_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" "$CONFIG_FILE" \
        > "$PANGROVA_LOG_DIR/${ROLE}.out" 2>&1 &
    echo $! > "$PID_FILE"
    echo "  PID:    $(cat "$PID_FILE") (daemon)"
else
    exec "$JAVA" $JAVA_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" "$CONFIG_FILE"
fi
