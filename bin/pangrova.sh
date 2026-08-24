#!/usr/bin/env bash
# ==============================================================================
# pangrova.sh — Unified entry point for Pangrova cluster management
#
# Usage:
#   bin/pangrova.sh <command> [subcommand] [options...]
#
# Commands:
#   start {coordinator|worker|standalone} [--config <path>] [--daemon]
#   stop  {coordinator|worker|standalone|all}
#   status  [--coordinator-url <url>] [--token <token>]
#   health-check [--scheme <http|https>] [--verbose] [--insecure] <host:port>
#   cli   [--host <host>] [--port <port>] [--catalog <name>] [--query-timeout <ms>]
#   catalog {list|show <name>|test <name>}
#   rolling-upgrade --new-version-dir <path> --workers <list> --coordinators <list>
#
# This script delegates to the corresponding standalone scripts.
# It does not implement any logic itself — pure delegation.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [subcommand] [options...]

Unified entry point for Pangrova cluster management.

Commands:
  start {coordinator|worker|standalone} [--config <path>] [--daemon]
      Start the specified Pangrova role.

  stop {coordinator|worker|standalone|all}
      Stop the specified Pangrova process(es).

  status [--coordinator-url <url>] [--token <token>]
      Display cluster status (queries Coordinator REST API).

  health-check [--scheme <http|https>] [--verbose] [--insecure] [--timeout <s>] <host:port>
      Check the health of an Pangrova node.

  cli [--host <host>] [--port <port>] [--catalog <name>] [--query-timeout <ms>]
      Open an interactive SQL CLI session against a running Coordinator.

  catalog {list|show <name>|test <name>} [--coordinator-url <url>] [--token <token>]
      Manage catalog configurations.

  rolling-upgrade --new-version-dir <path> --workers <list> --coordinators <list> [opts]
      Perform a rolling cluster upgrade (SSH-based reference implementation).

Examples:
  $(basename "$0") start coordinator --daemon
  $(basename "$0") start worker --config /etc/pangrova/worker.yml --daemon
  $(basename "$0") start standalone
  $(basename "$0") stop coordinator
  $(basename "$0") stop standalone
  $(basename "$0") stop all
  $(basename "$0") status --coordinator-url https://coordinator:8080
  $(basename "$0") health-check --scheme https --verbose coordinator:8080
  $(basename "$0") cli --host localhost --catalog my_iceberg
  $(basename "$0") catalog list
  $(basename "$0") catalog show my_iceberg
  $(basename "$0") catalog test my_iceberg --coordinator-url https://coordinator:8080
EOF
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

COMMAND="$1"
shift

case "$COMMAND" in
    start)
        if [[ $# -eq 0 ]]; then
            echo "ERROR: 'start' requires a role: coordinator, worker, or standalone." >&2
            exit 1
        fi
        ROLE="$1"
        shift
        case "$ROLE" in
            coordinator)
                exec "$SCRIPT_DIR/pangrova-coordinator.sh" "$@"
                ;;
            worker)
                exec "$SCRIPT_DIR/pangrova-worker.sh" "$@"
                ;;
            standalone)
                exec "$SCRIPT_DIR/pangrova-standalone.sh" "$@"
                ;;
            *)
                echo "ERROR: Unknown role '${ROLE}'. Use: coordinator, worker, or standalone." >&2
                exit 1
                ;;
        esac
        ;;
    stop)
        if [[ $# -eq 0 ]]; then
            echo "ERROR: 'stop' requires a target: coordinator, worker, standalone, or all." >&2
            exit 1
        fi
        TARGET="$1"
        shift
        case "$TARGET" in
            coordinator)
                exec "$SCRIPT_DIR/pangrova-stop.sh" --coordinator "$@"
                ;;
            worker)
                exec "$SCRIPT_DIR/pangrova-stop.sh" --worker "$@"
                ;;
            standalone)
                exec "$SCRIPT_DIR/pangrova-stop.sh" --standalone "$@"
                ;;
            all)
                exec "$SCRIPT_DIR/pangrova-stop.sh" --all "$@"
                ;;
            *)
                echo "ERROR: Unknown stop target '${TARGET}'. Use: coordinator, worker, standalone, or all." >&2
                exit 1
                ;;
        esac
        ;;
    status)
        exec "$SCRIPT_DIR/pangrova-cluster-status.sh" "$@"
        ;;
    health-check)
        exec "$SCRIPT_DIR/pangrova-health-check.sh" "$@"
        ;;
    cli)
        exec "$SCRIPT_DIR/pangrova-cli.sh" "$@"
        ;;
    catalog)
        exec "$SCRIPT_DIR/pangrova-catalog.sh" "$@"
        ;;
    rolling-upgrade)
        exec "$SCRIPT_DIR/pangrova-rolling-upgrade.sh" "$@"
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    *)
        echo "ERROR: Unknown command '${COMMAND}'." >&2
        echo "Run '$(basename "$0") --help' for usage." >&2
        exit 1
        ;;
esac
