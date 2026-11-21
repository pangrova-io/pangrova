#!/usr/bin/env bash
# ==============================================================================
# pangrova-rolling-upgrade.sh — Rolling upgrade for Pangrova cluster (reference impl)
#
# Usage:
#   bin/pangrova-rolling-upgrade.sh --new-version-dir <path> --workers <list>
#       --coordinators <list> [--coordinator-url <url>] [--ssh-user <user>]
#       [--ssh-key <path>] [--dry-run] [--help]
#
# Performs a rolling upgrade of the Pangrova cluster:
#   1. Pre-flight: verify Coordinator is reachable and cluster is healthy
#   2. Workers first (sequentially), then Coordinators
#   3. Each node: stop → replace lib/ → start → health check
#   4. On failure: automatic rollback of the failed node
#
# NOTE: This is a reference implementation that assumes SSH access to all
#       cluster nodes. For production use, consider Ansible or Kubernetes
#       rolling updates instead.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PANGROVA_HOME="${PANGROVA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
HEALTH_CHECK_SCRIPT="$SCRIPT_DIR/pangrova-health-check.sh"
HEALTH_CHECK_RETRIES=12
HEALTH_CHECK_INTERVAL=5

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Perform a rolling upgrade of the Pangrova cluster.

Upgrade order: Workers first, then Coordinators.
Each node is stopped, its lib/ directory replaced, restarted, and health-checked.
If a node fails health check after upgrade, it is automatically rolled back.

Required Options:
  --new-version-dir <path>    Path to the new Pangrova version directory (must contain lib/)
  --workers <list>            Comma-separated worker host:port list (e.g. worker-1:8081,worker-2:8081)
  --coordinators <list>       Comma-separated coordinator host:port list

Optional Options:
  --coordinator-url <url>     Coordinator API base URL for pre-flight cluster state check
  --ssh-user <user>           SSH login username (default: current user)
  --ssh-key <path>            Path to SSH private key file
  --dry-run                   Print commands without executing them
  --help                      Show this help message

NOTE:
  This is a reference implementation assuming SSH access to cluster nodes.
  For production environments, use Ansible, Terraform, or Kubernetes
  rolling update strategies instead.

Examples:
  $(basename "$0") \\
    --coordinator-url http://coordinator-1:8080 \\
    --new-version-dir /opt/pangrova-1.1.0 \\
    --workers worker-1:8081,worker-2:8081,worker-3:8081 \\
    --coordinators coordinator-1:8080,coordinator-2:8080

  $(basename "$0") --dry-run \\
    --new-version-dir /opt/pangrova-1.1.0 \\
    --ssh-user pangrova --ssh-key ~/.ssh/pangrova_rsa \\
    --workers worker-1:8081 --coordinators coordinator-1:8080
EOF
    exit 0
}

# --- Parse arguments ---
COORDINATOR_URL=""
NEW_VERSION_DIR=""
WORKERS=""
COORDINATORS=""
SSH_USER=""
SSH_KEY=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            usage
            ;;
        --coordinator-url)
            COORDINATOR_URL="$2"; shift 2
            ;;
        --new-version-dir)
            NEW_VERSION_DIR="$2"; shift 2
            ;;
        --workers)
            WORKERS="$2"; shift 2
            ;;
        --coordinators)
            COORDINATORS="$2"; shift 2
            ;;
        --ssh-user)
            SSH_USER="$2"; shift 2
            ;;
        --ssh-key)
            SSH_KEY="$2"; shift 2
            ;;
        --dry-run)
            DRY_RUN=true; shift
            ;;
        *)
            echo "ERROR: Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$NEW_VERSION_DIR" ]]; then
    echo "ERROR: --new-version-dir is required." >&2
    echo "Run '$(basename "$0") --help' for usage." >&2
    exit 1
fi

if [[ ! -d "$NEW_VERSION_DIR" ]]; then
    echo "ERROR: New version directory does not exist: $NEW_VERSION_DIR" >&2
    exit 1
fi

if [[ ! -d "$NEW_VERSION_DIR/lib" ]]; then
    echo "ERROR: New version directory does not contain a lib/ subdirectory: $NEW_VERSION_DIR/lib" >&2
    exit 1
fi

if [[ -z "$WORKERS" && -z "$COORDINATORS" ]]; then
    echo "ERROR: At least one of --workers or --coordinators is required." >&2
    exit 1
fi

if [[ ! -x "$HEALTH_CHECK_SCRIPT" ]]; then
    echo "ERROR: Health check script not found or not executable: $HEALTH_CHECK_SCRIPT" >&2
    exit 1
fi

# --- Helper functions ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# SSH passes the command to the remote login shell as text.
quote_remote_path() {
    local escaped=${1//\'/\'\\\'\'}
    printf "'%s'" "$escaped"
}

# Build SSH command with optional user, key, and standard options
run_ssh() {
    local host="$1"
    shift
    local ssh_args=(-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes)
    [[ -n "$SSH_USER" ]] && ssh_args+=(-l "$SSH_USER")
    [[ -n "$SSH_KEY" ]] && ssh_args+=(-i "$SSH_KEY")

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] ssh ${ssh_args[*]} ${host} $*"
        return 0
    fi
    ssh "${ssh_args[@]}" "$host" "$@"
}

wait_for_healthy() {
    local host_port="$1"
    local attempt=0

    while [[ $attempt -lt $HEALTH_CHECK_RETRIES ]]; do
        if "$HEALTH_CHECK_SCRIPT" "$host_port" >/dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        log "  Health check attempt ${attempt}/${HEALTH_CHECK_RETRIES} for ${host_port}..."
        sleep "$HEALTH_CHECK_INTERVAL"
    done
    return 1
}

upgrade_node() {
    local role="$1"
    local host_port="$2"
    local host="${host_port%%:*}"
    local remote_home remote_release
    remote_home=$(quote_remote_path "$PANGROVA_HOME")
    remote_release=$(quote_remote_path "$NEW_VERSION_DIR")

    log "--- Upgrading ${role} at ${host_port} ---"

    # Step 1: Backup current lib/ (only binaries, not logs or config)
    log "  Backing up lib/ on ${host}..."
    # Reserve a fresh destination. cp into an existing backup would nest lib/
    # and leave rollback pointing at an older or incomplete snapshot.
    run_ssh "$host" "mkdir -- ${remote_home}/lib.backup && cp -Rp -- ${remote_home}/lib/. ${remote_home}/lib.backup/" || {
        log "  ERROR: lib/ backup failed on ${host}; aborting before stopping the node."
        return 1
    }

    # Step 2: Stop the node
    log "  Stopping ${role} on ${host}..."
    if ! run_ssh "$host" "${remote_home}/bin/pangrova-stop.sh --${role}"; then
        log "  ERROR: Stop failed on ${host}; aborting before replacing libraries. Backup retained."
        return 1
    fi

    # Step 3: Deploy new lib/ (only replace binaries; config and logs are preserved)
    log "  Deploying new lib/ from ${NEW_VERSION_DIR}/lib/ ..."
    if [[ "$DRY_RUN" == "true" ]]; then
        log "  [DRY-RUN] rsync -a --delete -- ${remote_release}/lib/ ${remote_home}/lib/ (on ${host})"
    else
        run_ssh "$host" "rsync -a --delete -- ${remote_release}/lib/ ${remote_home}/lib/" 2>/dev/null || {
            log "  ERROR: Failed to deploy new lib/ to ${host}."
            rollback_node "$role" "$host_port" "$host"
            return 1
        }
    fi

    # Step 4: Start the node
    log "  Starting ${role} on ${host}..."
    run_ssh "$host" "${remote_home}/bin/pangrova-${role}.sh --daemon" 2>/dev/null || {
        log "  ERROR: Failed to start ${role} on ${host}."
        rollback_node "$role" "$host_port" "$host"
        return 1
    }

    # Step 5: Health check
    log "  Waiting for ${role} at ${host_port} to become healthy..."
    if [[ "$DRY_RUN" == "true" ]]; then
        log "  [DRY-RUN] health check skipped"
        log "  [DRY-RUN] ${role} at ${host_port} upgrade simulation complete."
        run_ssh "$host" "rm -rf -- ${remote_home}/lib.backup" 2>/dev/null || true
        return 0
    fi

    if wait_for_healthy "$host_port"; then
        log "  OK: ${role} at ${host_port} upgraded successfully."
        run_ssh "$host" "rm -rf -- ${remote_home}/lib.backup" 2>/dev/null || true
        return 0
    else
        log "  FAIL: ${role} at ${host_port} failed health check after upgrade."
        rollback_node "$role" "$host_port" "$host"
        return 1
    fi
}

rollback_node() {
    local role="$1"
    local host_port="$2"
    local host="$3"
    local remote_home
    remote_home=$(quote_remote_path "$PANGROVA_HOME")

    log "  Rolling back ${role} at ${host_port}..."

    if ! run_ssh "$host" "${remote_home}/bin/pangrova-stop.sh --${role}"; then
        log "  ERROR: Stop failed on ${host}; aborting rollback before restoring libraries. Backup retained."
        return 1
    fi
    if ! run_ssh "$host" "test -d ${remote_home}/lib.backup"; then
        log "  ERROR: No lib.backup found on ${host}. Manual intervention required."
        return 1
    fi
    if ! run_ssh "$host" "rm -rf -- ${remote_home}/lib && mv -- ${remote_home}/lib.backup ${remote_home}/lib"; then
        log "  ERROR: Rollback failed for ${host}. Manual intervention required."
        return 1
    fi
    if ! run_ssh "$host" "${remote_home}/bin/pangrova-${role}.sh --daemon"; then
        log "  ERROR: Previous libraries restored, but restart failed on ${host}. Manual intervention required."
        return 1
    fi
    log "  Rollback complete for ${role} at ${host_port}."
}

# --- Pre-flight: verify Coordinator API is reachable ---
if [[ -n "$COORDINATOR_URL" ]]; then
    log "=== Pre-flight: Checking cluster state at ${COORDINATOR_URL} ==="
    HEALTH_URL="${COORDINATOR_URL}/v1/health"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 10 --max-time 10 \
        "$HEALTH_URL" 2>/dev/null) || HTTP_CODE="000"

    if [[ "$HTTP_CODE" == "200" ]]; then
        log "  Coordinator health: OK (HTTP 200)"
    else
        log "  WARN: Coordinator at ${COORDINATOR_URL} returned HTTP ${HTTP_CODE} — proceeding anyway."
        log "  Ensure the cluster is in a stable state before upgrading."
    fi
fi

if [[ "$DRY_RUN" == "true" ]]; then
    log "=== DRY-RUN mode — no changes will be made ==="
fi

# --- Main upgrade sequence ---
FAILED=0

# Phase 1: Upgrade Workers
if [[ -n "$WORKERS" ]]; then
    log "=== Phase 1: Upgrading Workers ==="
    IFS=',' read -ra WORKER_LIST <<< "$WORKERS"
    for worker in "${WORKER_LIST[@]}"; do
        if ! upgrade_node "worker" "$worker"; then
            log "ERROR: Worker upgrade failed at ${worker}. Stopping upgrade."
            FAILED=1
            break
        fi
    done
fi

# Phase 2: Upgrade Coordinators (only if Workers succeeded)
if [[ $FAILED -eq 0 && -n "$COORDINATORS" ]]; then
    log "=== Phase 2: Upgrading Coordinators ==="
    IFS=',' read -ra COORDINATOR_LIST <<< "$COORDINATORS"
    for coordinator in "${COORDINATOR_LIST[@]}"; do
        if ! upgrade_node "coordinator" "$coordinator"; then
            log "ERROR: Coordinator upgrade failed at ${coordinator}. Stopping upgrade."
            FAILED=1
            break
        fi
    done
fi

# --- Summary ---
echo ""
if [[ $FAILED -eq 0 ]]; then
    if [[ "$DRY_RUN" == "true" ]]; then
        log "=== DRY-RUN complete — no changes were made ==="
    else
        log "=== Rolling upgrade completed successfully ==="
    fi
    exit 0
else
    log "=== Rolling upgrade FAILED — some nodes may need manual attention ==="
    exit 1
fi
