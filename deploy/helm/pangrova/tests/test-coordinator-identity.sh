#!/usr/bin/env bash
# Render-only Coordinator identity regression. Does not connect to a cluster.
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

render() {
  "$helm_bin" template identity-test "$chart" --set developmentMode=true "$@"
}

expect_failure() {
  local message="$1"
  shift
  if render "$@" >"$work/output" 2>"$work/error"; then
    printf 'Expected Helm to reject: %s\n' "$message" >&2
    exit 1
  fi
  grep -Fq "$message" "$work/error" || { cat "$work/error" >&2; exit 1; }
}

render >"$work/default"
grep -Fq 'id: "coordinator-1"' "$work/default"
if grep -Fq 'name: PANGROVA_COORDINATOR_ID' "$work/default"; then
  echo 'Default single Coordinator ID must not be overridden' >&2
  exit 1
fi

cat >"$work/ha.yml" <<'YAML'
coordinator:
  ha:
    enabled: true
security:
  enabled: true
  auth:
    type: NOOP
YAML
sed 's/^    enabled: true$/    enabled: false/' "$work/ha.yml" >"$work/no-ha.yml"
for id in coordinator-1 explicit-site-node null; do
  awk -v id="$id" '{print} /^coordinator:$/ {print "  id: " id}' \
    "$work/ha.yml" >"$work/$id.yml"
done

# Single-replica customers keep their configured ID by default.
render --set-file "config.coordinator=$work/explicit-site-node.yml" >"$work/single"
grep -Fq 'id: explicit-site-node' "$work/single"
if grep -Fq 'name: PANGROVA_COORDINATOR_ID' "$work/single"; then
  echo 'Configured single Coordinator ID must not be overridden' >&2
  exit 1
fi

expect_failure 'coordinator.identityMode must be configured or podUid' \
  --set coordinator.identityMode=unknown
expect_failure 'multiple Coordinator replicas require coordinator.ha.enabled=true' \
  --set coordinator.replicas=2 --set coordinator.identityMode=podUid \
  --set-file "config.coordinator=$work/no-ha.yml"
expect_failure 'multiple Coordinator replicas require coordinator.identityMode=podUid' \
  --set coordinator.replicas=2 --set-file "config.coordinator=$work/ha.yml"
expect_failure 'requires removing coordinator.id from config.coordinator' \
  --set coordinator.identityMode=podUid
for id in coordinator-1 explicit-site-node null; do
  expect_failure 'requires removing coordinator.id from config.coordinator' \
    --set coordinator.replicas=2 --set coordinator.identityMode=podUid \
    --set-file "config.coordinator=$work/$id.yml"
done

{ cat "$work/ha.yml"; echo 'coordinator.id: flat-fixed-id'; } >"$work/flat-id.yml"
expect_failure 'requires removing coordinator.id from config.coordinator' \
  --set coordinator.replicas=2 --set coordinator.identityMode=podUid \
  --set-file "config.coordinator=$work/flat-id.yml"

# Keep the identity policy through a deliberate scale-down to one Pod.
for replicas in 2 1; do
  render --show-only templates/coordinator-deployment.yaml \
    --set coordinator.replicas="$replicas" --set coordinator.identityMode=podUid \
    --set-file "config.coordinator=$work/ha.yml" >"$work/uid"
  grep -Fq "replicas: $replicas" "$work/uid"
  grep -A3 -F 'name: PANGROVA_COORDINATOR_ID' "$work/uid" \
    | grep -Fq 'fieldPath: metadata.uid'
done
echo 'Coordinator identity Helm renders passed'
