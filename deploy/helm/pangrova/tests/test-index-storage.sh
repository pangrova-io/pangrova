#!/usr/bin/env bash
# Render-only index storage regression. Does not connect to a cluster.
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

config() {
  cat >"$work/coordinator.yml" <<YAML
security:
  enabled: true
  auth:
    type: NOOP
indexing:
  enabled: true
  base-path: '$1'
YAML
}
render() {
  "$helm_bin" template index-test "$chart" --set developmentMode=true \
    --set-file "config.coordinator=$work/coordinator.yml" "$@"
}
reject() {
  local expected="$1"
  shift
  if render "$@" >"$work/output" 2>"$work/error"; then
    echo "Expected Helm to reject: $expected" >&2
    exit 1
  fi
  grep -Fq "$expected" "$work/error" || { cat "$work/error" >&2; exit 1; }
}

# Default disabled indexing retains the no-PVC profile.
"$helm_bin" template index-test "$chart" --set developmentMode=true >"$work/default"
if grep -Fq 'mountPath: /opt/pangrova/indexes' "$work/default"; then
  echo 'Disabled indexing must not implicitly mount a PVC' >&2
  exit 1
fi

config /opt/pangrova/indexes
reject 'requires indexing.persistence.enabled=true'
for root in /opt/pangrova/indexes /opt/pangrova/indexes/subdir file:///opt/pangrova/indexes; do
  config "$root"
  render --set indexing.persistence.enabled=true >"$work/shared"
  test "$(grep -Fc 'mountPath: /opt/pangrova/indexes' "$work/shared")" = 2
  test "$(grep -Fc 'claimName: "pangrova-coordinator-index-data"' "$work/shared")" = 2
  grep -Fq 'accessModes: ["ReadWriteMany"]' "$work/shared"
done
config /opt/pangrova/indexes
render --set indexing.persistence.enabled=true --set indexing.persistence.existingClaim=shared-index >"$work/existing"
test "$(grep -Fc 'claimName: "shared-index"' "$work/existing")" = 2
if grep -Fq 'name: pangrova-coordinator-index-data' "$work/existing"; then
  echo 'Existing claim must not create an additional PVC' >&2
  exit 1
fi
for root in /mnt/indexes relative /opt/pangrova/indexes-other /opt/pangrova/indexes/../other file://host/opt/pangrova/indexes; do
  config "$root"
  reject 'local indexing.base-path must be' --set indexing.persistence.enabled=true
done
config '${PANGROVA_INDEX_BASE_PATH:-/opt/pangrova/indexes}'
reject 'requires a literal indexing.base-path' --set indexing.persistence.enabled=true
config ''
reject 'requires a literal indexing.base-path' --set indexing.persistence.enabled=true

config /opt/pangrova/indexes
cp "$work/coordinator.yml" "$work/literal.yml"
# Only the indexing section is changed; security stays enabled.
awk '/^indexing:$/ { in_index = 1 } in_index && /^  enabled:/ { $0 = "  enabled: dynamic" } { print }'   "$work/literal.yml" >"$work/coordinator.yml"
reject 'indexing.enabled must be a literal true or false' --set indexing.persistence.enabled=true

config /opt/pangrova/indexes
printf '%s\n' 'indexing.base-path: /mnt/indexes' >>"$work/coordinator.yml"
reject 'without dotted overrides' --set indexing.persistence.enabled=true
config /opt/pangrova/indexes
reject 'not shared across tenant namespaces' --set indexing.persistence.enabled=true \
  --set multiTenancy.enabled=true --set 'multiTenancy.tenants[0].name=alpha'

config s3a://warehouse/pangrova/indexes
render >"$work/object"
if grep -Fq 'mountPath: /opt/pangrova/indexes' "$work/object"; then
  echo 'Object-store root does not require a shared PVC' >&2
  exit 1
fi
render --set multiTenancy.enabled=true --set 'multiTenancy.tenants[0].name=alpha' >"$work/tenant"

echo 'Index storage Helm renders passed'
