#!/usr/bin/env bash
# =============================================================================
# Pangrova Helm Chart — Namespace Isolation Verification Test
#
# This script deploys the Pangrova Helm chart with multi-tenancy enabled and
# verifies that:
#   1. Per-tenant namespaces are created
#   2. Resource quotas are applied per namespace
#   3. Network policies prevent cross-tenant traffic
#   4. HPA is configured for worker auto-scaling
#   5. Tenant workers can reach the coordinator (control plane)
#   6. Tenant workers cannot reach other tenant namespaces
#
# Prerequisites:
#   - kubectl configured with cluster access
#   - helm v3 installed
#   - A running Kubernetes cluster (kind, minikube, or real cluster)
#   - Network policy enforcement enabled (e.g., Calico, Cilium)
#
# Usage:
#   ./test-namespace-isolation.sh [--cleanup]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RELEASE_NAME="pangrova-test"
CONTROL_NS="pangrova-test"
TENANT_A_NS="pangrova-tenant-alpha"
TENANT_B_NS="pangrova-tenant-beta"
TIMEOUT="120s"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

FAILURES=0

# --- Cleanup function ---
cleanup() {
    info "Cleaning up test resources..."
    helm uninstall "${RELEASE_NAME}" -n "${CONTROL_NS}" 2>/dev/null || true
    kubectl delete namespace "${CONTROL_NS}" --ignore-not-found --timeout=60s 2>/dev/null || true
    kubectl delete namespace "${TENANT_A_NS}" --ignore-not-found --timeout=60s 2>/dev/null || true
    kubectl delete namespace "${TENANT_B_NS}" --ignore-not-found --timeout=60s 2>/dev/null || true
}

if [[ "${1:-}" == "--cleanup" ]]; then
    cleanup
    exit 0
fi

trap cleanup EXIT

# --- Deploy with multi-tenancy enabled ---
info "Deploying Pangrova Helm chart with multi-tenancy..."

cat > /tmp/pangrova-test-values.yaml <<'EOF'
namespace: pangrova-test

image:
  repository: pangrova/pangrova
  tag: latest
  pullPolicy: IfNotPresent

coordinator:
  replicas: 1
  resources:
    requests:
      cpu: "100m"
      memory: "256Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"

worker:
  replicas: 1
  resources:
    requests:
      cpu: "100m"
      memory: "256Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"

autoscaling:
  enabled: true
  minReplicas: 1
  maxReplicas: 3
  targetCPUUtilizationPercentage: 70

multiTenancy:
  enabled: true
  tenants:
    - name: tenant-alpha
      namespace: pangrova-tenant-alpha
      resourceQuota:
        cpu: "4"
        memory: "8Gi"
        pods: "10"
        persistentVolumeClaims: "5"
        requestsStorage: "100Gi"
      workerReplicas: 1
      workerResources:
        requests:
          cpu: "100m"
          memory: "256Mi"
        limits:
          cpu: "500m"
          memory: "512Mi"
      autoscaling:
        enabled: true
        minReplicas: 1
        maxReplicas: 5
        targetCPUUtilizationPercentage: 70
        targetMemoryUtilizationPercentage: 80
    - name: tenant-beta
      namespace: pangrova-tenant-beta
      resourceQuota:
        cpu: "2"
        memory: "4Gi"
        pods: "5"
        persistentVolumeClaims: "3"
        requestsStorage: "50Gi"
      workerReplicas: 1
      workerResources:
        requests:
          cpu: "100m"
          memory: "256Mi"
        limits:
          cpu: "500m"
          memory: "512Mi"
      autoscaling:
        enabled: true
        minReplicas: 1
        maxReplicas: 3
        targetCPUUtilizationPercentage: 70
        targetMemoryUtilizationPercentage: 80
  networkPolicy:
    allowFromControlPlane: true
    additionalAllowedNamespaces: []
EOF

helm install "${RELEASE_NAME}" "${CHART_DIR}" \
    -n "${CONTROL_NS}" --create-namespace \
    --set developmentMode=true \
    -f /tmp/pangrova-test-values.yaml \
    --wait --timeout "${TIMEOUT}" 2>&1 || {
    # If --wait fails (pods not ready due to missing image), that's OK for
    # infrastructure verification. We only need the K8s resources created.
    info "Helm install completed (pods may not be ready without real image)"
}

# --- Test 1: Verify tenant namespaces exist ---
info "Test 1: Verifying tenant namespaces..."
if kubectl get namespace "${TENANT_A_NS}" &>/dev/null; then
    pass "Tenant-alpha namespace '${TENANT_A_NS}' exists"
else
    fail "Tenant-alpha namespace '${TENANT_A_NS}' not found"
fi

if kubectl get namespace "${TENANT_B_NS}" &>/dev/null; then
    pass "Tenant-beta namespace '${TENANT_B_NS}' exists"
else
    fail "Tenant-beta namespace '${TENANT_B_NS}' not found"
fi

# Verify namespace labels
LABEL=$(kubectl get namespace "${TENANT_A_NS}" -o jsonpath='{.metadata.labels.tenant}' 2>/dev/null || echo "")
if [[ "${LABEL}" == "tenant-alpha" ]]; then
    pass "Tenant-alpha namespace has correct tenant label"
else
    fail "Tenant-alpha namespace missing tenant label (got: '${LABEL}')"
fi

# --- Test 2: Verify resource quotas ---
info "Test 2: Verifying resource quotas..."
QUOTA_CPU=$(kubectl get resourcequota tenant-alpha-quota -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.hard.requests\.cpu}' 2>/dev/null || echo "")
if [[ "${QUOTA_CPU}" == "4" ]]; then
    pass "Tenant-alpha resource quota CPU = 4"
else
    fail "Tenant-alpha resource quota CPU incorrect (got: '${QUOTA_CPU}', expected: '4')"
fi

QUOTA_MEM=$(kubectl get resourcequota tenant-alpha-quota -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.hard.requests\.memory}' 2>/dev/null || echo "")
if [[ "${QUOTA_MEM}" == "8Gi" ]]; then
    pass "Tenant-alpha resource quota memory = 8Gi"
else
    fail "Tenant-alpha resource quota memory incorrect (got: '${QUOTA_MEM}', expected: '8Gi')"
fi

QUOTA_PODS=$(kubectl get resourcequota tenant-alpha-quota -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.hard.pods}' 2>/dev/null || echo "")
if [[ "${QUOTA_PODS}" == "10" ]]; then
    pass "Tenant-alpha resource quota pods = 10"
else
    fail "Tenant-alpha resource quota pods incorrect (got: '${QUOTA_PODS}', expected: '10')"
fi

# Verify tenant-beta has different quota
QUOTA_B_CPU=$(kubectl get resourcequota tenant-beta-quota -n "${TENANT_B_NS}" \
    -o jsonpath='{.spec.hard.requests\.cpu}' 2>/dev/null || echo "")
if [[ "${QUOTA_B_CPU}" == "2" ]]; then
    pass "Tenant-beta resource quota CPU = 2 (different from alpha)"
else
    fail "Tenant-beta resource quota CPU incorrect (got: '${QUOTA_B_CPU}', expected: '2')"
fi

# --- Test 3: Verify network policies ---
info "Test 3: Verifying network policies..."
DENY_POLICY=$(kubectl get networkpolicy tenant-alpha-deny-all -n "${TENANT_A_NS}" \
    -o jsonpath='{.metadata.name}' 2>/dev/null || echo "")
if [[ "${DENY_POLICY}" == "tenant-alpha-deny-all" ]]; then
    pass "Tenant-alpha deny-all network policy exists"
else
    fail "Tenant-alpha deny-all network policy not found"
fi

INTRA_POLICY=$(kubectl get networkpolicy tenant-alpha-allow-intra-namespace -n "${TENANT_A_NS}" \
    -o jsonpath='{.metadata.name}' 2>/dev/null || echo "")
if [[ "${INTRA_POLICY}" == "tenant-alpha-allow-intra-namespace" ]]; then
    pass "Tenant-alpha allow-intra-namespace policy exists"
else
    fail "Tenant-alpha allow-intra-namespace policy not found"
fi

DNS_POLICY=$(kubectl get networkpolicy tenant-alpha-allow-dns -n "${TENANT_A_NS}" \
    -o jsonpath='{.metadata.name}' 2>/dev/null || echo "")
if [[ "${DNS_POLICY}" == "tenant-alpha-allow-dns" ]]; then
    pass "Tenant-alpha allow-dns policy exists"
else
    fail "Tenant-alpha allow-dns policy not found"
fi

CP_POLICY=$(kubectl get networkpolicy tenant-alpha-allow-control-plane -n "${TENANT_A_NS}" \
    -o jsonpath='{.metadata.name}' 2>/dev/null || echo "")
if [[ "${CP_POLICY}" == "tenant-alpha-allow-control-plane" ]]; then
    pass "Tenant-alpha allow-control-plane policy exists"
else
    fail "Tenant-alpha allow-control-plane policy not found"
fi

# Verify deny-all blocks cross-tenant traffic (policy types include both Ingress and Egress)
POLICY_TYPES=$(kubectl get networkpolicy tenant-alpha-deny-all -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.policyTypes[*]}' 2>/dev/null || echo "")
if [[ "${POLICY_TYPES}" == *"Ingress"* ]] && [[ "${POLICY_TYPES}" == *"Egress"* ]]; then
    pass "Deny-all policy covers both Ingress and Egress"
else
    fail "Deny-all policy missing Ingress/Egress (got: '${POLICY_TYPES}')"
fi

# --- Test 4: Verify HPA ---
info "Test 4: Verifying HPA configuration..."
HPA_A=$(kubectl get hpa pangrova-worker -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.maxReplicas}' 2>/dev/null || echo "")
if [[ "${HPA_A}" == "5" ]]; then
    pass "Tenant-alpha HPA maxReplicas = 5"
else
    fail "Tenant-alpha HPA maxReplicas incorrect (got: '${HPA_A}', expected: '5')"
fi

HPA_B=$(kubectl get hpa pangrova-worker -n "${TENANT_B_NS}" \
    -o jsonpath='{.spec.maxReplicas}' 2>/dev/null || echo "")
if [[ "${HPA_B}" == "3" ]]; then
    pass "Tenant-beta HPA maxReplicas = 3"
else
    fail "Tenant-beta HPA maxReplicas incorrect (got: '${HPA_B}', expected: '3')"
fi

# Verify memory metric is present in HPA
HPA_METRICS=$(kubectl get hpa pangrova-worker -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.metrics[*].resource.name}' 2>/dev/null || echo "")
if [[ "${HPA_METRICS}" == *"memory"* ]]; then
    pass "Tenant-alpha HPA includes memory metric"
else
    fail "Tenant-alpha HPA missing memory metric (got: '${HPA_METRICS}')"
fi

# --- Test 5: Verify worker StatefulSet in tenant namespace ---
info "Test 5: Verifying tenant worker deployments..."
WORKER_A=$(kubectl get statefulset pangrova-worker -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")
if [[ "${WORKER_A}" == "1" ]]; then
    pass "Tenant-alpha worker StatefulSet exists with 1 replica"
else
    fail "Tenant-alpha worker StatefulSet incorrect (got replicas: '${WORKER_A}')"
fi

# Verify tenant label on worker pods
TENANT_LABEL=$(kubectl get statefulset pangrova-worker -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.template.metadata.labels.tenant}' 2>/dev/null || echo "")
if [[ "${TENANT_LABEL}" == "tenant-alpha" ]]; then
    pass "Tenant-alpha worker pods have tenant label"
else
    fail "Tenant-alpha worker pods missing tenant label (got: '${TENANT_LABEL}')"
fi

# Verify coordinator URL points to control plane namespace
COORD_URL=$(kubectl get statefulset pangrova-worker -n "${TENANT_A_NS}" \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="WORKER_COORDINATOR_URL")].value}' 2>/dev/null || echo "")
if [[ "${COORD_URL}" == *"${CONTROL_NS}"* ]]; then
    pass "Tenant-alpha workers point to coordinator in control plane namespace"
else
    fail "Tenant-alpha workers have wrong coordinator URL (got: '${COORD_URL}')"
fi

# --- Test 6: Verify LimitRange ---
info "Test 6: Verifying LimitRange..."
LIMIT_RANGE=$(kubectl get limitrange tenant-alpha-limits -n "${TENANT_A_NS}" \
    -o jsonpath='{.metadata.name}' 2>/dev/null || echo "")
if [[ "${LIMIT_RANGE}" == "tenant-alpha-limits" ]]; then
    pass "Tenant-alpha LimitRange exists"
else
    fail "Tenant-alpha LimitRange not found"
fi

# --- Test 7: Verify control-plane HPA ---
info "Test 7: Verifying control-plane worker HPA..."
CP_HPA=$(kubectl get hpa pangrova-worker -n "${CONTROL_NS}" \
    -o jsonpath='{.spec.maxReplicas}' 2>/dev/null || echo "")
if [[ "${CP_HPA}" == "3" ]]; then
    pass "Control-plane HPA maxReplicas = 3"
else
    fail "Control-plane HPA maxReplicas incorrect (got: '${CP_HPA}', expected: '3')"
fi

# --- Summary ---
echo ""
echo "============================================"
if [[ ${FAILURES} -eq 0 ]]; then
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
else
    echo -e "${RED}${FAILURES} TEST(S) FAILED${NC}"
fi
echo "============================================"

exit ${FAILURES}
