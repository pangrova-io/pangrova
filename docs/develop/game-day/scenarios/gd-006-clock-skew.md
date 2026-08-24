# GD-006: Clock Skew

| Field | Value |
|-------|-------|
| ID | GD-006 |
| Severity | **Medium** |
| Target component | Coordinator and Worker (NTP / system clock) |
| RTO target | < 2 minutes after NTP resync |
| Runbook | [clock-skew-runbook.md](../runbooks/clock-skew-runbook.md) |
| Prerequisites | `timedatectl` or `date` with sudo on target node; `chronyc` available |
| Estimated duration | 30 minutes |

---

## Objective

Verify that Pangrova correctly handles **clock skew** between cluster nodes. Distributed systems depend on synchronized clocks for:

1. **JWT token validation** — tokens appear expired when the verifying node's clock is ahead
2. **JDBC lease acquisition** — coordinator HA uses wall-clock timestamps for lease expiry; skewed clocks can cause premature lease expiry or false leadership conflicts
3. **Distributed snapshot timestamps** — Iceberg/Paimon snapshot IDs include wall-clock time; large skew can cause ordering violations
4. **Trace correlation** — OpenTelemetry spans from skewed nodes appear out of order in the tracing backend

The system must:
- Reject JWT tokens from clients whose clocks are significantly skewed (> 5 minutes — the OIDC standard clock tolerance)
- Emit clear metrics and alerts when node clock drift is detected
- Survive moderate skew (≤ 5 minutes) without data corruption
- Recover automatically when NTP is restored

---

## Blast Radius

| Resource | Impact |
|----------|--------|
| JWT authentication on skewed node | `HTTP 401` if token appears expired; `HTTP 401` if token appears issued in the future |
| JDBC lease expiry | May expire prematurely if coordinator clock jumps forward |
| Query timestamps | Iceberg snapshot timestamps may be disordered for rows written during skew |
| Trace correlation | Spans from skewed node appear displaced in time |
| Other nodes | Unaffected; queries route to non-skewed nodes |

---

## Prerequisites

```bash
# Identify target node (test on a follower coordinator or a worker first)
TARGET_NODE="coordinator-2"

# Verify current clock synchronization status
ssh "${TARGET_NODE}" "chronyc tracking"
# Expected: "System time" drift < 1ms

# Verify NTP is running
ssh "${TARGET_NODE}" "systemctl is-active chronyd || systemctl is-active ntp || systemctl is-active ntpd"

# Confirm timedatectl works
ssh "${TARGET_NODE}" "timedatectl status"

# Verify the coordinator cluster is healthy
curl -s https://coordinator-1:8080/v1/cluster/status | python3 -m json.tool | grep -E '"role"'

# Get the current token and its expiry for reference
echo "${PANGROVA_TOKEN}" | cut -d. -f2 | base64 -d 2>/dev/null | python3 -m json.tool | grep -E '"exp"|"iat"|"nbf"'
```

---

## Baseline Recording

```bash
ENDPOINT="https://coordinator-1:8080"
TOKEN="${PANGROVA_TOKEN}"

./tools/measure-recovery-time.sh baseline \
  --endpoint "${ENDPOINT}" \
  --token    "${TOKEN}" \
  --query    "SELECT current_timestamp" \
  --duration 60s \
  --output   /tmp/gd-006-baseline.json

# Record baseline clock state across all nodes
for node in coordinator-1 coordinator-2 coordinator-3 worker-0; do
  TS=$(ssh "${node}" "date -u +%Y-%m-%dT%H:%M:%S.%3NZ")
  echo "${node}: ${TS}"
done
```

---

## Scenario A: Moderate Clock Skew (Forward, 2 Minutes)

This simulates clock drift that stays within the OIDC 5-minute tolerance window. The system should continue operating.

### Step 1: Disable NTP and advance the clock

```bash
# Disable NTP so the clock can be manually adjusted
ssh "${TARGET_NODE}" "
  sudo timedatectl set-ntp false
  echo 'NTP disabled'
  timedatectl status | grep 'NTP service'
"

# Advance the clock by 2 minutes (within OIDC clock tolerance)
ssh "${TARGET_NODE}" "
  CURRENT_TIME=\$(date -u +%Y-%m-%dT%H:%M:%S)
  NEW_TIME=\$(date -u -d '+2 minutes' +%Y-%m-%dT%H:%M:%S 2>/dev/null || \
              date -u -v+2M +%Y-%m-%dT%H:%M:%S)
  sudo timedatectl set-time \"\${NEW_TIME}\"
  echo \"Clock advanced: \${CURRENT_TIME} → \$(date -u +%Y-%m-%dT%H:%M:%S)\"
"

INJECT_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Clock skew injected at: ${INJECT_TIME} (target: ${TARGET_NODE} +2min)"
```

### Step 2: Observe system behavior under moderate skew

```bash
# Verify queries still succeed on the skewed node
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${TARGET_NODE}:8080/v1/sql" \
  -d '{"query": "SELECT current_timestamp"}')
echo "Query on skewed node: HTTP ${STATUS}"
# Expected: 200 — within OIDC tolerance

# Verify coordinator logs show clock skew detection
ssh "${TARGET_NODE}" "
  grep -i 'clock skew\|time drift\|ntp\|chrony' /var/log/pangrova/pangrova-coordinator.log | tail -10
"

# Check cluster status still shows valid state
curl -s https://coordinator-1:8080/v1/cluster/status | python3 -m json.tool | grep '"role"'
```

---

## Scenario B: Severe Clock Skew (Forward, 10 Minutes)

This simulates clock drift beyond the OIDC 5-minute tolerance. JWT validation should reject tokens as "issued in the future" or "already expired."

### Step 1: Advance clock by 10 minutes

```bash
# Ensure NTP is still disabled from Scenario A
ssh "${TARGET_NODE}" "
  sudo timedatectl set-time \"\$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%S 2>/dev/null || \
                                  date -u -v+10M +%Y-%m-%dT%H:%M:%S)\"
  echo 'Clock advanced by 10 minutes'
  date -u
"

echo "Severe skew injected at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### Step 2: Verify JWT rejection on severely skewed node

```bash
# Query against the skewed coordinator — should fail due to clock skew in JWT validation
SKEWED_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${TARGET_NODE}:8080/v1/sql" \
  -d '{"query": "SELECT 1"}')
echo "Query on severely skewed node: HTTP ${SKEWED_STATUS}"
# Expected: 401 — token 'iat' (issued-at) is in the future from the node's perspective

# Verify the error message is clear
curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${TARGET_NODE}:8080/v1/sql" \
  -d '{"query": "SELECT 1"}' | python3 -m json.tool
# Expected JSON with error code indicating JWT clock issue
```

### Step 3: Verify queries still work on correctly-clocked nodes

```bash
# coordinator-1 (not skewed) should still serve queries normally
NORMAL_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator-1:8080/v1/sql" \
  -d '{"query": "SELECT 1"}')
echo "Query on normal coordinator-1: HTTP ${NORMAL_STATUS}"
# Expected: 200
```

### Step 4: Observe JDBC lease behavior

```bash
# Monitor whether the skewed coordinator's JDBC lease is affected
# The skewed coordinator may think its lease has expired (clock is ahead)
watch -n 3 'curl -s https://coordinator-1:8080/v1/cluster/status \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for n in data[\"nodes\"]:
    print(f\"{n[\"host\"]}: role={n[\"role\"]} lastSeen={n.get(\"lastSeenMs\",\"?\")}ms\")
"'
```

---

## Recovery Procedure

### Step 1: Re-enable NTP

```bash
ssh "${TARGET_NODE}" "
  sudo timedatectl set-ntp true
  echo 'NTP re-enabled'
  sleep 5  # wait for NTP to sync
  chronyc tracking
  echo \"Corrected time: \$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
"

HEAL_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "NTP restored at: ${HEAL_TIME}"
```

### Step 2: Verify clock sync

```bash
# Check that clock has converged with the reference
ssh "${TARGET_NODE}" "
  chronyc tracking | grep 'System time'
  # Expected: drift < 100ms
"

# Verify all nodes agree on time
for node in coordinator-1 coordinator-2 coordinator-3 worker-0; do
  TS=$(ssh "${node}" "date -u +%Y-%m-%dT%H:%M:%S.%3NZ")
  echo "${node}: ${TS}"
done
```

### Step 3: Verify service recovery

```bash
# Queries should succeed on the recovered node
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${TARGET_NODE}:8080/v1/sql" \
  -d '{"query": "SELECT current_timestamp"}')
echo "Query after NTP restore: HTTP ${STATUS}"
# Expected: 200

# Generate report
./tools/measure-recovery-time.sh report \
  --baseline /tmp/gd-006-baseline.json \
  --recovery /tmp/gd-006-recovery.json \
  --rto-coordinator 120
```

---

## Pass Criteria

| # | Criterion | How to verify |
|---|-----------|---------------|
| 1 | **Moderate skew tolerated** (≤ 5min) — queries succeed | HTTP 200 on skewed node with +2min clock |
| 2 | **Severe skew rejected** (> 5min) — JWT validation fails correctly | HTTP 401 on skewed node with +10min clock |
| 3 | **Non-skewed nodes unaffected** — majority coordinator continues serving | HTTP 200 on coordinator-1 throughout the scenario |
| 4 | **No cluster-wide outage** — skewed node failure is isolated | Cluster status shows remaining nodes HEALTHY |
| 5 | **Clock alert fires** — `node_clock_skew_detected` metric/alert fires | Alert visible in Grafana within 120s of severe skew |
| 6 | **Recovery is automatic** — NTP restore fixes the node without restart | HTTP 200 within 120s of re-enabling NTP |
| 7 | **JDBC lease not corrupted** — exactly 1 LEADER throughout | `/v1/cluster/status` shows 1 LEADER at all times |

---

## Observability Checkpoints

| What to monitor | Expected behavior |
|-----------------|-------------------|
| `node_clock_skew_seconds` metric | Spikes to 2.0 (scenario A) then 10.0 (scenario B) |
| Coordinator logs on skewed node | `WARN: clock skew detected: +Xs vs NTP reference` |
| JWKS validation errors | `HTTP 401: JWT clock skew rejected` in skewed node logs |
| Leader lease events | No unexpected leadership changes during scenario |
| Trace timestamps in Tempo | Spans from skewed node appear displaced in timeline |

---

## Cleanup Safety Check

```bash
# Verify NTP is active on all nodes
for node in coordinator-1 coordinator-2 coordinator-3 worker-0; do
  NTP_STATUS=$(ssh "${node}" "timedatectl status | grep 'NTP service'")
  echo "${node}: ${NTP_STATUS}"
done

# Verify clock skew is below 100ms across all nodes
for node in coordinator-1 coordinator-2 coordinator-3 worker-0; do
  DRIFT=$(ssh "${node}" "chronyc tracking | grep 'System time' | awk '{print \$4, \$5}'")
  echo "${node}: drift=${DRIFT}"
done

# Verify no lingering 401 errors in error logs
for node in coordinator-1 coordinator-2 coordinator-3; do
  RECENT_401=$(ssh "${node}" "
    grep '401\|Unauthorized\|clock skew' /var/log/pangrova/pangrova-coordinator.log | \
    grep \"\$(date -u +%Y-%m-%d)\" | tail -3
  ")
  echo "=== ${node} recent auth errors ==="
  echo "${RECENT_401:-none}"
done
```
