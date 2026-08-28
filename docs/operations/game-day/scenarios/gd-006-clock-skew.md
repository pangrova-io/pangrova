---
title: "GD-006 Clock Skew"
description: "Game-day scenario for coordinator and metadata clock skew"
---

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

1. **JWT token validation** — Pangrova's JWT parser applies a 60-second clock-skew tolerance; with a verifying node's clock ahead, tokens whose `exp` claim falls within the skew window appear expired
2. **JDBC lease acquisition** — lease timing is anchored on the metadata database clock (single authoritative clock), so skewed coordinator clocks cannot disagree on lease expiry; skew instead surfaces as heartbeat/renewal failures
3. **Distributed snapshot timestamps** — Iceberg/Paimon snapshot IDs include wall-clock time; large skew can cause ordering violations
4. **Trace correlation** — OpenTelemetry spans from skewed nodes appear out of order in the tracing backend

The system must:
- Reject JWT tokens whose validity window falls outside the 60-second clock-skew tolerance
- Survive moderate skew without data corruption
- Recover automatically when NTP is restored

---

## Blast Radius

| Resource | Impact |
|----------|--------|
| JWT authentication on skewed node | `HTTP 401` if the token's `exp` falls within the node's clock-ahead skew (beyond the 60s tolerance) |
| JDBC lease expiry | Evaluated on the metadata database clock — immune to coordinator skew; skew only disrupts renewals |
| Query timestamps | Iceberg snapshot timestamps may be disordered for rows written during skew |
| Trace correlation | Spans from skewed node appear displaced in time |
| Other nodes | Unaffected; queries route to non-skewed nodes |

---

## Prerequisites

```bash
# Identify target node (test on a STANDBY coordinator or a worker first)
TARGET_NODE="coordinator-2"

# Verify current clock synchronization status
ssh "${TARGET_NODE}" "chronyc tracking"
# Expected: "System time" drift < 1ms

# Verify NTP is running
ssh "${TARGET_NODE}" "systemctl is-active chronyd || systemctl is-active ntp || systemctl is-active ntpd"

# Confirm timedatectl works
ssh "${TARGET_NODE}" "timedatectl status"

# Verify the coordinator cluster is healthy (there is no cluster-status REST
# endpoint — check /v1/health on each node and the JDBC lease table)
for coord in coordinator-1 coordinator-2 coordinator-3; do
  curl -s -o /dev/null -w "${coord}: HTTP %{http_code}\n" "https://${coord}:8080/v1/health"
done
psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_ha -At \
  -c "SELECT holder_id, expires_at FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP;"

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

This simulates clock drift that exceeds the JWT parser's 60-second skew tolerance but stays inside the remaining lifetime of a long-lived token. A token with plenty of lifetime left should still be accepted.

### Step 1: Disable NTP and advance the clock

```bash
# Disable NTP so the clock can be manually adjusted
ssh "${TARGET_NODE}" "
  sudo timedatectl set-ntp false
  echo 'NTP disabled'
  timedatectl status | grep 'NTP service'
"

# Advance the clock by 2 minutes (beyond the 60s JWT tolerance, but within the
# remaining lifetime of a long-lived token)
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
  -X POST "https://${TARGET_NODE}:8080/v1/query/submit" \
  -d '{"query": "SELECT current_timestamp"}')
echo "Query on skewed node: HTTP ${STATUS}"
# Expected: 200 — the token's exp claim is still more than 2 minutes away.
# NOTE: with a freshly issued short-lived token (lifetime < skew), this returns 401.

# Check the coordinator log for auth/time-related warnings (there is no dedicated
# clock-skew log message; skew surfaces through JWT and lease errors)
ssh "${TARGET_NODE}" "
  grep -iE 'expired|token|401|lease' /var/log/pangrova/pangrova-coordinator.log | tail -10
"

# Check the leader still holds the lease (authoritative source: database clock)
psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_ha -At \
  -c "SELECT holder_id FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP;"
```

---

## Scenario B: Severe Clock Skew (Forward, 10 Minutes)

This simulates clock drift far beyond the 60-second JWT tolerance. With the node's clock 10 minutes ahead, any token with less than ~10 minutes of remaining lifetime fails `exp` validation and is rejected with `HTTP 401`.

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
  -X POST "https://${TARGET_NODE}:8080/v1/query/submit" \
  -d '{"query": "SELECT 1"}')
echo "Query on severely skewed node: HTTP ${SKEWED_STATUS}"
# Expected: 401 — with the node clock 10 minutes ahead, any token expiring within
# that window fails validation. Ensure the test token has < 10 minutes of remaining
# lifetime (mint a short-lived one from the IdP if necessary).

# Verify the error message is clear
curl -s \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${TARGET_NODE}:8080/v1/query/submit" \
  -d '{"query": "SELECT 1"}' | python3 -m json.tool
# Expected JSON error indicating the token expired (JWT validation failure)
```

### Step 3: Verify queries still work on correctly-clocked nodes

```bash
# coordinator-1 (not skewed) should still serve queries normally
NORMAL_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator-1:8080/v1/query/submit" \
  -d '{"query": "SELECT 1"}')
echo "Query on normal coordinator-1: HTTP ${NORMAL_STATUS}"
# Expected: 200
```

### Step 4: Observe JDBC lease behavior

```bash
# Monitor the JDBC lease during skew. Lease timing is anchored on the database
# server clock (single authoritative clock), so a skewed coordinator cannot
# silently extend an expired lease — at worst it fails to renew and steps down.
watch -n 3 'psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_ha -At \
  -c "SELECT holder_id, expires_at FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP;"'
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
  -X POST "https://${TARGET_NODE}:8080/v1/query/submit" \
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
| 1 | **Long-lived token tolerated** — a token with ample remaining lifetime survives +2min skew | HTTP 200 on skewed node with +2min clock |
| 2 | **Short-lived token rejected** — token expiring within the skew window fails beyond the 60s tolerance | HTTP 401 on skewed node with +10min clock |
| 3 | **Non-skewed nodes unaffected** — majority coordinator continues serving | HTTP 200 on coordinator-1 throughout the scenario |
| 4 | **No cluster-wide outage** — skewed node failure is isolated | `GET /v1/health` returns 200 on the remaining coordinators; `GET /api/workers` still lists all workers |
| 5 | **Clock alert fires** — `node_clock_skew_detected` metric/alert fires | Alert visible in Grafana within 120s of severe skew |
| 6 | **Recovery is automatic** — NTP restore fixes the node without restart | HTTP 200 within 120s of re-enabling NTP |
| 7 | **JDBC lease not corrupted** — exactly 1 LEADER throughout | `distributed_lease` table holds exactly one unexpired row at all times |

---

## Observability Checkpoints

| What to monitor | Expected behavior |
|-----------------|-------------------|
| Node exporter clock offset | Spikes to ~120s (scenario A) then ~600s (scenario B) |
| Coordinator logs on skewed node | JWT `401` / token-expired errors or lease renewal failures after skew injection |
| JWKS validation errors | `HTTP 401` token-expired entries in skewed node logs |
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
