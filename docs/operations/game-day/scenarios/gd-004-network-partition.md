---
title: "GD-004 Network Partition"
description: "Game-day scenario for a cluster network partition"
---

# GD-004: Network Partition (Split Brain)

| Field | Value |
|-------|-------|
| ID | GD-004 |
| Severity | **Critical** |
| Target component | Coordinator cluster (all nodes) |
| RTO target | < 30 seconds for minority partition to surrender leadership |
| Runbook | [network-partition-runbook.md](../runbooks/network-partition-runbook.md) |
| Prerequisites | Minimum 3-node Coordinator cluster; iptables available on all Coordinator nodes |
| Estimated duration | 30 minutes |

---

## Objective

Verify that the Coordinator cluster correctly handles a **network partition** that splits the cluster into two groups. A split-brain occurs when both groups believe they hold quorum and both elect a LEADER simultaneously. This is the most dangerous failure mode in the distributed consensus layer.

The JDBC lease mechanism must guarantee:
1. At most one active LEADER exists at any time — even during a partition
2. The minority partition (fewer than half the nodes) surrenders its lease within one lease period (30s)
3. The majority partition continues serving queries with minimal interruption
4. Workers connected to the minority coordinator are re-routed or receive retryable errors

---

## Blast Radius

| Resource | Impact |
|----------|--------|
| Queries on majority side | Retried automatically — minimal disruption (< 10s re-routing) |
| Queries on minority side | Fail with retryable `TRANSPORT-0001` (`TRANSPORT_CONNECTION_FAILED`) error codes |
| Metadata writes (DDL) | Rejected on minority side for the partition duration |
| Workers | Workers contact multiple coordinators; re-register with the majority LEADER |
| Data integrity | **Must not be compromised** — no dual-write, no split commit |

**Safety line**: If you observe two nodes reporting `role=LEADER` simultaneously, the scenario has already revealed a critical defect. Stop immediately and escalate — do not attempt self-healing. See runbook §Emergency.

---

## Prerequisites

```bash
# Verify 3-node coordinator cluster is up (no cluster-status REST API exists yet —
# check health on each node and the JDBC lease table for the current leader)
for coord in coordinator-1 coordinator-2 coordinator-3; do
  curl -s -o /dev/null -w "${coord}: HTTP %{http_code}\n" "http://${coord}:8080/v1/health"
done

# Verify initial leader from the HA lease table
LEADER_ID=$(psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_meta -At \
  -c "SELECT holder_id FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP LIMIT 1;")
echo "Current leader (lease holder): ${LEADER_ID}"

# Confirm iptables is available on coordinator nodes
ssh coordinator-1 "sudo iptables -L OUTPUT -n | head -5"
ssh coordinator-2 "sudo iptables -L OUTPUT -n | head -5"
ssh coordinator-3 "sudo iptables -L OUTPUT -n | head -5"

# Verify baseline health (all workers ACTIVE)
curl -s http://coordinator-1:8080/api/workers | python3 -c "
import json, sys
workers = json.load(sys.stdin)
print(sum(1 for w in workers if w['healthState'] == 'ACTIVE'), '/', len(workers), 'workers ACTIVE')
"
```

---

## Baseline Recording

Record 60 seconds of healthy-state metrics before injecting the fault:

```bash
ENDPOINT="https://coordinator-1:8080"
TOKEN="${PANGROVA_TOKEN}"

../tools/measure-recovery-time.sh baseline \
  --endpoint "${ENDPOINT}" \
  --token    "${TOKEN}" \
  --query    "SHOW CATALOGS" \
  --duration 60s \
  --output   /tmp/gd-004-baseline.json

echo "Baseline complete. Proceeding to fault injection."
```

---

## Fault Injection Procedure

### Step 1: Identify the leader node

```bash
# Record which nodes are coordinator-1, coordinator-2, coordinator-3
# and their actual IPs for iptables rules
COORD1_IP=$(dig +short coordinator-1.internal)
COORD2_IP=$(dig +short coordinator-2.internal)
COORD3_IP=$(dig +short coordinator-3.internal)

echo "Coordinator IPs: ${COORD1_IP}, ${COORD2_IP}, ${COORD3_IP}"
```

### Step 2: Start recovery measurement

Open a second terminal and start measuring immediately before partition:

```bash
../tools/measure-recovery-time.sh measure \
  --endpoint "https://coordinator-1:8080" \
  --token    "${PANGROVA_TOKEN}" \
  --timeout  120s \
  --output   /tmp/gd-004-recovery.json
```

### Step 3: Inject partition — isolate coordinator-1 from coordinator-2 and coordinator-3

This creates a partition where coordinator-1 is a minority of 1, and coordinator-2 + coordinator-3 form the majority of 2. If coordinator-1 is the LEADER, it must surrender leadership.

```bash
# On coordinator-1: block outbound and inbound traffic to/from the other coordinators
# This simulates coordinator-1 being network-isolated
ssh coordinator-1 "
  sudo iptables -A OUTPUT -d ${COORD2_IP} -j DROP
  sudo iptables -A OUTPUT -d ${COORD3_IP} -j DROP
  sudo iptables -A INPUT  -s ${COORD2_IP} -j DROP
  sudo iptables -A INPUT  -s ${COORD3_IP} -j DROP
  echo 'Partition active on coordinator-1'
"

# Record injection timestamp
INJECT_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Partition injected at: ${INJECT_TIME}"
```

### Step 4: Observe cluster behavior (do not intervene for 60 seconds)

```bash
# Watch the majority side for leader election (roles come from coordinator logs
# and the JDBC lease table — there is no cluster-status REST endpoint)
watch -n 2 'ssh coordinator-2 "grep event=ha_started /var/log/pangrova/coordinator.log | tail -1"'

# In another terminal, watch coordinator-1 (minority side) behavior
watch -n 2 'ssh coordinator-1 "grep event=ha_started /var/log/pangrova/coordinator.log | tail -1" || echo "UNREACHABLE"'
```

**Expected observations (within 30s of injection):**
- coordinator-1 fails to renew its lease and steps down to `STANDBY` (log: `role=STANDBY`)
- coordinator-2 or coordinator-3 wins the lease and logs `event=ha_started ... role=LEADER`
- At no point should two nodes simultaneously hold an unexpired lease
- Workers re-register with the new leader on the majority side

### Step 5: Run a workload on majority side during partition

```bash
# Execute 10 sequential queries against the majority coordinator
for i in $(seq 1 10); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST "https://coordinator-2:8080/v1/query/submit" \
    -d '{"query": "SELECT 1"}')
  echo "Query ${i}: HTTP ${STATUS}"
  sleep 3
done
```

### Step 6: Attempt a query on the minority coordinator-1 side

```bash
# This should return an error — coordinator-1 must not serve queries as LEADER
MINORITY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator-1:8080/v1/query/submit" \
  -d '{"query": "SELECT 1"}')
echo "Minority coordinator response: HTTP ${MINORITY_STATUS}"
# Expected: 503 Service Unavailable or 307 redirect to majority leader
```

---

## Partition Healing

After observing the cluster behavior for at least 60 seconds, heal the partition:

```bash
# Remove iptables rules on coordinator-1 to restore connectivity
ssh coordinator-1 "
  sudo iptables -D OUTPUT -d ${COORD2_IP} -j DROP
  sudo iptables -D OUTPUT -d ${COORD3_IP} -j DROP
  sudo iptables -D INPUT  -s ${COORD2_IP} -j DROP
  sudo iptables -D INPUT  -s ${COORD3_IP} -j DROP
  echo 'Partition healed on coordinator-1'
"

HEAL_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Partition healed at: ${HEAL_TIME}"
```

---

## Recovery Verification

```bash
# 1. Verify exactly one LEADER in the cluster (authoritative: JDBC lease table)
LEADER_COUNT=$(psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_meta -At \
  -c "SELECT COUNT(*) FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP;")
echo "Active lease holders: ${LEADER_COUNT}"
[[ "${LEADER_COUNT}" -eq 1 ]] && echo "PASS: exactly 1 leader" || echo "CRITICAL: not exactly 1 leader!"

# 2. Verify all workers are ACTIVE
WORKER_COUNT=$(curl -s http://coordinator-1:8080/api/workers \
  | python3 -c "
import json, sys
workers = json.load(sys.stdin)
healthy = [w for w in workers if w['healthState'] == 'ACTIVE']
print(f'{len(healthy)}/{len(workers)} workers ACTIVE')
")
echo "$WORKER_COUNT"

# 3. Verify coordinator-1 rejoined as STANDBY (not attempting to steal leadership)
NODE1_ROLE=$(ssh coordinator-1 "grep 'event=ha_started' /var/log/pangrova/coordinator.log | tail -1" \
  | grep -o 'role=[A-Z]*' | cut -d= -f2)
echo "coordinator-1 role after heal: ${NODE1_ROLE}"
# Expected: STANDBY

# 4. Query succeeds on all coordinators
for coord in coordinator-1 coordinator-2 coordinator-3; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST "https://${coord}:8080/v1/query/submit" \
    -d '{"query": "SELECT 1"}')
  echo "${coord}: HTTP ${STATUS}"
done

# 5. Generate recovery report
../tools/measure-recovery-time.sh report \
  --baseline        /tmp/gd-004-baseline.json \
  --recovery        /tmp/gd-004-recovery.json \
  --rto-coordinator 30
```

---

## Pass Criteria

| # | Criterion | How to verify |
|---|-----------|---------------|
| 1 | **No split-brain** — at most 1 LEADER at any instant during partition | `distributed_lease` table never holds more than one unexpired lease; HA logs show ≤ 1 `role=LEADER` at a time |
| 2 | **Minority surrenders within 30s** — coordinator-1 stops acting as LEADER within one lease period | Logs: `role=LEADER` followed by `role=STANDBY` within 30s of partition |
| 3 | **Majority continues serving** — queries to coordinator-2 succeed during partition | `HTTP 200` on all 10 probe queries to coordinator-2 |
| 4 | **Minority rejects leadership** — coordinator-1 does not serve write queries as LEADER | Minority query returns `HTTP 503` or `503` during partition |
| 5 | **Full recovery after heal** — coordinator-1 rejoins as STANDBY, all workers ACTIVE | Lease table shows 1 holder; coordinator-1 log shows `role=STANDBY`; all workers ACTIVE in `GET /api/workers` |
| 6 | **No data corruption** — catalog remains consistent after heal | `SHOW CATALOGS` returns the same catalogs as the baseline |
| 7 | **Error codes are retryable** — queries during partition return retryable error codes, not permanent failures | Error response body contains `"retryable": true` |

---

## Fail Criteria (Stop Immediately)

- Two nodes simultaneously report `role=LEADER` at any point
- A DDL statement executes on both sides of the partition
- Coordinator process crashes (as opposed to gracefully stepping down)
- Workers commit data to both sides of the partition

If any fail criterion is met, **do not heal the partition manually**. Alert the team and follow the emergency escalation path in the runbook.

---

## Observability Checkpoints

During the scenario, verify the following in Grafana:

| Dashboard panel | What to look for |
|-----------------|-----------------|
| Coordinator leader count | Must never exceed 1 |
| JDBC lease holder | Changes from coordinator-1 to coordinator-2 or 3 during partition |
| Worker registration count per coordinator | Drops to 0 on minority coordinator |
| Query error rate (majority side) | Spike < 15s, then returns to 0% |
| `coordinator_leader_lost` alert | Fires within 15s of partition injection |

---

## Cleanup

After the scenario is complete and verified:

```bash
# Verify iptables is clean on all coordinators (no lingering DROP rules)
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} iptables ==="
  ssh "${coord}" "sudo iptables -L OUTPUT -n --line-numbers | grep DROP || echo 'clean'"
  ssh "${coord}" "sudo iptables -L INPUT -n --line-numbers | grep DROP || echo 'clean'"
done

# If any rules remain, flush them
# ssh coordinator-1 "sudo iptables -F OUTPUT && sudo iptables -F INPUT"

# Verify cluster is fully healthy before next scenario
curl -s http://coordinator-1:8080/v1/health
```
