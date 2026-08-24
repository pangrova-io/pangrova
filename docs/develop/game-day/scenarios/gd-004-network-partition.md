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
| Queries on minority side | Fail with retryable `COORDINATOR_UNAVAILABLE` error codes |
| Metadata writes (DDL) | Rejected on minority side for the partition duration |
| Workers | Workers contact multiple coordinators; re-register with the majority LEADER |
| Data integrity | **Must not be compromised** — no dual-write, no split commit |

**Safety line**: If you observe two nodes reporting `role=LEADER` simultaneously, the scenario has already revealed a critical defect. Stop immediately and escalate — do not attempt self-healing. See runbook §Emergency.

---

## Prerequisites

```bash
# Verify 3-node coordinator cluster is up
curl -s https://coordinator-1:8080/v1/cluster/status | python3 -m json.tool
# Expected: exactly one node with role=LEADER, others role=FOLLOWER

# Verify initial leader
LEADER_NODE=$(curl -s https://coordinator-1:8080/v1/cluster/status \
  | python3 -c "import json,sys; nodes=json.load(sys.stdin)['nodes']; \
    [print(n['host']) for n in nodes if n['role']=='LEADER']")
echo "Current leader: ${LEADER_NODE}"

# Confirm iptables is available on coordinator nodes
ssh coordinator-1 "sudo iptables -L OUTPUT -n | head -5"
ssh coordinator-2 "sudo iptables -L OUTPUT -n | head -5"
ssh coordinator-3 "sudo iptables -L OUTPUT -n | head -5"

# Verify baseline health (all workers HEALTHY)
curl -s https://coordinator-1:8080/v1/cluster/workers | python3 -m json.tool | grep -c '"status": "HEALTHY"'
```

---

## Baseline Recording

Record 60 seconds of healthy-state metrics before injecting the fault:

```bash
ENDPOINT="https://coordinator-1:8080"
TOKEN="${PANGROVA_TOKEN}"

./tools/measure-recovery-time.sh baseline \
  --endpoint "${ENDPOINT}" \
  --token    "${TOKEN}" \
  --query    "SELECT count(*) FROM system.pangrova_catalog.tables" \
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
./tools/measure-recovery-time.sh measure \
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
# Watch coordinator-2 (majority side) for leader election
watch -n 2 'curl -s https://coordinator-2:8080/v1/cluster/status \
  | python3 -m json.tool | grep -E "\"role\"|\"host\""'

# In another terminal, watch coordinator-1 (minority side) behavior
watch -n 2 'curl -s https://coordinator-1:8080/v1/cluster/status \
  | python3 -m json.tool | grep -E "\"role\"|\"host\"" || echo "UNREACHABLE"'
```

**Expected observations (within 30s of injection):**
- coordinator-1 detects lease expiry and transitions to `CANDIDATE` then `FOLLOWER` or stops serving
- coordinator-2 or coordinator-3 wins lease and reports `role=LEADER`
- At no point should two nodes simultaneously report `role=LEADER`
- Workers re-register with the new leader on the majority side

### Step 5: Run a workload on majority side during partition

```bash
# Execute 10 sequential queries against the majority coordinator
for i in $(seq 1 10); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST "https://coordinator-2:8080/v1/sql" \
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
  -X POST "https://coordinator-1:8080/v1/sql" \
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
# 1. Verify exactly one LEADER in the cluster
curl -s https://coordinator-1:8080/v1/cluster/status | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
leaders = [n for n in data['nodes'] if n['role'] == 'LEADER']
followers = [n for n in data['nodes'] if n['role'] == 'FOLLOWER']
print(f'Leaders: {len(leaders)} — {[n[\"host\"] for n in leaders]}')
print(f'Followers: {len(followers)}')
assert len(leaders) == 1, 'CRITICAL: not exactly 1 leader!'
print('PASS: exactly 1 leader')
"

# 2. Verify all workers are HEALTHY
WORKER_COUNT=$(curl -s https://coordinator-1:8080/v1/cluster/workers \
  | python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
healthy = [w for w in workers if w['status'] == 'HEALTHY']
print(f'{len(healthy)}/{len(workers)} workers HEALTHY')
")
echo "$WORKER_COUNT"

# 3. Verify coordinator-1 rejoined as FOLLOWER (not attempting to steal leadership)
NODE1_ROLE=$(curl -s https://coordinator-1:8080/v1/cluster/status \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for n in data['nodes']:
    if 'coordinator-1' in n['host']:
        print(n['role'])
")
echo "coordinator-1 role after heal: ${NODE1_ROLE}"
# Expected: FOLLOWER

# 4. Query succeeds on all coordinators
for coord in coordinator-1 coordinator-2 coordinator-3; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST "https://${coord}:8080/v1/sql" \
    -d '{"query": "SELECT 1"}')
  echo "${coord}: HTTP ${STATUS}"
done

# 5. Generate recovery report
./tools/measure-recovery-time.sh report \
  --baseline        /tmp/gd-004-baseline.json \
  --recovery        /tmp/gd-004-recovery.json \
  --rto-coordinator 30
```

---

## Pass Criteria

| # | Criterion | How to verify |
|---|-----------|---------------|
| 1 | **No split-brain** — at most 1 LEADER at any instant during partition | `/v1/cluster/status` on all nodes shows ≤ 1 LEADER simultaneously |
| 2 | **Minority surrenders within 30s** — coordinator-1 stops acting as LEADER within one lease period | Logs: `role=LEADER → role=FOLLOWER` within 30s of partition |
| 3 | **Majority continues serving** — queries to coordinator-2 succeed during partition | `HTTP 200` on all 10 probe queries to coordinator-2 |
| 4 | **Minority rejects leadership** — coordinator-1 does not serve write queries as LEADER | Minority query returns `HTTP 503` or `503` during partition |
| 5 | **Full recovery after heal** — coordinator-1 rejoins as FOLLOWER, all workers HEALTHY | `/v1/cluster/status` shows 1 LEADER, N-1 FOLLOWERS; all workers HEALTHY |
| 6 | **No data corruption** — catalog remains consistent after heal | `SELECT count(*) FROM system.pangrova_catalog.tables` returns same count as baseline |
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
curl -s https://coordinator-1:8080/v1/health
```
