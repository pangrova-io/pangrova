# Runbook: Network Partition (GD-004)

**Use this runbook when:** A network partition has split the Coordinator cluster, or when two Coordinator nodes both claim to be LEADER (split-brain).

| Field | Value |
|-------|-------|
| Scenario | [GD-004: Network Partition (Split Brain)](../scenarios/gd-004-network-partition.md) |
| Severity | Critical |
| Primary risk | Dual-leader split-brain → potential dual-write and metadata corruption |
| Expected resolution | Automatic (minority surrenders within 30s); manual intervention only if automatic fails |

---

## Decision Tree

```
Network partition suspected?
  │
  ├─► Is the cluster status reachable on any coordinator?
  │     │
  │     ├─► YES ──► Step 1: Verify partition scope
  │     │
  │     └─► NO  ──► Total outage (not just partition). Use coordinator-failover-runbook instead.
  │
  └─► Are two coordinators both reporting role=LEADER?
        │
        ├─► YES ──► CRITICAL: Split-brain active → skip to §Emergency: Split-Brain
        │
        └─► NO  ──► Partition with correct leader isolation → Step 2: Wait 30s, then verify
```

---

## Step 1: Confirm the Partition

```bash
# Check each coordinator's view of the cluster
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} ==="
  curl -s --max-time 3 "https://${coord}:8080/v1/cluster/status" | \
    python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for n in data.get('nodes', []):
        print(f\"  {n['host']}: role={n['role']} reachable={n.get('reachable', '?')}\")
except:
    print('  UNREACHABLE or ERROR')
" 2>&1 || echo "  UNREACHABLE"
done
```

**Interpret results:**

| Pattern | Interpretation |
|---------|---------------|
| Some coordinators show full cluster, some show only themselves | Classic partition: full-cluster views are the majority |
| All coordinators show a complete cluster with 1 LEADER | No partition — investigate other causes |
| Any coordinator shows `role=LEADER` while isolated | Potential split-brain — escalate immediately |

---

## Step 2: Wait for Automatic Recovery (0–30s)

The JDBC lease mechanism should automatically resolve the partition:
- The isolated minority coordinator will fail to renew its lease within `lease-duration` (30s)
- It will step down to `CANDIDATE` or `FOLLOWER`
- The majority side will elect a new LEADER if needed

**Do not intervene during this window.** Watch the cluster status:

```bash
# Live watch: poll all coordinators every 3 seconds for 60 seconds
END_TIME=$(( $(date +%s) + 60 ))
while [[ $(date +%s) -lt $END_TIME ]]; do
  echo "=== $(date -u +%H:%M:%S) ==="
  for coord in coordinator-1 coordinator-2 coordinator-3; do
    ROLE=$(curl -s --max-time 2 "https://${coord}:8080/v1/cluster/status" | \
      python3 -c "
import json,sys
try:
    nodes = json.load(sys.stdin)['nodes']
    for n in nodes:
        if '${coord}' in n['host']:
            print(n['role'])
except:
    print('ERR')
" 2>/dev/null || echo "DOWN")
    echo "  ${coord}: ${ROLE}"
  done
  sleep 3
done
```

**Expected resolution:** Within 30s, the cluster converges to exactly 1 LEADER and 2 FOLLOWERs.

---

## Step 3: Verify Single Leader

```bash
# Count leaders across all coordinators
LEADER_COUNT=$(for coord in coordinator-1 coordinator-2 coordinator-3; do
  curl -s --max-time 3 "https://${coord}:8080/v1/cluster/status" | \
    python3 -c "
import json, sys
try:
    nodes = json.load(sys.stdin)['nodes']
    leaders = [n for n in nodes if n['role'] == 'LEADER']
    print(len(leaders))
except:
    print(0)
" 2>/dev/null || echo 0
done | sort -rn | head -1)

echo "Maximum leader count observed: ${LEADER_COUNT}"

if [[ "${LEADER_COUNT}" -eq 1 ]]; then
  echo "PASS: Exactly 1 leader. Cluster is healthy."
elif [[ "${LEADER_COUNT}" -eq 0 ]]; then
  echo "WARN: No leader elected. May need to force election — see Step 5."
else
  echo "CRITICAL: Split-brain detected! ${LEADER_COUNT} leaders — see §Emergency."
fi
```

---

## Step 4: Verify Query Service Restored

```bash
# Find the active leader and verify queries succeed
LEADER_HOST=$(curl -s "https://coordinator-1:8080/v1/cluster/status" | \
  python3 -c "
import json, sys
nodes = json.load(sys.stdin)['nodes']
leaders = [n for n in nodes if n['role'] == 'LEADER']
print(leaders[0]['host']) if leaders else print('')
")

echo "Active leader: ${LEADER_HOST}"

if [[ -n "${LEADER_HOST}" ]]; then
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST "https://${LEADER_HOST}/v1/sql" \
    -d '{"query": "SELECT 1"}')
  echo "Query to leader: HTTP ${STATUS}"
fi
```

---

## Step 5: Force Leader Election (If No Leader After 60s)

Use only if the cluster has no LEADER 60 seconds after the partition healed, and automatic election has not occurred.

```bash
# Force election on the coordinator with the freshest lease timestamp
# (try each in turn until one succeeds)
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "Attempting force-election on ${coord}..."
  RESULT=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "https://${coord}:8080/v1/cluster/force-election" \
    -H "Authorization: Bearer ${PANGROVA_ADMIN_TOKEN}")
  echo "  Response: HTTP ${RESULT}"
  if [[ "${RESULT}" == "200" ]]; then
    echo "  Force-election accepted by ${coord}"
    break
  fi
  sleep 5
done

# Wait 15s and verify a leader was elected
sleep 15
curl -s https://coordinator-1:8080/v1/cluster/status | python3 -m json.tool | grep '"role"'
```

---

## Step 6: Verify Workers Re-Registered

After a partition, workers on the minority side may have lost their coordinator connection. Verify all workers are HEALTHY:

```bash
curl -s https://coordinator-1:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
healthy   = [w for w in workers if w['status'] == 'HEALTHY']
unhealthy = [w for w in workers if w['status'] != 'HEALTHY']
print(f'HEALTHY: {len(healthy)}/{len(workers)}')
for w in unhealthy:
    print(f'  NOT HEALTHY: {w[\"host\"]} status={w[\"status\"]}')
"

# If workers are stuck in DISCONNECTED state, trigger re-registration check
if [[ $? -ne 0 ]]; then
  # Workers re-register automatically; wait up to 60s
  echo "Waiting for worker re-registration (up to 60s)..."
  sleep 60
  curl -s https://coordinator-1:8080/v1/cluster/workers | python3 -m json.tool
fi
```

---

## Step 7: Root Cause Investigation

After the cluster is stable, investigate the partition cause:

```bash
# Check for iptables rules that may have caused the partition
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} iptables ==="
  ssh "${coord}" "sudo iptables -L OUTPUT -n --line-numbers | grep DROP || echo 'no DROP rules'"
  ssh "${coord}" "sudo iptables -L INPUT -n --line-numbers | grep DROP || echo 'no DROP rules'"
done

# Check network interface status
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} network ==="
  ssh "${coord}" "ip link show | grep -E 'state|mtu'"
done

# Check coordinator logs for partition detection messages
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} partition-related log entries ==="
  ssh "${coord}" "
    grep -E 'lease.*expired|leader.*lost|FOLLOWER|partition|heartbeat.*failed' \
      /var/log/pangrova/pangrova-coordinator.log | tail -20
  "
done
```

---

## Step 8: Restore Redundancy

After confirming the isolated coordinator has rejoined as FOLLOWER and all workers are HEALTHY, verify the cluster is back to full redundancy:

```bash
# Confirm 3-node coordinator cluster
curl -s https://coordinator-1:8080/v1/cluster/status | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
nodes = data['nodes']
leaders  = [n for n in nodes if n['role'] == 'LEADER']
followers = [n for n in nodes if n['role'] == 'FOLLOWER']
print(f'Coordinators: {len(nodes)} total — {len(leaders)} LEADER, {len(followers)} FOLLOWER')
print('PASS: Full 3-node redundancy restored' if len(nodes) == 3 else 'WARN: Not at full redundancy')
"
```

---

## Emergency: Split-Brain Detected

**Condition:** Two coordinators simultaneously report `role=LEADER`.

This is a critical defect that must not be resolved by simply restarting one coordinator — doing so could cause data loss if both sides accepted DDL commits.

**Immediate actions:**

```bash
# 1. FREEZE all DDL operations — alert the team immediately
echo "SPLIT-BRAIN DETECTED: Escalating to on-call lead"

# 2. Capture the state of both "leaders"
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} full cluster status at $(date -u) ==="
  curl -s "https://${coord}:8080/v1/cluster/status" | python3 -m json.tool
done > /tmp/split-brain-state-$(date +%Y%m%d-%H%M%S).json

# 3. Capture the PostgreSQL lease table directly
psql "jdbc:postgresql://${PANGROVA_DB_HOST}:5432/pangrova_ha" \
  -U pangrova \
  -c "SELECT * FROM coordinator_leases ORDER BY acquired_at;" | \
  tee /tmp/split-brain-lease-state.txt

# 4. Stop the newer "leader" (whichever acquired the lease more recently)
# DO NOT restart — stop only, to prevent further state divergence
# ssh <newer-leader> "sudo systemctl stop pangrova-coordinator"

# 5. Escalate — do not attempt automated recovery
echo "Manual investigation required. Do not restart any coordinator."
echo "See post-mortem template for investigation checklist."
```

**Escalation path:** On-call lead → Data Platform lead → Pangrova core team
