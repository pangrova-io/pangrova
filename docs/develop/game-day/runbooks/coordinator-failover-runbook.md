# Runbook: Coordinator Failover

**Use this runbook when:** The active Coordinator is unresponsive, the cluster has no LEADER, or a scheduled failover is needed for maintenance.

| Field | Value |
|-------|-------|
| Scenario | [GD-001: Coordinator Failover](../scenarios/gd-001-coordinator-failover.md) |
| Severity | Critical |
| Primary risk | All new queries blocked until a new LEADER is elected |
| Expected resolution | Automatic within 30s via JDBC lease expiry; manual intervention only if automatic fails |

---

## Decision Tree

```
Coordinator appears down?
  │
  ├─► Is /v1/health returning HTTP 200 on any coordinator?
  │     │
  │     ├─► YES on coordinator-2 or 3 ──► Check if a LEADER exists → Step 2
  │     │
  │     └─► NO on all coordinators ──► Full cluster outage → escalate immediately
  │
  └─► Is this a scheduled maintenance failover?
        │
        ├─► YES ──► Begin graceful failover → Step 1 (planned variant)
        │
        └─► NO  ──► Unplanned failure → Step 1 (emergency variant)
```

---

## Step 1: Confirm Failure

```bash
# Check health endpoint on the suspected failed coordinator
curl -s --max-time 5 https://coordinator-active:8080/v1/health || echo "CONFIRMED DOWN"

# Check process status on the failed node
ssh coordinator-active "systemctl status pangrova-coordinator"

# Check system logs for OOM kill or native crash
ssh coordinator-active "
  journalctl -u pangrova-coordinator --since '10 min ago' | tail -50
  dmesg | grep -i 'oom\|killed\|out of memory' | tail -10
"

# Check for heap dump (indicates OOM)
ssh coordinator-active "ls -lh ${PANGROVA_LOG_DIR:-/var/log/pangrova}/heapdump-coordinator*.hprof 2>/dev/null && echo 'Heap dump found'"
```

---

## Step 2: Verify Standby Promotion

The JDBC lease should cause the standby to promote automatically within `lease-duration` (30s).

```bash
# Poll all coordinators for role change
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} ==="
  curl -s --max-time 3 "https://${coord}:8080/v1/cluster/status" | \
    python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for n in data.get('nodes', []):
        print(f\"  {n['host']}: role={n['role']}\")
except:
    print('  UNREACHABLE')
" 2>&1 || echo "  UNREACHABLE"
done

# Specifically check for a LEADER
LEADER_HOST=$(curl -s https://coordinator-2:8080/v1/cluster/status 2>/dev/null | \
  python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    leaders = [n for n in data['nodes'] if n['role'] == 'LEADER']
    print(leaders[0]['host']) if leaders else print('')
except:
    print('')
")
echo "Current leader: '${LEADER_HOST}'"
```

If standby has **not** promoted after 60 seconds, force election manually:

```bash
# Attempt force-election on each remaining coordinator in turn
for coord in coordinator-2 coordinator-3; do
  echo "Attempting force-election on ${coord}..."
  RESULT=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "https://${coord}:8080/v1/cluster/force-election" \
    -H "Authorization: Bearer ${PANGROVA_ADMIN_TOKEN}")
  echo "  Response: HTTP ${RESULT}"
  [[ "${RESULT}" == "200" ]] && echo "  Accepted" && break
  sleep 10
done

# Wait and verify
sleep 15
curl -s https://coordinator-2:8080/v1/cluster/status | \
  python3 -c "import json,sys; nodes=json.load(sys.stdin)['nodes']; [print(f'{n[\"host\"]}: {n[\"role\"]}') for n in nodes]"
```

---

## Step 3: Validate Cluster Health

```bash
NEW_LEADER="coordinator-2"  # replace with actual new leader

# Run a simple query to confirm the new leader is serving
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${NEW_LEADER}:8080/v1/sql" \
  -d '{"query": "SELECT 1"}')
echo "Simple query: HTTP ${STATUS}"  # Expected: 200

# Confirm worker count matches expected
curl -s "https://${NEW_LEADER}:8080/v1/cluster/workers" | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
healthy = [w for w in workers if w['status'] == 'HEALTHY']
print(f'{len(healthy)}/{len(workers)} workers HEALTHY')
"

# Check for orphaned queries from the failed coordinator
curl -s "https://${NEW_LEADER}:8080/v1/admin/queries/orphaned" | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'Orphaned queries: {len(data.get(\"queries\", []))}')
" 2>/dev/null || echo "Orphaned query check: endpoint not available"

# Test DDL to confirm metadata access
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://${NEW_LEADER}:8080/v1/sql" \
  -d '{"query": "SHOW SCHEMAS"}')
echo "SHOW SCHEMAS: HTTP ${STATUS}"  # Expected: 200
```

---

## Step 4: Client Notification

```bash
# If using a load balancer VIP — health check auto-removes the failed coordinator
# Verify the LB has already removed the failed backend:
# aws elbv2 describe-target-health --target-group-arn <arn>  (AWS)
# gcloud compute backend-services get-health <name>          (GCP)

# If clients use direct coordinator URLs, update DNS or notify clients:
echo "Notify: clients connecting directly to ${FAILED_COORDINATOR}:8080 must reconnect to ${NEW_LEADER}:8080"
```

---

## Step 5: Root Cause Investigation

```bash
FAILED_COORDINATOR="coordinator-active"

# Collect crash artifacts before restarting the process
ssh "${FAILED_COORDINATOR}" "
  TIMESTAMP=\$(date +%Y%m%d-%H%M%S)
  ARCHIVE=/tmp/coordinator-crash-\${TIMESTAMP}.tar.gz
  tar czf \"\${ARCHIVE}\" \
    /var/log/pangrova/pangrova-coordinator*.log \
    /var/log/pangrova/gc-coordinator.log \
    /var/log/pangrova/heapdump-coordinator*.hprof 2>/dev/null \
    /var/log/syslog 2>/dev/null || true
  echo \"Artifacts archived: \${ARCHIVE}\"
"

# Classify the failure
ssh "${FAILED_COORDINATOR}" "
  echo '=== OOM check ==='
  dmesg | grep -i 'out of memory\|oom\|killed process' | tail -10

  echo '=== GC log tail ==='
  tail -20 /var/log/pangrova/gc-coordinator.log 2>/dev/null || echo 'No GC log'

  echo '=== Error log tail ==='
  tail -30 /var/log/pangrova/pangrova-coordinator-error.log 2>/dev/null || echo 'No error log'
"
```

---

## Step 6: Restore Redundancy

```bash
FAILED_COORDINATOR="coordinator-active"

# Start the previously-failed coordinator as a new FOLLOWER
ssh "${FAILED_COORDINATOR}" "systemctl start pangrova-coordinator"

# Wait for it to register
sleep 30

# Verify it joined as FOLLOWER (not competing for leadership)
ROLE=$(curl -s "https://${FAILED_COORDINATOR}:8080/v1/cluster/status" | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
for n in data['nodes']:
    if '${FAILED_COORDINATOR}' in n['host']:
        print(n['role'])
")
echo "${FAILED_COORDINATOR} role: ${ROLE}"
# Expected: FOLLOWER

# Verify full cluster redundancy restored
curl -s "https://${NEW_LEADER}:8080/v1/cluster/status" | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
nodes = data['nodes']
print(f'Cluster: {len(nodes)} coordinators — {len([n for n in nodes if n[\"role\"]==\"LEADER\"])} LEADER, {len([n for n in nodes if n[\"role\"]==\"FOLLOWER\"])} FOLLOWER')
"
```

---

## Recovery Verification Checklist

- [ ] Exactly 1 LEADER elected; all other coordinators are FOLLOWER
- [ ] All workers registered with the new LEADER as HEALTHY
- [ ] Simple SQL query returns HTTP 200 on the new LEADER
- [ ] DDL (`SHOW SCHEMAS`) returns HTTP 200
- [ ] Previously-failed coordinator rejoined as FOLLOWER
- [ ] No orphaned queries with leaked resources
- [ ] Monitoring alerts cleared in Grafana/Alertmanager

---

## Escalation Criteria

| Condition | Action |
|-----------|--------|
| No LEADER after 60s + force-election failed | Page on-call SRE lead |
| Both primary and standby coordinators down | Page engineering manager + SRE immediately |
| Failed coordinator in crash loop (3+ restarts in 10 min) | Page on-call SRE lead; collect heap dumps |
| Data corruption detected (inconsistent catalog) | Page data platform lead; freeze writes |

---

## Post-Incident

1. File incident report within 24 hours using the post-mortem template
2. Update this runbook if any step was unclear or incorrect
3. Review coordinator heap configuration if OOM was the cause
4. Review GC logs for pause time pattern preceding the crash
5. Confirm `coordinator_leader_lost` alert fired within 15s of failure
