---
title: "Coordinator Failover Runbook"
description: "Diagnose and recover from coordinator failover"
---

# Runbook: Coordinator Failover

**Use this runbook when:** The active Coordinator is unresponsive, the cluster has no LEADER, or a scheduled failover is needed for maintenance.

| Field | Value |
|-------|-------|
| Scenario | [GD-001: Coordinator Failover](../scenarios/gd-001-coordinator-failover.md) |
| Severity | Critical |
| Primary risk | All new queries blocked until a new LEADER is elected |
| Expected resolution | Automatic within 30s via JDBC lease expiry; manual intervention only if automatic fails |

> **Endpoint note:** There is no cluster-status REST API today
> (`GET /v1/cluster/status` is not yet implemented). HA role is determined from
> coordinator logs (`event=ha_started ... role=LEADER|STANDBY`) and from the
> JDBC `distributed_lease` table; worker health from `GET /api/workers`.

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
curl -s --max-time 5 http://coordinator-active:8080/v1/health || echo "CONFIRMED DOWN"

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

The JDBC lease should cause the standby to promote automatically within the lease
timeout (default 30s).

```bash
# Check each coordinator's latest HA role from its logs
for coord in coordinator-1 coordinator-2 coordinator-3; do
  echo "=== ${coord} ==="
  ssh "${coord}" "grep 'event=ha_started' /var/log/pangrova/pangrova-coordinator.log 2>/dev/null | tail -1" \
    || echo "  UNREACHABLE"
done

# The authoritative leader is the current lease holder in the HA database
LEADER_ID=$(psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_ha -At \
  -c "SELECT holder_id FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP LIMIT 1;")
echo "Current leader (lease holder): '${LEADER_ID}'"
```

If no standby has promoted after 60 seconds, force election manually. There is no
force-election REST API — election is lease-driven, so restart a standby to make
it contend for the (expired) lease:

```bash
for coord in coordinator-2 coordinator-3; do
  echo "Attempting election via restart on ${coord}..."
  ssh "${coord}" "sudo systemctl restart pangrova-coordinator"
  sleep 20
  ssh "${coord}" "grep 'event=ha_started' /var/log/pangrova/pangrova-coordinator.log | tail -1" \
    | grep -q "role=LEADER" && echo "  ${coord} acquired leadership" && break
done

# Wait and verify a lease holder exists
sleep 15
psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_ha \
  -c "SELECT lease_key, holder_id, epoch, acquired_at, expires_at FROM distributed_lease;"
```

---

## Step 3: Validate Cluster Health

```bash
NEW_LEADER="coordinator-2"  # replace with actual new leader

# Run a simple query to confirm the new leader is serving
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Content-Type: application/json" \
  -X POST "http://${NEW_LEADER}:8080/v1/query/submit" \
  -d '{"query": "SELECT 1"}')
echo "Simple query: HTTP ${STATUS}"  # Expected: 200

# Confirm worker count matches expected
curl -s "http://${NEW_LEADER}:8080/api/workers" | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)
healthy = [w for w in workers if w['healthState'] == 'HEALTHY']
print(f'{len(healthy)}/{len(workers)} workers HEALTHY')
"

# Queries owned by the failed coordinator are reclaimed automatically by the
# orphan-query reclaimer. Verify no query is stuck:
curl -s "http://${NEW_LEADER}:8080/api/queries" | \
  python3 -c "
import json, sys
queries = json.load(sys.stdin)
stuck = [q for q in queries if q['status'] in ('RUNNING', 'SUBMITTED')]
print(f'Active queries: {len(stuck)}')
for q in stuck:
    print(f'  {q[\"queryId\"]}: {q[\"status\"]}')
"

# Test metadata access with a second query
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Content-Type: application/json" \
  -X POST "http://${NEW_LEADER}:8080/v1/query/submit" \
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

# Start the previously-failed coordinator as a new STANDBY
ssh "${FAILED_COORDINATOR}" "systemctl start pangrova-coordinator"

# Wait for it to register
sleep 30

# Verify it joined as STANDBY (not competing for leadership)
ROLE=$(ssh "${FAILED_COORDINATOR}" \
  "grep 'event=ha_started' /var/log/pangrova/pangrova-coordinator.log | tail -1" \
  | grep -o 'role=[A-Z]*' | cut -d= -f2)
echo "${FAILED_COORDINATOR} role: ${ROLE}"
# Expected: STANDBY

# Verify full cluster redundancy restored
for coord in coordinator-1 coordinator-2 coordinator-3; do
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://${coord}:8080/v1/health")
  echo "${coord}: HTTP ${HTTP}"
done
psql -h "${PANGROVA_DB_HOST}" -p 5432 -U pangrova -d pangrova_ha -At \
  -c "SELECT COUNT(*) FROM distributed_lease WHERE expires_at >= CURRENT_TIMESTAMP;" \
  | xargs -I{} echo "Active lease holders: {} (expect exactly 1)"
```

---

## Recovery Verification Checklist

- [ ] Exactly 1 LEADER elected; all other coordinators are STANDBY
- [ ] All workers registered with the new LEADER as HEALTHY (`GET /api/workers`)
- [ ] Simple SQL query returns HTTP 200 on the new LEADER (`POST /v1/query/submit`)
- [ ] `SHOW SCHEMAS` query returns HTTP 200
- [ ] Previously-failed coordinator rejoined as STANDBY
- [ ] No queries stuck in RUNNING/SUBMITTED after orphan reclamation
- [ ] Monitoring alerts cleared in Grafana/Alertmanager

---

## Escalation Criteria

| Condition | Action |
|-----------|--------|
| No LEADER after 60s + restart-driven election failed | Page on-call SRE lead |
| Both primary and standby coordinators down | Page engineering manager + SRE immediately |
| Failed coordinator in crash loop (3+ restarts in 10 min) | Page on-call SRE lead; collect heap dumps |
| Data corruption detected (inconsistent catalog) | Page data platform lead; freeze writes |

---

## Post-Incident

1. File incident report within 24 hours using the post-mortem template
2. Update this runbook if any step was unclear or incorrect
3. Review coordinator heap configuration if OOM was the cause
4. Review GC logs for pause time pattern preceding the crash
5. Confirm the coordinator-leader-loss alert fired within 15s of failure
