# Runbook: Worker Crash Recovery

**Use this runbook when:** A worker node's heartbeat has been missed for > 15 seconds, the worker process has exited, or the Coordinator has marked a worker as DEAD.

| Field | Value |
|-------|-------|
| Scenario | [GD-002: Worker Crash During Shuffle](../scenarios/gd-002-worker-crash-shuffle.md) |
| Severity | High |
| Primary risk | Queries with active tasks on the crashed worker fail; in-flight shuffle data lost |
| Expected resolution | Worker restart + automatic re-registration within 60s |

---

## Decision Tree

```
Worker down?
  │
  ├─► Is only one worker affected, or multiple?
  │     │
  │     ├─► ONE worker ──► Single-node failure → proceed with Step 1
  │     │
  │     └─► MULTIPLE workers ──► Infrastructure event or coordinator issue
  │           → Escalate to on-call lead before proceeding
  │
  └─► Did the worker crash or was it intentionally stopped?
        │
        ├─► CRASHED ──► Collect artifacts before restarting → Step 1
        │
        └─► STOPPED ──► Skip artifact collection; proceed to Step 4
```

---

## Step 1: Confirm Worker Down

```bash
# Check the coordinator's view of all worker statuses
curl -s https://coordinator:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
for w in workers:
    status_flag = '⚠' if w['status'] != 'HEALTHY' else ' '
    print(f\"{status_flag} {w['host']}: {w['status']}  tasks={w.get('activeTasks', '?')}  lastSeen={w.get('lastSeenMs', '?')}ms ago\")
"

# Confirm from the worker node directly
TARGET_WORKER="worker-N"  # replace with actual worker hostname

ssh "${TARGET_WORKER}" "
  systemctl status pangrova-worker
  echo '---'
  ps aux | grep pangrova-worker | grep -v grep || echo 'Process: NOT RUNNING'
"

# Check for crash evidence
ssh "${TARGET_WORKER}" "
  ls /var/log/pangrova/heapdump-worker*.hprof 2>/dev/null && echo 'Heap dump found (OOM)' || echo 'No heap dump'
  ls /tmp/hs_err_pid*.log 2>/dev/null && echo 'JVM crash log found' || echo 'No JVM crash log'
  dmesg | grep -i 'oom\|killed process.*pangrova' | tail -5
"
```

---

## Step 2: Assess Query Impact

```bash
# Identify queries that were running on the crashed worker
curl -s "https://coordinator:8080/v1/admin/queries?status=FAILED" | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
queries = [q for q in data.get('queries', []) if '${TARGET_WORKER}' in str(q.get('failedWorker', ''))]
print(f'Queries failed due to worker crash: {len(queries)}')
for q in queries[:10]:
    print(f'  queryId={q[\"queryId\"]}  retryable={q.get(\"retryable\", \"?\")}')
" 2>/dev/null || echo "(query impact API not available)"

# Check if affected queries are being retried automatically
curl -s "https://coordinator:8080/v1/admin/queries?status=RETRYING" | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'Queries currently retrying: {len(data.get(\"queries\", []))}')
" 2>/dev/null

# Inform affected users if queries cannot be retried automatically
echo "If non-retryable query failures are detected, notify affected clients."
```

---

## Step 3: Clean Up Orphaned Shuffle Resources

When a worker crashes mid-shuffle, it may leave orphaned shuffle partitions on surviving workers. These consume disk space and must be cleaned up.

```bash
# Trigger coordinator-side shuffle cleanup for the crashed worker
curl -s -X POST "https://coordinator:8080/v1/admin/shuffle/cleanup" \
  -H "Authorization: Bearer ${PANGROVA_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"workerId\": \"${TARGET_WORKER}\"}" | python3 -m json.tool

# Verify cleanup on surviving workers
for worker in worker-1 worker-2 worker-3; do
  [[ "${worker}" == "${TARGET_WORKER}" ]] && continue
  echo "=== ${worker} shuffle directory ==="
  ssh "${worker}" "
    SHUFFLE_DIR=/mnt/spill/pangrova/shuffle  # or configured spill path
    find \"\${SHUFFLE_DIR}\" -name \"*${TARGET_WORKER}*\" 2>/dev/null | wc -l | \
      xargs -I{} echo 'Orphan files from crashed worker: {}'
    du -sh \"\${SHUFFLE_DIR}\" 2>/dev/null
  "
done
```

---

## Step 4: Restart Worker

```bash
# Collect logs and crash artifacts BEFORE restarting (they may be overwritten)
ssh "${TARGET_WORKER}" "
  TIMESTAMP=\$(date +%Y%m%d-%H%M%S)
  ARCHIVE=/tmp/worker-crash-\${TIMESTAMP}.tar.gz
  tar czf \"\${ARCHIVE}\" \
    /var/log/pangrova/pangrova-worker*.log \
    /var/log/pangrova/gc-worker.log \
    /var/log/pangrova/heapdump-worker*.hprof 2>/dev/null \
    /tmp/hs_err_pid*.log 2>/dev/null || true
  echo \"Artifacts archived: \${ARCHIVE}\"
"

# Identify root cause before restarting
ssh "${TARGET_WORKER}" "
  echo '=== OOM evidence ==='
  dmesg | grep -i 'out of memory\|killed process.*java' | tail -5

  echo '=== JVM error log ==='
  cat /tmp/hs_err_pid*.log 2>/dev/null | head -30 || echo 'No JVM error log'

  echo '=== Worker error log ==='
  tail -30 /var/log/pangrova/pangrova-worker-error.log 2>/dev/null
"

# Restart the worker process
ssh "${TARGET_WORKER}" "systemctl start pangrova-worker"

# Tail the log to confirm startup
ssh "${TARGET_WORKER}" "journalctl -u pangrova-worker -f --no-pager &" &
TAIL_PID=$!
sleep 20
kill $TAIL_PID 2>/dev/null
```

---

## Step 5: Verify Recovery

```bash
# Wait for the worker to complete registration (up to 60s)
echo "Waiting for worker registration..."
for i in $(seq 1 12); do
  STATUS=$(curl -s https://coordinator:8080/v1/cluster/workers | \
    python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(w['status'])
")
  echo "  [${i}] ${TARGET_WORKER} status: ${STATUS}"
  [[ "${STATUS}" == "HEALTHY" ]] && echo "RECOVERED" && break
  sleep 5
done

# Verify no stale task state on the restarted worker
curl -s "https://coordinator:8080/v1/admin/workers/${TARGET_WORKER}/tasks" | \
  python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'Active tasks on worker: {len(data.get(\"tasks\", []))}')
" 2>/dev/null

# Run a test query to confirm the worker accepts new work
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/sql" \
  -d '{"query": "SELECT count(*) FROM benchmark.lineitem"}')
echo "Test query result: HTTP ${STATUS}"  # Expected: 200

# Verify overall cluster health
curl -s https://coordinator:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
healthy = sum(1 for w in workers if w['status'] == 'HEALTHY')
print(f'Cluster health: {healthy}/{len(workers)} workers HEALTHY')
"
```

---

## Recovery Verification Checklist

- [ ] Worker registered with coordinator as HEALTHY
- [ ] Worker accepting new query tasks
- [ ] No orphaned shuffle files remaining on any surviving worker
- [ ] Previously-failed queries retried (or clients notified for non-retryable failures)
- [ ] Worker memory metrics back to normal baseline
- [ ] No crash loop (no restart within 10 minutes of Step 4)
- [ ] Monitoring alerts cleared in Grafana/Alertmanager

---

## Escalation Criteria

| Condition | Action |
|-----------|--------|
| Worker crash loop (≥ 3 restarts in 10 min) | Page on-call SRE lead; do not keep restarting |
| Multiple workers crashed simultaneously | Page engineering lead; may be infrastructure event |
| Heap dump present but worker OOM config correct | Page data platform lead to investigate query workload |
| Shuffle cleanup API fails or orphan files remain after cleanup | Page on-call SRE lead |
| Worker starts but stays in `REGISTERING` state > 60s | Check coordinator logs; may be authentication or TLS issue |

---

## Post-Incident

1. Review GC logs for heap pressure patterns before the crash
2. Check if query workload had unusual memory consumption (large sorts, joins, aggregations)
3. Verify `PANGROVA_WORKER_MEMORY_BUDGET_BYTES` is appropriate for the node's total RAM
4. Consider adding `worker_heap_usage_high` alert at 80% threshold
5. Update `auto-scaling` configuration if capacity was insufficient during recovery period
