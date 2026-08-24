# Runbook: Disk Full on Worker (GD-005)

**Use this runbook when:** A worker node's spill directory or Velox SSD cache is full, causing query failures or cache degradation.

| Field | Value |
|-------|-------|
| Scenario | [GD-005: Disk Full on Worker](../scenarios/gd-005-disk-full-worker.md) |
| Severity | Medium |
| Primary risk | Query `SPILL_FAILED` errors; Velox cache write failures causing performance regression |
| Expected resolution | Manual disk cleanup; worker process stays up throughout |

---

## Decision Tree

```
Disk full suspected on worker?
  │
  ├─► Which directory is full?
  │     │
  │     ├─► /mnt/spill (spill directory)
  │     │     └─► Queries that need spill are failing → Step 1A: Spill Recovery
  │     │
  │     ├─► /mnt/nvme0/velox-cache (SSD cache)
  │     │     └─► Cache writes failing, performance degraded → Step 1B: Cache Recovery
  │     │
  │     └─► Root filesystem (/)
  │           └─► Critical: OS disk full → escalate immediately, may need worker restart
  │
  └─► Is the worker still HEALTHY in the cluster?
        │
        ├─► YES ──► Proceed with cleanup — do not restart
        │
        └─► NO  ──► Worker process may have crashed → use worker-crash-runbook instead
```

---

## Step 1: Identify Disk Usage

```bash
TARGET_WORKER="<worker-node>"  # e.g., worker-0

# Check disk usage on all relevant mounts
ssh "${TARGET_WORKER}" "
  echo '=== Disk usage ==='
  df -h /mnt/spill /mnt/nvme0/velox-cache / 2>/dev/null || df -h

  echo ''
  echo '=== Spill directory contents ==='
  ls -lhS /mnt/spill/ | head -20

  echo ''
  echo '=== Velox cache directory ==='
  ls -lhS /mnt/nvme0/velox-cache/ | head -20
"

# Verify worker health
curl -s https://coordinator:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(f'Worker {w[\"host\"]}: {w[\"status\"]}')
"
```

---

## Step 1A: Spill Directory Recovery

The spill directory holds temporary files written by the query engine during memory-pressure events. Spill files for completed queries (including failed ones) should be automatically cleaned up. Files for in-progress queries must not be deleted.

```bash
# Check for stale spill files (older than 1 hour are safe to remove)
ssh "${TARGET_WORKER}" "
  echo '=== Spill files older than 1 hour ==='
  find /mnt/spill -type f -mmin +60 | head -20
  echo ''
  echo '=== Active query IDs (in-progress) ==='
  # Do not delete spill files for these query IDs
  curl -s http://localhost:8081/v1/tasks/active 2>/dev/null | \
    python3 -c \"import json,sys; tasks=json.load(sys.stdin); [print(t['queryId']) for t in tasks.get('tasks',[])]\" || \
    echo '(unable to fetch active tasks)'
"

# Get active query IDs before deleting anything
ACTIVE_QUERIES=$(ssh "${TARGET_WORKER}" "
  curl -s http://localhost:8081/v1/tasks/active 2>/dev/null | \
    python3 -c \"import json,sys; tasks=json.load(sys.stdin); print(' '.join([t['queryId'] for t in tasks.get('tasks', [])]))\"
" 2>/dev/null || echo "")
echo "Active query IDs: ${ACTIVE_QUERIES}"

# Delete stale spill files — skip files matching active query IDs
ssh "${TARGET_WORKER}" "
  find /mnt/spill -type f -mmin +60 | while read f; do
    FILENAME=\$(basename \"\$f\")
    # Skip if filename contains an active query ID
    SKIP=false
    for qid in ${ACTIVE_QUERIES}; do
      [[ \"\${FILENAME}\" == *\"\${qid}\"* ]] && SKIP=true && break
    done
    if [[ \"\${SKIP}\" == false ]]; then
      echo \"Removing: \${f}\"
      rm -f \"\${f}\"
    else
      echo \"Skipping (active query): \${f}\"
    fi
  done
  echo ''
  df -h /mnt/spill
"
```

If the spill directory is full because of a game day filler file:

```bash
# Remove game day filler file (if present)
ssh "${TARGET_WORKER}" "
  ls /mnt/spill/gd-*.dat 2>/dev/null && rm -f /mnt/spill/gd-*.dat && echo 'Filler removed' || echo 'No filler file'
  df -h /mnt/spill
"
```

---

## Step 1B: Velox SSD Cache Recovery

The Velox SSD cache stores remote data cached locally for faster repeat access. Cache files are managed by Velox internally. Unlike the spill directory, **do not delete Velox cache files manually** — use the API to trigger cache eviction.

```bash
# Check current cache metrics
curl -s https://coordinator:8080/metrics | \
  grep -E "velox_cache_hit_rate|velox_cache_size|velox_cache_evict"

# Trigger cache eviction via the worker management API
curl -s -X POST "https://${TARGET_WORKER}:8081/v1/cache/evict" \
  -H "Authorization: Bearer ${PANGROVA_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"target_free_bytes": 10737418240}' | python3 -m json.tool
# Frees ~10 GB from the SSD cache

# If the cache API is unavailable and disk is critically full:
# Remove game day filler file if present (safe — not a Velox cache file)
ssh "${TARGET_WORKER}" "
  ls /mnt/nvme0/velox-cache/gd-*.dat 2>/dev/null && \
    rm -f /mnt/nvme0/velox-cache/gd-*.dat && echo 'Filler removed' || echo 'No filler file'
  df -h /mnt/nvme0/velox-cache
"
```

**Warning:** Deleting Velox cache files directly (e.g., `rm -rf /mnt/nvme0/velox-cache/*.cache`) is destructive. Only delete game-day filler files (named `gd-*.dat`). Let the Velox eviction API manage real cache files.

---

## Step 2: Verify Disk Space Restored

```bash
ssh "${TARGET_WORKER}" "
  echo '=== Disk usage after cleanup ==='
  df -h /mnt/spill /mnt/nvme0/velox-cache /
  echo ''
  echo '=== Free space check ==='
  SPILL_FREE=\$(df --output=avail /mnt/spill | tail -1)
  CACHE_FREE=\$(df --output=avail /mnt/nvme0/velox-cache | tail -1)
  echo \"Spill free: \${SPILL_FREE}K\"
  echo \"Cache free: \${CACHE_FREE}K\"
  [[ \${SPILL_FREE} -gt 102400 ]] && echo 'Spill: OK (>100 MB free)' || echo 'WARN: Spill still low'
  [[ \${CACHE_FREE} -gt 1048576 ]] && echo 'Cache: OK (>1 GB free)' || echo 'WARN: Cache still low'
"
```

---

## Step 3: Verify Worker Recovery

```bash
# Verify worker health
curl -s https://coordinator:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(f'Status: {w[\"status\"]}  activeQueries={w.get(\"activeQueries\", \"?\")}')
"

# Verify a spill-capable query succeeds
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/sql" \
  -d '{"query": "SELECT l_orderkey, SUM(l_extendedprice) FROM benchmark.lineitem GROUP BY l_orderkey LIMIT 100"}' | \
  python3 -m json.tool | head -5

# Verify cache hit rate recovering (may take several minutes)
curl -s https://coordinator:8080/metrics | grep velox_cache_hit_rate
```

---

## Step 4: Root Cause Investigation

```bash
# Check worker logs for disk error messages
ssh "${TARGET_WORKER}" "
  echo '=== Spill errors ==='
  grep -E 'spill|SPILL|no space|disk full|ENOSPC' \
    /var/log/pangrova/pangrova-worker.log | tail -30

  echo ''
  echo '=== Velox cache write failures ==='
  grep -E 'velox.*cache.*write|cache.*failed|SSD.*full' \
    /var/log/pangrova/pangrova-worker.log | tail -20

  echo ''
  echo '=== Recent error log entries ==='
  tail -50 /var/log/pangrova/pangrova-worker-error.log
"

# Check if disk growth is expected (large join spills, data ingestion) or unexpected
ssh "${TARGET_WORKER}" "
  echo '=== Disk usage trend (du on spill dir) ==='
  du -sh /mnt/spill/
  echo ''
  echo '=== Oldest spill file age ==='
  find /mnt/spill -type f | xargs ls -lt 2>/dev/null | tail -5
"
```

---

## Step 5: Preventive Actions

Based on findings, take one or more of the following preventive steps:

```bash
# Option A: Increase spill directory size (coordinate with infra team)
# Requires resize of the underlying volume and filesystem

# Option B: Lower the spill threshold to trigger earlier (before disk is full)
# Edit /etc/pangrova/worker.yml:
# spill:
#   spill-threshold: 0.65   # Spill earlier (was 0.75)
# Reload: systemctl reload pangrova-worker (if hot-reload supported)

# Option C: Reduce query memory budget to reduce spill frequency
# Edit /etc/pangrova/worker.yml:
# memory:
#   budget-bytes: "12G"   # Reduce from 16G

# Option D: Set up disk usage alerts at 80% threshold
# Add to Prometheus alerting rules:
# - alert: WorkerSpillDiskHigh
#   expr: (1 - node_filesystem_avail_bytes{mountpoint="/mnt/spill"} /
#              node_filesystem_size_bytes{mountpoint="/mnt/spill"}) > 0.80
#   for: 5m
```

---

## Escalation Criteria

Escalate to on-call lead if any of the following are true:

| Condition | Action |
|-----------|--------|
| Worker process crashed (not just query failure) | Use worker-crash-runbook; escalate if restart loop |
| Root filesystem (`/`) is full | Escalate immediately — OS stability at risk |
| Multiple workers simultaneously disk-full | Possible widespread data ingestion spike; alert Data Platform lead |
| Velox cache eviction API unresponsive | Worker may be in a hung state; escalate for graceful restart |
| Spill files for active queries cannot be identified | Risk of corrupting in-flight queries; escalate before deleting |
