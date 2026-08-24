# GD-005: Disk Full on Worker

| Field | Value |
|-------|-------|
| ID | GD-005 |
| Severity | **Medium** |
| Target component | Worker — spill directory and Velox SSD cache |
| RTO target | < 5 minutes to graceful degradation; < 15 minutes for manual recovery |
| Runbook | [disk-full-runbook.md](../runbooks/disk-full-runbook.md) |
| Prerequisites | Worker node with dedicated spill partition (`/mnt/spill`); `fallocate` or `dd` available |
| Estimated duration | 30 minutes |

---

## Objective

Verify that Pangrova workers degrade gracefully when disk space is exhausted. Two distinct failure modes are tested:

1. **Spill directory full** (`/mnt/spill`) — occurs during memory pressure when the query engine cannot spill intermediate results. Expected behavior: affected queries fail with `QUERY_SPILL_FAILED`, worker remains up and healthy for other queries.
2. **Velox SSD cache full** (`/mnt/nvme0/velox-cache`) — occurs when the native cache cannot write new cache segments. Expected behavior: cache misses increase, queries slow down but do not fail; Velox degrades to memory-only mode.

The cluster must not lose data, and the worker process must not crash in either scenario.

---

## Blast Radius

| Resource | Impact |
|----------|--------|
| Queries requiring spill | Fail with `QUERY_SPILL_FAILED` on the affected worker |
| Queries that fit in memory | Continue unaffected on the same worker |
| Queries routed to other workers | Unaffected |
| Velox cache hit rate | Drops to 0% for new segments; existing cached data still readable |
| Worker process | Remains running; does not restart |
| Data integrity | No data loss or corruption |

---

## Prerequisites

```bash
# Identify which worker node to target
TARGET_WORKER="worker-0"

# Verify dedicated spill mount exists and has free space
ssh "${TARGET_WORKER}" "df -h /mnt/spill"
# Expected: ≥ 200 GB free, mounted on dedicated partition

# Verify Velox SSD cache mount
ssh "${TARGET_WORKER}" "df -h /mnt/nvme0/velox-cache"
# Expected: ≥ 250 GB free, ext4 or xfs

# Verify the worker is HEALTHY in the cluster
curl -s https://coordinator:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(f'Worker {w[\"host\"]}: {w[\"status\"]}')
"

# Verify tools available
ssh "${TARGET_WORKER}" "which fallocate && fallocate --version | head -1"
```

---

## Baseline Recording

```bash
ENDPOINT="https://coordinator:8080"
TOKEN="${PANGROVA_TOKEN}"

./tools/measure-recovery-time.sh baseline \
  --endpoint "${ENDPOINT}" \
  --token    "${TOKEN}" \
  --query    "SELECT count(*) FROM benchmark.lineitem" \
  --duration 60s \
  --output   /tmp/gd-005-baseline.json

# Also record initial spill and cache metrics
curl -s "https://coordinator:8080/metrics" | grep -E "pangrova_worker_spill|velox_cache" | \
  grep "${TARGET_WORKER}" > /tmp/gd-005-metrics-baseline.txt
cat /tmp/gd-005-metrics-baseline.txt
```

---

## Scenario A: Spill Directory Exhaustion

### Step 1: Start a spill-triggering workload

In a separate terminal, run a memory-intensive query that will require spilling:

```bash
# Large sort + aggregation — designed to exceed worker memory and trigger spill
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/sql" \
  -d '{
    "query": "SELECT l_orderkey, SUM(l_extendedprice) AS revenue FROM benchmark.lineitem GROUP BY l_orderkey ORDER BY revenue DESC LIMIT 1000000",
    "hints": {"worker": "'"${TARGET_WORKER}"'"}
  }' &
WORKLOAD_PID=$!
echo "Workload started in background (PID: ${WORKLOAD_PID})"
```

### Step 2: Fill the spill directory

```bash
# Record free space before filling
ssh "${TARGET_WORKER}" "df -h /mnt/spill"

# Fill 98% of the spill directory using a sparse file (fast, does not write data)
SPILL_SIZE=$(ssh "${TARGET_WORKER}" "df --output=avail /mnt/spill | tail -1")
FILL_SIZE=$(( SPILL_SIZE * 98 / 100 ))K

ssh "${TARGET_WORKER}" "
  fallocate -l ${FILL_SIZE} /mnt/spill/gd-005-filler.dat
  echo 'Spill directory filled'
  df -h /mnt/spill
"

INJECT_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Disk fill injected at: ${INJECT_TIME}"
```

### Step 3: Observe behavior

```bash
# Wait for the workload to hit the spill limit
wait $WORKLOAD_PID
WORKLOAD_EXIT=$?
echo "Workload exit code: ${WORKLOAD_EXIT}"  # Expected: non-zero (query failed)

# Verify worker is still HEALTHY
curl -s https://coordinator:8080/v1/cluster/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)['workers']
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(f'Status: {w[\"status\"]}')
"
# Expected: HEALTHY — worker must remain up even when spill fails

# Verify other queries still work on the same worker
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/sql" \
  -d '{
    "query": "SELECT 1",
    "hints": {"worker": "'"${TARGET_WORKER}"'"}
  }' | python3 -m json.tool
# Expected: HTTP 200 — small queries still succeed

# Check the error in worker logs
ssh "${TARGET_WORKER}" "
  grep -i 'spill\|QUERY_SPILL_FAILED\|disk full\|no space' \
    /var/log/pangrova/pangrova-worker.log | tail -20
"
```

### Step 4: Cleanup spill filler

```bash
ssh "${TARGET_WORKER}" "
  rm -f /mnt/spill/gd-005-filler.dat
  echo 'Filler removed'
  df -h /mnt/spill
"
```

---

## Scenario B: Velox SSD Cache Exhaustion

### Step 1: Fill the Velox cache directory

```bash
# Record initial cache metrics
curl -s https://coordinator:8080/metrics | grep velox_cache_hit_rate

# Fill the Velox SSD cache directory
CACHE_SIZE=$(ssh "${TARGET_WORKER}" "df --output=avail /mnt/nvme0/velox-cache | tail -1")
FILL_SIZE=$(( CACHE_SIZE * 99 / 100 ))K

ssh "${TARGET_WORKER}" "
  fallocate -l ${FILL_SIZE} /mnt/nvme0/velox-cache/gd-005-cache-filler.dat
  echo 'Cache directory filled'
  df -h /mnt/nvme0/velox-cache
"

echo "Cache fill injected at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### Step 2: Run scan queries and observe cache behavior

```bash
# Run repeated scans — should complete but with degraded cache hit rate
for i in $(seq 1 5); do
  curl -s \
    -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST "https://coordinator:8080/v1/sql" \
    -d '{"query": "SELECT count(*) FROM benchmark.lineitem"}' | python3 -m json.tool
  sleep 5
done

# Check cache metrics — hit rate should degrade but not crash
curl -s https://coordinator:8080/metrics | grep -E "velox_cache"
```

**Expected behavior:**
- Queries complete successfully (no QUERY_FAILED errors)
- Cache hit rate drops toward 0% (cannot write new cache segments)
- Worker logs show `velox cache write failed: no space left on device` at WARN level, not ERROR

### Step 3: Verify Velox graceful cache-full behavior

```bash
# Verify no native crash / core dump
ssh "${TARGET_WORKER}" "
  ls /var/log/pangrova/hs_err_pid*.log 2>/dev/null && echo 'JVM crash found!' || echo 'No JVM crash'
  ls /tmp/core* 2>/dev/null && echo 'Core dump found!' || echo 'No core dump'
  grep -i 'SIGSEGV\|SIGABRT\|fatal error\|Caught signal' \
    /var/log/pangrova/pangrova-worker.log | tail -5 || echo 'No fatal signals'
"

# Verify worker still HEALTHY
curl -s https://coordinator:8080/v1/cluster/workers | python3 -m json.tool | grep -A3 "${TARGET_WORKER}"
```

### Step 4: Cleanup cache filler

```bash
ssh "${TARGET_WORKER}" "
  rm -f /mnt/nvme0/velox-cache/gd-005-cache-filler.dat
  echo 'Cache filler removed'
  df -h /mnt/nvme0/velox-cache
"
```

---

## Recovery Verification

```bash
# Verify disk space restored
ssh "${TARGET_WORKER}" "df -h /mnt/spill /mnt/nvme0/velox-cache"

# Verify spill queries work again
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/sql" \
  -d '{
    "query": "SELECT l_orderkey, SUM(l_extendedprice) FROM benchmark.lineitem GROUP BY l_orderkey LIMIT 100"
  }' | python3 -m json.tool

# Verify Velox cache is accepting writes again (hit rate should recover over time)
curl -s https://coordinator:8080/metrics | grep velox_cache_hit_rate

# Verify worker is HEALTHY and has accepted new tasks
curl -s https://coordinator:8080/v1/cluster/workers | python3 -m json.tool | grep -A5 "${TARGET_WORKER}"

# Check error log is clean after recovery
ssh "${TARGET_WORKER}" "
  grep -c 'ERROR' /var/log/pangrova/pangrova-worker-error.log || echo '0 errors'
"
```

---

## Pass Criteria

| # | Criterion | How to verify |
|---|-----------|---------------|
| 1 | **Spill failure is isolated** — only the spilling query fails | Other queries on the same worker return HTTP 200 |
| 2 | **Worker stays HEALTHY** — both during and after disk full | `/v1/cluster/workers` shows `HEALTHY` throughout |
| 3 | **Error code is correct** — `QUERY_SPILL_FAILED`, not `WORKER_CRASH` | Error response body contains correct error code |
| 4 | **Cache-full degrades gracefully** — queries slow down but do not fail | All 5 scan queries return HTTP 200 during cache fill |
| 5 | **No native crash** — Velox does not SIGSEGV or abort on cache write failure | No `hs_err_pid*` files; no SIGSEGV in logs |
| 6 | **Recovery is automatic** — no worker restart needed | After filler removal, spill and cache work without intervention |
| 7 | **Alert fires for disk full** — `worker_spill_disk_full` alert fires within 60s | Alert visible in Grafana/Alertmanager during scenario |

---

## Cleanup Safety Check

```bash
# Final verification: all filler files removed
ssh "${TARGET_WORKER}" "
  ls /mnt/spill/gd-005-*.dat 2>/dev/null && echo 'WARNING: filler still present' || echo 'Spill: clean'
  ls /mnt/nvme0/velox-cache/gd-005-*.dat 2>/dev/null && echo 'WARNING: filler still present' || echo 'Cache: clean'
"
```
