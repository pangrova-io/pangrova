---
title: "GD-005 Disk-Full Worker"
description: "Game-day scenario for exhausted worker storage"
---

# GD-005: Disk Full on Worker

| Field | Value |
|-------|-------|
| ID | GD-005 |
| Severity | **Medium** |
| Target component | Worker — spill directory and native SSD cache |
| RTO target | < 5 minutes to graceful degradation; < 15 minutes for manual recovery |
| Runbook | [disk-full-runbook.md](../runbooks/disk-full-runbook.md) |
| Prerequisites | Worker node with dedicated spill partition (`/mnt/spill`); `fallocate` or `dd` available |
| Estimated duration | 30 minutes |

---

## Objective

Verify that Pangrova workers degrade gracefully when disk space is exhausted. Two distinct failure modes are tested:

1. **Spill directory full** (`/mnt/spill`) — occurs during memory pressure when the query engine cannot spill intermediate results. Expected behavior: affected queries fail with `QUERY_SPILL_FAILED`, worker remains up and healthy for other queries.
2. **Native SSD cache full** (`/mnt/nvme0/velox-cache`) — occurs when the native cache cannot write new cache segments. Expected behavior: cache misses increase, queries slow down but do not fail; the native engine degrades to memory-only mode.

The cluster must not lose data, and the worker process must not crash in either scenario.

---

## Blast Radius

| Resource | Impact |
|----------|--------|
| Queries requiring spill | Fail with `QUERY_SPILL_FAILED` on the affected worker |
| Queries that fit in memory | Continue unaffected on the same worker |
| Queries routed to other workers | Unaffected |
| Native SSD cache hit rate | Drops to 0% for new segments; existing cached data still readable |
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

# Verify native SSD cache mount
ssh "${TARGET_WORKER}" "df -h /mnt/nvme0/velox-cache"
# Expected: ≥ 250 GB free, ext4 or xfs

# Verify the worker is HEALTHY in the cluster (flat WorkerSummary array;
# there is no /v1/cluster/workers endpoint)
curl -s https://coordinator:8080/api/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(f'Worker {w[\"host\"]}: {w[\"healthState\"]}')
"

# Verify tools available
ssh "${TARGET_WORKER}" "which fallocate && fallocate --version | head -1"
```

---

## Baseline Recording

```bash
ENDPOINT="https://coordinator:8080"
TOKEN="${PANGROVA_TOKEN}"

../tools/measure-recovery-time.sh baseline \
  --endpoint "${ENDPOINT}" \
  --token    "${TOKEN}" \
  --query    "SELECT count(*) FROM benchmark.lineitem" \
  --duration 60s \
  --output   /tmp/gd-005-baseline.json

# Also record a baseline of recent spill/cache-related worker log lines
# (Pangrova has no Prometheus /metrics scrape endpoint; use logs + OTLP export)
ssh "${TARGET_WORKER}" "grep -iE 'spill|cache' /var/log/pangrova/worker.log | tail -50" \
  > /tmp/gd-005-log-baseline.txt
cat /tmp/gd-005-log-baseline.txt
```

---

## Scenario A: Spill Directory Exhaustion

### Step 1: Start a spill-triggering workload

In a separate terminal, run a memory-intensive query that will require spilling:

```bash
# Large sort + aggregation — designed to exceed worker memory and trigger spill.
# Note: Pangrova cannot pin a query to a specific worker (submit accepts only
# "query" and "sessionProperties"), so run this while the target worker is the
# only busy node in the cluster.
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/query/submit" \
  -d '{
    "query": "SELECT l_orderkey, SUM(l_extendedprice) AS revenue FROM benchmark.lineitem GROUP BY l_orderkey ORDER BY revenue DESC LIMIT 1000000"
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

# Verify worker is still ACTIVE
curl -s https://coordinator:8080/api/workers | \
  python3 -c "
import json, sys
workers = json.load(sys.stdin)
for w in workers:
    if '${TARGET_WORKER}' in w['host']:
        print(f'Status: {w[\"healthState\"]}')
"
# Expected: HEALTHY — worker must remain up even when spill fails

# Verify other queries still work on the same worker
curl -s \
  -H "Authorization: Bearer ${PANGROVA_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST "https://coordinator:8080/v1/query/submit" \
  -d '{"query": "SELECT 1"}' | python3 -m json.tool
# Expected: HTTP 200 — small queries still succeed

# Check the error in worker logs
ssh "${TARGET_WORKER}" "
  grep -i 'spill\|QUERY_SPILL_FAILED\|disk full\|no space' \
    /var/log/pangrova/worker.log | tail -20
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

## Scenario B: Native SSD Cache Exhaustion

### Step 1: Fill the native SSD cache directory

```bash
# Record baseline latency of the scan query used below (no /metrics endpoint;
# compare end-to-end query latency instead)

# Fill the native SSD cache directory
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
    -X POST "https://coordinator:8080/v1/query/submit" \
    -d '{"query": "SELECT count(*) FROM benchmark.lineitem"}' | python3 -m json.tool
  sleep 5
done

# Compare per-run latencies — they may degrade but queries must not fail
```

**Expected behavior:**
- Queries complete successfully (no QUERY_FAILED errors)
- Queries may run slower because new cache segments cannot be written
- Worker logs may show cache write failures at WARN level, not ERROR

### Step 3: Verify graceful cache-full behavior

```bash
# Verify no native crash / core dump
ssh "${TARGET_WORKER}" "
  ls /var/log/pangrova/hs_err_pid*.log 2>/dev/null && echo 'JVM crash found!' || echo 'No JVM crash'
  ls /tmp/core* 2>/dev/null && echo 'Core dump found!' || echo 'No core dump'
  grep -i 'SIGSEGV\|SIGABRT\|fatal error\|Caught signal' \
    /var/log/pangrova/worker.log | tail -5 || echo 'No fatal signals'
"

# Verify worker still HEALTHY
curl -s https://coordinator:8080/api/workers | python3 -m json.tool | grep -A8 "${TARGET_WORKER}"
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
  -X POST "https://coordinator:8080/v1/query/submit" \
  -d '{
    "query": "SELECT l_orderkey, SUM(l_extendedprice) FROM benchmark.lineitem GROUP BY l_orderkey LIMIT 100"
  }' | python3 -m json.tool

# Verify the native SSD cache is writable again — re-run one scan query and
# confirm its latency returns toward the baseline recorded in Step 1

# Verify worker is HEALTHY and has accepted new tasks
curl -s https://coordinator:8080/api/workers | python3 -m json.tool | grep -A8 "${TARGET_WORKER}"

# Check error log is clean after recovery
ssh "${TARGET_WORKER}" "
  grep -c 'ERROR' /var/log/pangrova/worker-error.log || echo '0 errors'
"
```

---

## Pass Criteria

| # | Criterion | How to verify |
|---|-----------|---------------|
| 1 | **Spill failure is isolated** — only the spilling query fails | Other queries on the same worker return HTTP 200 |
| 2 | **Worker stays HEALTHY** — both during and after disk full | `GET /api/workers` shows `healthState: HEALTHY` throughout |
| 3 | **Error code is correct** — `QUERY_SPILL_FAILED`, not `WORKER_CRASH` | Error response body contains correct error code |
| 4 | **Cache-full degrades gracefully** — queries slow down but do not fail | All 5 scan queries return HTTP 200 during cache fill |
| 5 | **No native crash** — the native engine does not SIGSEGV or abort on cache write failure | No `hs_err_pid*` files; no SIGSEGV in logs |
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
