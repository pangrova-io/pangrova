---
title: "GD-002 Worker Crash During Shuffle"
description: "Game-day scenario for worker loss during shuffle"
---

# GD-002: Worker Crash During Shuffle

## Scenario Summary

| Field | Value |
|-------|-------|
| ID | GD-002 |
| Severity | High |
| Target | Worker node during active shuffle exchange |
| Fault Type | Process crash (SIGKILL) during data exchange |
| Expected Behavior | Query fails gracefully, retried queries succeed |
| RTO Target | < 60 seconds |

## Objective

Validate that a worker crash during shuffle does not corrupt data or leave the cluster in an inconsistent state. Verify that the coordinator detects the failure, cancels affected queries, and subsequent retries succeed on remaining workers.

## Prerequisites

- Staging cluster with 3+ worker nodes
- Multi-stage query workload that triggers shuffle (e.g., large JOIN or GROUP BY)
- Identify target worker via query plan (worker handling shuffle write)
- Monitoring dashboards open (worker health, shuffle metrics, query status)

## Execution Steps

### 1. Establish Baseline (5 min)

```bash
# Run shuffle-heavy query to identify active shuffle workers
./tools/measure-recovery-time.sh baseline \
  --endpoint http://coordinator:8080 \
  --query "SELECT l_orderkey, sum(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY l_orderkey LIMIT 1000" \
  --interval 2s \
  --output /tmp/gd-002-baseline.json
```

### 2. Inject Fault (1 min)

```bash
# Identify worker with active shuffle partitions
WORKER_PID=$(ssh worker-2 "pgrep -f pangrova-worker")

# Kill worker mid-shuffle
ssh worker-2 "kill -9 ${WORKER_PID}"

# Measure recovery
./tools/measure-recovery-time.sh measure \
  --endpoint http://coordinator:8080 \
  --probe-query "SELECT 1" \
  --timeout 120s \
  --output /tmp/gd-002-recovery.json
```

### 3. Observe (10 min)

Monitor and record:
- [ ] Time until coordinator detects worker heartbeat loss
- [ ] Queries cancelled due to worker failure
- [ ] Error messages returned to clients
- [ ] Shuffle data cleanup on remaining workers
- [ ] Whether coordinator re-schedules affected tasks

### 4. Validate Recovery

- [ ] Cluster reports healthy after worker removal
- [ ] Retried queries succeed on remaining workers
- [ ] No orphan shuffle files on surviving workers
- [ ] No memory leaks from abandoned shuffle buffers
- [ ] Worker re-join after restart is clean (no stale state)

## Expected Outcomes

| Outcome | Pass Criteria |
|---------|--------------|
| Detection time | < 15 seconds (heartbeat timeout) |
| Query failure | Clear error with retryable code |
| Data integrity | No partial/corrupt results returned |
| Cluster recovery | Healthy within 60 seconds |
| Shuffle cleanup | No orphan files after 5 minutes |

## Rollback Plan

1. Restart killed worker: `ssh worker-2 "systemctl start pangrova-worker"`
2. Verify worker re-registers with coordinator
3. If shuffle files are orphaned, trigger manual cleanup via admin API

## Observability Checklist

- [ ] Worker-down alert fired
- [ ] Query failure metrics incremented
- [ ] Shuffle partition reassignment logged
- [ ] No WARN/ERROR log storm on surviving workers

## Related Runbook

[Worker Crash Recovery Runbook](../runbooks/worker-crash-runbook.md)
