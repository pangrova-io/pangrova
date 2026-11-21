---
title: "GD-003 Metadata Partition"
description: "Game-day scenario for metadata-store isolation"
---

# GD-003: Metadata Store Partition

## Scenario Summary

| Field | Value |
|-------|-------|
| ID | GD-003 |
| Severity | Critical |
| Target | Metadata store (network partition from coordinator) |
| Fault Type | Network partition (iptables drop) |
| Expected Behavior | Coordinator degrades gracefully, recovers on partition heal |
| RTO Target | < 120 seconds |

## Objective

Validate that metadata-authority unavailability fails closed. New authorization,
admission, and mutations must not use a cache hit as a substitute for the shared
authority. Observe already admitted queries separately, and verify that normal
operation resumes only after the authority is reachable and state is reconciled.

## Prerequisites

- Staging cluster with the shared PostgreSQL metadata authority
- Network control capability on coordinator nodes (iptables or tc)
- Active workload with mix of DDL and DML queries
- Metadata cache warm (recent queries have populated cache)

## Execution Steps

### 1. Establish Baseline (5 min)

```bash
# Verify metadata store connectivity
../tools/measure-recovery-time.sh baseline \
  --endpoint http://coordinator:8080 \
  --query "SHOW TABLES IN benchmark" \
  --interval 2s \
  --output /tmp/gd-003-baseline.json
```

### 2. Inject Fault (1 min)

```bash
# Partition metadata store from coordinator
ssh coordinator "iptables -A OUTPUT -d metadata-store -j DROP"
ssh coordinator "iptables -A INPUT -s metadata-store -j DROP"

# Start recovery measurement
../tools/measure-recovery-time.sh measure \
  --endpoint http://coordinator:8080 \
  --probe-query "SHOW TABLES IN benchmark" \
  --timeout 180s \
  --output /tmp/gd-003-recovery.json
```

### 3. Observe During Partition (15 min)

Test and record behavior for each query type:

| Query Type | Expected Behavior | Actual |
|-----------|-------------------|--------|
| SELECT (cached table) | Rejects when required live authority is unavailable; a cache hit cannot bypass authorization | |
| SELECT (uncached table) | Fails with clear error | |
| CREATE TABLE | Fails with metadata unavailable error | |
| INSERT INTO (cached) | Succeeds or fails gracefully | |
| DROP TABLE | Fails with metadata unavailable error | |

Monitor:
- [ ] Coordinator logs metadata connection failures at WARN level
- [ ] No crash or panic on metadata timeout
- [ ] Cache continues serving known metadata
- [ ] Error messages indicate transient failure (retryable)

### 4. Heal Partition

```bash
# Remove iptables rules
ssh coordinator "iptables -D OUTPUT -d metadata-store -j DROP"
ssh coordinator "iptables -D INPUT -s metadata-store -j DROP"
```

### 5. Validate Recovery

- [ ] Metadata operations resume within RTO
- [ ] Cache refreshes with latest metadata
- [ ] No stale metadata served after recovery
- [ ] DDL operations succeed again
- [ ] No metadata corruption or inconsistency

## Expected Outcomes

| Outcome | Pass Criteria |
|---------|--------------|
| Cached queries | Continue working during partition |
| Uncached queries | Fail with clear, retryable error |
| DDL operations | Fail immediately with metadata unavailable |
| Recovery time | < 120 seconds after partition heals |
| Data consistency | Zero stale reads after recovery |

## Rollback Plan

1. Remove iptables rules (see step 4)
2. Restart coordinator if metadata connection pool is corrupted
3. Wait for metadata cache entries to expire (`metadata.cache.l1-ttl`, default 60s), or restart the coordinator to clear them immediately (there is no cache-invalidation API)

## Observability Checklist

- [ ] Metadata connection pool exhaustion alert fired
- [ ] Circuit breaker opened for metadata calls
- [ ] Cache hit rate spike visible in metrics
- [ ] Recovery logged at INFO level when partition heals

## Related Runbook

[Metadata Partition Runbook](../runbooks/metadata-partition-runbook.md)
