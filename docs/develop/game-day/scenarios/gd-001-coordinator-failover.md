# GD-001: Coordinator Failover

## Scenario Summary

| Field | Value |
|-------|-------|
| ID | GD-001 |
| Severity | Critical |
| Target | Coordinator (active instance) |
| Fault Type | Process crash (SIGKILL) |
| Expected Behavior | Standby coordinator assumes leadership within RTO |
| RTO Target | < 30 seconds |

## Objective

Validate that the Pangrova cluster survives coordinator failure with minimal query disruption. Verify that in-flight queries either complete on the new leader or fail gracefully with retryable error codes.

## Prerequisites

- Staging cluster with 2+ coordinator instances (active/standby)
- Active workload generating queries (use `pangrova-client-cli` benchmark mode)
- Monitoring dashboards open (coordinator health, query latency, error rate)
- Recovery time measurement tool ready (`tools/measure-recovery-time.sh`)

## Execution Steps

### 1. Establish Baseline (5 min)

```bash
# Start continuous query workload
./tools/measure-recovery-time.sh baseline \
  --endpoint http://coordinator-active:8080 \
  --query "SELECT count(*) FROM benchmark.lineitem" \
  --interval 1s \
  --output /tmp/gd-001-baseline.json
```

Record:
- Average query latency (p50, p95, p99)
- Queries per second
- Error rate (should be 0%)

### 2. Inject Fault (1 min)

```bash
# Kill the active coordinator process
ssh coordinator-active "kill -9 \$(pgrep -f pangrova-coordinator)"

# Start recovery time measurement
./tools/measure-recovery-time.sh measure \
  --endpoint http://coordinator-standby:8080 \
  --probe-query "SELECT 1" \
  --timeout 60s \
  --output /tmp/gd-001-recovery.json
```

### 3. Observe (10 min)

Monitor and record:
- [ ] Time until standby detects leader failure
- [ ] Time until standby assumes leadership
- [ ] Time until first successful query on new leader
- [ ] Number of queries that failed during failover
- [ ] Error codes returned to clients during failover
- [ ] Whether failed queries are retryable (error code indicates retry)

### 4. Validate Recovery

- [ ] New leader serves queries at baseline latency
- [ ] No data corruption in metadata store
- [ ] Client connections re-established automatically
- [ ] Query history preserved (no orphan query records)
- [ ] Worker nodes re-registered with new coordinator

## Expected Outcomes

| Outcome | Pass Criteria |
|---------|--------------|
| Failover time | < 30 seconds from kill to first successful query |
| Query failures | Transient only, all return retryable error code |
| Data integrity | Zero metadata corruption |
| Client recovery | Automatic reconnection within 60 seconds |

## Rollback Plan

If the standby fails to assume leadership:
1. Restart the original coordinator: `ssh coordinator-active "systemctl start pangrova-coordinator"`
2. Verify cluster health via `/api/v1/cluster/status`
3. Investigate standby logs for leadership election failure

## Observability Checklist

- [ ] Grafana alert fired for coordinator down
- [ ] Standby promotion logged at INFO level
- [ ] Client-side retry metrics visible
- [ ] No ERROR-level log spam after recovery

## Related Runbook

[Coordinator Failover Runbook](../runbooks/coordinator-failover-runbook.md)
