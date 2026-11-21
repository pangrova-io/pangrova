---
title: "GD-001 Coordinator Failover"
description: "Game-day scenario for coordinator failover"
---

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
- Recovery time measurement tool ready (`../tools/measure-recovery-time.sh`)

## Execution Steps

### 1. Record the Submission Baseline (5 min)

```bash
# Sample SQL submission responses
../tools/measure-recovery-time.sh baseline \
  --endpoint http://coordinator-active:8080 \
  --query "SELECT count(*) FROM benchmark.lineitem" \
  --interval 1s \
  --output /tmp/gd-001-baseline.json
```

The tool times only the HTTP response to the asynchronous SQL submission.
Record submission-response latency (average, p50, p95, p99), probe rate, and
non-200 rate. Do not read them as query latency, query throughput, or query
failure rate. Use a separate workload that consumes query results for those
metrics.

### 2. Inject Fault (1 min)

```bash
# Kill the active coordinator process
ssh coordinator-active "kill -9 \$(pgrep -f pangrova-coordinator)"

# Watch for POST /v1/query/submit to return HTTP 200 again
../tools/measure-recovery-time.sh measure \
  --endpoint http://coordinator-standby:8080 \
  --probe-query "SELECT 1" \
  --timeout 60s \
  --output /tmp/gd-001-recovery.json
```

The tool reports the interval from its first failed submission probe to its
first subsequent HTTP 200. This is not time from fault injection. If it never
observes a failed probe, it cannot measure a submission outage. HTTP 200 alone
does not show that a query finished or its result was fetched.

### 3. Observe (10 min)

Monitor and record:
- [ ] Time until standby detects leader failure
- [ ] Time until standby assumes leadership
- [ ] Interval between first failed submission probe and next HTTP 200 (if observed)
- [ ] Time from fault injection to the first new catalog-backed query whose
  expected result the client consumes on the surviving Coordinator
- [ ] Number of queries that failed during failover
- [ ] Error codes returned to clients during failover
- [ ] Whether failed queries are retryable (error code indicates retry)

### 4. Validate Recovery

- [ ] Completed queries return to the separately measured end-to-end baseline latency
- [ ] No data corruption in metadata store
- [ ] Client connections re-established automatically
- [ ] Query history preserved (no orphan query records)
- [ ] Worker nodes re-registered with new coordinator

## Expected Outcomes

| Outcome | Pass Criteria |
|---------|--------------|
| Query recovery time (RTO) | < 30 seconds from recorded fault injection to the first new catalog-backed query whose expected result is consumed; the submission probe alone cannot pass this criterion |
| Query failures | Transient only, all return retryable error code |
| Data integrity | Zero metadata corruption |
| Client recovery | Automatic reconnection within 60 seconds |

## Rollback Plan

If the standby fails to assume leadership:
1. Restart the original coordinator: `ssh coordinator-active "systemctl start pangrova-coordinator"`
2. Verify cluster health via `GET /v1/health` on each coordinator and the HA lease table (`distributed_lease` in `pangrova_meta`)
3. Investigate standby logs for leadership election failure

## Observability Checklist

- [ ] Grafana alert fired for coordinator down
- [ ] Standby promotion logged at INFO level
- [ ] Client-side retry metrics visible
- [ ] No ERROR-level log spam after recovery

## Related Runbook

[Coordinator Failover Runbook](../runbooks/coordinator-failover-runbook.md)
