# Game Day Post-Mortem: First Game Day — GD-001, GD-002, GD-003

> **Blame-free culture**: This document exists to improve the system, not to assign blame.
> Every failure reveals a gap in the system's design, monitoring, or documentation — not
> a failing of any individual. Write findings objectively; strip adjectives like "obvious",
> "should have", and "clearly".
>
> **Status**: Complete — shared with engineering team 2026-05-07

---

## Summary

| Field | Value |
|-------|-------|
| Date | 2026-05-06 |
| Start time | 09:00 CST (01:00 UTC) |
| End time | 13:15 CST (05:15 UTC) |
| Total duration | 4h 15min |
| Scenarios executed | GD-001 (Coordinator Failover), GD-002 (Worker Crash During Shuffle), GD-003 (Metadata Store Partition) |
| Facilitator | Zhang Wei |
| Executor | Liu Yang |
| Observer | Chen Fang |
| Scribe | Wang Jing |
| Environment | staging-cluster-01 — cn-hangzhou region |
| Pangrova version | `72bb20ac` (branch: `fix-wiring-pipeline-v3`) |

---

## Scenario Results

| # | Scenario ID | Scenario Title | Result | Recovery Time | RTO Target | RTO Met? |
|---|-------------|----------------|--------|---------------|------------|---------|
| 1 | GD-001 | Coordinator Failover | **Partial** | 47s | 30s | **No** |
| 2 | GD-002 | Worker Crash During Shuffle | **Pass** | 38s | 60s | Yes |
| 3 | GD-003 | Metadata Store Partition | **Pass** | 94s | 120s | Yes |

**Overall verdict**: Partial pass. Two of three scenarios met RTO. GD-001 failed the RTO gate by 17 seconds; root cause identified and action items filed.

---

## Timing Ladder

All timestamps in UTC. Reconstructed from coordinator logs, `measure-recovery-time.sh` output, and scribe notes.

### GD-001: Coordinator Failover

```
01:12:00.000  [BASELINE]  Baseline recording started (30 probes over 60s)
01:13:02.441  [BASELINE]  Baseline complete — avg=12ms p50=11ms p99=24ms success=100%
01:13:05.001  [MEASURE]   measure-recovery-time.sh started (timeout=120s, rto=30s)
01:13:10.204  [INJECT]    kill -9 on coordinator-1 PID 28443 (ssh coordinator-1 "kill -9 ...")
01:13:10.341  [DETECT]    First probe failure observed (HTTP timeout)
01:13:10.341  [DOWN]      Outage window started
01:13:15.883  [LOG]       coordinator-2: "JDBC lease acquisition attempt 1"
01:13:25.210  [LOG]       coordinator-2: "JDBC lease acquisition attempt 3"
01:13:35.003  [LOG]       coordinator-2: "Acquired coordinator lease — promoting to LEADER"
01:13:38.127  [DETECT]    coordinator-2 /v1/health returns {"role":"LEADER"} for first time
01:13:49.512  [LOG]       Workers re-registered with new leader: 4/4 HEALTHY
01:13:57.119  [RECOVER]   First probe success observed on coordinator-2 (HTTP 200)
01:13:57.119  [UP]        Outage window ended
01:13:57.120  [MEASURE]   Recovery time: 47.0s (RTO=30s) — MISS
01:14:10.003  [VERIFY]    SHOW SCHEMAS: HTTP 200 ✓
01:14:12.881  [VERIFY]    Worker count: 4/4 HEALTHY ✓
01:14:15.000  [CLOSE]     GD-001 complete
```

**Key observation**: The 47s recovery breaks down as:
- Time for JDBC lease to expire: ~24s (lease_duration=30s, lease was ~6s old at kill time)
- coordinator-2 lease acquisition: ~11s (polling interval 3s × 3 attempts + DB latency)
- Health endpoint first success: ~8s after lease acquired (worker re-registration delay)
- **The bottleneck is worker re-registration**, not the leader election itself.

### GD-002: Worker Crash During Shuffle

```
01:35:00.000  [BASELINE]  Baseline recording started — long shuffle query running
01:35:42.003  [INJECT]    kill -9 on worker-2 PID 31205
01:35:42.181  [DETECT]    coordinator: "Missed heartbeat from worker-2" (1st)
01:35:57.344  [DETECT]    coordinator: "Worker worker-2 marked DEAD (3 missed heartbeats)"
01:35:57.344  [DOWN]      Shuffle tasks on worker-2 declared failed
01:35:57.500  [LOG]       coordinator: "Rescheduling 3 tasks from worker-2"
01:36:02.009  [LOG]       coordinator: "Shuffle cleanup triggered for worker-2"
01:36:04.771  [LOG]       worker-3: "Cleaned 7 orphan shuffle partitions from worker-2"
01:36:05.209  [LOG]       worker-1: "Cleaned 4 orphan shuffle partitions from worker-2"
01:36:10.001  [RESTART]   systemctl start pangrova-worker on worker-2
01:36:28.440  [LOG]       coordinator: "worker-2 registered: HEALTHY"
01:36:20.448  [RECOVER]   Active queries resumed (no client-visible failure for retryable queries)
01:36:20.448  [UP]        Worker available again
01:36:20.449  [MEASURE]   Recovery time: 38.4s (RTO=60s) — PASS ✓
```

**Key observation**: Query retry happened automatically at 01:35:57.500. Users running retryable queries saw ~15s latency increase but no error. Two non-retryable queries (result-streaming type) failed with `WORKER_UNAVAILABLE` — clients received error codes as expected.

### GD-003: Metadata Store Partition

```
02:10:00.000  [BASELINE]  Baseline complete — DDL queries succeeding, avg=8ms
02:10:05.001  [INJECT]    iptables DROP added: coordinator-1,2,3 → metadata-store:5432
02:10:05.220  [DETECT]    coordinator-1: "JDBC connection failed (attempt 1)"
02:10:08.330  [DETECT]    coordinator-1: "Metadata circuit breaker: OPEN"
02:10:08.441  [LOG]       DDL queries begin failing: HTTP 503 (circuit open)
02:10:08.441  [DOWN]      Metadata unavailable window started
02:10:09.003  [LOG]       Cached metadata still serving SELECT queries (catalog cache HIT)
02:10:34.001  [LOG]       Cache TTL expiry — coordinator-2 attempts metadata refresh
02:10:34.221  [LOG]       coordinator-2: "Metadata refresh failed — serving stale cache (risk: stale)"
02:10:38.884  [RESTORE]   iptables DROP rules removed
02:10:39.002  [LOG]       coordinator-1: "JDBC reconnection attempt 1 — success"
02:10:39.110  [LOG]       coordinator-1: "Metadata circuit breaker: CLOSED"
02:11:39.220  [RECOVER]   HikariPool reconnected on all 3 coordinators
02:11:39.221  [MEASURE]   Recovery time: 94s from inject to full DDL success (RTO=120s) — PASS ✓
02:11:42.003  [VERIFY]    SHOW SCHEMAS: HTTP 200 ✓
02:11:45.334  [ACTION]    Cache invalidation triggered manually: POST /v1/admin/cache/invalidate
```

**Key observation**: The 94s includes ~30s of JDBC pool reconnect settling time after the network was restored. SELECT queries using cached catalog never failed. The 25s window of stale cache (02:10:09 → 02:10:34) served 3 queries against a schema that had been modified in the last 30s — this is a correctness risk documented as action item A-003-1.

---

## Metric Observations

### GD-001: Coordinator Failover

| Metric | Baseline | At Fault Inject | During Outage | Post-Recovery |
|--------|----------|-----------------|---------------|---------------|
| Queries/sec | 142 | 142 | 0 | 138 |
| p99 latency (ms) | 24 | 24 | — | 31 |
| Error rate | 0% | 0% | 100% | 0.4% |
| Active workers | 4/4 | 4/4 | 4/4 | 4/4 |
| Coordinator role | 1 LEADER | 1 LEADER (dying) | 0 LEADER | 1 LEADER |

### GD-002: Worker Crash During Shuffle

| Metric | Baseline | At Fault Inject | During Outage | Post-Recovery |
|--------|----------|-----------------|---------------|---------------|
| Queries/sec | 87 | 87 | 81 | 89 |
| p99 latency (ms) | 310 | 310 | 480 | 325 |
| Error rate | 0% | 0% | 2.3% | 0% |
| Active workers | 4/4 | 4/4 | 3/4 | 4/4 |
| Shuffle partitions cleaned | — | — | 11 orphan files | 0 |

### GD-003: Metadata Store Partition

| Metric | Baseline | At Fault Inject | During Outage | Post-Recovery |
|--------|----------|-----------------|---------------|---------------|
| DDL queries/sec | 8 | 8 | 0 | 8 |
| SELECT queries/sec | 134 | 134 | 131 (cache) | 134 |
| Circuit breaker state | CLOSED | OPENING | OPEN | CLOSED |
| Catalog cache hits | 68% | 68% | 100% | 68% |
| Stale cache serving window | — | — | 25s | — |

---

## Alert Performance

| Alert Name | Expected Fire Time | Actual Fire Time | Delta | Verdict |
|------------|-------------------|-----------------|-------|---------|
| `coordinator_leader_lost` | ≤ 15s after kill | 14s ✓ | -1s | Pass |
| `coordinator_failover_rto_exceeded` | N/A | 47s (fired) | N/A | Fired correctly |
| `worker_heartbeat_missed` | ≤ 20s after kill | 15s ✓ | -5s | Pass |
| `worker_dead` | ≤ 60s after kill | 57s ✓ | -3s | Pass |
| `metadata_store_unreachable` | ≤ 30s after partition | 28s ✓ | -2s | Pass |
| `metadata_circuit_breaker_open` | Immediate on CB open | 3s ✓ | +3s | Pass |
| `metadata_stale_serving_risk` | Not configured | **NOT FIRED** | — | **GAP** |

**Alert gap**: No alert fires when the metadata cache is serving stale data during a partition. Operators had no signal that consistency was degraded during the 25s window. See A-003-1.

---

## Findings

### GD-001: Coordinator Failover

**F-001-1** — Worker re-registration adds 8–12s to failover recovery time beyond the JDBC lease period.

When the new LEADER is elected, workers must re-register before they can receive new tasks. During this window, the new leader is LEADER but has no workers registered, so queries queue or fail. The RTO is measured to first successful query, not to leader election, which means worker re-registration is on the critical path.

**Impact**: RTO of 47s vs 30s target. Queries that cannot be retried fail during the 8–12s re-registration window.

**F-001-2** — The `measure-recovery-time.sh` tool was not installed on the executor's machine. The executor had to pull it manually during setup, consuming 12 minutes of preparation time.

**Impact**: Delayed start of GD-001 by 12 minutes; increased stress for the executor during setup.

### GD-002: Worker Crash During Shuffle

**F-002-1** — Non-retryable query failures (result-streaming queries) are not signaled to clients with actionable context.

Two queries failed with `WORKER_UNAVAILABLE` but the error message contained only the error code, not the affected worker or recommended action (e.g., "retry your query"). Clients had to inspect server logs to understand what happened.

**Impact**: Operator experience gap; no production impact beyond the expected query failure.

**F-002-2** — Shuffle cleanup API succeeded but took 4.7s per surviving worker (7+4 files). For larger shuffle stages, this could add 30–60s to recovery time.

**Impact**: Low for this test (11 files total). Could be higher for production shuffle workloads with hundreds of partitions.

### GD-003: Metadata Store Partition

**F-003-1** — SELECT queries serving stale catalog entries during the 25s stale-cache window had no differentiation in the response (no `X-Cache-Stale` header, no warning). A downstream system acting on a schema that was modified 20s earlier could produce incorrect results.

**Impact**: Correctness risk. If a schema DROP happened within the last `catalog_cache_ttl` seconds before a partition, queries running during the partition could reference columns that no longer exist.

**F-003-2** — Post-recovery, cache invalidation was performed manually. The system has no automatic invalidation trigger when the circuit breaker closes after a partition.

**Impact**: Manual step required; easy to forget under pressure. Operators must remember to run `POST /v1/admin/cache/invalidate` after every metadata partition recovery.

---

## Action Items

| # | Finding | Action | Owner | Priority | GitHub Issue | Due |
|---|---------|--------|-------|----------|--------------|-----|
| A-001-1 | F-001-1 | Investigate worker pre-registration: coordinator should begin accepting worker registrations as soon as lease acquisition starts, not after LEADER state is confirmed. Prototype "warm standby registration" where workers maintain dual registration with all coordinators. | Coordinator team | P1 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-05-21 |
| A-001-2 | F-001-2 | Add `measure-recovery-time.sh` to the staging cluster's `PATH` via the cluster provisioning script. | SRE | P2 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-05-13 |
| A-001-3 | F-001-1 | Review RTO target: if warm standby registration is not feasible in 2 sprints, formally update the coordinator failover RTO to 60s and update SLA documents accordingly. | SRE lead | P2 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-05-21 |
| A-002-1 | F-002-1 | Add `failedWorker`, `recommendedAction`, and `retryable` fields to `WORKER_UNAVAILABLE` error responses. Update client library to surface these fields in error messages. | Worker team | P2 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-05-28 |
| A-002-2 | F-002-2 | Benchmark shuffle cleanup API performance with 500 and 2000 orphan partitions. If > 10s, add parallel cleanup and page the on-call if cleanup time > 30s. | Worker team | P3 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-06-11 |
| A-003-1 | F-003-1 | Add `metadata_stale_serving_risk` Prometheus alert: fires when `catalog_cache_age_seconds > catalog_cache_ttl * 0.8 AND metadata_store_up == 0`. Add `X-Catalog-Cache-Age` response header to SQL responses so clients can detect staleness. | Metadata team | P1 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-05-21 |
| A-003-2 | F-003-2 | Trigger automatic cache invalidation when the metadata circuit breaker transitions from OPEN to CLOSED. The `CircuitBreakerStateChangeListener` SPI already exists — implement the invalidation hook there. | Metadata team | P2 | [#TBD](https://github.com/pangrova/pangrova/issues/) | 2026-05-28 |

**Priority guide**: P1 = address before next game day | P2 = address within 1 sprint | P3 = address within 2 sprints

**Owner accountability**: Each owner must update their GitHub issue with progress within 5 business days. Unstarted P1 items are escalated to the SRE lead at the 5-day mark.

---

## Runbook Updates

Based on findings, the following runbooks were updated after this game day:

| Runbook | Change |
|---------|--------|
| [coordinator-failover-runbook.md](../runbooks/coordinator-failover-runbook.md) | Added Step 2 note: after leader election, wait for all workers to re-register before declaring recovery complete. Added worker count verification to Step 3. |
| [metadata-partition-runbook.md](../runbooks/metadata-partition-runbook.md) | Added explicit Step 4 action: POST /v1/admin/cache/invalidate immediately after circuit breaker closes. Added stale-serving warning callout in Step 2. |
| [worker-crash-runbook.md](../runbooks/worker-crash-runbook.md) | Added non-retryable query identification in Step 2. Added shuffle cleanup timing note in Step 3 for large shuffle workloads. |

---

## Lessons Learned

| What we learned | How it changes the system |
|-----------------|--------------------------|
| Worker re-registration is on the critical path for coordinator failover RTO, not just leader election. The 30s RTO was designed around lease expiry alone and did not account for re-registration time. | We will prototype warm standby registration (A-001-1) and formally revise the RTO target to reflect the full recovery chain if the prototype is not feasible in 2 sprints. |
| The metadata circuit breaker pattern is effective at protecting query throughput during a partition (SELECT queries continued serving), but the stale-serving window is invisible to operators and clients. | Two changes: a Prometheus alert for the stale-serving condition (A-003-1) and an automatic cache invalidation on circuit breaker close (A-003-2). |
| Game day tooling setup belongs in the cluster provisioning script, not in the runbook. Missing tools extend setup time and increase operator stress. | `measure-recovery-time.sh` will be deployed to all staging cluster nodes during provisioning (A-001-2). |
| The non-retryable query failure UX is poor: `WORKER_UNAVAILABLE` alone gives the client no path forward. | Error responses will include `failedWorker`, `retryable`, and `recommendedAction` (A-002-1). |

---

## What Went Well

- Automatic worker crash detection and task rescheduling worked exactly as designed: the coordinator declared worker-2 DEAD at 57s, rescheduled tasks, and retryable queries completed with no client error.
- The JDBC metadata circuit breaker opened correctly at first failure and SELECT queries remained available throughout the 94s partition window via cached catalog. Cache hit rate reached 100% during the partition with no errors.
- All four Prometheus alerts fired within their expected windows (alert latency all < 30s).
- The staging cluster was representative of production topology: 3 coordinators, 4 workers, dedicated PostgreSQL for metadata.
- Scribe kept a live log throughout — this made the timing ladder reconstruction straightforward.

---

## What to Do Differently Next Time

- Pre-install all game day tools on the staging cluster before the session (not during).
- Run the `measure-recovery-time.sh baseline` command at least 5 minutes before fault injection so the baseline window captures steady-state traffic (not just the last 60s which may include startup effects).
- For GD-001, have coordinator-2 and coordinator-3 health endpoints pre-confirmed before killing coordinator-1. This session spent 3 minutes during recovery trying to figure out which coordinator became the new leader.
- Add a "freeze" 60-second window between scenarios to allow the cluster to return to full health and avoid scenario interactions.

---

## Next Game Day

| Field | Value |
|-------|-------|
| Tentative date | 2026-06-03 (first Wednesday of June) |
| Proposed scenarios | GD-004 (Network Partition / Split Brain), GD-005 (Disk Full on Worker) |
| Pre-work | Resolve A-001-1 before GD-004 to see if warm standby registration helps; A-003-1 before GD-004 to have the stale-serving alert available |
| Owner | Rotating to Liu Yang as facilitator, Chen Fang as executor |

---

## Appendix A: Raw measure-recovery-time.sh Output

### GD-001 Output

```
$ ./tools/measure-recovery-time.sh measure \
    --endpoint https://coordinator-2:8080 \
    --token "${PANGROVA_TOKEN}" \
    --rto-coordinator 30 \
    --query "SELECT 1"

[01:13:05] Starting recovery measurement (timeout=120s)
[01:13:10] [DOWN] First failure at probe 6 (HTTP 000 - timeout)
[01:13:11] [DOWN] probe 7: HTTP 000
...
[01:13:56] [DOWN] probe 51: HTTP 000
[01:13:57] [UP] First success at probe 52 (HTTP 200, latency=31ms)
[01:14:07] [STABLE] Recovery confirmed (10s stability window, 10/10 probes succeeded)

=== Recovery Report ===
Scenario      : coordinator
Down at       : 01:13:10 UTC
Up at         : 01:13:57 UTC
Recovery time : 47.0s
RTO target    : 30s
RTO result    : MISS (17.0s over target)

Latency (post-recovery, 10 probes):
  p50 = 28ms
  p95 = 33ms
  p99 = 34ms
```

### GD-002 Output

```
$ ./tools/measure-recovery-time.sh measure \
    --endpoint https://coordinator:8080 \
    --token "${PANGROVA_TOKEN}" \
    --rto-worker 60 \
    --query "SELECT count(*) FROM benchmark.lineitem"

[01:35:42] [DOWN] First failure at probe 1 (HTTP 503)
...
[01:36:19] [UP] First success at probe 37 (HTTP 200, latency=342ms)
[01:36:29] [STABLE] Recovery confirmed (10s stability window, 10/10 probes succeeded)

=== Recovery Report ===
Scenario      : worker
Down at       : 01:35:42 UTC
Up at         : 01:36:20 UTC
Recovery time : 38.4s
RTO target    : 60s
RTO result    : PASS (21.6s under target)
```

### GD-003 Output

```
$ ./tools/measure-recovery-time.sh measure \
    --endpoint https://coordinator-1:8080 \
    --token "${PANGROVA_TOKEN}" \
    --rto-metadata 120 \
    --query "SHOW SCHEMAS"

[02:10:08] [DOWN] First failure at probe 4 (HTTP 503 - circuit open)
...
[02:11:38] [UP] First success at probe 95 (HTTP 200, latency=9ms)
[02:11:48] [STABLE] Recovery confirmed (10s stability window, 10/10 probes succeeded)

=== Recovery Report ===
Scenario      : metadata
Down at       : 02:10:08 UTC
Up at         : 02:11:39 UTC
Recovery time : 94.0s
RTO target    : 120s
RTO result    : PASS (26s under target)
```

---

## Appendix B: Environment Configuration

| Parameter | Value |
|-----------|-------|
| Coordinator count | 3 (coordinator-1, coordinator-2, coordinator-3) |
| Worker count | 4 (worker-0, worker-1, worker-2, worker-3) |
| Coordinator heap | 8 GiB each |
| Worker heap | 16 GiB each |
| JDBC lease duration | 30s |
| JDBC lease poll interval | 3s |
| Metadata cache TTL | 30s |
| Circuit breaker threshold | 5 failures in 10s |
| Worker heartbeat timeout | 15s (3 missed at 5s interval) |
| Staging cluster load | Simulated: 140 queries/sec mix of short SELECT and DDL |

---

## Appendix C: Grafana Screenshots

_Screenshots archived in internal wiki: [game-day-001-grafana](https://wiki.internal/game-day/001/grafana)_

Panels captured:
- `pangrova_coordinator_role` — role transitions during GD-001
- `pangrova_worker_health_status` — worker-2 DEAD/HEALTHY transitions during GD-002
- `pangrova_metadata_circuit_breaker_state` — OPEN/CLOSED during GD-003
- `pangrova_query_success_rate` — all three scenarios overlaid
- `pangrova_p99_latency_ms` — latency envelope throughout the 4-hour session
