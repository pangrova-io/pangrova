# Game Day Post-Mortem: [GD-XXX] [Scenario Title]

> **Blame-free culture**: This document exists to improve the system, not to assign blame.
> Every failure reveals a gap in the system's design, monitoring, or documentation — not
> a failing of any individual. Write findings objectively; strip adjectives like "obvious",
> "should have", and "clearly".
>
> **48-hour deadline**: Post-mortem must be complete and shared within 48 hours of game day.

---

## Summary

| Field | Value |
|-------|-------|
| Date | YYYY-MM-DD |
| Start time | HH:MM (local timezone) |
| End time | HH:MM (local timezone) |
| Total duration | X hours |
| Scenarios executed | GD-XXX, GD-YYY |
| Facilitator | [Name] |
| Executor | [Name] |
| Observer | [Name] |
| Scribe | [Name] |
| Environment | Staging — [cluster name / region] |
| Pangrova version | [git SHA or release tag] |

---

## Scenario Results

| # | Scenario ID | Scenario Title | Result | Recovery Time | RTO Target | RTO Met? |
|---|-------------|----------------|--------|---------------|------------|---------|
| 1 | GD-XXX | [Title] | Pass / Fail / Partial | Xs | Xs | Yes / No |
| 2 | GD-YYY | [Title] | Pass / Fail / Partial | Xs | Xs | Yes / No |

**Overall verdict**: All pass / Partial pass / Fail

---

## Timing Ladder

Reconstruct the timeline from logs and measurement tool output. Use UTC timestamps.

```
HH:MM:SS.mmm  [BASELINE]  Baseline recording started
HH:MM:SS.mmm  [BASELINE]  Baseline complete — latency avg=Xms p99=Xms success=100%
HH:MM:SS.mmm  [MEASURE]   measure-recovery-time.sh started (timeout=120s)
HH:MM:SS.mmm  [INJECT]    Fault injected: [exact command / action taken]
HH:MM:SS.mmm  [DETECT]    First probe failure observed
HH:MM:SS.mmm  [ALERT]     Alert fired: [alert name, if applicable]
HH:MM:SS.mmm  [PROGRESS]  [Intermediate state observation]
HH:MM:SS.mmm  [RECOVER]   First successful probe — recovery_time=Xs
HH:MM:SS.mmm  [STABLE]    10s stability window confirmed
HH:MM:SS.mmm  [VERIFY]    Cluster health verified via /v1/cluster/status
HH:MM:SS.mmm  [END]       Scenario complete
```

---

## Detailed Findings

### Scenario 1: GD-XXX — [Title]

**Fault injected:**
```bash
# Exact command(s) executed to inject the fault
[paste command here]
```

**Expected behavior:**
1. [Expected step 1: what the system should detect and when]
2. [Expected step 2: what the system should do in response]
3. [Expected step 3: what the recovered state should look like]

**Actual behavior:**
1. [What actually happened, in order]
2. [Any deviations from expected behavior]
3. [Final recovered state]

**Recovery time:**
- Fault injected at: HH:MM:SS
- First failure detected at: HH:MM:SS (detect latency: Xs)
- First recovery probe at: HH:MM:SS
- **Total recovery time: Xs** (RTO target: Xs → [MET / MISSED by Xs])

**Metric observations:**

| Metric | Baseline | During fault | Post-recovery |
|--------|----------|--------------|---------------|
| Queries/sec | X | X | X |
| p99 latency | Xms | Xms | Xms |
| Error rate | 0% | X% | 0% |
| Active workers | X | X | X |
| Coordinator role | LEADER / FOLLOWER | — | LEADER / FOLLOWER |

**Observations:**
- [Observation 1: what worked correctly]
- [Observation 2: what was unexpected]
- [Observation 3: any cascading effects]

**Evidence:**
- Grafana screenshot: [link or attachment filename]
- Log snippet: [paste relevant log lines, or link to log archive]
- measure-recovery-time.sh output: [paste report output or link to JSON files]

---

### Scenario 2: GD-YYY — [Title]

**Fault injected:**
```bash
[paste command]
```

**Expected behavior:**
1. [Expected]

**Actual behavior:**
1. [Actual]

**Recovery time:** Xs (RTO: Xs → [MET / MISSED])

**Observations:**
- [Observation 1]

**Evidence:**
- [Link or attachment]

---

## What Went Well

List things that worked correctly or better than expected. Be specific — name the mechanism.

- [Example: Coordinator JDBC lease expiry fired correctly at 30s. Standby promoted to LEADER without manual intervention.]
- [Example: Alert `coordinator_leader_lost` fired within 5s of fault injection — under the 15s threshold.]
- [Example: Runbook step "Verify standby promotion" was unambiguous and executor completed it in under 2 minutes.]

---

## What Went Wrong

List gaps, failures, and unexpected behaviors. Do not editorialize.

- [Example: Worker cleanup of orphaned shuffle files took 90s instead of the expected 60s. No alert fired for this condition.]
- [Example: Runbook step 4 ("Check orphaned resources") did not specify which API endpoint to call. Executor had to look it up.]
- [Example: `measure-recovery-time.sh` returned HTTP 503 for 15s before any cluster-level alert fired, indicating an alerting gap.]

---

## Surprises

Things that were not predicted during scenario design. Include both positive surprises (better than expected) and negative ones.

- [Example: Metadata cache continued serving cached table schemas for 120s during the PostgreSQL partition, allowing 73% of queries to succeed — better than expected.]
- [Example: Two in-flight queries with open shuffle partitions were silently dropped instead of returning retryable error codes. This was not documented behavior.]
- [Example: iptables rule persisted across a worker container restart — the worker re-joined the cluster but remained isolated. Required manual rule cleanup.]

---

## Action Items

File each action item as a GitHub issue with label `game-day` immediately after the post-mortem.

| # | Action | GitHub Issue | Owner | Priority | Due | Status |
|---|--------|-------------|-------|----------|-----|--------|
| 1 | [Specific, actionable description of what needs to change] | #XXXX | [Name] | P1 | YYYY-MM-DD | Open |
| 2 | [Description] | #XXXX | [Name] | P2 | YYYY-MM-DD | Open |
| 3 | [Description] | #XXXX | [Name] | P2 | YYYY-MM-DD | Open |

**Priority guide:**
- **P1**: Blocks production deployment or safety — must fix before next game day
- **P2**: Significant gap in resilience or observability — fix within 2 sprints
- **P3**: Nice-to-have improvement — backlog

---

## Runbook Updates Required

Document every runbook change needed. The runbook owner must update and re-verify within 5 business days.

| Runbook | Section | Change Needed | Owner | Done? |
|---------|---------|--------------|-------|-------|
| [runbook-name.md] | [§ section name] | [What is wrong and what the correct content is] | [Name] | ☐ |

---

## Metrics Summary

### Recovery Times vs RTO

| Component | RTO Target | Actual | Delta | Result |
|-----------|-----------|--------|-------|--------|
| Coordinator failover | 30s | Xs | +/-Xs | PASS / FAIL |
| Worker crash recovery | 60s | Xs | +/-Xs | PASS / FAIL |
| Metadata partition | 120s | Xs | +/-Xs | PASS / FAIL |
| Query re-routing | 10s | Xs | +/-Xs | PASS / FAIL |

### Query Impact During Fault

| Metric | Pre-fault baseline | Peak during fault | Post-recovery (5m avg) |
|--------|-------------------|-------------------|----------------------|
| Queries/sec | X | X | X |
| Error rate | 0% | X% | 0% |
| p50 latency | Xms | Xms | Xms |
| p99 latency | Xms | Xms | Xms |

### Alert Performance

| Alert Name | Expected fire time | Actual fire time | Delta | Fired? |
|------------|-------------------|------------------|-------|--------|
| coordinator_leader_lost | < 15s | Xs | +/-Xs | Yes / No |
| worker_dead | < 30s | Xs | +/-Xs | Yes / No |
| [other alert] | < Xs | Xs | +/-Xs | Yes / No |

---

## Lessons Learned

Extract reusable insight. Format: **what we learned** and **how it changes the system or process**.

1. **[Concise lesson title]**: [What we learned from this exercise. How it should change system design, monitoring, runbooks, or operational procedures going forward.]

2. **[Concise lesson title]**: [Lesson.]

3. **[Concise lesson title]**: [Lesson.]

---

## Next Game Day Planning

- **Suggested date**: [Next month's first Wednesday]
- **Suggested scenarios**: [Based on gaps discovered, which scenarios should be prioritized next]
- **Prerequisites**: [What action items must be closed before the next game day can proceed safely]

---

## Appendix A — Raw Recovery Time Data

Paste the JSON output from `measure-recovery-time.sh report` here.

```json
{
  "scenario": "GD-XXX",
  "baseline": {},
  "recovery": {}
}
```

## Appendix B — Relevant Log Snippets

Annotate each snippet with what it shows.

```
# Coordinator leader election — showing FOLLOWER → LEADER transition
[timestamp] role=FOLLOWER → role=LEADER  leaseId=...  elapsed=Xs

# Worker heartbeat timeout detection
[timestamp] worker=worker-2  status=DEAD  lastSeen=Xs ago  threshold=30s
```

## Appendix C — Grafana Dashboard State

Attach or link to dashboard screenshots captured at:
- T-5min (baseline)
- T+0 (fault injection)
- T+recovery (first recovery)
- T+5min (post-recovery steady state)

[Screenshot links or filenames]
