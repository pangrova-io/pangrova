# Monthly Game Day Process

## Overview

Game days are scheduled monthly exercises where the team intentionally injects faults into the Pangrova cluster to validate resilience, runbooks, and recovery procedures. The goal is to discover weaknesses before they manifest in production.

## Schedule

- **Frequency**: Monthly (first Wednesday of each month)
- **Duration**: 4 hours (2h execution + 2h post-mortem)
- **Participants**: On-call engineer, SRE lead, component owner for targeted subsystem
- **Environment**: Dedicated staging cluster mirroring production topology

## Process

### 1. Preparation (1 week before)

- Select 2-3 scenarios from the [scenario catalog](#scenario-catalog)
- Assign roles: Facilitator, Executor, Observer, Scribe
- Verify staging cluster is healthy and representative
- Notify stakeholders and freeze non-essential deployments on staging

### 2. Execution (Game Day)

| Phase | Duration | Activity |
|-------|----------|----------|
| Briefing | 15 min | Review scenarios, confirm roles, verify monitoring |
| Baseline | 15 min | Record healthy-state metrics (latency, throughput, error rate) |
| Injection | 60 min | Execute fault scenarios sequentially |
| Recovery | 30 min | Validate full recovery, measure recovery times |
| Debrief | 60 min | Initial findings discussion, assign action items |

### 3. Post-Mortem (within 48 hours)

- Complete [post-mortem template](templates/post-mortem-template.md)
- File action items as GitHub issues with `game-day` label
- Update runbooks based on findings
- Share report with engineering team

## Scenario Catalog

| ID | Scenario | Target Component | Severity |
|----|----------|-----------------|----------|
| GD-001 | [Coordinator Failover](scenarios/gd-001-coordinator-failover.md) | Coordinator | Critical |
| GD-002 | [Worker Crash During Shuffle](scenarios/gd-002-worker-crash-shuffle.md) | Worker / Shuffle | High |
| GD-003 | [Metadata Store Partition](scenarios/gd-003-metadata-partition.md) | Metadata | Critical |
| GD-004 | [Network Partition (Split Brain)](scenarios/gd-004-network-partition.md) | Cluster-wide | Critical |
| GD-005 | [Disk Full on Worker](scenarios/gd-005-disk-full-worker.md) | Worker | Medium |
| GD-006 | [Clock Skew](scenarios/gd-006-clock-skew.md) | Coordinator / Metadata | Medium |

## Success Criteria

A game day is successful when:
1. All scenarios executed without unplanned cluster damage
2. Recovery time measured for each fault
3. At least one runbook improvement identified
4. Post-mortem document completed within 48 hours

## Recovery Time Objectives (RTO)

| Component | Target RTO | Measurement Tool |
|-----------|-----------|-----------------|
| Coordinator failover | < 30 seconds | [measure-recovery-time.sh](tools/measure-recovery-time.sh) |
| Worker crash recovery | < 60 seconds | [measure-recovery-time.sh](tools/measure-recovery-time.sh) |
| Metadata partition heal | < 120 seconds | [measure-recovery-time.sh](tools/measure-recovery-time.sh) |
| Query re-routing | < 10 seconds | [measure-recovery-time.sh](tools/measure-recovery-time.sh) |

## Runbooks

Each scenario has a corresponding runbook for live incident response. Runbooks are used both during game day execution (Executor follows the runbook) and in production.

| Scenario | Runbook | Severity |
|----------|---------|----------|
| GD-001: Coordinator Failover | [coordinator-failover-runbook.md](runbooks/coordinator-failover-runbook.md) | Critical |
| GD-002: Worker Crash During Shuffle | [worker-crash-runbook.md](runbooks/worker-crash-runbook.md) | High |
| GD-003: Metadata Store Partition | [metadata-partition-runbook.md](runbooks/metadata-partition-runbook.md) | Critical |
| GD-004: Network Partition (Split Brain) | [network-partition-runbook.md](runbooks/network-partition-runbook.md) | Critical |
| GD-005: Disk Full on Worker | [disk-full-runbook.md](runbooks/disk-full-runbook.md) | Medium |
| GD-006: Clock Skew | [clock-skew-runbook.md](runbooks/clock-skew-runbook.md) | Medium |

## Tools

- **Recovery Time Measurement**: [`tools/measure-recovery-time.sh`](tools/measure-recovery-time.sh) — baseline, measure, report, and watch subcommands; computes p50/p95/p99 latency; RTO assessment
- **Fault Injection**: Pangrova chaos testing framework (`pangrova-testing/pangrova-test-chaos/`)
- **Monitoring**: Grafana dashboards (staging environment)

### measure-recovery-time.sh Quick Reference

```bash
# Record steady-state baseline before injecting fault
./tools/measure-recovery-time.sh baseline \
  --endpoint https://coordinator-1:8080 --token "${PANGROVA_TOKEN}"

# Measure recovery time after fault injection
./tools/measure-recovery-time.sh measure \
  --endpoint https://coordinator-1:8080 --token "${PANGROVA_TOKEN}" \
  --rto-coordinator 30 --query "SELECT 1"

# Live health tail (detects down/up transitions automatically)
./tools/measure-recovery-time.sh watch \
  --endpoint https://coordinator-1:8080 --interval 2

# Generate final report from saved measurements
./tools/measure-recovery-time.sh report --scenario coordinator
```

## Reports

Post-mortem reports are filed within 48 hours of each game day and stored here. Use the [post-mortem template](templates/post-mortem-template.md) for new reports.

| # | Date | Scenarios Covered | Key Finding | Report |
|---|------|-------------------|-------------|--------|
| 001 | 2026-05-06 | GD-001, GD-002, GD-003 | GD-001 missed 30s RTO by 17s — worker re-registration is on the critical path, not just lease election | [Report](reports/001-first-game-day.md) |
