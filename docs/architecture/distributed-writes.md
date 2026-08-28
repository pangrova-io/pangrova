---
title: "Distributed Write"
sidebar_label: "Distributed Write"
---

# Pangrova — Distributed Write Design

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

## 1. Current State

Distributed write is partially implemented. Pure metadata/no-read DML still stays in Java, while read-dependent writes can use the distributed task path and report commit metadata back to the coordinator.

Implemented chain:

```
SQL → Parser/Planner → Stage/Task dispatch → Worker NativeWriteExecutor
    → TaskStatusReport write result → TaskStatusReceiver
    → WriteCommitJournal → ConnectorMetadata.finishDistributedWrite()
```

Current implementation anchors:

| Area | Status | Code anchor |
|------|--------|-------------|
| Worker write executor | Implemented | `NativeWriteExecutor` owns per-task sink sessions and commit metadata isolation |
| Coordinator commit aggregation | Implemented | `TaskStatusReceiver` collects write results and calls `finishDistributedWrite()` once per query |
| Idempotency journal | Implemented | `WriteCommitJournal`, `InMemoryWriteCommitJournal`, `JdbcWriteCommitJournal` |
| Connector atomic commit | Implemented for Iceberg and Paimon | `IcebergMetadata.finishDistributedWrite()`, `PaimonMetadata.finishDistributedWrite()` |
| Coordinator-local DML | Still supported | `DmlExecutor` handles no-read or coordinator-level paths |
| Full relational distributed UPDATE/DELETE/MERGE planning | In progress | Planner/stage support is not the only DML route yet |

Routing rule: no-read operations and connector metadata operations remain Java-local. Read-dependent write tasks may execute on workers, but the final catalog commit remains coordinator-owned through the connector SPI.

---

## 2. Write Transaction Atomicity

### Problem Statement

The current write path has an unprotected boundary between connector commit and coordinator acknowledgment. If the worker crashes between `ConnectorPageSink.finish()` (which commits to storage) and the coordinator receiving the task status report, data is committed but the coordinator believes the query failed. User retry produces duplicates.

### Failure Scenarios

| Scenario | Data State | Coordinator State | User Impact |
|----------|-----------|-------------------|-------------|
| Worker crash after `finish()`, before status report | Committed | FAILED (timeout) | Duplicate on retry |
| Network partition after `finish()` | Committed | FAILED (timeout) | Duplicate on retry |
| Coordinator crash after receiving FINISHED | Committed | Unknown (restart) | Orphaned query |
| Worker crash during `finish()` | Partial/None | FAILED | Correct (retry safe) |
| Worker crash before `finish()` | None | FAILED | Correct (retry safe) |

### Two-Phase Write Commit Protocol

The fix moves catalog commit authority from the worker to the coordinator. Workers write data files and produce commit metadata fragments; the coordinator performs the single atomic catalog commit.

SPI change on `ConnectorPageSink`:

```java
default byte[] finishWithMetadata() {
    return new byte[0];
}
```

Non-empty return signals two-phase commit support. Empty return triggers fallback to single-phase `finish()`.

---

## 3. Distributed DML Architecture

Distributed DML follows the same planner→stage→task chain as SELECT when the operation needs worker-side data processing:

```
SQL → Parser → Planner (TableModify) → StageFragmenter → TaskDispatch → Workers
```

### TableModify Relational Operator

Wraps Calcite's `LogicalTableModify`. Captures: target table handle (serializable via `ConnectorOutputTableHandle`), operation type (INSERT/UPDATE/DELETE/MERGE), column assignments, filter predicate, source/target matching expressions. Converts to Substrait `WriteRel` with `ExtensionSingleRel`.

### Stage Fragmentation for Writes

```
Stage 0 (leaf):   TableScan → Filter/Project (read source data)
Stage 1 (middle): Transform (apply UPDATE assignments / MERGE logic)
Stage 2 (root):   Write (via ConnectorPageSink on each worker)
```

### Worker-Side Write Execution

1. Deserialize `WriteRel` from the task request proto
2. Instantiate local `ConnectorPageSink` from serialized output handle
3. Write transformed pages via `appendPage()`
4. Call `sink.finishWithMetadata()` or the connector-specific write executor path to produce commit metadata without final catalog commit
5. Report commit metadata to coordinator via task status

---

## 4. SPI Extensions

```java
// ConnectorPageSink — materialize without commit
default byte[] finishWithMetadata() { return new byte[0]; }

// ConnectorMetadata — single atomic commit from fragments
void finishDistributedWrite(ConnectorSession session,
                            ConnectorOutputTableHandle outputHandle,
                            List<byte[]> fragments);

// ConnectorMetadata — serialize/deserialize output handle for workers
byte[] serializeOutputHandle(ConnectorSession session,
                             ConnectorOutputTableHandle handle);
ConnectorOutputTableHandle deserializeOutputHandle(ConnectorSession session,
                                                    byte[] data);
```

---

## 5. Failure Handling and Recovery

### Orphaned File Cleanup

When writes fail after file materialization but before commit, data files remain unreferenced. Periodic cleanup scans for orphaned files older than retention period (24h default). Iceberg provides `RemoveOrphanFilesAction`; Paimon has `ExpireSnapshotsAction`.

### Write Transaction Journal

Before calling `finishDistributedWrite()`, the coordinator persists aggregated commit metadata to a durable journal. On restart, recovery scans for PENDING entries and attempts idempotent commit.

`finishDistributedWrite()` must be idempotent: check if a snapshot already contains all files from the fragments before committing.

---

## 6. Atomicity by Table Format

**COW Tables (Iceberg)**: Workers serialize `DataFile` descriptors as commit metadata. Coordinator builds single `AppendFiles` operation. Commit is one atomic metadata swap — no partial commits possible.

**MOR Tables (Paimon)**: Workers call `tableWrite.prepareCommit()` to get `List<CommitMessage>` (native two-phase support). Coordinator collects all fragments and triggers single compaction commit.

---

## 7. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Partial commit on worker failure | Coordinator tracks per-worker status; abort uncommitted on failure |
| Connector sink not serializable | `serializeOutputHandle()` SPI; fallback to single-phase for non-implementing connectors |
| Memory pressure on write workers | Account write buffers through the existing worker memory-arbitration path |
| Atomicity for COW tables | Single coordinator-side commit after all workers report success |
| Coordinator crash before commit | `WriteCommitJournal` persists commit intent; recovery replays pending entries idempotently |
| Duplicate commit on recovery | Idempotent `finishDistributedWrite()` checks existing snapshot |

---

## 8. Remaining Work

| Area | Remaining work |
|------|----------------|
| Planner coverage | Complete full relational distributed planning for every UPDATE/DELETE/MERGE shape |
| Fault cleanup | Expand orphan-file cleanup and abort handling across all write failure cases |
| Recovery automation | Wire durable journal recovery into coordinator startup for production HA |
| Performance policy | Add threshold-based routing and benchmarks for coordinator-local vs distributed write choices |
| Connector coverage | Keep Iceberg/Paimon as implemented connectors; add new connectors only with `finishDistributedWrite()` contract tests |
