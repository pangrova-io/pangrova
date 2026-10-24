# Lakehouse access and write production readiness

This audit covers both existing Iceberg/Paimon table access and data written through
Pangrova. An implemented interface or a passing component test is not production
qualification. Existing uncommitted materialization work remains part of the source
to validate, not a reason to omit affected integration gates.

## Acceptance matrix

| Requirement | Current evidence / remaining work |
|---|---|
| Discover and query externally created tables | Connector factories, read bindings and SDK-generated fixtures exist; run real catalog/storage and packaged Native SQL tests for both formats. |
| Preserve existing Paimon table semantics | `PaimonNativeMergeAdmission` rejects unrepresented policies; implement and test sequence groups, field aggregators and remaining collection/numeric policies before claiming general compatibility. |
| SQL create/insert/query and copy-on-write mutations | Implementations and E2E fixtures exist; run current-source read/write tests and resolve failures. Conditional MERGE combinations remain rejected. |
| Multi-worker writes preserve rows and policies | Requires distinct Worker processes and endpoints, exact data assertions and fault injection; routing tests do not qualify. |
| Durable commit identity and external catalog fence | Journal and connector markers exist. Ambiguous concurrent recovery, marker loss/rollback and table-incarnation races need catalog-authoritative proof and tests. |
| Complete CDC checkpoints | Implemented and component-tested: persist all topics, partitions and exact ranges; restore only committed checkpoints; acknowledge source after durable state; retain failed batches. Stream incarnation and publication reconciliation remain separate open requirements below. |
| CDC Worker execution and atomic target publication | Still open: coordinator-local writer splits delete/insert publication, Paimon rejects context-free commit. Replace with canonical Worker execution and one target snapshot. |
| CDC recovery and authorization | Stream creations now have persistent checkpoint identities; state updates/deletes check tenant, identity and epoch. Still open: bind current principal authority, ownership epoch, table UUID, base snapshot and output binding to the catalog publication intent, and reconcile ambiguous publication before acknowledging source. |
| Lifecycle and operational safety | Verify reconnect/restart, schema evolution, object-store credentials, orphan cleanup, compaction and snapshot-retention interactions. |
| Production validation | Matching Native build/tests/package; full Java reactor; full-regression and isolated Native owner suites; distinct-process fault tests; required sanitizer gates. No skipped required checks. |

## Current checkpoint repair

The old CDC record persisted one representative partition and reconstructed its
topic from the stream ID. That cannot restore a multi-topic/multi-partition batch.
The replacement stores a canonical vector of actual topic/partition start/end
ranges in the intent row. Reusing a batch identity with different ranges fails.
The COMMITTED transition exposes the entire vector together. Source acknowledgement
follows this transition, and a failed batch stays pending until it succeeds.
`StreamingBootstrap` now supplies the JDBC commit coordinator to `StreamScheduler`;
the scheduler previously passed a null coordinator to every production pipeline.
Startup recovery failure is reported through the pipeline failure listener before
polling, so unresolved publication cannot leave a silently active stream.
Kafka restart offsets are deferred until the consumer's partition-assignment
callback, before records are fetched. Calling `seek` immediately after `subscribe`
previously failed because the consumer had no assigned partitions. An explicit
seek on an already assigned partition still applies immediately, and offsets for
another topic are rejected rather than silently ignored.

Migration V73 preserves old representative columns for operator reconciliation.
It does not invent missing topics or ranges. Recovery of old active/committed rows
without complete checkpoints fails explicitly; those rows require reconciliation
with actual source and catalog publication evidence before streams resume.

Migration V75 adds an immutable checkpoint identity to stream state. Existing
streams retain their original source key and default Kafka consumer group; the
migration does not guess which historical publication belongs to which creation.
New stream creations receive independent identities used by both the durable
checkpoint coordinator and default Kafka group. Dropping/recreating a name no
longer inherits the previous creation's checkpoints or default group offsets.
An explicitly configured Kafka group remains user-controlled and may intentionally
share an existing group's position. Legacy ambiguous histories still require
reconciliation; this migration cannot repair identity evidence never recorded.

Creation uses insert-only semantics. Updates and deletions match tenant,
checkpoint identity and epoch, and a deletion cascades cached offsets atomically.
Late callbacks cannot insert a deleted stream or overwrite its replacement.
These metadata guards do not fence an already-running external catalog commit;
the catalog publication/ownership requirement remains open.

This repair does not make separate catalog and JDBC transactions atomic, implement
Worker CDC execution, or complete the acceptance matrix above.

## CDC scheduler lifecycle repair

Production bootstrap now supplies the coordinator identity to the scheduler.
Each admitted start claims a new durable epoch, including a restart on the same
coordinator. Schedulers start paused until election. Re-election clears the local
pause and submits reconciliation on a separate control executor; source factories
do not block the maintenance-lease renewal callback. Periodic five-second passes
discover remote CREATE/RESUME and stop superseded attempts after remote DDL.
SUSPEND and DROP advance the stored epoch before stopping the local pipeline, so
an old failure callback cannot overwrite the requested lifecycle transition.
RESUME, property changes and terminal failures also advance the epoch. Property
changes restart RUNNING pipelines with the new configuration. CREATE retains the
full target name rather than stripping its catalog and schema. Maintenance-lease
renewal errors demote local services, and a new backend epoch reactivates them.

A stream slot is reserved before source/sink factories run. Concurrent duplicate
starts do not allocate a second reader or claim a second epoch. A factory result
that arrives after stop or replacement is closed rather than installed. Queued
cancellation and execution claim exclusive ownership of resource cleanup; factory
failure and executor rejection close all resources already allocated. Running
pipelines close both source and sink on their own thread, and release their thread
reference before the executor can reuse that thread. Late completion/failure
removes only its own slot, preserving a newer attempt and its watermark tracking.

These fixes protect local lifecycle and metadata updates. They do not establish a
catalog publication fence or guarantee cancellation of an external commit already
in progress. Distinct-process ownership transfer, lease-expiry races during blocked
backend calls, and publication fencing still need qualification alongside the
Worker/publication requirements above.

## Evidence boundaries

- The focused checkpoint/wiring selection initially passed 65 tests without skips,
  including a real PostgreSQL migration and scheduler checkpoint test. The expanded
  Streaming module subsequently passed 234 tests with zero failures, errors or
  skips. Retry properties exercise both failure thresholds and reset after success.
- Streaming Checkstyle and SpotBugs now pass with zero findings. The final focused
  checkpoint test also passes after making the immutable list copy explicit.
  SpotBugs still emits upstream missing-class diagnostics for invokedynamic method
  names; passing the configured gate does not prove analysis of every call site.
- Four real Kafka integration tests pass without skips. They cover batch
  truncation, two-partition startup recovery overriding Kafka group offsets,
  explicit seeks after assignment, and rejection of unrelated-topic/negative
  recovery offsets. This proves the reader recovery boundary, not target
  publication or Worker CDC execution.
- The current Release Native library was rebuilt and packaged, with the packaged
  bytes verified against the build output. All 4,323 Native tests passed with the
  real PostgreSQL/Lance fixture. Sanitizer checks and the remaining
  Java/independent-process chain are not yet qualified by this result.
- The first stream-identity selection passed 37 tests without skips, covering
  state creation/update/delete, epoch properties, cached offsets, CDC recovery and
  scheduler wiring. A subsequent nine-test selection also passed without skips,
  including the V75 upgrade, real Kafka same-name recreation, checkpoint identity
  isolation, scheduler shutdown and watermark cleanup. Full Streaming verification
  after the identity and pipeline-lifecycle changes passed 236 tests without failures,
  errors or skips, including Checkstyle and SpotBugs (with the invokedynamic analysis
  limitation noted above).
- The scheduler/reconciliation selection passed 17 tests without skips, including
  real PostgreSQL state transitions, queued/running resource ownership, stale
  callbacks, property-change restarts, and two local coordinator instances accepting
  remote CREATE/SUSPEND/RESUME. This tests the production reconciliation loop with
  bounded test source/sink collaborators, not a Kafka-to-Worker publication chain.
- The expanded leadership/scheduler selection passed 23 tests without skips. It
  additionally exercises renewal failure followed by recovery, partial activation
  cleanup, and a backend ownership epoch change. Coordinator Checkstyle was rerun:
  the same 36 findings remain in the existing materialization/bootstrap/security
  work; none are in the changed scheduler, stream DDL or leadership files.
- A full Java reactor `full-regression` test run has started against the current
  source. Its outcome and the isolated owner suites remain open acceptance gates.
- The matching ASan Native build succeeded. Its full 4,323-test run is in progress;
  no sanitizer pass is claimed until it finishes. macOS execution does not qualify
  the separate Linux leak-sanitizer requirement.
- `StreamingCdcE2ETest` exercises SQL INSERT/UPDATE/DELETE and SELECT. It does not
  exercise a Kafka source, the production CDC scheduler, recovery offsets or
  Worker CDC dispatch, and cannot qualify the CDC requirements.
- `StreamingCdcWriterE2ETest` includes a stub data sink and separate SQL checks;
  it also cannot qualify the production Kafka-to-Worker CDC publication path.
- The first migration-only invocation was filtered by the default metadata
  integration exclusion and provided no test evidence. The subsequent selection
  enabled `full-regression` and executed the migration test successfully.
- Broader validation exposed an existing outdated `PaimonTableHandle` test caller
  and two missing SPI constructor Javadocs; both were repaired without changing
  the materialization behavior already present in the worktree.
- The Coordinator reactor's static-analysis run is currently blocked upstream by
  25 Checkstyle violations in the existing materialization refactor, including a
  1,099-line service, constructor complexity and excessive argument counts. This
  run did not reach Coordinator analysis and is not a passing full-reactor gate.
  A separate Coordinator Checkstyle invocation reports another 36 violations in
  existing materialization/bootstrap/security changes; none are in the CDC files
  modified here. These gates remain open and must pass before release acceptance.
  Metadata Checkstyle separately reports 42 violations in existing materialization
  persistence code, including a 2,050-line repository. None are in the changed
  stream-state files. Metadata SpotBugs independently reports three findings in
  the existing materialization catalog/repository (two mutable DataSource
  references and one hash comparison); no finding is in the changed stream-state
  files. Neither module-wide gate is passing.
