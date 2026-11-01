# Pangrova Streaming 模块深度 Review

Status: Final — review baseline for the 2026-10 streaming refactor. Class names in the
per-file inventory describe the pre-refactor state and include types intentionally
removed by this refactor.

## Implementation status (2026-10-31)

Implemented and verified in the streaming leaves:

- Four Maven modules: `pangrova-streaming`, `pangrova-streaming-format`,
  `pangrova-streaming-maintenance`, `pangrova-streaming-jdbc`.
- `CdcCommitStateStore` and its data model live in `pangrova-spi`.
- Watermark restore/delay, dead-letter policy, late-data gate, security/fencing/
  metrics wiring, maintenance scheduler concurrency, and commit-state pruning.
- Registration subsystem and all unwired writer/aggregation/query/interceptor code removed.

Verified: 171 streaming tests (64 core + 48 format + 18 maintenance + 41 JDBC),
module-dependency and state-enum property tests, doc-reference, dead-stub, and
SPI-annotation gates. Coordinator/e2e compilation is currently blocked by an
unrelated in-flight metadata/protocol refactor in the shared worktree.

## 1. Scope and baseline

- Module: `pangrova-streaming` (Maven, JDK 17).
- Main sources: 87 Java files, 6,577 LOC across 9 packages.
- Tests: 42 test classes plus 2 test-infrastructure files.
- Production consumer: `pangrova-coordinator` only. `pangrova-worker` has no
  `org.pangrova.streaming` imports.
- SPI contracts live in `pangrova-spi` (`org.pangrova.spi.streaming`), except the
  write model and commit-state types currently duplicated inside this module.

### 1.1 Production topology

```text
CoordinatorBootstrap
├── ModuleIntegrationBootstrap.startStreaming()
│   ├── MaintenanceScheduler + WatermarkTracker → StreamingContext
│   ├── CdcRegistrationHandler (registration; removed by this refactor)
│   └── metadata JdbcWatermarkStore / InMemoryWatermarkStore
└── StreamingBootstrap.build()
    ├── CdcRecordConverterRegistry(Debezium/Canal/Maxwell)
    ├── TwoPhaseCommitCoordinator(JdbcCdcCommitStateStore)
    └── StreamScheduler
        └── per-stream CdcPipelineLoop
            ├── CdcSourceReader (KafkaCdcSourceReader)
            ├── CdcWriteSink (ConnectorPageSinkCdcWriter)
            ├── CdcQuotaEnforcer / BackoffStrategy / Listener
            ├── CdcTwoPhaseCommitter → CdcCommitCoordinator
            ├── WatermarkTracker.updateWatermark()
            └── MaintenanceScheduler.onWriteComplete()

Coordinator maintenance DDL → TableMaintenanceSpi ← ControlledTableMaintenanceSpi
```

The CDC write path is coordinator-local (`ConnectorPageSinkCdcWriter`); it does
not dispatch to a Worker and logs `worker_dispatch_unavailable` at bootstrap.

## 2. Per-package and per-file responsibilities

Legend: **Live** = has a production consumer; **Dead** = no production consumer;
**Partial** = used but missing documented behavior.

### 2.1 `org.pangrova.streaming` (2 files)

| File | Responsibility | Status |
|---|---|---|
| `StreamingContext` | Facade holding `MaintenanceScheduler` + `WatermarkTracker`; `close()` closes only the scheduler. Constructed by `ModuleIntegrationBootstrap`, consumed by `CoordinatorBootstrap`. | Live |
| `StreamingWiring` | Duplicate facade plus `Map<String,TableWriter>` writer registry; only constructed by legacy `ModuleIntegrationWiring` (no production instantiation). | Dead |

There is no root `package-info.java`.

### 2.2 `streaming.aggregation` (9 files)

| File | Responsibility | Status |
|---|---|---|
| `package-info` | Describes retraction aggregation and window support. | Dead |
| `StreamingAccumulator` | `accumulate` / `retract` / `result` / `reset` contract. | Dead |
| `AvgAccumulator` | Running sum/count with retraction; returns `DoubleValue`. | Dead (unit-tested only) |
| `CountDistinctAccumulator` | Exact frequency map; distinct count. | Dead |
| `HyperLogLogCountDistinctAccumulator` | HLL precision 4–18; append-only, retract is a documented no-op. | Dead |
| `WindowAssigner` | Assigns event time to TUMBLE/HOP/SESSION/CUMULATE windows; computes watermark-closed windows. | Dead |
| `WindowBounds` | Inclusive-start/exclusive-end record; `contains`, `isClosedByWatermark`. | Dead |
| `WindowSpec` | Window type + size/slide/gap/step factories. | Dead |
| `WindowType` | TUMBLE/HOP/SESSION/CUMULATE. | Dead |

No operator consumes these types. Window/session merging is described as
"handled externally", but no external operator exists.

### 2.3 `streaming.cdc` (27 files)

| File | Responsibility | Status |
|---|---|---|
| `package-info` | Claims a 10/15 file governance threshold while the package has 27 files. | Stale |
| `CdcPipelineLoop` | Core loop: poll → convert → 2PC/at-least-once write → commit offsets → watermark → maintenance notification; retry/poison-offset handling; retains pending batch across failures. | Live |
| `CdcPipelineLoopContext` | Dependency record (source, converter, sink, quota, watermark, maintenance, runtime hooks). | Live |
| `CdcPipelineConfig` | Stream identity, batch/poll settings, failure thresholds, unused `flushInterval`; no epoch/policies. | Live (incomplete) |
| `CdcPipelineRuntimeHooks` | Listener, retry backoff, optional commit coordinator. | Live |
| `CdcPipelineListener` | `onFailed` callback. | Live |
| `CdcQuotaEnforcer` | Atomic active-write/pending-byte counters; heuristic pause. | Live |
| `CdcTwoPhaseCommitter` | Stages a batch through `CdcWriteSink`, publishes after prepare, discards on abort, commits source offsets. | Live |
| `CdcPipelineMetrics` | Nine metrics + noop singleton; documents a consumer-lag gauge that no source API supports. | Dead (zero callers) |
| `CdcRecordConverterRegistry` | Format-id → converter lookup; last-one-wins; immutable. | Live |
| `CdcRegistrationHandler` | Implements `CdcLifecycleManager`; random `streamId` registration, heartbeat, expiry, `isAvailable`, snapshot-offset passthrough. | Dead after this refactor |
| `CdcRegistrationRequest` / `CdcRegistrationResponse` | Local DTOs for the handler overload. | Dead |
| `CdcStreamInfo` | Local view of a registration. | Dead |
| `CdcStreamState` | Local enum duplicating `CdcRegistration.CdcStreamState`. | Dead |
| `InMemoryCdcRegistrationStore` | Map-backed registration store. | Dead after this refactor |
| `JdbcCdcRegistrationStore` | H2 `MERGE ... KEY` duplicate of the metadata store; table conflicts with the canonical PostgreSQL schema. | Dead |
| `IdempotentExecutor` | Key→supplier dedup strategy; only `CdcConsumer` uses it. | Dead |
| `CdcConsumer` | Legacy Debezium consumer: parse, group by table, UPSERT with retries, dead-letter counter/log. Duplicates `DebeziumJsonConverter` with divergent schema-version behavior. | Dead |
| `CdcConfig` | Legacy consumer config (topics/group/consumer/tenant/quotas). | Dead |
| `PipelineSecurityContext` | Definer-identity security context; `SYSTEM` constant. | Dead (unwired) |
| `PipelineSecurityContextHolder` | ThreadLocal install/require/clear. | Dead (unwired) |
| `WritePolicyEnforcer` | Write-time authorization callback. | Dead (unwired) |
| `CdcFencingToken` | Epoch validation contract; `NOOP`. | Dead (unwired) |
| `StaleFenceException` | Non-retryable zombie-pipeline exception. | Dead (unwired) |
| `StreamFileInterceptor` | Decodes `content_base64` FILE payloads and rewrites them as FILE structs via a blob-writer function. | Dead |
| `StreamEmbeddingInterceptor` | After-commit hook enqueuing embedding work for configured tables. | Dead (unit-tested only) |

### 2.4 `streaming.cdc.commit` (9 files)

| File | Responsibility | Status |
|---|---|---|
| `CdcCommitCoordinator` | Prepare/commit/rollback/recover/atomic-commit/find-offsets contract. | Live |
| `CdcCommitState` | PREPARED → COMMITTING → COMMITTED / PREPARED → ROLLING_BACK → ROLLED_BACK. | Live |
| `CdcBatchCheckpoint` | Canonical, ordered, per-partition exact ranges; `nextOffsets`, SHA-256 fingerprint. | Live |
| `OffsetRange` | Inclusive range validation, count, contains. | Live |
| `SourceId` | Non-blank source identity. | Live |
| `PrepareToken` | Transaction id + source + checkpoint + deadline. | Live |
| `CommitParticipant` | Prepare/commit/abort participant contract with orphan-reclaimability rule. | Live |
| `TwoPhaseCommitCoordinator` | Dedup prepare, CAS state machine, recovery (expired prepare rollback, ambiguous COMMITTING quarantine), participant orchestration. | Live |
| `JdbcCdcCommitStateStore` | PostgreSQL jsonb store: insert, CAS, state update, duplicate/in-flight/committed-offset queries. No terminal-row pruning; `mapToken` drops `batchKey`. | Live (incomplete) |

Commit publication and JDBC state are not one transaction; `COMMITTING` requires
connector reconciliation, but no reconciliation API exists in this module.

### 2.5 `streaming.cdc.format` (4 files)

| File | Responsibility | Status |
|---|---|---|
| `CdcRowParser` | JSON → `CdcRow`; typed value extraction; schema version from `source.struct_version` or sorted field-name hash. | Live |
| `DebeziumJsonConverter` | Debezium payload/op/table/source-info parsing; `c,r,u,d` mapping. | Live |
| `CanalJsonConverter` | Canal multi-row `data`/`old`; overrides `convertBatch` for per-payload dead-letter. | Live |
| `MaxwellJsonConverter` | Maxwell `ts` in seconds; `data`/`old`; delete handling. | Live |

### 2.6 `streaming.watermark` (10 files)

| File | Responsibility | Status |
|---|---|---|
| `WatermarkTracker` | Per-table watermark/state/last-event maps with stripe locks; persist-before-memory; regression sets STALE; idle/lag checks; failed/reset; table removal. | Live (incomplete: no restore, no delay) |
| `WatermarkPropagator` | Static union/join/aggregation/pass-through watermark rules. | Partial (only pass-through used) |
| `WatermarkState` | INITIALIZING/ACTIVE/IDLE/STALE/FAILED. | Live |
| `WatermarkConfig` | Watermark column, delay, idle timeout. | Dead (never read by the live pipeline) |
| `InMemoryWatermarkStore` | Map-backed store. | Live |
| `JdbcWatermarkStore` | H2 `MERGE` duplicate of `org.pangrova.metadata.streaming.JdbcWatermarkStore`; conflicting table `cdc_watermarks`. | Dead |
| `LateDataConfig` | Policy, side-output suffix, retention, allowed lateness. | Dead (never read) |
| `LateDataPolicy` | DROP/UPDATE/SIDE_OUTPUT. | Dead |
| `LateDataRouter` | Routes late records to a side-output `TableWriter`; UPDATE behaves like SIDE_OUTPUT; `allowedLateness` unused. | Dead |
| `LateDataRouteException` | Runtime failure for side-output writes. | Dead |

### 2.7 `streaming.writer` (8 files)

| File | Responsibility | Status |
|---|---|---|
| `LakeTableWriter` | Iceberg writer: groups by primary key; same-batch duplicates become "position deletes" (full rows), single non-INSERT becomes equality delete; sink writes; completion callback. `commitIfSelfManaged` only logs; delete-file counts are record counts. | Dead |
| `MergeTreeTableWriter` | Paimon writer: maps operations to `RowKind` only for debug logging; delegates all records to sink; notifies scheduler. | Dead |
| `DataFileSink` | Module-local duplicate of `org.pangrova.spi.connector.data.write.DataFileSink`; no consumers. | Dead |
| `StreamingWriter` | Mini-batch buffer with byte/row/time flush triggers; no background timer; not thread-safe. | Dead |
| `StreamingWriterConfig` | Buffer limits. | Dead |
| `IncrementalWriter` | Byte-for-byte duplicate of `StreamingWriter` with a different config type. | Dead |
| `IncrementalWriterConfig` | Buffer limits. | Dead |
| `WriteRecordConverter` | `CdcEnvelope` → SPI `WriteRecord`; only `CdcConsumer` uses it. | Dead |

No production type implements `TableWriter`; the live CDC path uses `CdcWriteSink`.

### 2.8 `streaming.maintenance` (16 files)

| File | Responsibility | Status |
|---|---|---|
| `MaintenanceScheduler` | Priority queue + semaphore + per-table locks + periodic scan; interval gating; timeout; backpressure. Global lock is held across SPI calls; blocked join holds table lock; permit/in-flight leak on rejected submit. | Live (defective) |
| `MaintenanceTask` | Table + priority + created time; `compareTo` ignores time. | Live |
| `MaintenanceConfig` | 14 settings; `orphanCleanupScanInterval` and `snapshotRetainCount` unread. | Live (partly dead) |
| `MaintenanceStatus` | 11 status fields; scheduler reads two. | Live |
| `MaintenanceTableSource` | Supplies managed table names. | Live |
| `TableMaintenanceSpi` | Compact/clean-orphans/expire-snapshots/status plus scheduled-cycle defaults. | Live |
| `CompactionResult` / `OrphanCleanupResult` / `SnapshotExpirationResult` | Operation results. | Live |
| `package-info` | Package description. | Live |
| `policy/TriggerPriority` | THRESHOLD / INTERVAL / SCHEDULED_SCAN. | Live |
| `policy/CompactionPolicy` | Duplicate of `MaintenanceConfig` fields. | Dead |
| `policy/SnapshotExpirationPolicy` | Duplicate of `MaintenanceConfig` fields. | Dead |
| `policy/RetentionPolicy` / `RetentionAction` / `RetentionPolicyManager` | Per-table retention island; never invoked. Coordinator `ControlledTableMaintenanceSpi` already drives retention. | Dead |

### 2.9 `streaming.query` (2 files)

| File | Responsibility | Status |
|---|---|---|
| `ContinuousQueryConsumer` | Periodically scans a live query map and schedules per-query refresh callbacks. | Dead (no production caller) |
| `ContinuousQueryExecutor` | Refresh callback contract. | Dead |

`StreamScheduler.registerContinuousQuery` / `startContinuousQueryConsumer` have no
production callers; only a 36-line unit test exists.

## 3. Findings

### P0 — correctness and data safety

1. **Watermark persistence is never restored.** `WatermarkTracker` does not call
   `WatermarkStore.load`; restart resets every table to `Instant.MIN`, so late data
   is accepted as on-time. `WatermarkConfig.watermarkDelay` and `inferWatermark`
   are never used; the loop feeds raw max event time to `updateWatermark`.
2. **Late-data handling is disconnected and mis-implemented.** The live loop never
   calls `LateDataRouter`; `UPDATE` is routed to side output; `allowedLateness` is
   ignored; `SIDE_OUTPUT` has no wired side-output sink.
3. **Dead-letter is a poison pill.** `CdcPipelineLoop.convertBatch`'s callback
   always throws, so one malformed record fails the whole batch and eventually
   stops the pipeline. Documentation claims dead-letter routing.
4. **Security, fencing, and metrics are unwired.** The loop never installs
   `PipelineSecurityContextHolder`, never validates `CdcFencingToken`, never calls
   `WritePolicyEnforcer`, and never records `CdcPipelineMetrics`. `CdcPipelineConfig`
   has no epoch.
5. **Registration is an inert parallel registry.** See §4.
6. **Maintenance scheduling holds locks across I/O and can leak capacity.**
   `PriorityBlockingQueue`'s constructor argument is not a capacity bound; a
   rejected submit leaves the permit and in-flight marker held; `getTableMaintenanceLock`
   has no caller, so CDC writes and compaction are not mutually excluded.
7. **Commit state grows without bound and has no reconciliation API.** One row per
   batch is never pruned; `COMMITTING` can only throw, not reconcile.

### P1 — duplication and maintainability

8. Duplicate implementations: `StreamingWiring` vs `StreamingContext`; `CdcConsumer`
   vs `DebeziumJsonConverter`; `StreamingWriter` vs `IncrementalWriter`; module-local
   `DataFileSink`; local `CdcStreamState`; streaming vs metadata JDBC stores
   (H2 `MERGE` vs PostgreSQL `ON CONFLICT`).
9. Writer semantics do not match their names: full rows used as position deletes,
   no cross-batch equality delete, record counts returned as file counts, no actual
   self-managed commit, and Paimon `RowKind` computed only for a log line.
10. Dead configuration/status: `CdcPipelineConfig.flushInterval`,
    `MaintenanceConfig.orphanCleanupScanInterval`, `MaintenanceConfig.snapshotRetainCount`;
    `MaintenanceStatus` exposes far more than the scheduler reads.
11. Module hygiene: stale file-count governance note, missing package-info in
    root/writer/watermark/query, misleading class names (`MergeTree` for Paimon,
    `Lake` for Iceberg), and architecture docs describing unimplemented paths.
12. Test gaps: window/accumulator coverage is one class; continuous query coverage
    is 36 lines; registration/security/fencing/late-data have no tests; boundary
    tests hardcode a repository-relative path; PostgreSQL tests run without a tag.

## 4. Registration store deep dive

`CdcRegistration` is a lease/quota record keyed by a random `streamId`:

| Field | Meaning | Drives a pipeline? |
|---|---|---|
| `streamId` | Random UUID created at registration | No; unrelated to `StreamState.streamName` |
| `consumerId` | External consumer identity | No |
| `tenantId` | Tenant | No; the pipeline uses `StreamState.tenantId` |
| `state` | ACTIVE / PAUSED / EXPIRED | No; lifecycle lives in `StreamLifecycleState` |
| `maxConcurrentWrites` | Requested write concurrency | No; `StreamScheduler` uses `StreamConfig` defaults |
| `maxPendingBytes` | Requested pending bytes | No |
| `lastHeartbeat` / `createdAt` | Lease heartbeat and creation time | No; `expireStaleStreams()` has no caller |

It does **not** store target tables, source properties, format, stream properties,
creator identity, offsets, checkpoints, watermarks, or any link to
`ingestion_stream_state`.

| Store | Key | Contents | Driver |
|---|---|---|---|
| `cdc_registrations` | `stream_id` UUID | Consumer lease + requested quotas | HTTP register/heartbeat only |
| `ingestion_stream_state` | `stream_name` | Definition, lifecycle, epoch, target, source, creator, counters | `StreamDdlExecutor`, `StreamScheduler` |
| `stream_offsets` | `stream_name` + partition | Committed source offsets | `StreamScheduler`, `CdcPipelineLoop` |
| `cdc_commit_state` | UUID | Batch checkpoint, 2PC state, deadline, batch key | `TwoPhaseCommitCoordinator` |
| `watermark_positions` | `source_id` | Latest watermark | `WatermarkTracker` |

Because quotas are never enforced, leases never expire, and table availability is
not recorded, the registration path is an inert second source of truth. The
first version removes it entirely: SPI, handler, stores, HTTP endpoints, DDL,
migration, tests, and documentation. `ingestion_stream_state` remains the only
stream definition authority.

## 5. Target architecture

### 5.1 Maven modules

| Artifact | Package roots | Depends on | Contents |
|---|---|---|---|
| `pangrova-spi` (existing) | `org.pangrova.spi.streaming` | `common` | Standard store API: `CdcCommitStateStore`, `SourceId`, `CdcBatchCheckpoint`, `OffsetRange`, `PrepareToken`, `CdcCommitState` |
| `pangrova-streaming` (core jar) | `org.pangrova.streaming.runtime`, `.cdc`, `.cdc.commit`, `.watermark` | `spi`, `common`, `pangrova-streaming-maintenance`, slf4j | `StreamingContext`, pipeline, converter registry, `TwoPhaseCommitCoordinator`, watermark |
| `pangrova-streaming-format` | `org.pangrova.streaming.format` | `spi`, `common`, Jackson, slf4j | Debezium/Canal/Maxwell converters and row parser |
| `pangrova-streaming-maintenance` | `org.pangrova.streaming.maintenance`, `.maintenance.policy` | `spi`, `common`, slf4j | Scheduler, SPI, results, config, trigger priority |
| `pangrova-streaming-jdbc` | `org.pangrova.streaming.jdbc` | `spi`, `common`, Jackson, slf4j | PostgreSQL commit-state store; no core dependency |

No leaf owns `org.pangrova.streaming`; package roots do not prefix-overlap.
`pangrova-streaming-jdbc` implements only the SPI store API, so additional
backends (Redis, object store, embedded) can be added as siblings without
touching core.

### 5.2 Standard store API

`org.pangrova.spi.streaming.CdcCommitStateStore` is backend-neutral:

```text
insert(PrepareToken, CdcCommitState, String batchKey)
findDuplicate(SourceId, CdcBatchCheckpoint, String batchKey)
findStateById(UUID)
compareAndSetState(UUID, CdcCommitState expected, CdcCommitState next)
updateState(UUID, CdcCommitState)
findInFlight(SourceId)
findLastCommittedOffsets(SourceId)
pruneTerminalBefore(Instant)
```

Constraints: domain types only; no `Connection`, SQL, JSONB, or `DataSource`;
duplicate insert returns false; CAS is the cross-process claim; committed-offset
queries return exact next offsets; terminal rows are prunable.

## 6. Refactor and split plan

- **Phase A** — this document.
- **Phase B** — delete dead/duplicate code: `StreamingWiring`, aggregation,
  writer package, query package, `CdcConsumer`/`CdcConfig`/`IdempotentExecutor`,
  interceptors, duplicate JDBC stores, local registration DTOs/enum,
  `LateDataRouter`, retention/compaction policy duplicates, SPI write contracts
  (except `WriteResult`), and the entire registration subsystem across SPI,
  streaming, coordinator, metadata, HTTP, and tests.
- **Phase C** — P0 fixes: watermark restore + delay (and no STALE on regression),
  dead-letter policy, late-data gate, security/fencing/metrics wiring,
  maintenance scheduler concurrency and CDC/compaction lock, commit-state pruning.
- **Phase D** — move commit data model + store API to `pangrova-spi`; move format
  to `org.pangrova.streaming.format`; move JDBC store to
  `org.pangrova.streaming.jdbc`; move `StreamingContext` to `.runtime`.
- **Phase E** — create the three new leaves, update POMs, module mappings, and
  static-analysis lists.
- **Phase F** — update architecture rules, long-term architecture, streaming docs,
  capability status, and CHANGELOG; run the full verification matrix.

## 7. Regression preparation

- Build order: `spi` → `metadata` → `streaming` → `coordinator` → e2e.
- Registration removal: delete metadata store/test and the `cdc_registrations`
  DDL (`V5` plus the `V1` block); reset development databases; assert the table is
  absent on a fresh PostgreSQL migration.
- HTTP: assert `/cdc/register` and `/cdc/heartbeat` are gone (404) after removing
  `StreamingResource`.
- Behavior: `CREATE STREAM` DDL, scheduler start/stop, offset/checkpoint recovery,
  watermark restore, and CDC writes must be unaffected because registration never
  drove the pipeline.
- Gates: module dependency property test must scan every new leaf directory; run
  `check-doc-class-refs.sh`, `detect-dead-stubs.sh`, `check-spi-annotations.sh`,
  and the static-analysis profile.
