---
title: "AI Native Snowflake and Gluten Distributed Architecture Review"
description: "Primary-source review and Pangrova architecture decisions for large-scale AI Native execution"
---

# AI Native Snowflake and Gluten Distributed Architecture Review

> Research baseline: 2026-09-04; core ownership/scheduling sources rechecked 2026-09-30. Only official documentation, project
> documentation, source repositories, and primary engineering publications are
> used as evidence. This review defines architecture decisions; it does not claim
> that every decision is implemented.
>
> Scope: distributed query scheduling, Native execution, resource isolation,
> shuffle, adaptive execution, failover, background indexing/embedding jobs, and
> production rollout.

The primary-source claim ledger and reconciliation record are kept in
[`ai-native-research/report-source.md`](ai-native-research/report-source.md).

## 1. Decision summary

Snowflake and Apache Gluten solve different parts of the problem and are
complementary:

- Snowflake is the reference for separation of storage, compute, and services;
  workload-isolated elastic compute; durable ownership of globally visible side
  effects; immutable storage artifacts; and managed background compute.
- Spark/Gluten is the reference for retaining a mature distributed scheduler
  while replacing compute-intensive physical regions with Native execution;
  columnar shuffle; adaptive stage boundaries; stage-specific resource profiles;
  and end-to-end Native observability.

Pangrova adopts both patterns without copying either system wholesale:

1. `pangrova-coordinator` remains the JVM control plane and owns no candidate,
   row, file, model-input, or training-data bytes.
2. Existing Pangrova `StageDAG` scheduling is retained and extended. Typed
   Substrait regions execute in C++ Native; Pangrova does not add a second Native
   cluster scheduler.
3. Every query/job side effect is protected by an explicit durable execution
   anchor and fencing epoch, verified at the metadata commit boundary.
4. Foreground query, serving, refresh/indexing, embedding/backfill, and training
   export use isolated resource pools with separate admission and SLOs.
5. Resource admission is stage-aware and accounts for JVM heap, Native memory,
   spill, shuffle, network, provider concurrency, logical items, physical send
   attempts, encoded entity bytes, and media bytes. Per-request model input/output
   token constraints are not a cumulative LLM usage or billing ledger.
6. Shuffle state must survive Worker loss or decommission through reliable remote
   storage, replication, or deterministic recomputation; Worker lifetime cannot
   be the only durability boundary.
7. Adaptive changes occur at fenced stage barriers. They may coalesce/split
   partitions, change reducer fan-in, or reroute replicas, but may not change the
   pinned source snapshot, model version, index generation, policy, or score
   semantics.
8. Every relational path uses Velox, without JVM/interpreter/alternate-serde
   fallback. Unsupported capabilities must be implemented as real Velox
   extensions and admitted only after the fixed cohort is verified. Existing
   fallback declarations or branches are cleanup gaps, not approved alternatives.
9. Arrow is the sole interoperability/wire contract and Velox Vector the sole
   task-local representation. The fixed-SDK layout evidence, lifecycle and copy
   acceptance requirements are recorded in
   [the boundary decision](ai-native-arrow-velox-boundary-decision.md); no absolute
   cross-machine zero-copy or unmeasured performance advantage is promised.

## 2. Snowflake: what is structurally important

### 2.1 Storage, compute, and services are separate failure domains

Snowflake's architecture separates persistent data storage, independent Virtual
Warehouse compute clusters, and Cloud Services. A warehouse is an isolated MPP
compute cluster; Cloud Services handles authentication, metadata, parsing,
optimization, and dispatch. The original SIGMOD paper also describes immutable
table files in object storage and local compute-node caching.

Primary evidence:

- [Snowflake key concepts and architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts)
- [The Snowflake Elastic Data Warehouse, SIGMOD](https://www.snowflake.com/wp-content/uploads/2019/06/Snowflake_SIGMOD.pdf)

Pangrova consequence:

- catalog and query-control state stays in the JVM/metadata plane;
- immutable index/model/cache artifacts live in object storage and are addressed
  by manifests and checksums, never by eventually consistent directory listing;
- Native Workers cache immutable artifacts locally but do not own their truth;
- different workload classes receive independent Worker pools, queue limits, and
  failure budgets.

This separation is a performance feature as well as an availability feature. It
allows concurrency to scale horizontally by adding pools without forcing a
single large query to share caches, memory, or queues with background refresh.

### 2.2 Execution Anchor makes ownership a commit-time invariant

Snowflake records an explicit query-to-Global-Services-instance assignment in
FoundationDB. Exactly one instance may commit query metadata at a time, and a
guard verifies ownership before every persistent transaction. The common path
piggybacks acquisition/release and uses an in-memory check; retry and recovery
use explicit transfer protocols. Unlike an expiry-only lease, the anchor is held
for the lifecycle and transferred deliberately.

Primary evidence:

- [Strengthening Snowflake's Distributed Query Lifecycle with Execution Anchors](https://www.snowflake.com/en/blog/engineering/snowflake-distributed-query-execution-anchors/)

Pangrova consequence:

```text
ExecutionAnchor
  subject_type: QUERY | BACKGROUND_JOB
  subject_id
  owner_instance_id
  ownership_epoch
  lifecycle_state
  acquired_at
  transfer_from_epoch (optional)
```

All globally visible mutations require a transaction predicate equivalent to:

```text
anchor.owner == caller.instance
AND anchor.ownership_epoch == request.ownership_epoch
AND attempt.fencing_token == request.fencing_token
```

This predicate applies to query finalization, result registration,
index generation publication, embedding row-version commit,
Feature Store publication, and training-dataset manifest publication. A heartbeat
lease may detect failure, but expiry alone must never authorize an old owner to
commit.

Required transfer protocols:

- normal completion: acquire with first transaction, retain through asynchronous
  finalization, release with the last transaction;
- voluntary retry/handoff: old owner self-blocks, durably transfers the anchor,
  then new owner begins;
- crash recovery: recovery proves old-owner death/epoch invalidation before the
  new anchor becomes commit-capable;
- split-brain: both runtime and metadata transaction guards reject the stale
  epoch.

### 2.3 Elastic compute means isolation plus deliberate scaling direction

Snowflake Virtual Warehouses isolate workloads. Official guidance distinguishes
vertical scaling for a single query from horizontal multi-cluster scaling for
concurrency, and documents queueing, timeout, spill, and memory pressure as
assignment inputs rather than treating task count as sufficient.

Primary evidence:

- [Virtual warehouses overview](https://docs.snowflake.com/en/user-guide/warehouses-overview)
- [Warehouse considerations: scaling up versus scaling out](https://docs.snowflake.com/en/user-guide/warehouses-considerations)
- [Multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster)
- [Snowflake Well-Architected Framework: performance](https://www.snowflake.com/en/developers/guides/well-architected-framework-performance/)
- [Warehouse query performance](https://docs.snowflake.com/en/user-guide/performance-query-warehouse)

Pangrova consequence:

```text
WorkloadClass                 Scaling objective
FOREGROUND_INTERACTIVE       P95/P99 latency and strict queue deadline
FOREGROUND_BATCH             throughput and predictable spill budget
SEARCH_SERVING               cache locality, bounded fan-out, low tail latency
INDEX_REFRESH                throughput, preemptible, no query interference
EMBEDDING_BACKFILL           provider quota/cost and checkpoint progress
FEATURE_MATERIALIZATION      throughput and snapshot correctness
TRAINING_EXPORT              large sequential I/O and durable sink bandwidth
```

- More Workers/replicas handle concurrency; greater per-query parallelism and
  memory handle a large individual query.
- Scaling is driven by queue age, runnable partition count, memory headroom,
  spill, network, provider quota, and observed service time—not CPU alone.
- Background pools may yield or pause without changing foreground correctness.

### 2.4 Managed background work is a durable task graph

Snowflake Automatic Clustering uses an internal compute-service framework for
asynchronous compute-intensive work. A partition-selection task determines work,
then creates fixed-size successor execution tasks. Managed compute prevents
resource contention with foreground queries and reacts to DML state.

Primary evidence:

- [Automatic Clustering at Snowflake](https://www.snowflake.com/en/blog/engineering/automatic-clustering-at-snowflake/)
- [Automatic Clustering documentation](https://docs.snowflake.com/en/user-guide/tables-auto-reclustering)

Pangrova consequence:

```text
DISCOVER -> PLAN_PAGES -> EXECUTE_PAGE* -> VALIDATE -> PUBLISH -> GC_OLD
```

- discovery reads only metadata and CDC watermarks;
- pages are immutable, fixed-size, persisted, leaseable, retryable, and
  idempotent;
- data-dependent work is a Native DAG in a background pool;
- validation produces a manifest and quality report;
- publication is one fenced catalog CAS from staging to active generation;
- no Coordinator collection contains all source files, rows, or vectors.

The same state machine is used for Search Service refresh, index build/compaction,
embedding backfill, Feature Store materialization, and training-data export.

### 2.5 Rollout is part of the architecture

Snowflake rolled the execution anchor out using dry-run enforcement, dedicated
signals, independent feature flags, and phased activation. Architecture that
cannot be shadowed, measured, or rolled back is incomplete.

Pangrova consequence:

- exactly one current protocol/capability epoch is admitted; mismatched deployments
  fail closed rather than negotiate, retain an older parser, or silently downgrade;
- anchors and publication guards are enforced from initial deployment. Qualification
  may compare decisions offline, but production has no audit-only correctness path;
- new Native operators may run an out-of-band deterministic oracle during
  qualification, but production dispatch has exactly one canonical protocol and
  one execution path;
- no artifact generation switch occurs without an atomic rollback pointer;
- every phase has zero-tolerance correctness alerts and a bounded rollback time.

## 3. Spark and Gluten: what is structurally important

### 3.1 Reuse control flow; offload compute-intensive regions

Gluten converts Spark physical plans to Substrait, executes them in a Native
backend, returns columnar results through Spark's columnar API, and exposes Native
metrics in Spark UI. Its project principles explicitly reuse Spark control flow
while offloading data processing.

Primary evidence:

- [Apache Gluten architecture](https://gluten.apache.org/)
- [Apache Gluten source and design principles](https://github.com/apache/gluten)

Pangrova consequence:

- reuse `PlanFragmenter`, `StageDAG`, placement, retry, and attempt tracking;
- form maximal typed Native regions rather than one JNI call per operator/row;
- exchange and result boundaries carry Arrow-compatible columnar batches;
- Native operator metrics are correlated by query/stage/task/attempt/operator and
  summarized, not materialized as unbounded Coordinator objects;
- the only allowed JVM callbacks inside a Native region are bounded node-local
  governance adapters such as model/provider and credentialed file access.

### 3.2 Columnar shuffle is an end-to-end property

Gluten avoids row/column conversions with columnar shuffle and can reuse Spark's
shuffle infrastructure. Its Velox documentation exposes hash-vs-sort shuffle
memory trade-offs, remote shuffle integrations, spill, and limitations around
large partition counts.

Primary evidence:

- [Gluten configuration](https://gluten.apache.org/docs/v1.5.0/Configuration.html)
- [Gluten Velox backend](https://gluten.apache.org/docs/v1.5.0/get-started/Velox.html)

Pangrova consequence:

- retrieval leaf, partial merge, global merge, fusion, rerank, fill, and final
  result remain columnar across every boundary;
- the plan admission report counts conversion boundaries; AI/retrieval paths
  require zero row/column conversion boundaries;
- hash shuffle is allowed only when `partition_count * buffer_per_partition`
  fits its grant; sort/range shuffle is selected for high partition count or
  skew and spills deterministically;
- receiver-driven credit controls in-flight bytes per query and per channel;
- reliable remote shuffle or replicated local shuffle decouples intermediate
  state from an executor's lifetime.

### 3.3 Stage resource profiles prevent hidden JVM/Native contention

Gluten documents stage-level resource adjustment because a whole-Native stage
uses a different heap/off-heap ratio from a stage containing Spark fallback.
Spark exposes `ResourceProfile` and dynamic allocation at stage boundaries.

Primary evidence:

- [Gluten stage resource adjustment](https://gluten.apache.org/docs/v1.5.0/get-started/VeloxStageResourceAdj.html)
- [Spark job scheduling](https://spark.apache.org/docs/latest/job-scheduling)

Pangrova consequence:

```text
StageResourceProfile
  cpu_slots
  jvm_heap_bytes
  native_memory_bytes
  shuffle_send_bytes / shuffle_receive_bytes
  spill_bytes / object_store_read_bytes
  candidate_rows / candidate_bytes / fill_keys
  provider_concurrency / input_tokens / output_tokens / media_bytes
  deadline / priority / preemptibility
```

The scheduler reserves the complete profile before launch. Worker admission and
Native `MemoryPool` receive the same immutable grant ID. Untracked allocation is
a correctness failure, not a best-effort metric. Resource release is tied to the
winning fenced attempt and is idempotent.

### 3.4 AQE should adapt topology, not semantics

Spark AQE uses runtime statistics to coalesce post-shuffle partitions, use local
shuffle reads, and mitigate skew. These are barrier-safe physical changes.

Primary evidence:

- [Spark SQL performance tuning and AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

Pangrova consequence:

- initial plans deliberately expose enough partitions for scale-out;
- stage completion reports bounded size, skew, candidate, spill, and coverage
  statistics;
- a new `plan_epoch` may coalesce small partitions, split a hot partition,
  change Native merge fan-in, choose a replica, or convert hash shuffle to sort;
- old-epoch outputs cannot feed new-epoch reducers unless the barrier manifest
  explicitly imports them;
- AQE cannot change snapshot, Search Service generation, analyzer, distance
  metric, model version, policy, approximation profile, or tie-break order.

### 3.5 Dynamic allocation requires durable shuffle semantics

Spark documents that executor removal must not destroy required shuffle data;
it uses an external shuffle service, shuffle tracking, decommissioning, or a
reliable shuffle plugin. It also grows requested executors cautiously and then
exponentially while backlog persists.

Primary evidence:

- [Spark dynamic resource allocation and graceful decommission](https://spark.apache.org/docs/latest/job-scheduling)

Pangrova consequence:

- Worker scale-in enters `DRAINING`, rejects new attempts, and preserves or
  transfers shuffle/artifact-cache obligations before exit;
- the scheduler never assumes an idle Worker is safe to remove merely because
  it has no currently running task;
- scale-out starts conservatively and accelerates on sustained schedulable
  backlog, subject to tenant/workload caps;
- map-output manifests include attempt epoch, checksum, partition sizes, and
  durability class so reducers reject stale or incomplete output.

## 4. What Pangrova must not copy

### 4.1 No Spark-style correctness fallback on AI/retrieval paths

Gluten's automatic fallback to vanilla Spark is useful for general SQL
compatibility, but it is not a safe default for Pangrova AI Native. JVM fallback
would change score behavior, memory accounting, failure semantics, and data
placement, and could reintroduce Coordinator/Worker candidate processing.

Rules:

- capability validation happens before stage admission;
- SQL execution has one typed Native physical path; unsupported operators fail
  planning/admission rather than add a JVM execution alternative;
- retrieval/model/index operators have no JVM implementation path;
- exact fallback is a typed Native operator with complete snapshot coverage and
  explicit `FALLBACK_EXACT` result state.

### 4.2 No Snowflake product API imitation

Pangrova adopts mechanisms, not undocumented internals or vendor surface syntax.
It must not claim Snowflake-equivalent scale or reliability without Pangrova's
own load, chaos, and soak evidence.

### 4.3 No second scheduler in C++

Native owns driver scheduling inside a task and data-plane reduction, but cluster
placement, retries, workload fairness, and durable ownership remain in the JVM
control plane. Two cluster schedulers would produce conflicting admission,
cancellation, and fencing decisions.

### 4.4 No control-plane final reduce

Snowflake's service layer coordinates query execution; it is not evidence that
Pangrova's JVM Coordinator should consume candidate rows. Any unavoidable final
bounded ordering executes in a Native reducer scheduled by the Coordinator.

## 5. Pangrova scheduling architecture

```text
SQL / Search Service / background trigger
                     |
                     v
JVM: bind snapshot + artifact/model/policy versions
     acquire anchor + admit workload + build StageDAG
                     |
              typed TaskRequest + Substrait
                     v
Native leaf stages -- columnar reliable shuffle --> Native reduction tree
       |                    stage barrier/AQE                |
       +--> bounded local top-K / build page                 v
                                              Native fusion/rerank/fill/sink
                                                             |
                                  bounded metadata/report ---+
                                                             v
JVM: fenced finalization/publication
```

### 5.1 Query lifecycle

```text
CREATED -> ANCHORED -> ADMITTED -> PLANNED -> RUNNING
        -> FINALIZING -> COMMITTED | FAILED | CANCELLED
```

- `ANCHORED` precedes any durable side effect.
- `ADMITTED` owns explicit resource reservations.
- each task attempt is `(query_id, stage_id, partition_id, plan_epoch,
  attempt_id, fencing_token)`;
- stage barriers commit only bounded partition manifests and metrics;
- final query results flow through the ordinary Native result transport;
- Coordinator finalization validates report completeness and commits metadata,
  but never opens the result/candidate stream.

### 5.2 Background job lifecycle

```text
DISCOVERING -> PLANNED -> RUNNING -> VALIDATING -> PUBLISHING -> SUCCEEDED
                              \-> RETRY_WAIT | FAILED | CANCELLED
```

- discovery and planning are metadata-only;
- execution pages pin one source snapshot and target staging generation;
- leases allow recovery; fencing prevents an expired owner from publishing;
- publication uses `expected_active_generation` CAS;
- object artifacts become garbage only after publication rollback retention and
  all query-generation leases expire.

### 5.3 Workload placement

Placement score is constrained, not purely heuristic:

```text
eligible = protocol/capability epoch
        AND tenant/policy/egress eligibility
        AND snapshot/artifact visibility
        AND complete resource grant
        AND failure-domain constraints

score = cache locality + data locality + free grant + observed service rate
      - queue delay - tail latency - failure rate - cross-zone bytes
```

No placement score may override an eligibility predicate. Replica hedging is
allowed only for idempotent reads and charges a separate resource budget.

## 6. Reliability and performance invariants

| Invariant | Required measurement and failure action |
|---|---|
| Coordinator data-plane isolation | candidate/row/file/model/training bytes and retrieval CPU counters are exactly zero; CI dependency gate and production alert fail closed. |
| Unique durable owner | every metadata transaction supplies anchor epoch; stale-owner commit tests must always be rejected. |
| Bounded memory | every JVM/Native/shuffle/provider allocation maps to a stage grant; grant breach cancels the attempt and quarantines the capability version. |
| Snapshot/generation immutability | every task and output manifest repeats the pinned IDs; mismatch is never coerced or merged. |
| Deterministic exact result | repartition/replica/merge-tree permutations return the same ordered result and tie-break key. |
| Explicit approximation | ANN profile, candidate budget, exact-rescore window, Recall@K baseline, coverage, and fallback state are visible in the result report. |
| Reliable shuffle | Worker loss cannot cause silent partition loss; retry/recompute preserves attempt fencing and coverage. |
| No hot-path conversions | retrieval and AI physical plans report zero row/column conversion boundaries and zero per-row JNI/provider calls. |
| Bounded control metadata | Coordinator heap/network grows with stages/tasks/attempts, never rows or `shards * K'`; 1,024-shard slope gate enforces it. |
| Background isolation | refresh/backfill/training saturation cannot violate foreground admission or P99 SLO; fault injection verifies preemption/yield. |

## 7. Capacity model and qualification

For a retrieval query with shard count `S`, admitted leaf candidate budget `K'`,
candidate width `W`, merge fan-in `F`, and final result `K`:

- leaf candidate traffic is bounded by `S * K' * W` before compression;
- each streaming merge node retains `O(F + K')` rows plus bounded channel
  credits, not `O(S * K')`;
- final fill reads only deduplicated final/bounded keys proportional to `K`;
- Coordinator candidate traffic and candidate memory are exactly zero.

Minimum qualification matrix:

| Gate | Required qualification |
|---|---|
| Scale-out | at least 70% parallel efficiency from 4 to 32 Workers for scan, leaf retrieval, build, and embedding before a documented bottleneck. |
| Shard fan-out | 1, 16, 128, 512, and 1,024 shards with bounded queues/grants and no super-linear Coordinator heap/CPU. |
| Skew | adversarial 100:1 partition and key skew; AQE reduces P99 without changing exact/shard-invariant results. |
| Shuffle loss | kill/decommission map and reducer Workers at every barrier; no silent coverage loss or stale attempt acceptance. |
| Anchor split-brain | concurrent owner, retry, crash, delayed finalizer, and clock-skew tests reject all stale commits. |
| Quality | exact oracle; production ANN profile Recall@10 >= 0.95 unless a lower profile is explicitly selected; hybrid NDCG/MRR regressions block publication. |
| Long stability | multi-tenant refresh/query/provider-throttle/cancel/generation-GC soak with no leak, deadlock, starvation, or monotonic queue growth. |
| Release scale | 1,024 shards and 100M/1B vectors on production-shaped infrastructure, with documented hardware, dataset, build, and confidence intervals. |

These thresholds establish evidence; they do not imply that passing one benchmark
eliminates future defects. Production readiness requires deterministic CI,
nightly distributed tests, release-scale qualification, chaos, soak, telemetry,
and a proven rollback path together.

## 8. Implementation decisions and sequence

1. **R0 canonical boundary:** one proto source, dependency-complete descriptor
   handshake, generated enums, typed dynamic filters, JNI symbol/package gates.
2. **R1 generic v2:** typed task/result/resource/coverage contracts and typed
   Substrait extension relations; remove Vector-specific task/result semantics.
3. **R2 whole-stage Native retrieval:** leaf retrieval and exact oracle through
   ordinary Native execution and columnar exchange; delete Worker JVM interceptors.
4. **R3 Native distributed reduction:** reliable shuffle, bounded merge tree,
   fusion/rerank/fill, stage barriers, plan epochs, and explicit completeness.
5. **R4 durable service/jobs:** Search Service, immutable active/staging
   generations, anchor/fencing, page tasks, validation, CAS publication, GC.
6. **R5 governed model/embedding:** one model invocation contract, batch local JNI
   callback, durable row-version embedding commit and bounded compute/resource
   admission. LLM billing and usage ledgers remain outside the database core.
7. **R6-R8 platform completion:** FILE/multimodal, semantic cache,
   self-optimization proposals, Feature Store, training datasets, Semantic View,
   evaluation, security, observability, and distributed qualification.

No later phase may weaken an earlier boundary or convert an explicit failure into
silent fallback. A phase closes only when implementation, wiring, failure
semantics, telemetry, and valid end-to-end tests all exist.
