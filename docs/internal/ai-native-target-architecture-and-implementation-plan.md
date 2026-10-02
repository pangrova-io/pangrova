---
title: "AI Native Target Architecture and Implementation Plan"
description: "Industry-researched, implementation-grade architecture for Pangrova AI Native infrastructure"
---

# AI Native Target Architecture and Implementation Plan

> **2026-10-01 scope and single-path decision:** the latest user specification
> includes database-native evaluation, Feature Store, model governance and
> multimodal infrastructure. Agent orchestration and LLM Usage Ledger remain out
> of scope. Arrow owns the interoperability/network contract; Velox owns task-local
> vectors and execution. Required physical conversions are documented and measured,
> not mislabeled as end-to-end zero-copy. No runtime execution or serde fallback
> is accepted. Exact retrieval may be selected as the original admitted plan, not
> as an unreported replacement after failure. The prior scope table and older
> `FALLBACK_EXACT` proposals are superseded where they conflict with
> [the current root-cause specification](ai-native-single-path-root-cause-design.md).

> Baseline: 2026-09-13 working tree, including all existing uncommitted changes.
>
> Status: approved implementation specification. This document describes target
> behavior and migration gates; it is not evidence that the behavior already
> exists.
>
> Scope (2026-09-27 decision): the database owns large-scale write, durable
> storage, and distributed query execution. AI-native work in scope is the
> storage/query substrate: typed VECTOR and FILE/BLOB, governed embedding
> materialization, ANN/BM25/hybrid search, physical query cache, snapshot-pinned
> feature materialization, and the minimal model binding needed by SQL workers.
> Agent/application orchestration, prompt workflows, token billing/usage ledger,
> model-release evaluation, training-data product APIs, and Semantic
> View/Analyst are out of scope. Older sections below remain architectural
> research, not implementation obligations where they conflict with this decision.

### 2026-09-27 ownership decision (supersedes older scope text)

| Boundary | Pangrova database owns | External Agent/application owns |
|---|---|---|
| Write and storage | Fenced distributed writes, immutable file/index generations, snapshot-consistent reads, FILE/BLOB bytes and metadata | Prompt or conversation state |
| Query and search | Typed SQL plan, Native scan/filter/ANN/BM25/fusion/reduction, complete shard coverage, bounded exchange and result stream | Retrieval orchestration across systems and answer synthesis |
| Embedding and models | Exact provider/version binding only when SQL materialization or search requires it; task-scoped secret, deadline, concurrency and egress limits | General LLM serving policy, token/cost accounting, provider billing and model-release decisions |
| Feature and evaluation | Snapshot-pinned relational feature materialization and physical correctness/performance regression tests | Training dataset product, online feature serving, prompt/model evaluation and release workflows |
| Governance | Tenant/RBAC on data objects, durable job state, resource safety, audit of database mutations | Content guardrails and application-level AI policy |

There is one canonical v2 task/capability/result contract and one Native
distributed data path. The database does not retain a second JVM row/candidate
path or a compatibility protocol. Existing metadata-only objects that belong to
the external column are not evidence of a database capability and must not be
expanded or advertised; remove their wiring and documentation as the core path
is completed. Resource admission remains necessary for Worker safety, but a
durable LLM usage ledger is not a release gate.

This deliberately adopts Snowflake's separation of durable storage, cloud
control services, independent compute, and independently operated Search
indexing/serving, plus Gluten's Spark-controlled Substrait-to-Native columnar
execution. It does **not** adopt Gluten's JVM row fallback: Pangrova can reject
an unsupported plan before dispatch because it has no Spark compatibility
requirement. Sources: [Snowflake architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts),
[Cortex Search indexing/serving](https://docs.snowflake.com/en/sql-reference/sql/alter-cortex-search),
[Apache Gluten architecture](https://github.com/apache/gluten/blob/main/README.md).
>
## 1. Executive decision

Pangrova will implement AI Native as governed distributed SQL infrastructure,
not as a collection of Java-side feature interceptors.

The architectural boundary is absolute:

- The **Coordinator JVM is the control plane**. It parses and plans SQL, resolves
  catalog objects, authorizes access, pins snapshots and artifact generations,
  admits resources, schedules a DAG, tracks attempts, and commits metadata. It
  never transports, deserializes, scores, fuses, reranks, or materializes query
  candidates.
- The **Worker JVM is a node runtime**. It owns admission, lifecycle, JNI, secret
  resolution, governed provider clients, and local callbacks. It must not bypass
  Native execution with a Java retrieval path or expose a feature-specific
  candidate result endpoint.
- The **C++ Native engine is the data plane**. It scans data and indexes, executes
  vector/BM25/hybrid operators, performs local and hierarchical global reduction,
  invokes bounded model batches through a governed local Worker callback, fetches
  final rows, spills under pressure, and streams the normal result.
- The **index and background-job plane is durable**. Java owns catalog state,
  leases, fencing, admission, and atomic publication; distributed Native tasks own
  data-dependent build, refresh, compaction, backfill, and export work.
- The **serving plane is explicit**. Model and Search Service objects have
  independently governable lifecycle, capacity, freshness, usage, and audit
  state. Provider endpoints are not inferred from arbitrary strings.

The word "coordinator" in search products often means a node that also performs
data-plane reduction. That is not Pangrova's `pangrova-coordinator`. Pangrova
expresses the equivalent global reduction as a Native execution stage scheduled
by the JVM control plane.

## 2. Industry research and adopted patterns

This design uses current official documentation and primary project
specifications. It adopts architectural patterns, not vendor APIs.

### 2.1 Product and system comparison

| System | Official architecture signal | Pattern adopted by Pangrova |
|---|---|---|
| Snowflake distributed execution | Storage, independent Virtual Warehouses, and Cloud Services are separate; query ownership is enforced by an explicit transaction-level Execution Anchor; background compute services isolate asynchronous tasks. | Separate control/data/artifact planes, add durable query/job anchors and fenced commits, isolate foreground/serving/background pools, and express refresh as persisted discovery plus fixed-size Native successor tasks. |
| Spark + Apache Gluten | Spark retains cluster control flow while maximal physical regions are converted to Substrait and executed Native; columnar shuffle, AQE, dynamic allocation, and stage resource profiles coordinate heap/off-heap needs. | Reuse Pangrova `StageDAG`, offload whole typed regions, keep all retrieval exchanges columnar, add reliable shuffle and barrier AQE, and reserve complete stage resources instead of task-count-only admission. |
| Snowflake Cortex Search | Search is a managed schema object with hybrid retrieval, target lag, refresh history, independently controllable indexing/serving state, request logging, and cost controls. | Introduce governed Search Service metadata, separate indexing from serving, expose freshness and refresh history, and make usage auditable. |
| Databricks Vector Search | Endpoint lifecycle is separate from Delta Sync or direct-vector index lifecycle; Delta Sync incrementally consumes source changes. | Separate query serving from durable CDC/backfill jobs and make source ownership explicit. |
| Elasticsearch | Each shard produces bounded candidates; global top-K is merged after shard search; approximate kNN can rescore original vectors; partial results and batched reduction are explicit controls. | Use bounded local candidates, Native hierarchical reduction, optional exact rescoring, explicit partial-result policy, and bounded merge memory. |
| OpenSearch | Hybrid subqueries execute per shard and are normalized/fused after shard results; large shard counts expose coordinator-memory trade-offs. | Compile fusion into explicit distributed stages and never normalize independently per Worker when the algorithm requires global statistics. |
| Vespa | Matching/ranking runs near data; first phase returns IDs and scores, then a fill phase fetches selected fields; response includes coverage. | Use candidate-then-fill execution and a first-class coverage envelope. |
| Milvus | Stateless routing/control is separated from Query/Data/Streaming nodes; index artifacts live in object storage; search uses multi-level reduction and a guarantee timestamp. | Use immutable object-store generations, distributed build tasks, hierarchical reduction, and an explicit read epoch/snapshot. |
| Lucene | Readers hold a consistent point-in-time view; immutable segment cores are published through index commits. | Pin an immutable index generation for a query and publish generations atomically. |
| Substrait | Semantic-changing extensions cannot be ignored; new row-producing semantics belong in typed custom relations with a documented output schema. | Encode retrieval, merge, rerank, and candidate fill as generated typed extension relations, never JSON strings or session properties. |
| Arrow Flight | Control is Protobuf; data is a stream of columnar record batches; partition endpoints can be consumed in parallel. | Keep control metadata in the canonical protocol and move candidate/result bytes only through the normal columnar exchange/result path. |
| Snowflake/Databricks Model platforms | Models are versioned governed objects with signatures, lifecycle, serving endpoints, RBAC, usage logs, and batch/online execution. | Make MODEL and model service first-class catalog objects and route every call through one invocation contract. |
| Snowflake/Databricks Feature platforms | Features are governed definitions with lineage, point-in-time correctness, scheduled/streaming materialization, and online/offline serving. | Retain Feature Store as infrastructure, with one definition and versioned materializations; remove placeholder-only SPI behavior. |
| Redis semantic cache | Correct reuse requires similarity plus hard tenant/model/policy metadata boundaries, TTL, invalidation, and bounded eviction. | Keep semantic cache non-authoritative and dependency-aware; partition before ANN and never cache empty or ungoverned results. |

### 2.2 Official evidence

The detailed Snowflake/Gluten scheduling analysis and Pangrova ADRs are recorded
in [AI Native Snowflake and Gluten Distributed Architecture Review](ai-native-snowflake-gluten-distributed-architecture-review.md).

- Snowflake separates storage, independent warehouse compute, and Cloud Services;
  its Execution Anchor makes one query owner a guard on every metadata commit,
  while Automatic Clustering uses isolated managed compute and durable
  partition-selection/successor tasks:
  [Snowflake architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts),
  [Execution Anchor](https://www.snowflake.com/en/blog/engineering/snowflake-distributed-query-execution-anchors/),
  [Automatic Clustering engineering](https://www.snowflake.com/en/blog/engineering/automatic-clustering-at-snowflake/).
- Gluten reuses Spark control flow, converts physical plans to Substrait, and
  executes an end-to-end columnar Native pipeline. Spark's AQE, dynamic
  allocation/decommissioning, and Gluten's stage resource adjustment establish
  the required shuffle and resource boundaries:
  [Apache Gluten](https://gluten.apache.org/),
  [Spark scheduling](https://spark.apache.org/docs/latest/job-scheduling),
  [Spark AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html),
  [Gluten stage resources](https://gluten.apache.org/docs/v1.5.0/get-started/VeloxStageResourceAdj.html).

- Snowflake separates Cortex Search **INDEXING** and **SERVING**, supports target
  lag and manual refresh, and records full/incremental refresh history with a
  source data timestamp:
  [Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview),
  [ALTER CORTEX SEARCH SERVICE](https://docs.snowflake.com/en/sql-reference/sql/alter-cortex-search),
  [refresh history](https://docs.snowflake.com/en/sql-reference/functions/cortex_search_refresh_history).
- Snowflake AI SQL exposes governed text/image functions, guardrails, and actual
  token usage; its Model Registry uses versioned first-class objects and typed
  method signatures:
  [AI SQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql),
  [AI_COMPLETE details](https://docs.snowflake.com/en/sql-reference/functions/ai_complete-single-string),
  [Model Registry](https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/overview),
  [model signatures](https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/model-signature).
- Snowflake per-user AI quotas are first-class objects with daily/monthly limits,
  monitoring, and blocking; Databricks Unity AI Gateway centralizes rate limits,
  guardrails, routing, budgets, and request logging:
  [Snowflake per-user quotas](https://docs.snowflake.com/en/user-guide/budgets/per-user-quotas),
  [Databricks Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/).
- Databricks Model Serving unifies real-time and batch inference, autoscaling,
  SQL AI functions, versioned models, and request logs:
  [Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving),
  [production optimization](https://docs.databricks.com/aws/en/machine-learning/model-serving/production-optimization),
  [inference tables](https://docs.databricks.com/aws/en/machine-learning/model-serving/inference-tables).
- Elasticsearch documents global kNN merging, per-shard candidate budgets,
  exact rescoring, explicit partial-result behavior, and bounded reductions:
  [kNN search](https://www.elastic.co/docs/solutions/search/vector/knn),
  [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search),
  [RRF](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion).
- BM25 scores require corpus statistics. Elasticsearch can gather global query
  statistics through DFS, while Milvus maintains corpus document frequency and
  average document length:
  [consistent scoring](https://www.elastic.co/docs/solutions/search/full-text/search-relevance/consistent-scoring),
  [Milvus BM25](https://milvus.io/docs/bm25-function.md).
- Vespa exposes coverage and separates the ID/score query phase from the field
  fill phase; Milvus uses multi-level reduction and a guarantee timestamp:
  [Vespa Query API](https://docs.vespa.ai/en/querying/query-api.html),
  [Milvus architecture](https://milvus.io/docs/architecture_overview.md),
  [Milvus consistency](https://milvus.io/docs/consistency.md).
- Lucene readers are point-in-time views over immutable segment cores and index
  changes become visible through a commit:
  [Lucene index package](https://lucene.apache.org/core/10_2_1/core/org/apache/lucene/index/package-summary.html),
  [IndexCommit](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/index/IndexCommit.html).
- Substrait defines typed advanced extensions and requires consumers to reject
  unknown semantic enhancements; Arrow Flight separates Protobuf control from
  columnar data streams:
  [Substrait extensions](https://substrait.io/extensions/),
  [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
- Snowflake FILE and Databricks FILE/Volume designs treat files as governed
  references with metadata and permissions; delayed materialization avoids
  loading large documents into JVM heap:
  [Snowflake FILE](https://docs.snowflake.com/en/sql-reference/data-types-unstructured),
  [Databricks unstructured data](https://docs.databricks.com/aws/en/unstructured/),
  [ai_parse_document](https://docs.databricks.com/gcp/en/sql/language-manual/functions/ai_parse_document).
- Provider wire dialects are capability-specific rather than interchangeable:
  OpenAI Responses uses `input_image`/`input_file`, chat vision accepts image
  URLs or image data URLs, and transcription uses multipart
  `/v1/audio/transcriptions`. Pangrova therefore binds the exact provider
  dialect into model capability admission and never serializes PDF/audio/video
  as chat `image_url`:
  [Responses image/file input](https://platform.openai.com/docs/quickstart/make-your-first-api-request),
  [Responses input-file schema](https://platform.openai.com/docs/api-reference/responses-streaming/response/refusal),
  [Audio transcription API](https://platform.openai.com/docs/api-reference/audio/voice-consent-list).
- Snowflake and Databricks both model semantic/metric views as governed catalog
  objects queryable through SQL:
  [Snowflake Semantic View SQL](https://docs.snowflake.com/en/user-guide/views-semantic/sql),
  [Databricks metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/basic-modeling).
- Feature definitions, model lineage, point-in-time data, and online/offline
  serving are core platform concerns:
  [Snowflake Feature Store](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/overview),
  [Databricks Feature Store](https://docs.databricks.com/aws/en/machine-learning/feature-store).
- A semantic cache must combine vector similarity with hard metadata isolation,
  TTL, and eviction:
  [Redis semantic cache](https://redis.io/docs/latest/develop/use-cases/semantic-cache/).

### 2.3 Primary-evidence decision and acceptance ledger

This ledger records the narrow inference Pangrova makes from each primary
source. A vendor feature is evidence for a design property, not evidence that
Pangrova has implemented or qualified that property. Living documentation was
re-checked on 2026-09-13; repository evidence is pinned to the commit shown in
the Sources section.

| Primary fact | Pangrova design decision | Acceptance threshold |
|---|---|---|
| Snowflake separates durable storage, independent Virtual Warehouse compute, and Cloud Services; a warehouse reserves resources for a query or queues it when insufficient capacity remains.[^1][^2] | Keep the Coordinator JVM as metadata/planning/admission control, keep scan/search/rerank/fill bytes in Native Workers, and isolate foreground, serving, and background resource groups. Admission is based on a complete stage resource envelope, not only task count. | Under mixed foreground/search/build load, every queue and reservation is bounded; background load cannot consume the foreground minimum; overload is rejected or queued before Native allocation; Coordinator candidate and row payload bytes remain zero. |
| Snowflake's Execution Anchor binds a query to one Global Services owner and guards every durable metadata transaction; transfer is explicit and the rollout used dry-run observation before enforcement.[^3] | A Pangrova query or durable job has one `ExecutionOwnerIdentity` and fencing epoch. Every status mutation, artifact publication, result registration, and catalog commit verifies it transactionally. Heartbeat expiry detects failure but is not commit authority. | In retry, coordinator-kill, delayed-callback, and lease-steal chaos tests, the superseded owner has zero successful mutations after transfer. Dry-run counters are zero before enforcement is enabled, and the steady-state guard-rejection SLO is zero. |
| Snowflake Automatic Clustering separates expensive partition selection from fixed-size successor execution jobs and runs those tasks on managed compute isolated from foreground queries.[^4] | Persist discovery pages and work-unit state; schedule bounded Native build/refresh/compaction successors; validate immutable output and publish with one fenced CAS. Never hold a corpus or partition list in Coordinator heap. | Restart at every state transition resumes without duplicate publication; a million-partition synthetic job keeps Coordinator heap within its configured job-state bound; work-unit duration and bytes stay within the admitted batch envelope. |
| Cortex Search is a governed service object with hybrid vector/keyword retrieval and semantic reranking. Creation binds a refresh warehouse and target lag; monitoring records request/latency/status data behind object privileges.[^5][^6][^7] | Make `SEARCH SERVICE`, source snapshot, active generation, indexing state, serving state, target lag, refresh history, request logging, RBAC, and budget explicit catalog contracts. Search SQL compiles to typed Native retrieval/merge/rerank/fill stages. | Suspend/resume indexing and serving independently; stale/missing generation fails according to the explicit freshness policy; refresh history and request telemetry are queryable; a role without `USAGE`/`MONITOR` cannot query or inspect payloads. |
| Cortex model access is governed by function privilege plus model-specific RBAC, and AI usage history attributes query, warehouse, role, user, model, tokens/pages, credits, and completion state.[^8][^9] | Resolve an exact `MODEL` version and policy before dispatch, reserve cost before execution, invoke only through the Worker-local governed callback, and reconcile actual usage without placing provider credentials or payloads in the Coordinator. | Unknown, suspended, unauthorized, modality-incompatible, or over-budget models cause zero outbound requests. Audit joins query/attempt/model/policy/provider request IDs, and reserved versus actual token/page usage reconciles within the declared tolerance. |
| Snowflake AI Observability separates live traces from dataset-based evaluation runs; evaluations preserve per-record traces and compute relevance, groundedness, correctness, latency, and custom metrics with object-scoped access.[^10] | Store immutable `EvaluationDataset`, `EvaluationRun`, baseline, candidate version, metric definition, trace reference, and publication decision. Sensitive prompts/results require a privilege distinct from aggregate telemetry. | Re-running a pinned dataset/model/index/policy tuple is reproducible; promotion is blocked on configured quality/safety/cost/latency regression; aggregate telemetry remains usable when sensitive payload access is denied. |
| Spark dynamic allocation requires shuffle tracking, an external shuffle service, decommissioned block migration, or reliable shuffle storage so executor removal does not erase shuffle output. AQE changes the physical plan from runtime statistics.[^11][^12] | Worker elasticity is allowed only when shuffle durability/ownership is explicit. Adaptive decisions may change partitioning, fan-in, and replica placement at stage barriers, but may not change snapshot, model/index generation, policy, score semantics, or fallback policy. | Decommission any Worker after producing shuffle and prove the downstream stage either completes from durable output or fails explicitly. AQE property tests preserve the semantic binding tuple and exact-result oracle across every topology rewrite. |
| Gluten reuses Spark control flow, transforms physical plans to Substrait, executes Native operator chains through JNI, retains a columnar path, and exposes Native metrics. Its row fallback is an explicit product choice, not a correctness requirement.[^13] | Reuse Pangrova's JVM DAG control flow while offloading maximal typed regions to C++/Velox. Do not copy Gluten's row fallback: an unsupported AI/retrieval relation is an admission error because a Java fallback would create a second semantic engine. | Every retained AI/retrieval plan has one typed Substrait representation and one Native implementation. Static and packaged E2E tests prove there is no Java candidate producer, row/column fallback transition, feature-specific result path, or silent fallback. |
| Velox consumes an already optimized plan; `Task` turns it into operator pipelines and Drivers, while the embedding application supplies remote `ExchangeSource` transport. Velox memory is accounted through query/task/node/operator pools with arbitration and spill.[^14][^15][^16] | Calcite and the Coordinator remain the global optimizer and distributed scheduler. A Native Worker maps one canonical task to one Velox task and implements Flight exchange, memory accounting, spill, cancellation, and metrics around it. Do not build a second cluster scheduler in C++. | Plan-node IDs correlate Coordinator, JNI, Velox operator, exchange, and trace metrics. Per-query and node RSS remain under hard limits in spill/load tests; cancellation drains Drivers/exchange/provider I/O; 512+ shard reduction remains bounded and deterministic. |
| `boostscale/velox4j` describes itself as experimental. It maps Java objects to Velox JSON serde, exposes JNI object handles and Arrow conversion, and its documented query API returns a serial Velox task.[^17] | Treat Velox4J as a reference for RAII/object ownership, Arrow C Data interop, split feeding, and lifecycle tests only. Do not adopt its JSON plan mapping or serial-task API as Pangrova's network/JNI contract, and do not add it as a production dependency. | The production dependency graph contains no Velox4J artifact. Any isolated experiment must be outside the runtime path and prove canonical-v2/typed-Substrait parity, cancellation, bounded memory, concurrency, distributed exchange, and platform packaging before a new ADR can reconsider adoption. |
| Snowflake exposes query history by warehouse/user/session and deterministic per-execution operator statistics, including rows, bytes, spill, cache, external-call latency, throttling, retries, and errors.[^18][^19] | Make `query_id`, stage/task/attempt, plan-node ID, capability/model/index versions, resource grant, and trace context joinable from the control plane through Native operator telemetry. Persist bounded summaries; export detailed events instead of retaining them in Coordinator query objects. | A sampled query can be reconstructed from admission through every Native operator and external batch; counters reconcile at stage boundaries; telemetry cardinality and retention stay within configured limits; sensitive payloads are absent without explicit privilege. |

The negative conclusion on Velox4J is deliberate. It solves an embedding API
problem, while Pangrova needs a distributed, versioned, capability-governed
protocol. Replacing canonical v2 with direct Velox JSON would couple the JVM to
Velox internals, duplicate Pangrova's planner-to-runtime semantics, and restore
the dual-path risk this design removes.

## 3. Target planes and ownership

```text
Client / JDBC / REST
        |
        v
+----------------------- JVM CONTROL PLANE ------------------------+
| Parser -> Planner -> Catalog/RBAC -> Admission -> DAG Scheduler  |
|          snapshot/index/model binding -> attempt tracking         |
+-------------------------------+-----------------------------------+
                                | canonical TaskRequest + Substrait
                                v
+------------------------ WORKER NODE RUNTIME ----------------------+
| task admission | JNI lifecycle | secret resolution | provider I/O |
|              no retrieval interception or row merge               |
+-------------------------------+-----------------------------------+
                                | local JNI
                                v
+------------------------- NATIVE DATA PLANE -----------------------+
| scan/index -> retrieve -> local top-K -> exchange -> tree reduce  |
| -> fusion -> bounded model rerank -> candidate fill -> result     |
+--------------------------+------------------+---------------------+
                           |                  |
                           v                  v
                 immutable artifacts   governed model endpoints
                    object storage      or local model service
```

### 3.1 Coordinator JVM responsibilities

Allowed:

- SQL parsing, validation, logical/physical planning, and capability admission;
- exact resolution of MODEL, SEARCH SERVICE, Semantic View, index generation,
  source snapshot, credentials reference, and policy version;
- stage construction, placement, deadlines, retry budgets, resource groups,
  replica choice, and attempt fencing;
- durable job state, catalog transactions, atomic generation publication, and
  usage reservation/reconciliation metadata;
- validation of the final generic execution envelope.

Forbidden:

- candidate/result payload fetch from Workers;
- `List<Candidate>` construction or JVM heap Top-K;
- RRF, score normalization, cross-encoder ordering, or row materialization;
- direct file content reads or data-dependent training scans;
- inference provider calls for query rows;
- guessed index paths, silent shard omission, or silent fallback.

### 3.2 Worker JVM responsibilities

Allowed:

- process startup/shutdown, task admission, JNI initialization, and capability
  handshake;
- exact credential reference resolution and outbound network policy enforcement;
- governed, bounded, asynchronous provider calls requested by a Native operator;
- local metrics and audit event emission;
- object-store client construction where the Native connector contract requires
  a Worker callback.

Forbidden:

- intercepting `TaskRequest` to run Vector/BM25 in Java;
- feature-specific HTTP result buffers;
- local result fusion outside the Native plan;
- provider selection by map iteration order or implicit default model.

### 3.3 Native responsibilities

- all read-dependent SQL work;
- VECTOR distance, exact scan, HNSW, IVFPQ, DiskANN, Lance-backed retrieval,
  inverted lookup, BM25 scoring, local candidate production, and exact rescoring;
- exchange, spill, streaming merge, global ranks, RRF/linear fusion, bounded
  cross-encoder/cascade stages, deterministic Top-K, candidate deduplication, and
  keyed row fill;
- distributed index build/validation/compaction tasks and data-dependent
  embedding/feature/training export batches;
- normal result stream and a generic execution report.

### 3.4 Stable row identity and Native fill

An ANN graph node ordinal is never a table row identity. Every immutable
artifact generation declares one generic `RowLocatorContract` in the canonical
v2 protocol, and its sidecar maps graph/posting ordinals to locators:

- Iceberg and append/copy-on-write files use
  `(source_object_id, zero_based_physical_row_position)`. The position is
  synthesized by the Native file reader before delete filtering; neither JVM
  may generate or rewrite it.
- Paimon merge-on-read uses a canonically encoded, typed primary-key tuple.
  A physical file position is invalid after LSM merge and must be rejected.
- Node ordinals, hashes of keys, concatenated `path#offset` strings and user
  supplied BIGINT columns are not row locators.
- Candidate exchange preserves the locator as typed columns. Native candidate
  fill resolves it against the same pinned snapshot and artifact generation;
  no candidate or row payload crosses the Coordinator JVM.

The locator contract, binary sidecar schema, artifact format major version and
capability signature are one compatibility unit. Any mismatch fails Worker
admission or operator construction. Format v2 initially admits only the
source-object-scoped physical locator; Paimon MOR stays fail-closed until its
primary-key encoder, sidecar reader and snapshot-aware fill path all exist.

## 4. One canonical protocol

### 4.1 Ownership

`pangrova-protocol/src/main/proto` is the only source tree for every message that
crosses a JVM/native or process boundary. Maven and CMake generate from the same
files. Multiple files are organizational only; they remain one versioned package
and one descriptor set.

The following are prohibited:

- a `vector_search.proto` protocol with an independent lifecycle;
- mirrored `.proto` files under Worker or Native trees;
- Java numeric command constants that duplicate a generated enum;
- hand-maintained field-parity tests as a substitute for generated descriptors;
- a REST/JSON task DTO that mirrors canonical task fields and is later rebuilt
  into `TaskRequest` by the Worker;
- JSON or colon-delimited semantics in Substrait details/session properties;
- changing v1 field meanings in place.

HTTP, gRPC, and JNI are delivery mechanisms, not alternate contracts. A
transport may frame, authenticate, compress, and checksum the serialized
canonical v2 message, but it must not re-declare or reinterpret task, attempt,
split, binding, resource, deadline, exchange, result, or coverage fields. The
Coordinator serializes the authoritative `TaskRequest` once; Worker admission
and Native execution parse and validate those exact bytes.

VECTOR is a logical SQL type and retrieval is a relational capability. Neither
owns a parallel wire protocol.

### 4.2 Canonical v2 layout

```text
pangrova/protocol/v2/
  common.proto              # identities, typed values, errors, trace, usage
  capability.proto          # descriptors, refs, version/signature hashes
  task.proto                # generic task control, splits, resources
  execution_binding.proto   # plan-node artifact/model/policy bindings
  runtime.proto             # JNI commands/status/capability handshake
  result.proto              # generic coverage/attempt/execution envelope
  model.proto               # model refs, invocation/result/usage
  index.proto               # immutable manifests and job reports
  plan_extension.proto      # typed Substrait relation details
```

There is no Vector-only task or result message. A generic `TypedValue` supports
scalar, list, struct, binary, FILE reference, and tensor/vector logical values;
dimension and element type are validated against the capability signature and
SQL type.

### 4.3 Task contract

```text
TaskRequest
  task/query/stage/partition/attempt identity
  tenant/principal/roles/policy context
  plan bytes and protocol epoch
  repeated generic splits
  repeated OperatorBinding keyed by plan_node_id
  resource, deadline, retry and completion policy
  trace context and budget reservation

OperatorBinding
  plan_node_id
  CapabilityRef
  repeated ArtifactBinding
  optional ModelBinding
  ExecutionPolicy

ArtifactBinding
  artifact_id/type/generation
  source_snapshot/read_epoch
  manifest_uri/checksum
  split-specific immutable objects
```

TaskRequest never repeats the query vector, BM25 text, fusion weights, or rerank
semantics. Those belong to the plan.

### 4.4 Typed Substrait relations

The planner emits generated typed details inside Substrait custom relations:

```text
RetrievalSourceRel   # ExtensionLeafRel: index/scan source -> candidate rows
RetrievalMergeRel    # ExtensionMultiRel: source streams -> globally ranked rows
RerankRel            # ExtensionSingleRel: bounded candidates -> reranked rows
CandidateFillRel     # ExtensionSingleRel: keys -> requested table columns
AiInvokeRel          # ExtensionSingleRel: row batches -> typed model outputs
```

Each detail contains `CapabilityRef`, typed Substrait expressions, declared
limits, and output schema. Unknown semantic relations are admission failures.
Algorithm choice is a capability ID such as:

- `retrieval.vector.exact`, `retrieval.vector.hnsw`,
  `retrieval.vector.ivfpq`, `retrieval.vector.diskann`,
  `retrieval.vector.lance`;
- `retrieval.text.bm25`;
- `merge.top_k`, `merge.rrf`, `merge.linear`;
- `rerank.cross_encoder`, `rerank.cascade`;
- `ai.invoke`, `embedding.invoke`, `candidate.fill`.

### 4.5 Handshake and upgrade

Worker startup returns:

- protocol major/minor and protocol epoch;
- SHA-256 of the complete canonical `FileDescriptorSet`, including imports;
- generated plan-extension schema hash;
- capability IDs, semantic versions, signatures, implementation digests, and
  candidate/result schema versions;
- build identity and supported index artifact formats.

Admission uses the intersection of healthy Workers required by the stage. A
descriptor or major-version mismatch fails before dispatch.

The repository has no permanent backward-compatibility shims. Cluster rollout
uses homogeneous protocol epochs: deploy and validate a new Worker pool, drain
old tasks, atomically switch scheduling to the new epoch, then deploy the matching
Coordinator and remove the old pool. A task never spans protocol epochs.

## 5. Shared contracts

### 5.1 AI Capability Descriptor

```text
CapabilityDescriptor
  id, semantic_version, signature_hash, implementation_digest
  kind: SCALAR | TABLE | RETRIEVAL_SOURCE | MERGE | RERANK | MATERIALIZER
  arguments[], result_schema
  execution_placement: NATIVE | WORKER_CALLBACK | EXTERNAL_SERVICE
  modalities[], batch constraints, streaming support
  deterministic, idempotent, side_effect, retry_class
  score direction and normalization requirements
  required privileges and data-egress classification
  memory/network/candidate resource model
```

Planner definitions, Worker registration, and Native operator registration use
the same descriptor. A capability is advertised only after its implementation,
wiring, and dependencies are installed successfully.

### 5.2 MODEL and invocation

```text
MODEL
  object_id, catalog/schema/name, owner
  provider, provider_model_id, immutable version
  lifecycle: CREATING | READY | SUSPENDED | FAILED | DROPPING
  methods and typed signatures
  modalities, embedding dimension, metric, context/output bounds
  endpoint_ref, credential_ref, region, residency/egress class
  retry/rate/capacity policy, price metadata, policy tags

AiInvocationRequest
  invocation_id and idempotency_key
  capability/model object/version
  tenant/principal/roles/query/task/trace
  stable batch item IDs and typed inputs
  timeout/retry budget and requested output schema
  policy version and budget reservation ID

AiInvocationResult
  typed outputs keyed by batch item ID
  per-item status and atomicity outcome
  resolved model/provider request ID and finish reason
  warnings, classified error, and AiUsage
```

Execution order is mandatory:

```text
resolve exact object/version
  -> authorize model/capability/data
  -> validate input/file/egress policy
  -> estimate and reserve budget
  -> execute bounded idempotent batch
  -> validate count/identity/type/dimension
  -> reconcile actual usage
  -> emit audit/trace/lineage
  -> apply output policy
```

Unknown usage is `UNKNOWN`, never numeric zero. Unknown model, ambiguous alias,
suspended version, missing privilege, incompatible modality, or missing secret
fails before network I/O.

### 5.3 Candidate schema

Candidates use the normal columnar exchange schema:

| Field | Contract |
|---|---|
| `candidate_key` | Struct of source snapshot, stable file/object ID, row position, and optional primary key. |
| `source_id` | Stable retrieval source identity. |
| `raw_score` | Original source score. |
| `score_direction` | Higher- or lower-is-better, fixed by capability. |
| `source_rank` | Global rank within one retrieval source after source merge. |
| `fused_score` | Nullable score after fusion. |
| `features` | Bounded typed feature struct for reranking. |
| `artifact_generation` | Exact immutable index generation. |
| `source_snapshot` | Exact table/file snapshot. |
| `execution_mode` | Indexed approximate, indexed exact, exact scan fallback. |
| `tie_break_key` | Stable deterministic total-order key. |

Candidate identity is never a concatenated path string. Candidate batches are
bounded, spillable, backpressured, and reference-counted.

### 5.4 Generic result and completeness

```text
ExecutionResultEnvelope
  coverage: COMPLETE | PARTIAL
  algorithm: EXACT | APPROXIMATE
  fallback: NONE | EXACT_SCAN
  expected/completed/skipped/failed split and shard IDs
  coverage ratio and failure details
  source snapshot/read epoch
  index/service generations and freshness
  attempt IDs, timings, spill, memory and network usage
  capability/model versions, warnings and audit reference
```

Coverage, approximation, and fallback are orthogonal. `COMPLETE` never claims ANN
mathematical exactness. Default policy is `REQUIRE_COMPLETE`; `ALLOW_PARTIAL` is
an explicit query/API option and is visible in JDBC/REST metadata and audit.

### 5.5 Immutable index manifest

```text
IndexManifest
  service/index/object identity and generation
  source snapshot/read epoch and source schema fingerprint
  index format/scorer/analyzer/embedding model versions
  vector dimension/element type/metric
  global BM25 document count, token count and term statistics reference
  complete shard/segment artifact list with row coverage and checksums
  build/validation metrics and quality profile
  CDC start/end watermarks
  creation/publish timestamps and previous generation
```

The manifest is written last using compare-and-set. READY is impossible before
all artifacts and checksums validate. Queries pin one generation for their entire
lifetime. Old generations remain until query leases expire and the GC grace
period elapses.

### 5.6 Durable work state

```text
CREATED -> QUEUED -> LEASED -> RUNNING -> VALIDATING -> COMMITTING -> SUCCEEDED
                         |          |             |
                         +------> RETRY_WAIT <----+
                         +------> FAILED / CANCELLED
```

Every job/partition persists owner, source snapshot, target generation, lease
owner/expiry, fencing token, attempt, idempotency key, checkpoint, staged output,
classified error, retry time, and timestamps. A stale lease holder cannot commit.

## 6. End-to-end capability design

### 6.1 AI SQL and inference providers

1. SQL functions resolve to a versioned capability and MODEL object during
   planning.
2. Planner emits `AiInvokeRel` or a scalar invocation expression with a typed
   signature.
3. Native batches rows by model and bounded token/media budget.
4. Native invokes the Worker-local governed callback through cached JNI handles.
5. Worker resolves the bound secret reference, enforces outbound policy, calls
   the exact provider endpoint, and returns typed item-keyed results and usage.
6. Native restores row order, applies SQL null/error policy, and continues the
   pipeline.

Provider I/O never occurs in the Coordinator. No query row is serialized through
Coordinator heap. Native does not hold a mutex while waiting for provider I/O.

### 6.2 Embedding SYNC, ASYNC, CDC, and backfill

- SQL `AI_EMBED` returns a first-class `VECTOR<F32,N>` whose dimension is bound
  to the MODEL signature.
- Provider batch results must have identical item IDs, count, uniqueness,
  dimension, type, and finite values. Default commit semantics are all-or-none.
- SYNC embedding validates before the transaction publishes the row version.
- ASYNC and backfill use durable work keyed by table/primary key/source row
  version/target column/model version. Duplicate delivery is idempotent.
- CDC records a source watermark; completion atomically publishes the new
  embedded-row version or index delta. A stale source row never overwrites a
  newer version.
- Backfill data scanning and provider batching run as distributed Native tasks;
  the Coordinator only leases and schedules partitions.

### 6.3 VECTOR and exact distance

- VECTOR is a SQL logical type with element type and dimension constraints.
- Substrait uses one canonical user-defined logical type and capability
  signatures; task/result protocols do not create separate Vector messages.
- Native SIMD exact L2/cosine/inner-product remains the correctness oracle.
- NaN, infinity, zero norms, dimension mismatch, null, and overflow behavior are
  defined and tested identically in Java planning validation and C++ execution.

### 6.4 HNSW, IVFPQ, DiskANN, and Lance

- Each engine implements the same Native retrieval source interface and artifact
  manifest contract.
- HNSW exposes `efConstruction`, `M`, `efSearch`, deletion/compaction semantics,
  and exact-rescore support.
- IVFPQ pins coarse centroids, codebooks, `nprobe`, quantization version, and
  original-vector availability for rescoring.
- DiskANN pins graph/data/metadata artifacts, cache budgets, beam width, and disk
  read limits.
- Lance is a storage/index format capability, not an unconditional ANN claim.
  Its advertised mode is derived from the loaded artifact and dependency build.
- Lance's [distributed-index contract](https://lance.org/guide/distributed_indexing/)
  separates fragment-scoped uncommitted segment builds from one manifest commit.
  A build assignment pins disjoint fragment coverage; shared IVF/PQ model
  artifacts are required when segments will be physically merged, while
  independently trained segments may only be composed as a logical index when
  the format supports that mode. Workers cannot each publish a separate live
  index generation.
- Missing implementation/dependency fails capability admission. A brute-force
  path may be used only as explicit `EXACT_SCAN` fallback with full snapshot
  coverage.

#### Lance table-write completion contract (not yet implemented)

The existing `LanceBatchWriter` and Rust C FFI prove file-format I/O, not a
distributed table write. In the pinned Lance 0.20 crate, the current FFI calls
`Dataset::write`, which commits a dataset version when the writer finishes.
Lance's [distributed-write contract](https://lance.org/guide/distributed_write/)
instead writes uncommitted fragments in parallel and commits their metadata in
one operation. The pinned Rust API is
`InsertBuilder::execute_uncommitted_stream` followed by `CommitBuilder`; a
private dataset produced by `Dataset::write` is not a substitute for an
uncommitted fragment of the target dataset. A production `INSERT` or overwrite
must use the same canonical task and fenced catalog-commit path as other table
formats:

1. Before dispatch, the connector persists a query-scoped target allocation:
   dataset URI, staging root, table snapshot/version, physical schema
   fingerprint, partition route, permitted write mode, target dataset identity,
   and commit authority. Once the final canonical task attempt (including task
   ID, plan/control epoch, attempt ID, and fencing token) is bound, the
   connector-owned allocator derives and persists a distinct attempt-private
   staging prefix beneath that root before sending the task. A query-level
   `WriteConfig.outputBindingBase64` value must never be copied unchanged to
   speculative or retried tasks. The Coordinator never writes rows or chooses
   a path by string concatenation. These fields belong in canonical v2
   `WriteTaskOptions`, not a Lance-specific task protocol.
   The deployed Worker must independently resolve the connector's write
   resources from a versioned, tenant-scoped catalog deployment snapshot and
   its own secret authority. A test-only call to
   `WorkerBootstrap.setCatalogResourceResolver` is not a production wiring
   path. The Coordinator distributes identity, version and a bounded
   authorization claim, never an in-process `ConnectorMetadata` instance or
   row payload; Worker registration must fail closed when the catalog snapshot
   or tenant binding is unavailable or stale.
2. The Native Worker validates the allocation against the task attempt and
   uses Lance's uncommitted-fragment write API against that target dataset.
   `finish()` may flush fragment data files but must not update the live
   manifest. A retry has a distinct allocation and fragment filenames; a
   stale attempt cannot overwrite the winning attempt's objects.
3. Each successful task returns typed, checksummed uncommitted fragment
   metadata with the allocation ID, attempt fence, target dataset identity,
   pinned version, schema fingerprint, partition identity, row/byte counts,
   data-file identities, and object-generation or content checksums. The
   connector validates every referenced object before commit. Zero rows use an
   explicit empty fragment report, not a missing report. Failure/cancellation
   returns no committable fragment and triggers idempotent orphan cleanup.
4. The Coordinator verifies complete winning-attempt coverage and invokes a
   single connector-owned atomic catalog transaction. The connector combines
   only winning uncommitted fragments into one Lance append/overwrite operation,
   compares the pinned target version, commits exactly one new manifest,
   records an idempotency key, and supports ambiguous-outcome recovery. Abort
   and GC may delete only unreferenced attempt-private data files after a
   manifest reachability check; they must never delete a committed winner.
5. The public SQL read-after-write test must cross parser, planner, canonical
   task, JNI, Native Lance writer, commit report, connector publication, and
   a fresh Native scan. Multi-worker retries, speculative losers, concurrent
   overwrites, zero-row writes, crash-after-file-before-commit, and object-store
   fault injection are required before advertising Lance table-write support.

Until these contracts and tests exist, both Native and Java admission must
continue to reject Lance table writes; a passing FFI writer unit test or an
available `.lance` fixture must not be counted as public SQL support.

### 6.5 BM25 and full text

- Indexing uses one analyzer fingerprint for ingestion and query processing.
- A generation contains global document count, total token count, average length,
  document frequency data, and per-segment postings checksums.
- Default scoring uses build-time global corpus statistics pinned by generation.
- An optional DFS statistics phase is allowed only for explicitly supported
  realtime delta modes; it is a Native reduction stage, not Coordinator code.
- Scores from different generations/analyzers are never fused.

### 6.6 Hybrid retrieval and reranking

```text
vector leaf(s) ---- local K' ---- source tree merge ---- global source ranks --+
                                                                                |
BM25 leaf(s) ------ local K' ---- source tree merge ---- global source ranks --+-> fusion
                                                                                     |
                                                        bounded Top-N -> rerank -> final K
```

- RRF operates on global per-source ranks and is invariant under shard
  repartitioning.
- Linear fusion uses declared calibration. Query-global min/max or z-score
  calibration adds a statistics-reduction stage before scoring.
- Cross-encoder runs only on bounded global Top-N. It uses the governed model
  invocation contract and returns item-keyed scores.
- Cascade is compiled as explicit ordered stages; each stage has timeout,
  candidate, and fallback policy. No catch-and-return-original behavior exists.
- All algorithms use a deterministic total order: score, source priority,
  stable candidate key.

### 6.7 Distributed routing and global Top-K

- The scheduler binds every expected split/shard and selects a healthy replica
  using locality, queue depth, observed latency, cache affinity, and failure rate.
- Attempts carry fencing IDs. Hedged attempts are allowed only for idempotent
  read stages; the first valid completion wins and superseded output is ignored.
- Leaf tasks emit sorted bounded candidates and a coverage footer.
- Native merge fan-in is bounded. Large queries use a tree of partial reduce
  stages; every node maintains an `O(K')` heap and spills sorted runs when its
  memory grant is exceeded.
- The last global reduce is still a Native task, even if it has one partition.
- Candidate budget `K'`, oversampling, rescore window, merge fan-in, and spill
  budget are visible admission inputs, not hidden constants.
- One missing shard fails by default. Exact fallback is valid only if it scans
  every uncovered row from the same source snapshot.

### 6.8 Candidate fill and results

- First-phase retrieval exchanges only key, scores, ranks, bounded features, and
  generation metadata.
- After final Top-K, `CandidateFillRel` groups keys by owning split/Worker and
  fetches only requested output columns.
- Deduplication occurs before fill. Stale or missing row locations fail snapshot
  validation.
- Filled rows return through the existing Native result transport; no special
  Vector endpoint exists.

### 6.9 FILE/BLOB and multimodal

`FILE` is a governed reference, not an arbitrary URL and not inline unbounded
bytes:

```text
FileReference
  object_id/version
  storage_scope and opaque location token
  content_type, size, checksum, last_modified
  owner/tenant and policy classification
```

- Creation persists bytes before returning a reference.
- The Worker applies one `GovernedBlobStore` contract over a required
  S3-compatible durable backend. Publication is tenant-scoped content-addressed
  conditional PUT followed by size/checksum/SSE/KMS verification; reads are
  exact-key GETs and never enumerate a namespace.
- Opaque reference capabilities use a per-tenant key derived from an externally
  resolved cluster master key. Per-node random or locally persisted signing keys
  are forbidden because they break cross-Worker verification.
- External fetch uses allowlisted schemes/hosts, DNS and redirect revalidation,
  private-address denial, bounded size/time, streaming checksum, MIME sniffing,
  and encrypted storage.
- Query execution checks reference permission and source row policy at use time.
- Native and Worker use streaming/bounded buffers; `readAllBytes()` is forbidden
  on untrusted content.
- Temporary files, decoded media, and provider uploads have explicit RAII/close,
  cancellation, retention, and orphan cleanup.
- Multimodal usage records text tokens, image/document/audio units, model version,
  and provider request ID.

### 6.10 Semantic cache

Semantic cache is a non-authoritative, governed query-result acceleration layer.

The lookup partition key contains tenant, principal/role-policy hash, capability,
MODEL/version, normalized prompt/input signature, locale, safety-policy version,
and all source snapshot/index generation dependencies. ANN is performed only
inside the authorized partition. A similarity threshold is part of a versioned
cache policy.

Entries contain real typed results, usage savings, validation status, TTL, and
dependency fingerprints. Empty results, unknown policy, partial search results,
nondeterministic functions, and side-effecting calls are not cacheable. Catalog
or policy changes invalidate through dependency events. Cache is bounded by
tenant quota with LFU/LRU eviction and is always rebuildable.

### 6.11 Self-optimization

- Collect workload fingerprints, candidate/recall metrics, index hit rates,
  freshness, spill, provider cost, and latency through governed telemetry.
- Recommendations are immutable proposals with evidence, expected benefit,
  resource cost, and rollback conditions.
- Automatic action is disabled until the proposal class has passed offline
  evaluation and shadow validation. Publication uses the same durable job and
  generation manifest path as manual index changes.
- The Coordinator makes decisions and schedules work; Native tasks inspect data.

### 6.12 Feature Store

Feature Store is retained as core infrastructure:

- a catalog Feature View owns entity keys, event timestamp, source query,
  transformation version, schema, freshness target, owner, RBAC, and lineage;
- offline materialization is a versioned table snapshot produced by distributed
  Native tasks;
- point-in-time joins prevent future-data leakage in training datasets;
- online publication is an SPI-backed serving target with an atomic version and
  CDC watermark;
- MODEL signatures can reference a Feature Specification; batch inference joins
  offline values, while online service resolves entity keys against a compatible
  published version;
- training/serving skew is detected by definition and materialization hashes.

An SPI without a production implementation is not advertised as a capability.

### 6.13 Training data service

- A training dataset is an immutable catalog object containing query plan,
  source snapshots, Feature View versions, labels, split specification, schema,
  and lineage.
- Dataset generation is a normal distributed Native query/export job. The
  Coordinator never scans rows or produces training batches itself.
- Random splits require stored seeds; temporal/entity splits are validated for
  leakage. Output files and manifest publish atomically.
- Streaming delivery is a bounded reader over a published dataset generation,
  not an untracked live Coordinator scan.

### 6.14 Semantic View

Pangrova keeps a governed Semantic View object containing logical tables,
relationships, dimensions, facts, measures, filters, owner, privileges, and
versioned validation. Querying it compiles to ordinary relational plans and
Native execution. Optional materializations use the durable refresh framework.

### 6.15 Governance, observability, budget, and evaluation

Every AI/retrieval operation passes these gates:

- RBAC for capability, MODEL, Search Service, FILE, Feature View, dataset, and
  underlying data;
- mandatory input/output/file/egress guardrails at the invocation boundary;
- rate/concurrency/token/media/candidate/memory limits;
- reservation before execution and actual-usage reconciliation after execution;
- audit with principal, resolved object versions, source snapshots, provider,
  request ID, usage source, policy decisions, coverage, fallback, and errors;
- OpenTelemetry traces spanning SQL query, Native stage, index read, model batch,
  and result fill, with sensitive payload access controlled separately;
- immutable Evaluation Dataset and Evaluation Run objects.

Evaluation covers:

- AI function exact/semantic quality, schema validity, latency, errors, safety,
  and cost;
- embedding dimension/order/stability and downstream recall;
- exact vector equivalence, ANN recall@K/QPS/P95/P99/memory/build time;
- BM25/RRF/linear/cross-encoder NDCG/MRR/Recall and shard invariance;
- freshness, completeness, failover, retry, and partial/fallback correctness;
- FILE security corpus and resource cleanup;
- Feature Store point-in-time correctness and training/serving skew.

Release gates compare a candidate model/index/capability version to a pinned
baseline. Regression blocks publication.

## 7. Search Service object

Search is exposed as infrastructure through a catalog object rather than an
implicit collection of indexes:

```text
SEARCH SERVICE
  object identity, owner, source query and primary key
  source snapshot policy and target lag
  vector/text fields and MODEL bindings
  retrieval/fusion/rerank capability refs and quality profile
  indexing state and serving state
  active generation and refresh watermark
  replicas/capacity/cache policy
  RBAC, budget, logging and retention
```

Lifecycle:

```text
CREATING -> BUILDING -> READY
READY <-> INDEXING_SUSPENDED
READY <-> SERVING_SUSPENDED
READY -> REFRESHING -> READY(new generation)
READY -> FAILED | DROPPING -> DROPPED
```

Refresh history records trigger, full/incremental/no-data action, source data
timestamp, inserted/updated/deleted rows, artifact bytes, embedding usage,
duration, generation, and error.

## 8. Large-scale distributed correctness

### 8.1 Snapshot and freshness

Every read pins a `ReadEpoch` that maps to table snapshots and exact Search
Service/index generations. Workers must prove visibility of that epoch before
execution. Supported policies are:

- `STRONG`: require the latest committed source epoch and a compatible exact or
  fully covered fallback path;
- `BOUNDED_STALENESS(duration)`: select the newest generation within the bound;
- `SESSION`: at least the caller's last committed write epoch;
- `GENERATION(id)`: explicitly reproducible search.

Freshness policy is separate from result coverage and ANN quality.

### 8.2 Failure and retry

- Transport, deadline, admission, provider, data corruption, protocol mismatch,
  policy denial, budget exhaustion, and result validation are distinct error
  classes.
- Retry is allowed only when the capability declares idempotence and the remaining
  deadline/budget permits it.
- Retries preserve query snapshot, model/index version, idempotency key, and
  attempt fencing.
- Provider fallback is never implicit. A declared fallback route is a different
  bound policy and is recorded in the result/audit.
- Cancellation propagates Coordinator -> Worker -> Native drivers -> exchange ->
  provider request -> temporary artifacts.

### 8.3 Capacity and backpressure

- Resource admission reserves Native memory, candidate rows, spill, network,
  model concurrency/tokens, and file bytes.
- Every queue is bounded. Overload returns a classified admission/rate error
  rather than accumulating unbounded futures.
- Columnar exchange uses receiver-driven backpressure and per-query isolation.
- Merge trees prevent `O(shards * K')` materialization on one control-plane node;
  the final Native reducer still has a bounded heap and spill path.
- Model batches use token/media-aware packing, maximum wait time, and per-tenant
  fairness.

### 8.4 Replicas and deterministic results

- Replica selection may be adaptive, but a query pins artifact generation and
  source epoch.
- Exact and fusion results use a stable total-order tie break independent of
  replica-local document IDs.
- Property tests repartition the same corpus across random shard/replica layouts
  and require identical exact, BM25, RRF, and linear results.

### 8.5 External shuffle service deployment contract

The canonical v2 `ShuffleService` is a separate data-plane service, not a JVM
Coordinator row relay. A production deployment must provide a real server for
registration, chunked writes, attempt commit, stage finalization, chunked reads,
cleanup, and heartbeat. Test-only `ShuffleServiceImplBase` implementations do
not satisfy this contract. REMOTE admission must fail before dispatch when no
healthy endpoint and durable execution owner are available.

- Registration persists the query/stage/plan-epoch owner fence, expected
  producer set and checksum, partition placement, and short-lived operation-
  scoped capabilities. Re-registration is idempotent only for identical input;
  stale epochs and changed ownership are rejected. Capability verification is
  performed on every data and lifecycle RPC, including streamed chunks.
- Each producer attempt writes immutable, checksummed, bounded chunks to
  partition-owner storage with receiver-driven flow control. The service
  validates stream and partition-local sequences, complete-batch checksums,
  bytes, and an authoritative row count supplied by the Native producer;
  opaque serialized bytes alone cannot prove rows. No full batch is copied
  through the Coordinator or retained in the Java Worker heap.
- `CommitProducerAttempt` publishes one fenced attempt manifest only after all
  endpoint segments and declared partition summaries match. Finalization
  verifies the complete expected producer set and publishes a read-only stage
  manifest atomically. Readers pin that manifest and receive all chunks plus a
  terminal checksum, producer count, snapshot/generation, byte count, and row
  count. No missing producer can be silently converted into an empty partition.
- Durable backing storage must outlive a Worker and shuffle-service process;
  locality cache or a single node's filesystem alone must never advertise
  `DURABLE`. Metadata operations require atomic create/compare-and-swap or an
  equivalent transactional journal. Services recover committed manifests after
  restart and reject loser attempts. Cleanup is fenced, idempotent, and GC's
  unreferenced data only after reader leases and a grace period.
- A capability's TTL is not a query lease. The service and Coordinator must
  verify the same durable `ExecutionAnchor` authority: ownership-sensitive
  metadata mutations compare the full query/owner/incarnation/fencing tuple
  against the current, unexpired anchor in one transactional boundary. A
  failover owner may adopt a query scope only after that authority confirms its
  newer fence; the old owner's data and cleanup capabilities then fail closed.
  A Coordinator-minted takeover assertion is insufficient if old Coordinators
  share the signing key. If shuffle metadata and anchors use separate stores,
  the anchor authority must issue independently verifiable claim attestation.
- Canonical v2 renewal must issue bounded, operation-scoped capabilities only
  while the same owner anchor remains current. A Worker presents mTLS, its
  exact task attempt and a still-valid old capability; it atomically swaps the
  new READ/WRITE token for subsequent RPCs. Long producer streams roll only
  between complete batches, preserving partition sequences and manifests;
  reads resume after the last verified complete sequence. Missed renewal or
  changed ownership fails closed and retries from a new task binding, never
  grants an expiry grace or silently continues an old attempt.
- Query terminal handling synchronously persists `cleanup-pending` before
  releasing the execution anchor. Remote cleanup completion is acknowledged
  before marking that intent done and releasing ownership; failover scans and
  claims pending intents independently of active-query journal replay. Service
  `CLEANING` leases permit bounded, idempotent continuation after a crash in
  the middle of object deletion. `ACTIVE` scopes must not be garbage-collected
  solely because a capability expired: doing so could delete a configurable
  long-running query. Tests must cover takeover versus old writes, TTL renewal,
  crash before cleanup RPC, crash during deletion, and duplicate cleanup.
- Deployments use mTLS with verified service identities, key rotation, bounded
  RPC/message limits, per-tenant quotas, and explicit channel shutdown. Fault
  tests kill producer Workers and shuffle-service nodes between chunk, commit,
  finalize, and read; multi-endpoint tests verify exact rows and checksums after
  retry, speculation, rescheduling, and cleanup. A protocol/client-only test
  cannot qualify this path as a production distributed exchange.

## 9. Current worktree recovery

R0 canonical source recovery was partially verified on the 2026-08-30 baseline.
The 2026-09-13 working tree has since moved into the v2 cutover. Source presence
or deletion below is not an acceptance claim; the migration is complete only
after the section 12 gates pass on the final integrated tree.

### 9.1 2026-09-20 source reality and remaining qualification gaps

Implemented in the current working tree:

- `pangrova-protocol/src/main/proto/pangrova/protocol/v2/` contains the generic
  task, capability, binding, model, index, runtime, result, shuffle, and typed
  relation schemas; Native generation points at that canonical tree;
- the Native parser rejects the wrong major/minor, rollout epoch, canonical
  descriptor hash, and Substrait plan checksum;
- v1 and Vector-specific protobuf sources, the Worker Vector task interceptor
  and result buffer, and the Coordinator JVM Vector merger/materializer are
  deleted in the working tree;
- typed retrieval source/merge/rerank bindings and Native relation conversion
  are present, with explicit stage resource grants and attempt fencing.

The canonical cutover items identified on 2026-09-13 have since been removed:

1. `SubmitTaskRequest` now contains only one Base64-encoded canonical v2
   `TaskRequest`; REST no longer mirrors task semantics and the Worker passes the
   same canonical bytes through admission to JNI.
2. `AiTableFunctionFactory` exposes only the bounded directory metadata function;
   Vector search is a typed Native relation and has no Java executor/result path.

The remaining qualification gaps are:

1. Source-level typed bindings and operators are not proof of a connected
   execution path. Planner golden tests and packaged JNI E2E must show that each
   public search/AI entry reaches the generated v2 relation, Native operator,
   ordinary exchange/result path, and generic report without test-only factory
   calls.
2. Full protocol-descriptor, JNI-symbol, Native Release/ASan, Java, packaged
   handshake, distributed fault, and scale qualification results have not been
   established by this research update. No new production-readiness claim is
   made.
3. Feature View has a single-source durable offline `TABULAR_EXPORT` path, but
   point-in-time multi-source joins and online serving remain open. Training
   Dataset has catalog records only and no executable export/serving chain.

Verified R0 work:

- `pangrova-protocol/src/main/proto` is the only source for Pangrova-owned v2
  cross-runtime messages; Native mirrors and the Worker-local command proto are
  removed from generation inputs;
- Maven and CMake generate the canonical manifest/runtime/plan-extension files;
- startup compares a deterministic, dependency-complete descriptor set and
  SHA-256 rather than relying only on `TaskRequest` field alignment;
- command dispatch uses the generated `RuntimeCommandType`; the manual Java enum
  is removed;
- dynamic-filter transport is typed and fail-closed across Planner, Coordinator,
  Worker, JNI command handling, and Native Velox filter conversion;
- canonical descriptor diff, focused Java tests, Release Native generation/build,
  and focused Native protocol/filter/plan tests pass.

Remaining R0 exit work is the final integrated Docker/provider/distributed
qualification. Lance is now a mandatory packaged C FFI dependency and its
focused real-fixture contract passes; that evidence is not a substitute for the
full multi-platform release pipeline.

Additional verified R0 closure evidence:

- architecture/source comments and golden-test naming no longer describe a
  deleted dual-proto design; obsolete Java/Native descriptor goldens were removed;
- macOS exports were reduced from approximately 43,825 globals to 26 symmetric
  `Java_*` methods plus `JNI_OnLoad`; the executable JNI symmetry gate passes;
- dead CacheMetrics JNI exports and an unimplemented/dead Worker Lance Java JNI
  bridge were removed. Lance format reads have one path: the Native connector
  linked to Lance C FFI, which is not advertised when that dependency is absent.
  The writer primitive is not a production table writer: public SQL dispatch
  still fixes write format to Parquet, and until canonical output allocation and
  external-file commit metadata are defined, a direct Lance task fails closed
  before the connector sink opens;
- the Release library was packaged and the canonical descriptor/hash handshake
  passed through real classpath loading, JNI, C++, and the generated response;
- repository adapters, descriptor diff, Java golden/typed-filter tests, 179
  focused Native tests, and the packaged handshake E2E passed;
- the Java validations above were repeated with the SHA-256-verified official
  Corretto 17.0.20.1 temporary toolchain, satisfying the repository JDK baseline.

The older v1, REST reconstruction, and Java Vector table-function stop-gaps are
historical findings and no longer describe the current source tree.

## 10. Architecture assurance and performance proof

No architecture can truthfully guarantee that no future defect will exist. The
production claim is therefore evidence-based: every structural risk below has a
measurable invariant, an adversarial test, an operational signal, and a rollback
path. A design review is failed when an invariant cannot be tested.

### 10.1 Complexity and data-movement invariants

Let `S` be shard count, `K'` the admitted candidate budget per shard/source, `F`
the merge fan-in, and `K` the final result size.

| Component | Required bound | Proof |
|---|---|---|
| Coordinator candidate memory/network | Exactly zero candidate and row payload bytes. Query state is proportional only to stages, tasks, bindings, and attempts. | Static dependency/import gate plus runtime allocation/network counters. |
| Native leaf | Candidate memory is bounded by its task grant and `K'`; no unbounded per-document accumulation. | Memory-limit and adversarial cardinality tests. |
| Native merge node | Streaming heap is `O(F + K')` plus bounded exchange buffers; spill is used before the grant is exceeded. | Heap/spill telemetry under 1,024-shard fan-in. |
| Candidate network | First-phase transfer is bounded by admitted `S * K' * candidateRowWidth`; it never transfers full source rows. | Exchange byte accounting compared with the plan estimate. |
| Fill network | Only final/deduplicated bounded keys fetch requested columns, proportional to `K`. | Candidate-fill counters and source-read assertions. |
| BM25 metadata | Workers fetch/cache statistics only for query terms, not the complete corpus dictionary per query. | Manifest reader I/O and cache tests. |
| Model rerank | Input is bounded global Top-N with token/media-aware batching and concurrency admission. | Request-size, queue-depth, and provider-call counters. |
| Background jobs | Work queues are persisted and page/lease based; no Coordinator in-memory corpus list. | Restart/heap tests over millions of partitions. |

A final global ordering has an unavoidable reduction point. Pangrova distributes
all reducible work through a tree and leaves only a bounded `K'` stream at the
last Native reducer. Moving that last step to the JVM would not remove the lower
bound; it would only put it in the wrong process.

### 10.2 Control-plane scalability and availability

- Coordinator query/job ownership uses a durable Execution Anchor plus fencing
  epoch, recoverable from durable attempt metadata. A heartbeat lease detects
  failure but does not itself authorize commits. Every metadata transaction
  verifies anchor ownership and epoch. Multiple Coordinator instances may own
  different queries; only one fenced owner mutates a specific query/job.
- Worker heartbeats and capability inventories are incrementally maintained. A
  query does not broadcast to every Worker through the Coordinator.
- Task/status updates are batched and bounded; detailed operator metrics flow to
  observability storage, not retained indefinitely in query objects.
- Catalog publication, protocol-epoch switch, and active-generation switch use a
  strongly consistent transaction/CAS. Large artifacts never enter the metadata
  database.
- A Coordinator loss may delay scheduling, but already-running Native data paths
  are not routed through it. Recovery is bounded by the configured lease expiry
  plus one scheduler interval.

### 10.3 Failure-mode review

| Failure | Required behavior | Forbidden behavior |
|---|---|---|
| Coordinator crash | New fenced owner restores query/job state; superseded owner cannot commit. | Losing candidate data held only in Coordinator memory. |
| Worker crash during leaf/merge/fill | Retry an idempotent task on the same snapshot/generation; ignore superseded attempt output. | Returning success with the shard silently missing. |
| Network partition/timeout | Default query failure; explicit partial mode reports exact failed coverage. | Empty-list substitution. |
| Object-store delayed listing | Fetch exact manifest URIs and verify checksums; metadata CAS defines visibility. | Discovering generation membership by directory listing. |
| Corrupt/mixed artifact | Fail before candidate output; quarantine generation/replica. | Falling through to another algorithm without result metadata. |
| Provider timeout after uncertain execution | Respect provider idempotency support; otherwise classify outcome as uncertain and do not blind-retry/bill as zero. | Duplicate non-idempotent invocation or fabricated usage. |
| Retry/hedge race | First fenced valid attempt wins; late output and temporary artifacts are discarded. | Double commit or duplicate candidates. |
| Lease steal during build/backfill | Old fencing token cannot publish. | Last-writer-wins generation corruption. |
| Index refresh during query | Query continues on pinned immutable generation. | Mid-query generation change. |
| Protocol/capability mismatch | Admission rejects the stage or schedules a homogeneous compatible pool. | Runtime parse failure after partial work. |
| Hot shard/key skew | Split/merge/fill skew metrics trigger repartition, replica routing, or bounded admission. | One unbounded Worker queue. |
| Semantic-cache false hit | Hard policy/model/snapshot partition plus threshold; cache may miss safely. | Cross-tenant/policy reuse or treating cache as authoritative. |

### 10.4 Quantitative qualification targets

These targets are release gates, not current claims. Benchmarks pin dataset,
hardware, build, compiler flags, model/index versions, and query mix.

| Dimension | Minimum production qualification |
|---|---|
| Coordinator isolation | Zero candidate/row bytes and zero retrieval/rerank CPU in Coordinator profiles; heap slope matches a non-AI DAG with the same stage/task count. |
| Scale-out throughput | For embarrassingly parallel scan, embedding, index build, and leaf retrieval, at least 70% parallel efficiency from 4 to 32 Workers before the documented saturation resource. |
| Search shard scale | 1, 16, 128, 512, and 1,024-shard tests complete without control-plane OOM, unbounded queue growth, or memory-grant violation. |
| Tail stability | At the admitted concurrency, a 10x shard-count increase must not cause super-linear Coordinator CPU/heap growth; Native P99 regression must be explained by measured network/reduction cost. |
| ANN quality | Each published quality profile declares Recall@K against the Native exact oracle; default production profile requires at least 0.95 Recall@10 unless an explicitly named lower-recall profile is selected. |
| Hybrid quality | BM25/RRF/linear/cross-encoder changes cannot regress the pinned evaluation set beyond its declared NDCG@10/MRR/Recall tolerance. |
| Exact correctness | Exact Vector, BM25 with pinned global statistics, RRF, and declared linear fusion are invariant under shard repartitioning and replica selection. |
| Gateway overhead | Local Native-to-Worker governance overhead is measured separately from provider latency and must remain within the declared per-batch budget; no per-row JNI/provider call is allowed. |
| Recovery | Worker/query/job recovery completes within the configured deadline/lease SLO without duplicate publication or silent coverage loss. |
| Long stability | Multi-tenant soak covers sustained query, refresh, backfill, provider throttling, cancellation, and generation GC with no leak, deadlock, starvation, or monotonic queue growth. |

Large qualification has three tiers:

1. Per-change deterministic CI uses small corpora and randomized partition
   properties.
2. Nightly distributed qualification uses at least 128/512 shards, sustained
   concurrency, fault injection, and packaged binaries.
3. Release qualification uses production-shaped 1,024-shard and 100M/1B-vector
   datasets where infrastructure is available, plus a multi-hour soak and chaos
   campaign.

### 10.5 Design go/no-go gates

Before feature implementation beyond baseline recovery, the design must pass:

- **Boundary review:** no proposed candidate, row, file byte, model batch, or
  training batch crosses Coordinator heap.
- **Protocol review:** one canonical generated descriptor, typed Substrait
  semantics, homogeneous epoch rollout, and no Vector-specific wire plane.
- **Correctness review:** snapshot/generation/coverage/fallback/score direction
  and deterministic tie rules are complete.
- **Failure review:** every external I/O and state transition has classified
  failure, retry, cancellation, fencing, audit, and cleanup semantics.
- **Capacity review:** every buffer, queue, batch, candidate set, request, and
  artifact read has a bound and admission source.
- **Operability review:** metrics, logs, traces, refresh history, quality, cost,
  and rollback identify the exact version/generation involved.
- **Verification review:** every claim maps to a static, property, integration,
  E2E, load, chaos, security, or soak test with an explicit threshold.

Any unresolved item is a stop signal. The architecture is not called production
ready merely because unit tests pass.

### 10.6 Repository feasibility map and mandatory corrections

The target architecture was mapped back to the 2026-08-30 implementation before
starting the protocol migration. The result is **feasible on the existing module
boundaries**, but the existing classes are foundations rather than proof of the
target behavior.

| Target requirement | Reusable repository foundation | Mandatory correction before the requirement is closed |
|---|---|---|
| Native multi-stage retrieval and reduction | Typed retrieval/merge/rerank/fill relations, `PlanFragmenter`, `StageDAG`, `ExchangeSourceBuilder`, Native exchange and the C++ task runner are present; the Coordinator JVM merger/result path is deleted. | Prove the physical reduction tree, ordinary columnar exchange and zero-Coordinator-payload invariant in packaged multi-Worker E2E and high-fan-in profiles. |
| Bounded final ordering | The planner already supports `SINGLETON` exchange and one OUTPUT stage. | Use a singleton only for the final bounded `K'` stream. Bulk AI transforms and index/embedding jobs write through distributed sink stages. A future parallel client-result endpoint is qualified separately; it is not a reason to route an unbounded result through the JVM. |
| One protocol source | `pangrova-protocol` is consumed by Java and CMake; one `pangrova.protocol.v2` package produces the dependency-complete descriptor set and hash. The REST task envelope contains the exact serialized canonical request. | Finish final integrated symbol/package/descriptor and rolling-mismatch qualification; no feature-specific task protocol or second descriptor may reappear. |
| Typed plan semantics | AI/retrieval source, merge, rerank, fill and artifact relations use generated typed v2 detail messages and reject unknown capability/detail types. | Keep non-AI legacy hints outside the AI execution contract; prove every public AI/search entry emits the typed relation and exact binding through planner goldens and packaged JNI E2E. |
| Worker-local governed model call | `NativeFunctionBridge`, `JvmScalarFunction`, `BatchableScalarFunction`, and `AiInvocationService` provide a batched Native-to-local-JVM callback and fail-closed provider checks. | Registration success, request context, deadlines, cancellation, bounded concurrency, idempotency, real usage, and audit must be part of one model invocation contract. Multimodal and FILE batches must not fall back to per-row provider calls. |
| Immutable search generations | `IndexDescriptor`, snapshot pins, generation checks, build claims, cache generation keys, maintenance election, and GC hooks are reusable. | Current `BUILDING/READY/STALE` describes a mutable logical index and does not provide an independent active and staging generation. File mappings can be produced during a build. Add immutable generation/manifests, active-generation CAS, query leases, validation-before-publish, and fencing-token checks at publication. |
| Distributed build and refresh | Build metrics, claim heartbeats, commit listeners, CDC snapshot watermarks, and object-store access abstractions exist. | `IndexBuildCoordinator` is currently an in-process Java executor that loops over files. Replace data-dependent build/validation/compaction with scheduled Native DAG tasks; persist page-level work and commit only the validated manifest. |
| Durable embedding/backfill | Generated values use persistent generic artifact jobs/pages, source-snapshot dependencies, leases, attempts, fenced reports and atomic generation publication; legacy Coordinator row scanning/queues are deleted. | Prove public SQL/commit-triggered recovery, duplicate delivery, cancellation, dead-letter and exact row-version visibility in crash/restart E2E. Explicit BACKFILL/CANCEL/SHOW SQL is not complete until its handlers use this same state machine. |
| Coordinator HA and ownership | Leader/query leases, epochs, orphan persistence, and recovery tests already exist. | Persist the AI/Search stage attempts and publication decisions needed for failover, fence every side effect by query/job attempt, and prove recovery against the production coordination backend rather than only in-memory tests. |
| Explicit result semantics | Canonical `TaskReport`/`CoverageReport` carries source snapshot, generation/freshness, approximation, fallback and work-unit coverage; Coordinator validation defaults to fail closed. | Pass the distributed timeout/corruption/stale/retry/fallback matrix and prove a result endpoint cannot make an incomplete report successful. |
| Large-fan-in resource control | Stage grants include memory/spill, candidate rows/bytes, provider calls/tokens/media, object-store I/O and deadlines; Native operators and terminal usage consume the canonical contract. | Qualify balanced reduction, spill, cancellation and accounting at 1/16/512+ shards and concurrent tenants; no scale claim is valid before those profiles pass. |

Current stop-ship gaps are qualification and remaining lifecycle work, not a
second Vector protocol or Coordinator merger: index build still has a legacy
in-process Java coordinator alongside the durable Native artifact path; public
search/provider fault E2E, high-fan-in spill/chaos, ASan, and full release gates
remain open. No performance or production-readiness claim is made until the
corresponding exit gates and section 10.4 qualification tiers pass.

## 11. Implementation workstreams

Each workstream is an independently reviewable batch. A batch cannot start until
the previous batch's exit gate passes.

Current source-state markers are intentionally conservative:

| Workstream | 2026-09-20 state | Next hard gate |
|---|---|---|
| R0 | Source ownership and descriptor groundwork implemented; historical focused checks recorded. | Re-run the final integrated protocol/JNI/package gates; no deleted mirror or generated artifact may reappear. |
| R1 | Generic v2 IDL, exact REST envelope, generated types, typed relations, capability inventory, and Native parsing are present. | Re-run the complete descriptor/JNI/package chain and rolling-mismatch rejection gates on the final tree. |
| R2 | Native retrieval primitives and typed conversion are present; Java Vector task/table-function/result execution paths are deleted. | Pass exact Vector/BM25 public-entry packaged JNI E2E. |
| R3 | Typed merge/rerank/fill planning and Native implementation work is present; qualification is unproven. | Pass shard-invariance, complete-coverage, bounded-spill, cancellation, and zero-Coordinator-payload gates at 1/2/16/512+ shards. |
| R4-R6 | Reusable foundations and partial implementations exist; provider dialects and durable Blob/embedding work require final integrated E2E. | Close each workstream's governance, security, scale, and full-chain exit gate in dependency order. |
| R7 | Semantic Cache stats and single-source Feature View durable offline export have focused contract evidence. Training Dataset remains catalog-only. | Pass cache provider-call-count E2E, point-in-time/online Feature gates, and build a real distributed Training Dataset export before advertising it. |
| R8 | Target-only. | Complete Semantic View/evaluation and full release hardening. |

### R0 — Freeze canonical protocol source ownership (historical precursor)

Changes:

1. [done] Complete Java/C++ generated-source wiring for canonical
   `common/task/runtime/plan_extension` files.
2. [done] Replace Java numeric command constants with generated enum values.
3. [done] Complete typed dynamic-filter producer/consumer migration.
4. [done] Generate and compare the dependency-complete `FileDescriptorSet` hash.
5. [done] Update protocol golden properties, source comments, architecture docs,
   JNI symbol/package tests, and repository gates to the single source tree.
6. [retired transition] The temporary v1 Vector fields were allowed only until
   the v2 replacement was wired; they have been deleted and must not return.

Exit gate:

- canonical Java/C++ generation succeeds;
- descriptor, proto diff, command parity, JNI symbol, Native focused tests, and
  Java protocol/Worker tests pass;
- no mirrored cross-boundary proto remains;
- `git diff` contains no hand-edited generated sources.

Rollback boundary: this batch is purely source ownership. Its last known good
commands are the protocol Maven tests and filtered Native parser/capability tests.

### R1 — Canonical generic v2 cutover

Changes:

1. [source complete] Add the canonical v2 files described in section 4.
2. [source complete] Add typed Substrait relation schemas and generated planner/native bindings.
3. [source complete] Migrate task/runtime/result/model/index callers.
4. [source complete] Delete v1 and all Vector-specific task/result protocol files and REST field mirrors.
5. [source complete] Update architecture rules so Native protobuf package/source ownership matches
   the canonical package.
6. [source complete] Enforce homogeneous protocol epoch admission.

Exit gate:

- no production `vector_search` TaskRequest field, Vector candidate protobuf, or
  feature session-property encoding remains;
- Java/C++ complete descriptor hashes match;
- unknown extension/capability fails before execution;
- golden plan/protocol/property tests pass.

### R2 — Native retrieval source and candidate pipeline

Changes:

1. Add Native candidate schema and `RetrievalSourceRel` conversion.
2. Wire exact VECTOR retrieval as the initial oracle.
3. Add manifest loader/cache with checksum, snapshot, dimension, metric, and
   analyzer validation.
4. Port HNSW/IVFPQ/DiskANN/Lance retrieval behind one Native interface.
5. Add BM25 postings and global corpus-statistics loading.
6. Emit coverage footers and generic task reports.

Exit gate:

- single-Worker exact Vector and BM25 SQL paths execute only in Native;
- missing/mixed/corrupt artifacts fail closed;
- indexed and exact-fallback modes are explicit;
- no Worker Java retrieval interception executes.

### R3 — Native distributed merge, hybrid, rerank, and fill

Changes:

1. Add tree Top-K and source-global rank operators over normal exchange.
2. Add deterministic RRF and calibrated linear fusion.
3. Add bounded cross-encoder/cascade `RerankRel` using governed Worker callback.
4. Add candidate deduplication and `CandidateFillRel`.
5. Delete `QueryVectorResultMerger`, `GlobalTopKMerger` data path,
   `VectorSearchResultBuffer`, result endpoint, fetchers, and Java interceptors.

Exit gate:

- no candidate bytes enter Coordinator production code;
- 1/2/16/512+ shard exact and fusion results match an offline oracle;
- shard timeout defaults to query failure and explicit partial mode reports exact
  coverage;
- memory remains bounded with spill under high fan-in;
- final rows use the normal result transport.

### R4 — Durable Search Service and index lifecycle

Changes:

1. Add Search Service, generation, artifact, refresh-history, and durable job
   metadata migrations.
2. Implement lease/fencing/idempotency state machine and distributed Native
   build/validation/compaction tasks.
3. Publish manifests atomically and implement query-generation leases and GC.
4. Add CDC watermarks, delta segments, full rebuild, suspend/resume, target lag,
   capacity, and replica placement.
5. Integrate BM25 global statistics and vector quality validation into publish.

Exit gate:

- crash/restart/lease-steal/duplicate/cancel tests prove atomicity;
- a query never mixes generations or source snapshots;
- refresh history and freshness are queryable;
- old-generation GC never deletes a live query artifact.

### R5 — MODEL, AI SQL, Embedding, and governance gateway

Changes:

1. Add MODEL/version/service catalog objects and exact resolution.
2. Consolidate current AI SPI types into one immutable invocation contract.
3. Install real Native-to-Worker callback wiring and advertise capabilities only
   after successful registration.
4. Make AI SQL functions and embedding return typed outputs; remove provider/model
   fallback and duplicate legacy contracts.
5. Add durable SYNC/ASYNC/CDC/backfill jobs with row-version idempotency.
6. Enforce RBAC, secret references, file/egress guardrails, budgets, audit, trace,
   rate/concurrency limits, and actual usage reconciliation.

Exit gate:

- every public AI SQL family has packaged JNI E2E with a controlled provider;
- unknown/suspended/unauthorized/incompatible model fails before network;
- embedding count/order/dimension and atomic failure properties pass;
- no provider caller exists outside the governed invocation service;
- restart and duplicate delivery cannot corrupt backfill results.

### R6 — FILE/BLOB and multimodal

Changes:

1. Replace synthetic inline URIs with persisted governed File References.
2. Implement streaming fetch/storage and SSRF/redirect/private-address/size/MIME/
   checksum/encryption policy.
3. Wire image/document/audio capability descriptors and typed usage.
4. Add cleanup, retention, cancellation, orphan scan, and audit.

Exit gate:

- adversarial URL/file corpus fails closed;
- large documents stay within bounded heap/off-heap limits;
- cleanup is proven on success, error, timeout, and cancellation;
- multimodal SQL E2E uses real stored references.

### R7 — Semantic cache, self-optimization, Feature Store, and training data

Changes:

1. Rebuild Semantic Cache as a read-through governed infrastructure service with
   real typed results, partition-before-ANN, dependency fingerprints, TTL, and
   invalidation.
2. Add durable recommendation/evidence objects and shadow validation.
3. Implement catalog Feature Views, offline point-in-time materialization, online
   publication SPI, lineage, and MODEL feature specifications.
4. Replace Coordinator training scans with distributed dataset export jobs and
   immutable dataset generations.

Exit gate:

- cache never crosses tenant/principal/model/policy/snapshot boundaries;
- Feature Store point-in-time oracle and training/serving hash tests pass;
- dataset rebuild is reproducible from pinned snapshots and seed;
- automatic optimization is auditable, cancellable, and rollback-safe.

### R8 — Semantic View, evaluation, documentation, and release hardening

Changes:

1. Complete governed Semantic View DDL/catalog/planner behavior.
2. Add Evaluation Dataset/Run, metrics, comparison, and publication gates.
3. Make public docs capability-driven: only a passing public-entry conformance
   test can produce a completed/GA statement.
4. Run performance, failure, security, upgrade, and full-chain release gates.

Exit gate:

- every retained capability is `CLOSED` in the declaration/implementation ledger;
- public examples are executed in CI;
- complete repository validation passes.

## 12. Test and acceptance matrix

Tests are marked only `PASS`, `FAIL`, `SKIPPED`, or `BLOCKED`. `SKIPPED` and
`BLOCKED` never count as passing.

| Layer | Mandatory proof |
|---|---|
| Static | Single proto owner; no mirrored descriptors/constants; SPI implementation and wiring; no orphan config; JNI declaration/export symmetry. |
| Protocol | Full descriptor hash parity; unknown semantic relation rejection; randomized serialization properties; protocol-epoch admission. |
| Planner | SQL -> typed plan golden tests for every capability; no JSON/string semantics; capability/model/artifact binding validation. |
| Native unit | Exact distance, ANN artifact validation, BM25 global stats, Top-K, RRF, linear calibration, candidate fill, spill, cancellation, resource cleanup. |
| Distributed property | Shard/replica/repartition invariance; deterministic ties; mixed generation rejection; bounded memory; coverage arithmetic. |
| Fault | Worker loss, timeout, duplicate attempt, corrupt artifact, stale lease, provider 429/5xx, budget denial, cancellation, object-store errors. |
| Security | RBAC, secret non-disclosure, SSRF/redirect/private IP, oversized/decompression bomb, MIME mismatch, cross-tenant cache, sensitive trace access. |
| Durable jobs | Crash at every transition, lease steal, idempotent retry, cancel, CAS conflict, old-generation GC safety. |
| Quality | ANN recall, BM25/RRF/linear NDCG/MRR, cross-encoder lift, embedding downstream recall, model safety/schema accuracy. |
| Performance | 1/16/512+ shards, concurrent tenants, provider saturation, P50/P95/P99, memory/spill/network, build/refresh throughput. |
| E2E | JDBC/REST -> parser -> planner -> canonical protocol -> Worker -> packaged JNI -> Native exchange/operators -> governed provider/object store -> normal result. |

Forbidden test evidence:

- `assumeTrue` or runtime-unavailable acceptance;
- constant-true assertions;
- calling factories/components directly and naming it E2E;
- stubbing the inference provider in E2E;
- treating a partial result or exact fallback as an ordinary success;
- using an unpackaged Native library;
- running Java with JDK 21 `--release 17` and claiming JDK 17 validation.

## 13. Release and rollback

### 13.1 Deployment order

1. Publish canonical protocol/code generators and validate descriptors.
2. Deploy a homogeneous new Worker pool with capabilities disabled until startup
   self-tests pass.
3. Build/publish required index/model artifacts without routing production reads.
4. Run shadow queries and evaluation against pinned snapshots.
5. Drain old tasks, switch the scheduling protocol epoch and Search Service
   generation atomically, then enable traffic gradually by tenant/resource group.
6. Monitor coverage, errors, P99, spill, usage, recall, freshness, and budget.
7. Delete old code/protocol only after the fleet and public-entry tests are green.

### 13.2 Rollback

- Query behavior rolls back by switching the catalog pointer to a previous
  validated Search Service/model/capability generation, not by interpreting a new
  protocol with old code.
- Worker binary rollback drains the affected protocol epoch and schedules only on
  a homogeneous compatible pool.
- Durable job rollback cancels future leases; published immutable artifacts remain
  available until query leases and the grace period expire.
- No destructive artifact cleanup occurs during the rollback decision window.

## 14. Definition of done

The goal is achieved only when all retained capabilities satisfy every item below:

1. Public SQL/API entry reaches the real planner, canonical protocol, packaged
   JNI, Native execution, and normal result path.
2. Coordinator production code never handles candidate or user row payloads.
3. One canonical descriptor and capability definition is used by planner, Worker,
   and Native.
4. MODEL, Search Service, FILE, Feature View, dataset, Semantic View, and
   evaluation objects have durable lifecycle, owner, RBAC, audit, and versioning.
5. Snapshot, generation, freshness, coverage, approximation, and fallback are
   explicit and fail closed by default.
6. Jobs are persistent, leased, fenced, retryable, idempotent, cancellable, and
   atomically published.
7. Usage, cost, latency, errors, quality, lineage, and policy decisions are
   queryable and enforceable.
8. Large shard counts, concurrent tenants, failures, retries, spill, and upgrades
   pass distributed tests and performance targets.
9. The repository's complete eight-step Native/package/Java/E2E/smoke pipeline,
    ASan, static analysis, protocol gates, and dead-code gates all pass under the
    required JDK 17/Docker/C++20 environment.

## Sources

Only first-party documentation, engineering publications, project repositories,
and the user-specified Velox4J repository are used for the decision ledger.

[^1]: Snowflake. ["Snowflake key concepts and architecture"](https://docs.snowflake.com/en/user-guide/intro-key-concepts). Living documentation; accessed 2026-09-13.
[^2]: Snowflake. ["Overview of warehouses"](https://docs.snowflake.com/en/user-guide/warehouses-overview). Living documentation; accessed 2026-09-13.
[^3]: Yu Zhang and Kirutthika Raja, Snowflake Engineering. ["Strengthening Snowflake's Distributed Query Lifecycle with Execution Anchors"](https://www.snowflake.com/en/blog/engineering/snowflake-distributed-query-execution-anchors/). 2026-05-05.
[^4]: Ryan Shelly, Snowflake Engineering. ["Automatic Clustering at Snowflake"](https://www.snowflake.com/en/blog/engineering/automatic-clustering-at-snowflake/). 2024-09-07.
[^5]: Snowflake. ["Cortex Search"](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview). Living documentation; accessed 2026-09-13.
[^6]: Snowflake. ["CREATE CORTEX SEARCH SERVICE"](https://docs.snowflake.com/en/sql-reference/sql/create-cortex-search), ["ALTER CORTEX SEARCH SERVICE"](https://docs.snowflake.com/en/sql-reference/sql/alter-cortex-search), and ["CORTEX_SEARCH_REFRESH_HISTORY"](https://docs.snowflake.com/en/sql-reference/functions/cortex_search_refresh_history). Living documentation; accessed 2026-09-13.
[^7]: Snowflake. ["Monitor Cortex Search requests"](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-monitor). Living documentation; accessed 2026-09-13.
[^8]: Snowflake. ["Privileges and model access for Cortex AI Functions"](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-privileges-and-access). Living documentation; accessed 2026-09-13.
[^9]: Snowflake. ["CORTEX_AI_FUNCTIONS_USAGE_HISTORY view"](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_ai_functions_usage_history). Living documentation; accessed 2026-09-13.
[^10]: Snowflake. ["Snowflake AI Observability Reference"](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-observability/reference) and ["Evaluate applications with TruLens"](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-observability/evaluate-applications-trulens). Living documentation; accessed 2026-09-13.
[^11]: Apache Spark. ["Job Scheduling"](https://spark.apache.org/docs/latest/job-scheduling.html), Spark 4.2.0 documentation when accessed 2026-09-13.
[^12]: Apache Spark. ["Performance Tuning: Adaptive Query Execution"](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution), Spark 4.2.0 documentation when accessed 2026-09-13.
[^13]: Apache Gluten. ["A Middle Layer for Offloading JVM-Based SQL Execution to Native Engines"](https://github.com/apache/gluten/tree/810810ce94b75f881205ab6d3e113fb409a26aaf), repository README and source at commit `810810ce94b75f881205ab6d3e113fb409a26aaf`, 2026-09-11.
[^14]: Velox. ["Velox: a composable and fully extensible C++ execution engine library"](https://github.com/facebookincubator/velox/tree/24ecd0ceaca40b956a2f4a8abcd774c29a69ea03), repository README and source at commit `24ecd0ceaca40b956a2f4a8abcd774c29a69ea03`; accessed 2026-09-13.
[^15]: Velox. ["What's in the Task?"](https://facebookincubator.github.io/velox/develop/task.html). Living developer documentation; accessed 2026-09-13.
[^16]: Velox. ["Memory Management"](https://facebookincubator.github.io/velox/develop/memory.html). Living developer documentation; accessed 2026-09-13.
[^17]: BoostScale. ["Velox4J: Community Java Bindings for Velox"](https://github.com/boostscale/velox4j/tree/b49c66cd76df7b04cfa24c4182873e7b99a4f996), README and source at commit `b49c66cd76df7b04cfa24c4182873e7b99a4f996`, 2026-09-12.
[^18]: Snowflake. ["QUERY_HISTORY, QUERY_HISTORY_BY_*"](https://docs.snowflake.com/en/sql-reference/functions/query_history). Living documentation; accessed 2026-09-13.
[^19]: Snowflake. ["GET_QUERY_OPERATOR_STATS"](https://docs.snowflake.com/en/sql-reference/functions/get_query_operator_stats). Living documentation; accessed 2026-09-13.
