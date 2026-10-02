# AI Native Retrieval Execution Architecture

> Status: design review required. This document is an implementation gate, not
> evidence that the described system is already available.
>
> Scope: Vector, full-text BM25, hybrid retrieval, RRF/linear/cross-encoder/
> cascade reranking, distributed Top-K, index artifacts, and result completeness.
The cross-capability target architecture, industry evidence, migration batches,
and release gates are defined in
[`ai-native-target-architecture-and-implementation-plan.md`](ai-native-target-architecture-and-implementation-plan.md).
This document remains the retrieval-specific execution specification.

## 1. Decision summary

Pangrova retrieval is a native relational execution capability, not a Java task
interceptor and not a Vector-specific RPC path.

1. SQL semantics are represented in the Substrait physical plan.
2. Task protobuf carries control-plane bindings only. It does not duplicate the
   query vector, full-text query, ranking expression, or merge semantics.
3. The Coordinator JVM resolves capabilities, snapshots, index manifests, and
   placement, then schedules the DAG. It never transports or fuses candidates.
4. Worker C++ operators produce, exchange, merge, rerank, and materialize
   candidates. Candidate data uses the normal columnar exchange and result path.
5. Every Java/C++ boundary message is generated from one canonical IDL tree.
   Mirrored Java/native TaskRequest, SplitInfo, VectorSearchInfo, and related
   messages are prohibited.
6. Vector and BM25 are implementations of a generic retrieval-source capability.
   RRF, linear fusion, cross-encoder, and cascade are merge capabilities.
7. Index generations are published through an atomic manifest. BM25 corpus
   statistics belong to that manifest and are global for its pinned source
   snapshot.
8. Coverage completeness, retrieval quality, and exact-fallback status are
   separate concepts and must not be collapsed into one `COMPLETE` flag.

## 2. Non-negotiable boundaries

### 2.1 Control plane

The Coordinator JVM may:

- parse and plan SQL;
- authorize capabilities and models;
- resolve an exact capability version;
- pin a table snapshot and an index-generation manifest;
- create stages, exchanges, placement, deadlines, and resource limits;
- enforce default fail-closed completion policy;
- validate the final execution envelope and publish audit/usage metadata.

It may not:

- fetch Worker candidate buffers over an auxiliary HTTP endpoint;
- deserialize all search candidates into JVM heap;
- calculate global Top-K, RRF, linear normalization, or cross-encoder order;
- infer physical index paths from naming conventions;
- turn a missing shard, timeout, or parse failure into an empty candidate list.

### 2.2 Data plane

Every read-dependent retrieval operation runs through the Native execution path:

```text
Substrait plan
  -> Native local retrieval operators
  -> columnar candidate exchange
  -> Native hierarchical source merge
  -> Native fusion/rerank
  -> keyed row materialization
  -> normal Native result transport
```

The Worker JVM owns process lifecycle, admission, JNI entry, and governed
provider callbacks. It does not bypass Native task execution with a Java search
interceptor.

## 3. One protocol, one owner

### 3.1 Canonical ownership

All messages that cross REST, gRPC, shuffle, or JNI boundaries live under one
canonical protocol source tree owned by `pangrova-protocol`. Maven and CMake
generate Java and C++ from those exact files.

The `pangrova-worker-native/proto` tree may contain only messages that never
cross a Java/C++ or process boundary. A message is not considered internal if
Java serializes it or C++ returns it through JNI.

File modularity is allowed; protocol duplication is not. Separate files such as
`task.proto`, `execution_binding.proto`, and `runtime_command.proto` remain one
versioned package and one descriptor set. There is no independently versioned
Vector protocol.

### 3.2 Versioning and handshake

The unified structural change is introduced as protocol package v2 rather than
reusing v1 field numbers with different semantics. During the migration window,
the Coordinator schedules a task only after the Worker reports:

- protocol major/minor;
- descriptor-set SHA-256;
- supported capability IDs and versions;
- supported candidate-batch schema version;
- supported result-envelope version.

The Coordinator plans against the intersection of healthy Workers. A major or
descriptor mismatch fails admission before task dispatch. Once every production
caller is migrated, v1 code and schemas are deleted; no permanent compatibility
shim remains.

CI generates both languages from the canonical tree and compares descriptor
hashes. Hand-maintained field-number parity tests are not an acceptable
substitute.

## 4. Where retrieval semantics live

### 4.1 Substrait extensions

Retrieval semantics belong in typed Substrait extension relations, because they
are relational operations that consume and produce rows. The extensions use a
generic capability invocation rather than one message per algorithm:

```text
CapabilityInvocation
  capability_ref: {id, version, signature_hash}
  named_arguments: typed Substrait expressions
  typed_options: repeated {name, oneof scalar value}

RetrievalSourceRel
  source_id
  invocation
  candidate_limit
  input relation

RetrievalMergeRel
  input source schemas
  invocation
  final_limit
```

Examples of capability IDs are `retrieval.vector.hnsw`,
`retrieval.vector.ivfpq`, `retrieval.text.bm25`, `merge.top_k`, `merge.rrf`,
and `merge.linear`. IDs identify behavior; index choice remains an execution
binding.

The query vector is a typed plan expression. The BM25 query is a typed plan
expression. Neither is repeated in TaskRequest.

### 4.2 Task execution bindings

TaskRequest v2 contains generic bindings keyed by plan-node ID:

```text
OperatorBinding
  plan_node_id
  capability_ref
  repeated IndexArtifactBinding
  ExecutionPolicy

IndexArtifactBinding
  index_id
  index_type
  generation
  source_snapshot
  manifest_uri
  manifest_checksum
  repeated per-split artifact references

ExecutionPolicy
  deadline
  retry_class
  coverage_policy
  memory/candidate/spill budgets
```

This removes Vector-specific TaskRequest fields and string-valued session
properties while still letting the Coordinator bind a logical plan to physical
artifacts.

## 5. Capability descriptor

One immutable descriptor is shared by planner admission, Worker registration,
and Native operator registration:

- capability ID, semantic version, and signature hash;
- kind: scalar, table, retrieval source, merge, or materializer;
- input/output logical types and score direction;
- execution placement: Native, governed JVM callback, or external service;
- supported modality and batch constraints;
- deterministic/idempotent/retry semantics;
- required privilege and data-egress class;
- resource model and maximum bounded inputs;
- implementation build identity.

A task carries only a `CapabilityRef`. Descriptor hashes must match at admission
and Native operator creation. Reporting a capability before its implementation
and wiring are successfully installed is a startup failure.

## 6. Candidate data contract

Candidates are columnar rows, not protobuf blobs and not JVM objects. Candidate
schema v1 is:

| Field | Type | Meaning |
|---|---|---|
| `candidate_key` | STRUCT | snapshot, stable file ID, and row position |
| `source_id` | VARCHAR | retrieval-source identity |
| `raw_score` | DOUBLE | source score with declared direction |
| `source_rank` | BIGINT nullable | populated after global source merge |
| `features` | MAP/VECTOR nullable | bounded reranker features |
| `artifact_generation` | INT | exact index generation |
| `source_snapshot` | VARCHAR | pinned data snapshot |
| `fallback_mode` | ENUM | indexed, exact scan, or none |

String concatenation such as `filePath + "#" + rowOffset` is not a valid
identity contract. File URI, snapshot, and row position remain separate typed
fields. Candidate batches use the normal Native exchange serializer with
backpressure and spill.

### 6.1 Artifact node-to-row locator

DiskANN/HNSW/IVFPQ node IDs and inverted-list ordinals are artifact-local
implementation details. A generic `RowLocatorContract` binds every readable
artifact generation to exactly one locator schema:

| Source semantics | Locator | Scope | Native production |
|---|---|---|---|
| Immutable append/COW file | `UINT64 physical_row_position` | `source_object_id` + pinned snapshot | DWIO row-index metadata before equality/position/DV filtering |
| Paimon MOR | canonical typed primary-key tuple | table + pinned snapshot | post-merge Native row stream |

The compact immutable locator sidecar is position-aligned with the graph or
posting nodes. Its header and length are validated before serving. Hash-derived
keys are forbidden because collisions violate exact fill semantics. Until a
connector's locator encoder and snapshot-aware Native fill implementation are
both advertised, artifact planning for that connector fails closed.

## 7. Distributed execution

### 7.1 Local retrieval

Leaf Workers execute every assigned split and emit a bounded, sorted candidate
stream per source. They also emit a control footer containing expected and
responded split IDs, artifact generations, source snapshots, and fallback mode.
Missing coverage is an error unless the query explicitly requested partial
results.

### 7.2 Hierarchical source merge

A fixed-fan-in merge tree combines sorted source streams in Native. Each merge
node retains the configured candidate budget and verifies that all inputs use
the same source snapshot and compatible artifact manifest. This avoids a single
Coordinator or Worker receiving unbounded cluster-wide candidates.

`candidate_k` is an explicit quality/resource budget, not a hidden oversampling
constant. Admission bounds it by query and tenant quotas.

### 7.3 Fusion

- **Top-K:** merge source scores using their declared direction.
- **RRF:** first establish global per-source ranks, then exchange by typed
  candidate key, sum reciprocal-rank contributions, and perform a final Native
  Top-K tree merge.
- **Linear:** use a declared calibration function. If query-dependent global
  min/max normalization is selected, Native performs a statistics reduction
  before scoring; Worker-local normalization is invalid.
- **Cross-encoder:** materialize only a bounded global Top-N after first-stage
  fusion. Invoke the governed model in batches, then perform final Native Top-K.
- **Cascade:** compile to explicit ordered merge stages. A failed stage fails the
  query unless its typed policy explicitly names an allowed fallback and the
  result envelope records it.

## 8. BM25 global correctness

Per-file document count, document frequency, and average length do not define a
globally comparable BM25 score. Every inverted-index generation therefore owns
an immutable corpus-statistics artifact containing:

- source snapshot and schema/analyzer fingerprint;
- global document count and total token count;
- global document frequency dictionary;
- per-shard artifact list and checksums;
- format and scorer version.

The build job writes shard artifacts and the statistics artifact under a new
generation, validates all checksums, then atomically publishes one manifest.
READY is impossible before manifest publication. Queries pin the manifest and
Workers read/cache only the query-term statistics they need.

CDC writes generation deltas and publishes a new manifest atomically. Removed
files subtract their recorded statistics. Compaction rewrites deltas without
changing query-visible semantics. A query never combines shard scores from
different manifests or source snapshots.

## 9. Completion, quality, and fallback

The normal result transport carries rows. A general execution envelope carries
control metadata:

- coverage status: complete or partial;
- expected/responded/failed split IDs and coverage ratio;
- pinned source snapshots and artifact manifest identities;
- algorithm mode: exact or approximate;
- configured candidate budget and quality profile;
- fallback status per shard;
- warnings, timings, spill, and resource usage.

`COMPLETE` means complete input coverage. It does not claim mathematical exactness
for ANN or bounded hybrid fusion. `FALLBACK_EXACT` is recorded per shard and does
not erase the query's approximate stages.

Default policy is `REQUIRE_COMPLETE`. `ALLOW_PARTIAL` must be explicit in the
request and visible in both result metadata and audit logs.

## 10. Index-generation state machine

```text
CREATED -> BUILDING -> VALIDATING -> PUBLISHING -> READY
                  \-> FAILED
READY -> REFRESHING -> VALIDATING -> PUBLISHING -> READY(new generation)
READY -> DROPPING -> DROPPED
```

Jobs are persisted, leased, retryable, idempotent, and fenced by generation.
Artifact upload is invisible until a manifest compare-and-set succeeds. GC may
delete an old generation only after no query lease references it and the grace
period expires.

## 11. Security and governance

- Capability authorization and index/model privileges are checked before DAG
  creation.
- Tenant, principal, query, trace, and budget reservation propagate in the
  canonical TaskRequest.
- Cross-encoder/model calls go through the unified governed invocation gateway;
  direct provider access is forbidden.
- Candidate and document payloads never pass through Coordinator heap.
- Audit records resolved capability versions, model/index manifests, data-egress
  classification, usage reconciliation, coverage, and fallback.

## 12. Current code disposition

The following current directions are transitional and must not become the target
architecture:

- `VectorSearchTaskSupport` bypassing Native task execution;
- `VectorSearchResultBuffer` plus Coordinator HTTP candidate fetch;
- `QueryVectorResultMerger` performing JVM Top-K/RRF/linear fusion;
- `VectorSearchInfo` duplicated in Java and Native proto trees;
- full-text and reranker configuration encoded in session-property strings;
- file-local BM25 corpus statistics;
- a Vector-specific result protocol parallel to normal query results.

Useful foundations to preserve are exact physical index-file bindings, typed
snapshot/generation metadata, fail-closed shard coverage, raw source score
preservation, governed model invocation, and Native distance/BM25 primitives.

## 13. Rejected alternatives

### JVM Coordinator merge

Rejected because it moves data-plane CPU, network, and memory into the control
plane and creates a cluster-wide bottleneck.

### Java Worker interception followed by HTTP result collection

Rejected because it bypasses the relational DAG, Native exchange, spill,
backpressure, and normal result lifecycle.

### One independent proto per feature with mirrored native copies

Rejected because field parity is manual, semantic ownership is ambiguous, and
capability evolution cannot be negotiated reliably.

### Full-text global statistics computed at query time

Rejected because it requires an extra cluster scan or unbounded candidate return.
Statistics are index-generation artifacts.

### Silent local oversampling constants

Rejected because resource cost and quality are invisible. Candidate budgets and
quality profiles are first-class admission inputs.

## 14. Implementation sequence and gates

No behavior migration starts until the protocol ownership and operator schemas
above pass design review.

1. **Protocol source unification:** generate Java/C++ boundary messages from one
   v2 tree; add descriptor-hash handshake and CI parity gate. No retrieval
   behavior change in this step.
2. **Typed physical plan:** emit generic retrieval-source and retrieval-merge
   Substrait extensions plus plan-node keyed artifact bindings. Add golden plans.
3. **Native local retrieval:** implement Native candidate schema, vector/BM25
   source operators, manifest loader, exact fallback, and coverage footers.
4. **Native distributed merge:** implement tree Top-K, global rank, RRF, and
   linear normalization over normal exchange with spill/backpressure.
5. **Materialization and model rerank:** keyed row fetch and governed batched
   cross-encoder/cascade stages.
6. **Remove Java data path:** delete Java search interception, special result
   buffer/endpoint, Coordinator candidate merger, and Vector-specific protocol.
7. **Lifecycle completion:** publish atomic index-generation and BM25 corpus
   manifests; complete CDC/refresh/GC fencing.

Mandatory acceptance for the migration includes:

- canonical descriptor hashes equal in Java and C++;
- no candidate deserialization in Coordinator production sources;
- loss of one shard fails under default policy;
- one/many Worker results equal the offline oracle for exact retrieval;
- RRF and linear results are invariant under shard repartitioning;
- BM25 results are invariant under file repartitioning for one snapshot;
- mixed snapshot/generation inputs fail before producing rows;
- bounded memory with spill under high fan-in and large candidate budgets;
- rolling protocol mismatch fails admission, never during execution;
- public SQL -> planner -> canonical protocol -> JNI -> Native operators ->
  exchange -> result transport E2E passes with the packaged Native library.

## 15. HNSW COSINE implementation gate (not yet implemented)

The current durable Native HNSW path is **L2-only**. SQL supports exact
`COSINE_DISTANCE`, but that does not imply COSINE index support. The blockers
are concrete: `DurableIndexBuildService.requireL2Metric` rejects COSINE;
`RetrievalIntentCollector` emits an artifact intent only for L2 and writes a
hard-coded `metric=L2`; Native build conversion accepts exactly three HNSW
properties; `HnswIndex` builds and searches with L2; and
`ArtifactRetrievalPlanner` rejects non-L2 options and rescoring uses L2. The
HNSW v1 graph header has no metric, so relabeling an L2 graph as COSINE is
invalid. Keep IVF-PQ and DiskANN COSINE requests rejected until their own
end-to-end implementations exist.

### 15.1 Contract and execution

- Canonicalize index metrics at DDL: `EUCLIDEAN` becomes `L2`, an omitted metric
  becomes explicit `L2`, and only `L2` or `COSINE` is valid for HNSW. Persist
  the canonical metric in the descriptor, durable build properties, immutable
  artifact binding, and HNSW graph header. An unrecognized metric fails before
  a build is submitted; a query/index/artifact metric mismatch fails before
  candidate production. Planner index matching must use the same canonical
  values.
- Extend the *generic* capability invocation and artifact-format references,
  not a Vector-specific protobuf. Bump the HNSW build/retrieval capability
  signatures and HNSW artifact format to v2. The Native converter validates the
  exact typed property and option sets, and the Worker advertises the new
  descriptors only when the implementation is registered. Do not accept an
  L2-only artifact under a COSINE plan.
- Keep graph vectors and the `RAW_VECTORS` sidecar as original finite FLOAT32
  values. Make graph construction, greedy traversal, layer search, pruning,
  query option validation, and candidate exact rescore dispatch on the pinned
  metric. The initial COSINE implementation uses the existing Native
  `cosineDistance` kernel; it does not normalize stored values. This preserves
  the SQL distance contract for zero vectors: if either operand has zero norm,
  distance is `1.0` (including zero versus zero). Subsequent normalized-vector
  optimization requires explicit zero flags and must retain that behavior.
- Pass the metric from the planned `VectorSearchNode` into the Coordinator's
  retrieval intent and typed Native invocation. The exact rescore uses the
  original sidecar, not an approximate graph score. Keep bounded candidates,
  per-source snapshot/generation binding, row-locator verification, Native
  distributed merge, and default complete-coverage policy unchanged.

### 15.2 v1 artifact cutover, without compatibility shims

The repository forbids long-lived backward-compatibility shims. There is no
source-level evidence that deployed stores contain no HNSW v1 artifacts, so
release preparation must inventory the durable artifact store and READY index
generations by `ArtifactFormatRef` and graph format. If the inventory is empty,
v2 can be the sole reader/writer. If v1 generations exist, use a controlled
cutover: stop new v1 builds, invalidate v1 HNSW generations for new query
leases, submit fenced durable rebuilds from pinned source snapshots, and admit
indexed queries only after a v2 manifest is atomically READY. Existing query
leases must drain before v1 GC. A missing v2 generation is not silently served
by an incompatible artifact or reported as a successful ANN query; any exact
fallback must be explicitly permitted and observable under the normal
retrieval policy. The v2 runtime contains one graph parser, with no v1
version-sniffing branch. Cutover is blocked if the inventory/rebuild/lease
checks cannot be completed.

### 15.3 Acceptance matrix

| Boundary | Executable acceptance |
|---|---|
| DDL and planner | HNSW `metric=COSINE` submits a durable build; default/EUCLIDEAN are stored as L2; unknown metrics and COSINE IVF-PQ/DiskANN fail closed; READY matching requires exact canonical metric. |
| Canonical capability/protocol | Java and C++ descriptor hashes and HNSW v2 format reference match; build properties and query options carry one typed metric; mismatches, missing fields, and v1 formats fail before candidate output. |
| Native build and persistence | Non-unit, opposite-direction, and zero vectors build, save, reload, and return the same COSINE candidate order; corrupt header/metric/checksum rejects before allocation or serving. |
| Native retrieval | `ef_search` bounds, graph metric, query metric, binding metric, and dimension are checked; exact rescore uses `cosineDistance` on raw sidecar vectors and deterministic row-locator ordering. |
| Distributed SQL E2E | Public JDBC `CREATE INDEX ... HNSW metric=COSINE` reaches READY through the durable Worker build/publish path; packaged JNI query selects that generation, spans multiple splits/Workers, and returns the oracle order for a dataset whose L2 and COSINE orders differ. Assert snapshot, coverage, and no silent fallback. |
| Upgrade | Inventory v1 generations, test fail-closed admission before rebuild, durable v2 rebuild and atomic publish, old-lease drain/GC, and restart/retry; no v1 parser remains in the v2 runtime. |
