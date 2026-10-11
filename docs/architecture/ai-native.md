---
title: "AI-Native Architecture"
sidebar_label: "AI-Native Architecture"
---

# AI-Native Architecture

This document defines the production boundary for Pangrova AI SQL, model
invocation, generated values, vector and text retrieval, multimodal data, and
their governance. It describes executable architecture, not prospective class
sketches. Current implementation evidence and open admission gates are tracked
in [AI Native roadmap](../internal/ai-native-roadmap.md).

## Non-negotiable boundary

Pangrova has one control plane and one data plane:

```text
SQL/API
  -> Coordinator JVM: parse, authorize, resolve, plan, admit, schedule, fence
  -> canonical v2 TaskRequest
  -> Worker JVM: node lifecycle, JNI, governed provider callback
  -> C++ Native: scan, build, retrieve, fuse, rerank, fill, write, stream
  -> canonical TaskReport/result stream
  -> Coordinator JVM: validate state, publish metadata, expose endpoint
```

The Coordinator never receives candidate rows, query vectors, document bytes,
model batches, training batches, score arrays, or merge heaps. A singleton final
Top-K stage is still a Native Worker stage; "coordinator" in search literature
does not mean the Pangrova Coordinator process.

The Worker JVM is not a second query engine. It owns process-local lifecycle,
resource admission, JNI, secrets, policy enforcement, and provider networking.
All row-dependent computation remains in the Native plan. A Java implementation
must not intercept a task and return feature-specific results.

## One canonical protocol

All Java/Native boundaries use generated messages from
`pangrova-protocol/src/main/proto/pangrova/protocol/v2`. There is no vector,
embedding, text-search, or model-specific transport protocol.

The shared building blocks are:

| Contract | Purpose |
|---|---|
| `CapabilityRef` | Exact capability ID, semantic version, and signature hash |
| `CapabilityDescriptor` | Signature, execution mode, modalities, determinism, batching, retry, privileges, and limits |
| `OperatorBinding` | Typed operator parameters and required resources |
| `ModelBinding` | Exact model object/version/signature, output `LogicalType`, and invocation limits |
| `ArtifactBinding` | Immutable generation, source snapshot, format, object checksum, and row-locator contract |
| `ArtifactWriteBinding` | Durable build page identity, attempt, fence, staging target, and output contract |
| `ResourceGrant` | Memory, spill, candidates, provider calls/tokens/media, object-store I/O, and deadline limits |
| `ExecutionPolicy` | Completeness, retry, fallback, freshness, and approximation policy |
| `TaskReport` | Attempt identity, output endpoint, coverage, artifacts, usage, and classified failure |

`SubmitTaskRequest` is only an encoded canonical `TaskRequest` envelope. The
Worker validates it and forwards the exact bytes to Native. REST DTO fields do
not mirror or reconstruct protocol state.

Protocol source ownership is enforced by the descriptor gate and JNI symbol
symmetry gate. Generated sources are never edited manually.

## Capability publication and admission

A capability is advertised only after its implementation and dependencies have
initialized successfully. Human-readable names are insufficient: planner,
Worker, and Native must agree on ID, version, signature, execution mode, format
schema, and hard limits.

Admission is fail-closed:

1. The Coordinator resolves catalog objects and immutable versions.
2. It selects Workers whose canonical inventory contains every required exact
   capability and compatible artifact format.
3. The Worker validates task identity, ownership epoch, resource grant, model
   binding, and artifact binding before JNI execution.
4. Native validates the same bindings before opening data or allocating an
   operator.

Unknown relations, capabilities, options, formats, model versions, or protocol
epochs fail before data-dependent work. There is no compatibility shim, version
sniffing path, or silent implementation substitution.

## AI SQL and model invocation

AI SQL expressions compile into ordinary Substrait expressions. Native batches
their inputs and invokes the process-local Worker callback. The callback is the
only provider egress path:

```text
Native batch
  -> exact ModelBinding
  -> authorize capability/model/principal
  -> apply input and egress policy
  -> reserve concurrency/request/token/media execution limits
  -> provider request with deadline, cancellation, and idempotency key
  -> validate output count/order/schema/model identity
  -> reconcile provider-reported usage
  -> audit and trace
  -> Native output vector
```

Model names are not provider endpoints. A configured model has one exact
provider model ID, version, endpoint, secret reference, signature, modalities,
resource limits, and lifecycle state. Unknown, disabled, unauthorized,
incompatible, over-budget, or secret-resolution failures cause zero provider
requests.

Batch results must preserve item order and cardinality. A provider cannot return
a partial batch as success. Embeddings additionally validate finite values and
the exact declared dimension before any row is made visible.

Model output types use the same canonical `LogicalType` carried by every other
operator binding. `AI_EMBED` is therefore bound as `VECTOR(n)` (a required FP32
list plus an exact dimension constraint), not as a vector-specific task message
or an unbounded array. Planner, Worker advertisement, Coordinator admission,
JNI/Native callback and the provider-result validator compare that same type.

Provider credentials and prompt/file payloads never enter Coordinator memory or
the canonical plan. Per-invocation results and audit records retain actual
provider usage and request identity; generic Native task reports carry physical
resource usage, not a separate LLM token/fee ledger or sensitive content.

## Generated values and embeddings

Generated embedding columns use a durable artifact workflow. `ASYNC` is the only
accepted mode until Native can compute a generated value inside the same atomic
table-write transaction; unsupported `SYNC` DDL is rejected instead of creating
an unwritable table.

```text
DDL metadata
  -> DML admission verifies exact model/materializer/format capabilities
  -> connector durable commit log
  -> maintenance leader creates immutable-snapshot ArtifactBuild pages
  -> Native generated-values build operator
  -> fenced TaskReport with generated-value and row-locator objects
  -> atomic generation publication
  -> query resolves source-object-scoped ArtifactBinding
  -> Native scan overlay joins by physical row position
```

Build jobs and pages are persistent, leased, retryable, idempotent, cancellable,
and fenced. Publication uses target generation plus exact source snapshot; a late
attempt may never replace a newer generation. Missing, duplicate, mismatched, or
wrong-snapshot overlay rows fail the task.

Generated-value materialization is wired only on a coordinator with a local
indexing bootstrap that can register the durable commit processor. A query-only
coordinator can start with indexing flags present, but does not create a dormant
materialization service or claim that it can process committed writes.

The generated segment and its row-locator object are immutable and checksummed.
The Coordinator handles their metadata only and never joins the values to rows.
Native derives each durable artifact object ID as a bounded SHA-256 identity over
the artifact, generation, source scope, role, and content checksum. The full
source-object ID remains a separate field, so long connector paths do not
overflow the metadata primary key or lose source-snapshot provenance.
Publication retains the checksummed build split bytes. Query attachment verifies
the same immutable connector, source snapshot, object, location and physical
range identity; query-only scan predicates and schema detail may differ.

The complete diagram describes the required contract, not a blanket capability
acceptance result. A focused SQL ASYNC regression has exercised authentic commit events,
durable queue consumption, Native generated-value/row-locator reports, atomic
publication, and a final public SQL SELECT against the actual provider result;
the complete E2E suite and release-scale qualification remain pending. Production query wiring now inserts
the exact generated binding at the physical READ, restores the complete scan schema
before scan filters, and preserves downstream filter/mask/aggregate ordinals.
Task-local objects still use the common checksummed artifact binding path.
Protected historical SDK schema verification is shared with relational exports.
FULL reads use the actual current SDK schema and declaration, while a declared
time-travel read uses its exact pinned declaration even when current generated
columns have been removed. `ConnectorTableReadMetadata` distinguishes `EXACT`
declaration evidence (including proven absence) from `UNAVAILABLE`; unavailable
history is an explicit unsupported operation, never an empty declaration.
Paimon uses the snapshot's schema ID and its versioned options. Pangrova Iceberg
commits store the existing canonical generated-property pair (including explicit
absence) and only logical-type properties in immutable snapshot summaries.
Third-party Iceberg snapshots can instead use the exact adjacent metadata-file
boundary where that snapshot first appeared, checking table UUID, location,
schema and snapshot identity. Expired, missing, cyclic or inconsistent metadata
history cannot be replaced by current properties; time travel then requires
retained declaration evidence. These SDK/PG regressions do not by themselves
establish public SQL overlay visibility.
Distributed Iceberg write handles now capture the actual SDK UUID, location,
full schema, partition specification, format version and canonical generated/vector
declaration in one immutable metadata observation. The sole output-handle codec
preserves this observation inside the journaled write-binding fingerprint. Every
SDK base before publication is checked against that declaration; a data-only
metadata revision may rebase, but a schema/spec/declaration change cannot. The
commit then uses the backend's complete-metadata CAS, not a check followed by an
unfenced append. Currently admitted backends are the exact SDK JDBC catalog and
verified local-file Hadoop operations; other backends explicitly reject this
write capability. Successful CAS is a known publication: later SDK cleanup
refreshes cannot revoke it, and the guarded operations cannot commit again.
The compiled mutation retains its planning-time `ConnectorTableMetadata`, rather
than querying a newer declaration and calling that compilation evidence. The
protected-plan observation carries its ordered SPI schema and canonical generated
declaration digest through admission; both the current connector observation and
the returned authorization receipt must match it before allocation. An unbacked
planner table is unknown and cannot authorize a production write. This SPI-level
comparison does not prove SDK constraints absent from the SPI, such as nullability;
the allocation-to-commit fence separately compares the full frozen SDK metadata.
Paimon generated targets are rejected before write allocation because its SDK
catalog lock expires without an owner/fencing token. Ordinary Paimon DML remains
available, but its schema/declaration-versus-snapshot atomic fence is not accepted:
a true shared SDK/catalog commit gate is still required, not a longer lock timeout
or a Pangrova-only PostgreSQL check. SYNC generated DDL remains rejected.
The shared SQL/AST Planner mutation conversion now injects typed `AI_EMBED`
expressions from its frozen canonical declaration before masks and row filters.
INSERT uses NEW values; affected UPDATE/MERGE assignments use simultaneous NEW
values, while the existing COW action branches preserve old vectors for physical
survivors. DELETE and updates that do not assign a declared source invoke no model.
Explicit writes to generated targets, including NULL placeholders, are rejected.
This is preparation for SYNC, not its activation: DDL and admission still reject
SYNC pending actual Worker/SDK write-commit acceptance. Both inline and durable
expressions share ordered unit-separator concatenation and strict NULL propagation.
The generated-value sink and scan overlay use one segment-v2 contract: a
LSB-first validity bitmap, dense little-endian FP32 payload, and an independent
physical-row locator for every row, including NULL. NULL payload slots are
canonical positive zero but are never exposed as a zero embedding: the overlay
restores the array's NULL bit. Readers reject old formats, nonzero padding and
invented NULL payload; Java/Native build and overlay capabilities both advertise
version 2.0.0 and the same exact format fingerprint. Dense materialization is
checked against the memory grant before allocation. Native component acceptance
alone does not imply broad public SQL/provider end-to-end qualification; focused
generated-value SQL acceptance has passed.
A syntactic source assignment may recompute even if its value is equal.
Lance dataset scans now request the SDK row-address metadata through the canonical
manifest-partition read unit. Each address is checked against the exact fragment
and physical row bounds; deletion holes retain their original offsets and cannot
be replaced by a contiguous batch counter. Native/Rust tests and matching-ASan
regression have run, but the final public SQL query path still requires acceptance;
these are not distributed deployment proof.
Dataset partitions must not be disguised as physical-file byte ranges. External SDK declaration
CAS at commit remains a separate, unclosed requirement from PostgreSQL
current-authority fencing; the latter cannot lock an external SDK metadata commit.

## VECTOR, ANN, BM25, and immutable artifacts

`VECTOR` is a first-class typed value with a declared element type and dimension.
Exact distance executes with Native SIMD kernels. HNSW, IVF-PQ, DiskANN, BM25,
and Lance storage are Native-only implementations behind the same artifact and
capability contracts.

Index DDL creates durable work rather than a JVM index object:

1. Resolve the typed column and algorithm to an exact build capability and
   immutable format.
2. Pin one source snapshot and enumerate bounded work pages.
3. Lease pages to capable Workers through normal task scheduling.
4. Build and validate source-object artifacts in Native.
5. Reduce/finalize where the algorithm requires global state.
6. Validate every report, checksum, row locator, and expected output.
7. Publish the complete generation through one fenced atomic transition.

Queries bind one published generation for their lifetime. The Native artifact
cache is keyed by immutable identity and verifies size/checksum/format before
use. Lance is a mandatory packaged dependency and initialization failure prevents
the Worker from advertising compatible capabilities. The production read path
resolves physical projection from the dataset schema and applies rename, widening,
missing-column/default, and partition evolution in the Native connector. It does
not serialize a Velox expression string into Lance SQL; exact residual filtering
remains a typed Native plan node.

`CREATE INDEX` submits an asynchronous generation; `ALTER INDEX ... REBUILD`
requires the initial generation to reach `READY` (or a later generation to be
`STALE`). Live distributed queries bind each exchange consumer to the exact
canonical output schema of its dispatched producer plan. All producer partitions
must agree, and the consumer's declared types must match before the plan is
dispatched. SQL result labels remain separate from these internal wire names.
Retrieval scans may append only the four fixed physical source-row identity
fields to the exact declared table schema, in canonical order and with required
types; generated-value reads reject any other schema difference.

The C++ Lance writer is currently a verified storage primitive, not a table-write
claim. Canonical v2 defines the requested write format, but the public SQL write
dispatcher currently fixes it to Parquet and the task contract does not carry the
connector-allocated output URI, partition route, or external-file commit fragment
needed for a Native writer followed by a metadata-only JVM commit. A directly
submitted `FILE_FORMAT_LANCE` task therefore fails closed before a connector page
sink opens; a table property must not be treated as proof of a Lance write.

JVM file-pruning indexes remain a separate family: they only evaluate small
per-file metadata and cannot build or search ANN/BM25 retrieval artifacts.

## Distributed retrieval

The planner emits typed retrieval source, merge, rerank, and fill relations. The
JVM schedules their stages but does not execute their row semantics.

```text
source shards
  -> Native local bounded candidates
  -> ordinary columnar exchange
  -> one or more Native bounded reducers
  -> Native global fusion/rerank
  -> Native candidate fill
  -> normal result stream
```

Candidate rows carry stable row identity, score value, score direction, source
identity, rank, snapshot, generation, and coverage metadata. `source_id` is an
opaque identity; semantic type comes from the exact capability, not a string
prefix.

RRF, calibrated linear fusion, cross-encoder reranking, and cascades are explicit
typed operators. Score direction is never inferred from a field called
`distance`. Tie ordering is deterministic from stable keys.

Reducer fan-in, candidate rows/bytes, oversampling, rescore window, provider
tokens, memory, and spill are granted limits. Large fan-in uses a reduction tree;
each node retains bounded state and spills sorted runs under pressure.

## Completeness, fallback, and freshness

A successful retrieval has an explicit state:

- `COMPLETE`: all required source partitions at the pinned snapshot contributed.
- `PARTIAL`: permitted only by an explicit query policy and accompanied by exact
  expected/responded coverage and failed-partition metadata.
- `FALLBACK_EXACT`: every uncovered row was scanned at the same snapshot and the
  result is equivalent to the exact oracle.

The default policy rejects incomplete Top-K. Timeout, corrupt artifact, missing
partition, stale generation, checksum mismatch, or reducer failure cannot be
converted into an empty candidate list. Approximation profile, index freshness,
source snapshot, and active generation are visible in reports and audit.

## FILE/BLOB and multimodal data

`FILE` represents a governed immutable object reference, never arbitrary
unbounded inline bytes. A valid reference includes opaque object identity,
version, storage scope, MIME type, size, SHA-256 checksum, owner/tenant policy,
and creation metadata.

Creation must persist and verify bytes before returning the reference. External
imports require scheme/host allowlists, DNS and redirect revalidation, private
address denial, bounded connect/read deadlines, content-length and streamed byte
limits, MIME sniffing, checksum validation, encryption, audit, and orphan cleanup.

`GovernedBlobStore` owns this single reference and integrity contract; durable
storage is supplied by `BlobStorage`. Distributed production explicitly selects
the S3-compatible backend. Object keys contain only opaque tenant scope and
SHA-256 identity; conditional PUT provides create-if-absent publication, and
HEAD proves size, checksum metadata, SSE mode and KMS key before a reference is
returned. Read uses exact GET without namespace listing. A Worker-local staging
file is forced before upload and removed on success or failure; bucket lifecycle
policy governs immutable-object retention. The filesystem backend exists only
for deterministic contract tests and is never selected by production config.

Foreground FILE callbacks consume the canonical task's media-byte grant. The
Coordinator supplies a FILE baseline of at most 64 MiB per task, capped by
available task memory; a larger governed model binding may raise the shared
grant. The baseline survives model-operator resource aggregation. Before
publishing each object, the Worker debits its actual staged byte count against
the task's cumulative media ledger; an exhausted grant cannot publish another
object. The FILE function's per-object limit and storage policy remain
additional bounds.
The internal Substrait/Native output schema retains physical field identities,
while Flight uses the SQL root's depth-first names for returned nested FILE
fields. Result rebinding changes schema metadata without copying payload bytes.

Reference capabilities are derived per tenant from one cluster key obtained via
an external secret reference at Worker startup. Workers neither generate their
own signing key nor persist key material beside objects, so a reference created
on one Worker verifies on every Worker configured for the same fleet. Missing,
short or non-base64 key material fails startup.

Workers resolve every allowlisted HTTPS host once, reject the entire answer set
when any address is non-public, and pass the validated addresses into the transport.
The pooled transport connects to that exact address while retaining the original
host for the HTTP authority, TLS SNI and certificate verification; it never performs
a second DNS lookup. Pool size, per-route concurrency, connection lifetime and idle
eviction are bounded by `file.url.max-connections`,
`file.url.max-connections-per-route`, `file.url.connection-ttl-ms` and
`file.url.idle-eviction-ms`. Redirects and automatic retries remain disabled.

Provider egress resolves a reference to a provider-dialect-specific input through
a bounded, permission-checked and checksum-verified stream or short-lived URL.
Internal storage URIs are never copied directly into provider payloads. The
chat-completions dialect accepts only TEXT/IMAGE and emits a verified image data
URL. The `openai-responses` dialect emits verified `input_image`/`input_file`
data URLs for IMAGE/DOCUMENT. The `openai-transcription` dialect uploads exactly
one verified AUDIO FILE as multipart `/v1/audio/transcriptions`; it never routes
audio through chat or Responses.
If the exact dialect and governed content path are unavailable, that modality is
not advertised and admission fails.

## Durable dataset and feature infrastructure

Feature definitions and training datasets are catalog objects, not in-process
maps or Coordinator scanners. A feature definition owns entity keys, event time,
typed output schema, source query, transformation hash, freshness, lineage, and
privileges. Offline materialization is a Native distributed job over pinned
snapshots; point-in-time joins prevent future-data leakage. Online publication,
when configured, uses an atomic version plus CDC watermark.

A dataset generation stores its normalized plan, all source snapshots, feature
versions, labels, deterministic split specification and seed, schema, lineage,
and immutable output manifest. Native Workers scan and export it. Streaming reads
consume published generations from the data plane; the Coordinator neither scans
rows nor serves batches.

The current production slice is narrower than this target: tenant-scoped Feature
View create/publish/materialize supports a single source pinned to one snapshot
and submits a canonical Native `TABULAR_EXPORT` durable artifact job. Creation of
the catalog export link, job, dispatch spec, and pages is one JDBC transaction;
zero-row input publishes a schema-bearing, checksummed empty object and manifest.
Point-in-time multi-source joins, online serving, and CDC watermark publication
are not complete, so they are not advertised.

Training Dataset currently has catalog records only. It has no SQL entry, durable
multi-source export wiring, Native generation publication, or streaming reader;
therefore no Training Dataset execution capability is advertised. Placeholder
contracts and component-only tests are not completion evidence.

## Cache, optimization, governance, and evaluation

Semantic reuse is non-authoritative. Its partition key includes tenant,
principal/policy hash, capability, exact model version, normalized input,
locale, safety policy, source snapshots, and artifact generations. Similarity is
searched only inside that authorized partition. Partial, nondeterministic,
side-effecting, unknown-policy, and empty results are not cacheable.

Automatic optimization produces durable proposals with evidence, expected
benefit, resource cost, quality profile, shadow result, and rollback condition.
Only a fenced, evaluated proposal may schedule the same durable Native artifact
workflow used by manual operations.

The target Evaluation service uses immutable, version-bound datasets and runs.
The current implementation is limited to durable metadata: dataset versions,
version-pinned run records, atomic terminal metrics, and one release decision.
A candidate pinned to a baseline cannot be approved unless it covers every
baseline metric with the same unit and all gate results pass. Candidate-only
absolute-policy metrics are permitted. There is not yet a
canonical Native Evaluation capability or a fenced terminal metric report, so
no production run submission is advertised and the metadata store must not be
treated as an executable Evaluation service. The eventual promotion gate covers
output validity, recall/NDCG/MRR, safety, latency, provider errors, resource use,
and cost; sensitive prompt/result access remains distinct from aggregate
telemetry access.

## Scale and failure invariants

- Every queue, batch, buffer, candidate set, merge heap, cache, and artifact read
  has an admission-derived bound.
- Foreground query, serving, and background build/export capacity are isolated.
- One query/job owner and fencing epoch guard every externally visible mutation.
- Retries use stable work identity and new attempt identity; stale attempts are
  unable to publish.
- Worker loss after shuffle production either recovers from explicitly durable
  output or fails; it never silently drops coverage.
- Adaptive scheduling may change topology and fan-in, but not snapshot, model or
  artifact version, policy, score semantics, or completeness requirements.
- Cancellation and deadlines propagate through exchange, Native operators, JNI,
  provider calls, object storage, and publication.
- No lock is held during provider, network, or object-store I/O.

## Required verification

Production completion requires all of the following:

1. canonical descriptor and JNI symbol symmetry gates;
2. mandatory Lance build and Release Native build;
3. all Native tests, including artifact corruption, coverage, cancellation,
   resource bounds, and distributed equivalence;
4. ASan with no finding;
5. packaged Native library and Java loader/JNI tests;
6. full Java reactor compile and tests on JDK 17;
7. public-entry Testcontainers E2E using PostgreSQL and Ollama where applicable;
8. smoke tests for SQL-to-Native-to-result connectivity;
9. scale, chaos, security, and soak qualification for release claims.

Skipped, assumed, constant-true, component-direct, or "runtime unavailable is
acceptable" tests are not end-to-end evidence.
