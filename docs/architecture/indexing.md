---
title: "Indexing Architecture"
description: "Index contracts, build and maintenance paths, and pruning boundaries"
---

# Indexing Architecture

> **Accuracy scope.** This is a living architecture document. JVM file-pruning
> indexes and durable Native retrieval artifacts are implemented paths; lifecycle,
> provider, and maintenance items explicitly marked remaining are target design.

## Scope and execution boundary

Pangrova has two deliberately separate index families:

- JVM file-pruning indexes (B-tree, Bloom, prefix, N-gram Bloom, bitmap, hash,
  sparse, R-tree and GIN) implement `IndexStrategyProvider`. They produce small
  per-file metadata used to decide whether a split can be skipped.
- Retrieval indexes (HNSW, IVF-PQ, DiskANN and BM25) are immutable durable
  artifacts. Their build and query data plane runs only in the C++ Native
  worker.

The Coordinator is a control plane. It resolves metadata, chooses a canonical
Native capability, pins a source snapshot, creates durable work pages and
dispatches ordinary canonical v2 tasks. It never builds, loads or searches an
ANN graph in the JVM.

## Code ownership

Index state and index orchestration have separate owners:

- `pangrova-indexing` separates API/SPI, common Codec, builtin strategies and Core
  orchestration (see `pangrova-indexing/README.md`). Persistence ports and their
  immutable values live in `pangrova-persistence-indexing-api`; existing SQL lives
  in `pangrova-persistence-indexing-postgresql`. Core depends on the ports, not the
  PostgreSQL implementation. Metadata Core does not own index storage.
- Server supplies the JDBC `CurrentTableAuthorization` participant explicitly.
  Publication invokes it on the same transaction connection at the existing lock
  boundary. The participant may neither commit nor close that connection. The
  PostgreSQL production artifact does not depend on Metadata or Indexing Core.
  Narrower persistence ports and removal of permissive optional defaults remain
  separate work; moving the existing broad port does not complete that redesign.
- Planner derives scalar predicates with one physical scan as their provenance.
  Self-joins and unions retain their full split sets when provenance is ambiguous.
  Coordinator looks up READY descriptors for the bound tenant and matching columns.
  Indexing evaluates predicate compatibility and applies pruning.
- `org.pangrova.coordinator.indexing` handles SQL entry, split dispatch and
  lifecycle integration. Vector search passes the compiled query to the indexing
  rewriter, which inspects the Calcite plan inside the indexing module.
- Index generation has exactly one owner: the indexing store allocates and
  fences generations, and the coordinator adapter may only advance them through
  that store.

## Canonical retrieval contract

Every retrieval algorithm is identified by the existing generic protocol
objects:

- `CapabilityRef` identifies the exact build or retrieval semantics, version
  and signature.
- `ArtifactFormatRef` identifies the immutable binary format and Native writer
  build.
- `ArtifactWriteBinding` fences a build work page and its staging output.
- `ArtifactBinding` binds a published generation and its source-object-scoped
  objects to a query task.
- `RowLocatorContract` defines how candidate ordinals map back to source rows.
- `ExecutionPolicy` defines coverage, retry and fallback policy.

There is no vector-specific task, heartbeat, result HTTP endpoint or Java
serialization format. Algorithm names in SQL catalog properties are resolved
fail-closed to canonical references from `BuiltInCapabilityCatalog`; unknown
names are rejected before dispatch.

## Durable build flow

1. Index DDL stores an `IndexDescriptor` in `BUILDING` state.
2. `DurableIndexBuildService` validates the indexed column and resolves the
   declared algorithm to a canonical build `CapabilityRef` and
   `ArtifactFormatRef`.
3. The service pins an immutable table snapshot, enumerates source splits and
   persists source-map, reduction and finalization pages in `GenerationJobStore`.
4. `DurableGenerationDispatcher` leases pages and dispatches normal v2
   `TaskRequest` objects with an `ArtifactWriteBinding`.
5. A Native worker verifies capability, format, fencing token and resource
   grant before building an immutable artifact object.
6. The Coordinator validates task reports and checksums, then atomically
   publishes the complete generation. Failed or superseded attempts cannot
   publish.

Retries reuse stable work-unit identities while attempt IDs and fencing tokens
advance. No in-memory build state is authoritative.

Incremental index maintenance observes lineage before opening the snapshot file inventories. For a forward commit, the Connector uses one bound table observation, reads each immutable inventory once and derives the file delta from the same target inventory used for coverage validation. Replayed or superseded snapshots require only the lineage decision.

## Query flow

1. The planner recognizes `ORDER BY <distance>(vector, literal) LIMIT k` and
   selects only a `READY` index whose column and metric match.
2. The plan carries the canonical Native retrieval `CapabilityRef`, a bounded
   search budget and the index generation identity.
3. The Coordinator resolves that generation through
   `PublishedGenerationBindingResolver`, checks source-snapshot coverage and
   materializes source-object-scoped `ArtifactBinding` objects for each task.
4. Normal distributed task routing selects workers advertising the exact
   capability and compatible artifact format.
5. Native operators load the immutable artifact, execute local retrieval and
   emit canonical candidate rows. Native merge operators produce the global
   bounded Top-K.

An unindexed SQL distance query remains an ordinary Native Substrait scan/sort.
It does not enter a Java brute-force implementation. Missing, stale,
incompatible or incomplete artifacts follow the explicit `ExecutionPolicy`;
they never silently return an empty or partial Top-K.

## Algorithm and format identities

The built-in catalog currently defines these independent contracts:

| SQL algorithm | Build capability | Retrieval capability | Artifact format |
|---|---|---|---|
| HNSW | `pangrova.artifact.build.hnsw` | `pangrova.retrieval.vector.hnsw` | `pangrova.hnsw.segment` |
| IVF-PQ | `pangrova.artifact.build.ivf_pq` | `pangrova.retrieval.vector.ivf_pq` | `pangrova.ivfpq.segment` |
| DiskANN | `pangrova.artifact.build.diskann` | `pangrova.retrieval.vector.diskann` | `pangrova.diskann.segment` |
| BM25 | `pangrova.artifact.build.bm25` | `pangrova.retrieval.text.bm25` | `pangrova.bm25.segment` |

Capability signatures and format schema hashes are compatibility boundaries;
matching a human-readable algorithm name is insufficient.

## Scalar build attempts

Each scalar build submission owns a UUID. The claim owner and the physical file
path both carry that identity. A writer uses
`v3/{catalog}/{schema}/{table}/{indexId}/g{generation}/{attemptId}/{dataFileHash}.idx`
relative to the index root. Reads and mapped-file cleanup use the stored path.
A delayed write by an expired owner can finish in its own directory after takeover
without changing the replacement’s bytes. Collecting files left by an attempt
that lost ownership before recording its mapping requires separate orphan tracking.

## JVM encoded-data ownership

Indexing API owns encoded index data and pruning hints. Bloom and N-gram arrays,
bitmap payload snapshots, bounding-box coordinate snapshots and row-group BitSet
snapshots cannot mutate their owning object. Bloom evaluation uses independent
read-only ByteBuffer views; Common decodes those views into owned words without
retaining an input alias. Bitmap membership and R-tree coordinate access use
focused methods, avoiding bulk copies in predicate evaluation and tree building.
Serialization may request owned snapshots. The binary layouts are unchanged.

This boundary does not yet establish immutability of arbitrary Comparable keys
or runtime wiring of every statistics DTO. The verified scope and remaining work
are bounded by the current Codec and evaluation tests.

Complete JVM BTree tuple keys now use `CompositeIndexKey` in Indexing API rather
than a Provider-private class. The generic comparable codec owns tag 11: a bounded
arity followed by typed scalar components. Keys require 2–1024 non-null components
of the exact supported immutable scalar classes (String, boxed numeric primitives,
BigDecimal, BigInteger and UUID). Nested tuples, mutable custom Comparable values
and incompatible comparison shapes are rejected. Existing scalar tags and the
common index envelope are unchanged; no historical data migration is introduced.

The builder skips rows with a null key component. Its payload does not prove
null-row coverage or the arity of an empty index. Consequently an empty key set
cannot justify `SKIP`, and a scalar predicate is not a complete tuple predicate.
Java tuple membership is verified separately from SQL execution: the current
Native hint consumer only prunes supported Bloom artifacts, so successful BTree
publication and correct SQL results do not prove BTree split pruning.

## JVM indexing SPI

`IndexEngineRegistry` discovers the nine scalar strategies with ServiceLoader. Each
`IndexStrategyProvider` supplies the serializer for the raw data returned by its
builder. Server constructs the file codec from the same provider instances used
for building and evaluation. A duplicate type, a missing payload codec or an empty
production registry fails initialization. The common artifact envelope, checksum
and stable type IDs belong to Indexing Codec. The native VECTOR and INVERTED/BM25
paths use their own durable artifact formats.

## Scalar file-pruning query path

Predicate analysis retains the selected immutable index descriptor. Split evaluation uses it alongside the exact active artifact generation and selected source snapshot. It reads published file mappings in batches and reuses the selected descriptor for availability checks and Worker hints.

The Coordinator evaluates connector file statistics and native connector
capabilities, then attaches snapshot-bound `IndexEvalHintDetail` values to
surviving canonical v2 file splits. It does not load scalar index artifacts.
The Native Worker validates the hint's data-file identity, reads the mapped
artifact through its configured filesystem, and verifies its exact byte length and
SHA-256 against the published mapping. It checks the envelope version and CRC
on those same bytes and tests Bloom membership before enqueuing the split for scan.
Only a proven negative result skips a split; missing, corrupt, incompatible or
unsupported indexes retain it. Equality and `IN` on canonical string,
integer and boolean keys currently participate in this Native Bloom path.
Bitmap has a JVM strategy and a typed key-to-Roaring payload. Worker scalar
file pruning recognizes Bloom. BSI has no registered scalar strategy. Paimon
deletion vectors use their connector-owned row-deletion path.
Calcite's [string literal wrapper](https://calcite.apache.org/javadocAggregate/org/apache/calcite/util/NlsString.html)
literals are unwrapped with `getValue()` before creating a hint: its
`toString()` returns quoted SQL with charset notation, not the stored value. Native
membership is checked against a Java-generated positive/negative byte fixture.
DATE, TIMESTAMP, DECIMAL, floating-point keys and other index types retain the
split until their cross-language value and artifact contracts are verified.
Multi-scan plans do not receive scalar hints until predicate provenance is
bound to a specific scan, because matching columns by name across joined
tables is insufficient to justify pruning either table.

`NativeTaskStats.index_splits_evaluated` and `index_splits_skipped` report
actual Worker decisions. An index-performance claim must use these counters
or scan I/O, not the Coordinator's earlier split-filter counters or Iceberg's
own manifest pruning statistics. The E2E regression uses overlapping file
min/max ranges to distinguish Bloom pruning from connector pruning.
This placement follows [Velox's split queue contract](https://facebookincubator.github.io/velox/develop/task.html):
the application decides which splits to add before the scan operator reads
them. [Iceberg's planning statistics](https://iceberg.apache.org/docs/nightly/performance/)
can independently remove data files using upper and lower bounds, so those
connector decisions must be measured separately.

## Distributed correctness invariants

- Every query and build uses one immutable source snapshot.
- Index builds that enumerate the current files select the snapshot and read its
  file set through one connector observation. Iceberg opens the catalog table
  once. Explicit historical reads continue to use the requested snapshot.
- Artifact generations are immutable and atomically published.
- Every object checksum, format identity and row-locator schema is validated.
- Workers must advertise the exact capability signature and compatible format.
- Work-page leases and publication fencing reject stale attempts.
- Candidate and merge budgets are bounded by the task resource grant.
- Global Top-K completeness is explicit; partial results are never silently
  presented as complete.
- Coordinator restart recovery uses durable jobs and manifests, not JVM graph
  state.

## Test gates

Changes to this path require:

- planner tests for algorithm-to-capability resolution and bounded parameters;
- protocol descriptor and capability inventory gates;
- Coordinator tests for snapshot binding, page fencing, publication and worker
  capability matching;
- Native build/query tests for every advertised artifact format;
- packaged JNI end-to-end tests that prove ordinary task routing, distributed
  local retrieval and global Top-K merge;
- static checks that no Java ANN engine, vector ServiceLoader provider,
  vector-result HTTP endpoint or algorithm-specific transport contract exists.

Index state-update contracts must preserve the requested fence and stale reason. The
legacy unconditional overload is not an implementation of optimistic concurrency.
The default timestamp-CAS, state/generation-CAS and reason-preserving overloads reject
unsupported calls before mutation; they never delegate to a weaker update or return a
synthetic success. PostgreSQL compares and updates in one SQL statement. A successful
fenced STALE update persists its reason atomically; non-STALE transitions clear it in
the reason-aware operation. Lifecycle transition validation remains the domain owner's
responsibility. Other persistence ports and remaining optional defaults still require
the planned domain/backend split; this state-update correction does not complete that work.
