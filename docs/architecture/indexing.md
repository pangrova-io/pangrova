# Indexing Architecture

## Scope and execution boundary

Pangrova has two deliberately separate index families:

- JVM file-pruning indexes (`BTREE`, bloom, bitmap, sparse, R-tree, GIN and
  inverted metadata) implement `IndexStrategyProvider`. They produce small
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

- `pangrova-indexing` owns index persistence and evaluation:
  `org.pangrova.indexing.store` and `org.pangrova.indexing.store.jdbc` hold the
  descriptor, commit queue, generation, publication-fencing and GC persistence;
  `org.pangrova.indexing.core.*`, `org.pangrova.indexing.engine.*` and
  `org.pangrova.indexing.eval.*` hold build/evaluation logic and the per-engine
  providers. `pangrova-metadata` does not own index storage.
- `pangrova-coordinator` owns only the control-plane adapter under
  `org.pangrova.coordinator.adapter.indexing` (DDL, filter, lifecycle/GC and the
  AI retrieval-index lifecycle). It depends on indexing contracts and store
  ports; it never owns a second index metadata store.
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

## JVM indexing SPI

`IndexStrategyProvider` is intentionally limited to build/evaluate operations
for JVM file-pruning metadata. It has no vector-search method. The default
registry assembled by `assembly.serde.DefaultTypeSerializers.createRegistry()`
likewise has no `VECTOR` serializer because Native retrieval artifacts are not
embedded in the JVM index-file envelope. `core.serde.TypeSerializerRegistry`
only owns registration and lookup; it does not import concrete engine serializers.
Duplicate registration fails without replacing the existing serializer.

This boundary prevents accidental ServiceLoader registration from creating a
second ANN implementation with different distance, row-identity, filtering or
failure semantics.

## Scalar file-pruning query path

The Coordinator evaluates connector file statistics and native connector
capabilities, then attaches snapshot-bound `IndexEvalHintDetail` values to
surviving canonical v2 file splits. It does not load scalar index artifacts.
The Native Worker validates the hint's data-file identity, reads the mapped
artifact through its configured filesystem, checks the versioned index envelope
and CRC, and tests Bloom membership **before** enqueuing the split for scan.
Only a proven negative result skips a split; missing, corrupt, incompatible or
unsupported indexes retain it. Equality and `IN` on canonical string,
integer and boolean keys currently participate in this Native Bloom path.
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
