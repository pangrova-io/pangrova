# Architecture

Pangrova is a **hybrid C++ / Java distributed OLAP engine** using the JNI bridge pattern.
Java handles coordination and planning; C++ handles high-performance vectorized execution.

## Java Side (Maven, JDK 17)

| Module | Responsibility |
|--------|---------------|
| `pangrova-common` | Shared utilities and base types |
| `pangrova-config` | Configuration management |
| `pangrova-spi` | Shared domain/capability contracts for connectors, functions, indexing, observability, security, AI, queries, coordination and File control; no protocol or implementation dependency |
| `pangrova-filesystem/pangrova-filesystem-api` | Common/JDK-only blob/object transfer, locations, immutable profiles and provider lifecycle contracts |
| `pangrova-filesystem/pangrova-filesystem-local` | Confined local blob/object transfer, durable staging and cooperating-process mutation exclusion |
| `pangrova-filesystem/pangrova-filesystem-s3` | S3 blob/object transfer, cancellation, SDK clients and presigners |
| `pangrova-filesystem/pangrova-filesystem-oss` | OSS blob publication, owned SDK/transport and encryption; OSS object/COPY support remains pending |
| `pangrova-persistence/pangrova-persistence-api` | Store ports and storage facts, organized by domain/capability; initially File, Query, Coordination and Security |
| `pangrova-persistence/pangrova-persistence-jdbc` | JDBC backend implementations; File, Coordination, Security and Query backend implementations |
| `pangrova-materialization/pangrova-materialization-core` | Unified Dynamic Table / Materialized View domain kernel: definitions, generations, attempts, publication protocol, scheduling, and extension ports |
| `pangrova-materialization/pangrova-materialization-store-jdbc` | Relational materialization repositories for deployments that externalize state; never imported by the kernel |
| `pangrova-shuffle/pangrova-shuffle-core` | Shuffle contract and engine-side exchange orchestration |
| `pangrova-shuffle/pangrova-shuffle-service` | Deployable remote shuffle transport and durable spool service |
| `pangrova-parser` | SQL parsing (Calcite dialect extensions) |
| `pangrova-planner` | Calcite logical plan → physical plan conversion |
| `pangrova-protocol` | Network protocol layer (gRPC / Arrow Flight) |
| `pangrova-observability/pangrova-observability-sdk` | OpenTelemetry providers, SDK configuration, and transport adapters |
| `pangrova-generation/pangrova-generation-core` | Canonical graph/evidence codecs, pure construction factories and publication routing; shared values belong to SPI and storage ports to Persistence API; no provider or Security dependency |
| `pangrova-query` | Query runtime services, profiling/audit orchestration and explicit backend assembly; all Query stores belong to Persistence |
| `pangrova-operations` | DDL/DML operation execution |
| `pangrova-persistence/pangrova-persistence-memory` | Instance-owned Coordination and Query stores, bounded audit cache and explicit structured logging; no state survives restart |
| `pangrova-coordination/pangrova-coordination-core` | Control plane only: parse, bind, authorize, admit, schedule, fence, validate terminal metadata and audit |
| `pangrova-coordination/pangrova-coordination-zookeeper` | Optional quorum provider over Persistence API and Coordination SPI; never depends on the Coordinator process artifact |
| `pangrova-worker/pangrova-worker-core` | Worker node process, JNI loading entry, task scheduling |
| `pangrova-schema-migrations` | Resource-only canonical PostgreSQL migration sequence; preserves historical names, versions and bytes; no Java code or runtime providers |
| `pangrova-metadata` | Metadata storage and Catalog management; adapters over the Table owner |
| `pangrova-table` | Table control, maintenance, statistics and governance-binding owner; acyclic foundation for metadata/coordinator adapters |
| `pangrova-file` | File access/URL/directory orchestration and Worker HTTP client adapter; consumes shared SPI, Filesystem API and Persistence API, never a JDBC implementation |
| `pangrova-ai/pangrova-ai-control` | Governed model/evaluation/feature state and the atomic catalog-export association adapter; consumes Generation contracts without exposing this dependency through AI API |
| `pangrova-ai/pangrova-ai-worker` | Optional Worker-side AI execution plane: governed provider invocation, AI SQL functions and ServiceLoader runtime adapters |
| `pangrova-connector-*` | Data source connector providers (Iceberg / Paimon / Lance) |
| `pangrova-standalone` | Single-node standalone deployment |
| `pangrova-security` | Authentication and authorization |
| `pangrova-streaming` | Streaming query support, CDC formats/writers, JDBC checkpoint/offset/watermark stores and the `coordinator` stream scheduler/DDL/Kafka adapter |
| `pangrova-governance` | Catalog/tag/classification, lineage providers, quality store/executors and the commit-blocking quality gate |
| `pangrova-indexing/pangrova-indexing-core` | Index metadata/store, generation/publication/GC persistence, engine providers, planner extension and build orchestration; the Coordinator only adapts it under `coordinator.indexing` |
| `pangrova-transport` | Transport layer abstraction; also hosts the client REST/HTTP and Arrow/Flight result transport (`org.pangrova.client.transport`) |
| `pangrova-client/pangrova-client-core` | Framework-free public client contract: query/catalog DTOs, errors, session configuration, result consumer |
| `pangrova-client/pangrova-client-cli` | Command-line client |
| `pangrova-client/pangrova-client-jdbc` | JDBC driver (`java.sql.Driver` provider) |
| `pangrova-testing` | Test infrastructure (e2e, benchmark, chaos, smoke) |
| `pangrova-native-bundle` | Native library packaging for distribution |

## C++ Side (CMake, C++20) — `pangrova-worker-native/`

Sources are grouped by CMake OBJECT target; one `libpangrova_native` shared
library is assembled from them.

| Target | Directory | Responsibility |
|--------|-----------|---------------|
| `pangrova_native_bridge_objects` | `src/bridge/` | JNI entry (`Java_*`, `JNI_OnLoad`), lifecycle, task creation/command dispatch, handle registry; the only JNI export boundary |
| `pangrova_native_core_objects` | `src/engine/` | Velox vectorized execution and plan conversion, task driver, shuffle/Flight, memory, columnar, result streaming, write, split, merge/delete, cache, variant, compat, observability |
| `pangrova_native_capability_objects` | `src/capabilities/` | Canonical capability catalog, function registry/bindings and artifact retrieval planners/builders |
| `pangrova_native_index_objects` | `src/indexing/` | Bloom/hint pruning, BM25 text indexing and retrieval fusion |
| `pangrova_native_vector_objects` | `src/vector/` | Vector distance functions and ANN index implementations (HNSW, IVF-PQ, DiskANN) |
| `pangrova_native_udf_objects` | `src/udf/` | Native UDF registry and JVM scalar-function callbacks |
| `pangrova_native_lance_objects` | `src/lance/` | Lance format batch reader/writer |
| — | `lance-ffi/` | Rust→C FFI for Lance vector storage |
| — | `proto/` | Empty/non-authoritative; C++ generates the canonical v2 sources from `pangrova-protocol` |

Include paths are rooted at `src/`, so headers are addressed as
`engine/velox/...`, `capabilities/capability/...`, `indexing/text/...`,
`bridge/JniBridge.h`, `vector/...`, `udf/...` and `lance/...`. Tests mirror the
same grouping under `test/`, with shared fixtures at `test/` root.

JNI entry points are defined only in `src/bridge/`; the macOS/Linux export maps
contain exactly those symbols. Native→Java upcalls (allocation listener,
shuffle egress, write executor, JVM scalar functions) remain engine-side
adapters and are the only other units that include `jni.h`.

## Interaction Boundary

```
┌───────────────────────────────────────────────────┐
│  Java (JVM)                                       │
│  pangrova-worker → JNI → libpangrova_native.dylib/so │
│                                                   │
│  Interop data: Arrow C Data / typed IPC           │
│  Control: Protobuf-serialized TaskRequest         │
└────────────────────┬──────────────────────────────┘
                     │ Classpath resource → System.load / JNI
┌────────────────────▼──────────────────────────────┐
│  C++ (Native)                                     │
│  libpangrova_native — single shared library         │
│                                                   │
│  Execution: Velox vectorized engine               │
│  Exchange:  Arrow Flight shuffle                  │
│  Storage:   Parquet / Lance / ORC readers         │
└───────────────────────────────────────────────────┘
```

## Native Dependency Graph

Velox Vector is the sole task-local execution representation. Arrow is the
interop and data-plane wire contract, not a requirement to replace Velox's
internal layouts. Share compatible buffers through the Arrow C Data Interface
with explicit ownership; necessary string, nested-offset, encoding and timestamp
conversions must preserve semantics and account for allocation/copy costs.
IPC serialization, compression, TLS and network transport are not physically
zero-copy. Measure each boundary's copy bytes and CPU before making performance
claims. Private page payloads and unmeasured row upcalls are migration gaps, not
alternative compliant execution paths.

```
pangrova-worker (Java)
  └── JNI loads → libpangrova_native.dylib (C++)
                    ├── Velox (vectorized execution)
                    ├── Arrow (columnar format + Flight)
                    ├── Lance C FFI (vector storage, Rust→C)
                    ├── Protobuf (task protocol)
                    ├── OpenTelemetry C++ SDK (tracing)
                    ├── CRoaring (bitmap indexes)
                    └── simdjson (JSON parsing)
```

## Module Dependency Policy

The target foundations are `pangrova-filesystem` (api/local/s3/oss),
`pangrova-persistence` (api/jdbc/memory) and `pangrova-spi`. Generic filesystem operations
belong to Filesystem; authorization, SQL, transfer budgets and domain orchestration
remain with callers. Shared contracts belong to `org.pangrova.spi.<domain>.<capability>`;
store ports belong to `org.pangrova.persistence.api.<domain>.<capability>`, and backend
implementations to `org.pangrova.persistence.jdbc.<domain>.<capability>`.
New store implementations must use this ownership. Existing domain stores and
remaining domain-owned general APIs are a migration backlog, not an alternate
architecture or evidence that the migration is complete. A backend migration must
preserve transaction ownership, authority checks, codecs and canonical SQL bytes.
In-memory and quorum providers require explicit backend placement; they must not
be mixed into the API or called JDBC implementations.

Dependency direction is Common → Filesystem API → SPI → Persistence API → JDBC
(the arrow means the right-hand artifact may consume the left-hand one).
Filesystem providers consume Filesystem API without SPI or domain stores. SPI must
not depend on Protocol: wire-specific `AiTaskLifecycle` stays in Protocol, while
`QueryId` is a shared SPI value. JDBC must not depend on process, File Core or
Security runtime implementations. Assembly selects real implementations explicitly.

Filesystem API must not depend on any provider or cloud SDKs, including on its test classpath.
Governed FILE consumes BlobStorage; COPY preparation consumes ManagedBlobStorage. Process assembly
selects providers explicitly. An admitted read exposes SDK-free abort semantics; source-version
authority and caller I/O budgets remain outside the provider. The current COPY source adapter
still requires exact S3 versions; a generic managed-storage port does not authorize LOCAL or OSS
sources. Filesystem publication copies and verifies caller staging before an atomic no-replace
link, and must never publish a mutable alias of caller-owned bytes.
OSS owns its official client and cancellable transport, and implements only the content-publication
port. Its never-versioned bucket policy must not be generalized into COPY source/version authority.
Filesystem API owns ObjectStoreAccessorProvider. LOCAL and S3 providers implement it;
Worker assembly selects and closes providers, while each provider owns its accessor cache and
resources. These providers must not depend on Worker, File Core or Persistence JDBC stores. SDK clients
must receive a non-owning credential view; only the provider may close an owned default chain.
Mutable-version S3 deletes require a conditional-delete-capable endpoint. Custom endpoints are
version-only unless explicitly qualified in local provider assembly; never retry unconditionally.

The authoritative per-artifact allow-list (`packageRoots`, `exports`,
`allowedImports`, `forbiddenImports`, `friendExports`, declared `providers`) lives
in `.pangrova/modules.yaml`; `.pangrova/module-baseline.yaml` records generated
facts. Do not restate the matrix here — a duplicated table drifts from policy.

Generation-job composition roots use `org.pangrova.generation.assembly` factories and
pass `org.pangrova.spi.generation` values to consumers. The routing implementation
in `org.pangrova.generation.publication` is not exported. Storage ports belong
to `org.pangrova.persistence.api.generation`, with caller-owned JDBC cooperation
explicitly under `transaction.jdbc`. Actual PostgreSQL providers belong to
`org.pangrova.persistence.jdbc.generation`, selected by application assembly.
Generation Core retains backend-independent canonical codecs and construction/routing;
Persistence JDBC may use those codecs, but Core must never depend on Persistence
implementations or Security, including through its test classpath. SPI values do
not depend on Persistence API or Generation Core. API classpath and minimal-loader
tests verify values/ports without serialization or backend implementations.
Generation does not own Dynamic Table or Materialized View lifecycles, which remain
in `pangrova-materialization`. Stage/exchange consumers use capability-specific
Persistence API ports rather than provider types or nested provider records.
A returned current-authority view
is process-local preparation evidence, never a credential accepted by a store.
Providers establish fresh authority on every operation. Partition preparation
callbacks run before transaction locks are released and must not perform network
I/O; callback failure rolls back the claim.
Domain adapters read persisted generation inputs through `GenerationTransactionInputs`
on the supplied connection. Ordinary auto-commit reads remain supported; a stored
compiler observation never grants current authority. PostgreSQL SQL and canonical
input validation stay in the real provider, selected by the composition root.

Coordination backends depend on Persistence API and Coordination SPI, never on the Coordinator process
or Metadata. The process composition root selects and closes providers; in-memory
leases do not provide inter-process exclusion. JDBC provider owner tests use the
Testing Kit migration fixture without importing Core or Metadata fixtures.

AI Control's `assembly.AiInfrastructureStores` requires the process root to supply
the real `GenerationJobSubmissionTransactions` provider, which owns its explicitly
injected `GenerationTransactionAuthority`. Submission and export association use the
caller connection; the participant must not commit, roll back or close it.
`GenerationExecutionTransactionFence` similarly exposes current owner checks while
Query retains anchor locking and post-lock database clock evaluation.
AI Control has no production or test-jar
dependency on Metadata. Cross-domain catalog-export and deferred-model authority
tests live in Testing Integration; owner tests use the canonical migration fixture.
Do not restore a default factory that discovers Metadata or bypasses authority.

Security owns the `CurrentModelAuthority` JDBC cooperation port and its deferred
receipt snapshots. AI Control implements current model/role validation on the
caller connection. Metadata consumes this explicit port without importing AI
Control; the process composition root supplies the real provider to Generation,
managed-source and generated-write consumers. No provider may acquire or finish
the caller transaction, and no static authority setter or permissive default is
allowed.

Resource-only JARs are explicit `resourceOnly` owners with `resourceRoots` in the
manifest. They cannot declare Java package roots, exports or providers, and cannot
contain Java sources or packaged bytecode. Their resources must exist under the
declared roots. Historical database migrations live only in `pangrova-schema-migrations`
at the unchanged `db/migration` classpath location; Metadata bootstrap consumes that
runtime dependency, and owner tests use the Testing Kit migration fixture. Do not
copy SQL into another module to restore a missing classpath dependency.

Hard gates:

- `ModuleDependencyPropertyTest` fails on any undeclared import, forbidden import,
  export violation, provider/descriptor mismatch, split package or stale baseline.
  Both main and test source facts come from Testing Kit's `JavaSourceReferences`
  JDK parser, also source-launched by the Python baseline generator. Facts include
  wildcard/static/nested imports and explicit qualified references in code and
  annotations; comments, string literals and package declarations are not edges.
  Malformed Java and invalid UTF-8 fail the scan. Dependency checks read current
  POMs, not the saved baseline; export failures include source file/line witnesses.
- `PackageDependencyAcyclicityPropertyTest` parses main Java sources from every
  default-reactor module and rejects all strongly connected package components,
  without sibling restrictions or exemptions. Imports (including wildcards and
  static/nested types) and explicit qualified type references form its source
  graph; comments and string literals do not. Its deterministic source witnesses
  are written to Testing Kit's `target/package-dependency-cycles.txt`. Generated
  bytecode and reflective runtime edges still require their own connectivity
  checks. Artifact cycles are also impossible to build in Maven.
- `.pangrova/scripts/check-module-manifest.sh` fails when the committed baseline
  is stale or an artifact has no ownership entry. It requires JDK 17 and Python,
  but no prior Maven build; the lightweight CI lint job always provisions both.

Java ↔ C++ boundaries: `worker-native` is the JNI bridge; it depends on the
protocol only. Native code never imports Java-side services.

## DML Execution Routing Table

The fundamental boundary between Java and C++ execution layers:

| Operation | Reads data? | Execution path | Key code |
|-----------|------------|----------------|----------|
| `SELECT` | Yes | **Native** — vectorized scan + filter + projection | `QueryDispatcher` → stage/task dispatch |
| `INSERT VALUES` | No | **Native** typed `ValuesRel` → worker-direct write → coordinator fenced atomic commit | `PlanExecutionFactory.executeDml()` |
| `INSERT OVERWRITE` (static) | No | **Native** typed `ValuesRel` → worker static-partition overwrite → coordinator fenced atomic commit | `PlanExecutionFactory.executeDml()` |
| `INSERT SELECT` | Yes | **Native** read → worker-direct write → coordinator fenced atomic commit | `PlanExecutionFactory.executeDml()` |
| `DELETE` (no WHERE) | Yes, when a row policy applies | **Native** — protected mutation predicate → surviving rows → fenced overwrite | `PlanExecutionFactory.executeDml()` |
| `DELETE WHERE` | Yes | **Native** distributed — rows not matching the protected mutation predicate → fenced overwrite | `PlanExecutionFactory.executeDml()` |
| `UPDATE` | Yes | **Native** distributed — workers apply assignments, coordinator atomic commit | `PlanExecutionFactory.executeDml()` via `NativeDmlCompileFunction` |
| `MERGE` | Yes | **Native** distributed — workers match/transform, coordinator atomic commit | `PlanExecutionFactory.executeDml()` via `NativeDmlCompileFunction` |
| DDL | No | **Java** — catalog metadata API | `DdlExecutor` |

**Routing rule**: All relational DML, including literal-only `INSERT VALUES`, goes through the
canonical planner → v2 task → Native/Worker write path. The Coordinator may validate types,
authorize, bind, admit, schedule, fence and commit metadata, but it must never construct a
`Page`, open a `ConnectorPageSink`, or retain a DML row payload. Workers produce
commit-metadata fragments via
`finishWithMetadata()`; the coordinator performs the single atomic catalog commit through
`ConnectorMetadata.finishDistributedWrite()` (fenced by `WriteCommitFencer`, journaled in
`WriteCommitJournal`). Only DDL remains a Java-local metadata operation. DELETE must not
bypass policy compilation, authenticated write admission or the durable commit journal,
even when a connector supports metadata-only deletion. Mutation row policies restrict
matches; they must never remove the unmodified physical rows needed by a replacement
write. Target masks protect predicates and assignments, not the physical survivor columns.
Final catalog commit always happens in Java via connector SPI.

## C++ Module Dependency Rules

Within `pangrova-worker-native/src/`, C++ components follow layered dependencies:

| Component | Allowed dependencies |
|-----------|---------------------|
| `bridge/` | Protocol, `engine/`, `capabilities/`, `indexing/`, `vector/`, `udf/`, `lance/`; JNI layer must stay THIN |
| `engine/velox/` | Velox SDK, Arrow, Protobuf, `engine/columnar/`, capability/index/vector registries; `engine/velox/runner/` may invoke `indexing/` for validated pre-scan split pruning; `capabilities/artifact/` and `indexing/` only from artifact/retrieval operators and their Substrait conversions |
| `engine/executor/` | `engine/velox/`, `engine/memory/`, `engine/shuffle/`, `engine/result/`, `engine/observability/`, `engine/write/`, `engine/lance/`; `capabilities/artifact/ArtifactWorkReportState.h` only as task-local report state |
| `engine/shuffle/` | Arrow Flight, `engine/velox/serde/` public VectorSerde boundary only, `engine/columnar/`, `engine/memory/`, `engine/observability/` |
| `engine/memory/` | Velox memory SDK, Arrow allocation API, `engine/observability/`, JVM allocation upcall |
| `engine/columnar/` | Arrow, `engine/memory/` only — no SQL type decoder, executor or JNI |
| `engine/merge/`, `engine/delete/`, `engine/split/` | `engine/velox/connector/`, `engine/memory/` |
| `engine/result/` | Arrow Flight, Velox output-buffer SDK, Protocol, `engine/columnar/`, `engine/memory/`; `engine/shuffle/` ticket-security APIs only (no shuffle buffers or executor ownership) |
| `engine/observability/` | OpenTelemetry SDK only (no business logic) |
| `engine/compat/` | Build-time platform/ABI adapters only; never protocol, semantic or execution fallback |
| `capabilities/` | Velox SDK, Protocol, `engine/velox/`, `indexing/`, `vector/` |
| `indexing/` | Velox SDK, `engine/observability/`, `engine/split/`, `capabilities/artifact/` |
| `vector/` | Arrow, `engine/memory/`, `engine/velox/`, `capabilities/artifact/` |
| `udf/` | Velox SDK, `bridge/` handle registry, `engine/` |
| `lance/` | Lance C FFI, Arrow, `engine/observability/`, `capabilities/` |

The in-process engine registers capability, indexing and vector implementations
directly, so include-level edges between `engine/`, `capabilities/`,
`indexing/` and `vector/` exist in both directions. The OBJECT-target split
defines ownership and build isolation, not an acyclic include graph; the single
shared library links them together by design.

The engine→capability edges are confined to named seams, all of them inside
capability-specific execution rather than the generic SELECT/DDL/DML path:
`engine/velox/plan/ArtifactBuildOperator`, `FilteredArtifactRetrievalOperator`,
`GeneratedValuesOverlayOperator`, the `Substrait*Artifact*`/`SubstraitRetrieval*`/
`SubstraitGeneratedValues*` conversions, `engine/velox/plan/RetrievalMergeOperator`
(`indexing/retrieval/`) and `engine/velox/plan/ArtifactBuildOperator`
(`indexing/text/bm25/`). Two seams reach into generic code:
`engine/velox/runner/VeloxTaskRunner` calls `indexing/index/IndexHintFilter` for
typed-split pruning, `engine/velox/filter/DynamicFilterConverter` calls
`indexing/index/FileIndexFilter` for Bloom probes, and
`engine/executor/driver/{TaskExecutor,PlanExecutionState}.h` hold artifact report
state. Making the execution core link-independent of capability implementations
requires inverting exactly those five seams behind engine-owned ports; until then
no new edge from `engine/` into `capabilities/artifact/` or `indexing/` is
allowed outside the list above.

### C++ Dependency Red Lines

- Only `src/bridge/` defines `Java_*`/`JNI_OnLoad` entry points; export maps list exactly those
- `jni.h` is included only by `src/bridge/` and the Native→Java upcall adapters
  (`engine/memory/PangrovaAllocationListener.h`, `engine/shuffle/ShuffleEgressExecutor.h`,
  `engine/write/NativeWriteExecutor.h`, `engine/velox/runtime/NativeEngineLifecycle.h`,
  `udf/JvmScalarFunction.h`)
- `engine/observability/` MUST NOT include business logic headers
- `engine/memory/` MUST NOT include executor or shuffle headers (direction: they depend on memory)
- `engine/columnar/` MUST NOT include SQL decoder, executor or JNI headers
- No new circular includes between C++ components outside the documented
  engine/capability/index/vector composition above

## Full-Chain Connectivity Requirements

The system ships as ONE integrated binary. These invariants must hold at all times:

### Wiring Invariants

1. **Every production-used SPI has a wired implementation** — experimental
   contracts may precede providers, but must not be advertised as runnable features
2. **Every implementation is wired** — registered via constructor injection or ServiceLoader
3. **Every Java `native` declaration has a C++ implementation** — no half-bridged JNI
4. **Every C++ JNI function is in `jni_exported_symbols.map`** — no invisible exports
5. **Every proto message is used** — no orphan message definitions
6. **Every config key is read** — no orphan configuration
7. **Public contracts are documented and observable behavior is tested** —
   existing tests may cover delegating methods and pure refactors

### Connectivity Verification

When a change can affect query execution or a boundary in this path, verify the
applicable links:
```
SQL → Parser → Planner → Protocol → JNI → Native Executor → Velox → Arrow Result → Client
```

If ANY link in this chain is broken, the system is broken. E2E tests validate this chain.

### Dead Code Policy

- **Zero tolerance for dead code**: if code is unreachable, delete it
- **Zero tolerance for stub implementations**: `throw new UnsupportedOperationException()` is acceptable ONLY with `@Experimental` annotation and tracking issue
- **Zero tolerance for orphan imports**: unused `import` (Java) or `#include` (C++) must be removed
- Run `.pangrova/scripts/detect-dead-stubs.sh` when changing SPI, wiring,
  registrations, or implementation connectivity
