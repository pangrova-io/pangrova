# Architecture

Java coordinates and plans; C++ executes vectorized work behind JNI.

| Source of truth | Owns |
|---|---|
| `.pangrova/modules.yaml` | Artifact paths, package roots, exports, allowed/forbidden imports, providers |
| `.pangrova/module-baseline.yaml` | Generated facts; regenerate with `.pangrova/scripts/generate-module-baseline.py` |
| [Domain modularization ADR](../../docs/architecture/domain-modularization-adr.md) | Contract/implementation/assembly decisions and cycle handling |
| `docs/architecture/` | Subsystem design and current capability limits |

Do not restate the manifest's per-artifact matrix in rules or docs. When a boundary
changes, update source, manifest, wiring and the ADR in the same change.

Before merging or splitting modules, follow
[Determining and Handling Cycles](../../docs/architecture/domain-modularization-adr.md#determining-and-handling-cycles):
check artifact, production package, native component and runtime wait relationships
separately, including indirect paths through third-party artifacts. Contract all
candidate groups of one batch in a single precheck, and state the cut edges, the
owner of each contract and state, and the recheck result.

## Module Families

Leaf artifacts are `<family>-<leaf>`; a family with one artifact keeps its POM in
the family directory.

| Family | Leaves | Responsibility |
|---|---|---|
| `pangrova-common` | — | Dependency-minimal utilities, client wire values, JSON helpers |
| `pangrova-types` | — | Engine logical types, signatures and scalar/row values |
| `pangrova-batch` | — | Batch values, ownership contracts and the Arrow codec |
| `pangrova-config` | — | Immutable YAML configuration |
| `pangrova-function` | `api` | Scalar, aggregate and table function contracts |
| `pangrova-execution` | `api`, `binding` | Task stage values; canonical task/split/lease binding |
| `pangrova-protocol` | — | Canonical v2 protobuf execution contracts, Substrait version validation, capability identities |
| `pangrova-parser` | — | AST, diagnostics, ANTLR parsing, recovery and formatting (`.api` / `.core`) |
| `pangrova-planner` | — | Calcite planning, optimization, fragmentation, Substrait conversion |
| `pangrova-query` | — | Submission, session, query/stage state, retention, audit, recovery |
| `pangrova-scheduler` | `api`, `policy`, `strategies-builtin` | Admission, placement eligibility, speculation; injected ranking |
| `pangrova-coordination` | `api`, `control`, `zookeeper` | Membership/lease values; control plane; optional ZooKeeper quorum |
| `pangrova-worker` | — | Worker bootstrap, task execution, JNI command channel, Native runtime lifecycle |
| `pangrova-shuffle` | `api`, `native`, `exchange`, `pangrova`, `local`, `service` | Shuffle contracts, Native codecs, exchange orchestration, remote/local providers, deployable service |
| `pangrova-transport` | `client`, `flight`, `http` | Arrow/Flight result client and server; embedded HTTP server and clients |
| `pangrova-metadata` | `api`, `codec`, `catalog` | Metadata API/SPI, shared encoding, catalog resolution, DDL and synchronization |
| `pangrova-table` | — | Table identity, ownership, versions, maintenance, statistics |
| `pangrova-file` | — | Governed file namespace, access and delegated Worker operations |
| `pangrova-filesystem` | `api`, `local`, `s3`, `oss` | Blob/object transfer contracts and providers |
| `pangrova-connector` | `api`, `runtime`, `iceberg`, `paimon`, `lance` | Connector API/SPI, plugin runtime, table-format providers |
| `pangrova-persistence` | `<domain>-api`, `<domain>-{postgresql,memory}`, `jdbc-api`, `jdbc-support`, `workflow-*` | Domain storage ports and backends; caller-owned JDBC cooperation |
| `pangrova-schema-migrations` | — | Resource-only canonical PostgreSQL migrations |
| `pangrova-cache` | `api`, `memory`, `redis` | Cache contracts and providers |
| `pangrova-generation` | `api`, `codec`, `workflow` | Durable generation values, canonical encoding, templates and publication routing |
| `pangrova-materialization` | `api`, `lifecycle`, `strategies-builtin` | Dynamic Table / Materialized View lifecycle and refresh strategies |
| `pangrova-indexing` | `api`, `codec`, `lifecycle`, `strategies-builtin` | Index contracts, payload codecs, build/evaluation, built-in strategies |
| `pangrova-semantic` | — | Semantic View definitions, validation and publication |
| `pangrova-ai` | `api`, `control`, `http`, `openai`, `qwen`, `worker` | AI contracts, governed model state, providers, Worker execution plane |
| `pangrova-security` | `api`, `codec`, `access`, `secrets`, `auth-*`, `policy-builtin`, `secret-*`, `kms-aws` | Identity, authorization, tenants, secrets and KMS |
| `pangrova-streaming` | `api`, `pipeline`, `source-kafka`, `formats-builtin` | CDC contracts, pipeline, Kafka source, change-format decoders |
| `pangrova-governance` | `api`, `runtime`, `lineage-*`, `quality-sql` | Tags, classification, lineage and quality |
| `pangrova-observability` | `api`, `instrumentation`, `noop`, `sdk` | SDK-free telemetry contracts, MDC/profiling, disabled and OpenTelemetry providers |
| `pangrova-operations` | — | Diagnosis, incident recovery, async DDL jobs, scaling and upgrades |
| `pangrova-server` | `bootstrap`, `coordinator`, `worker` | Process entry points and role composition |
| `pangrova-standalone` | — | Single-node deployment |
| `pangrova-client` | `http`, `cli`, `jdbc` | HTTP SDK, CLI and JDBC driver |
| `pangrova-testing` | `kit`, `integration-tests`, `benchmark`, `fuzzer` | Test infrastructure, E2E/smoke suites, JMH |
| `pangrova-native-bundle` | — | Platform classifier packaging of the Native library |

## Java / Native Boundary

`pangrova-worker` loads one `libpangrova_native` shared library from a classpath
resource and sends Protobuf-serialized task requests; data crosses through the
Arrow C Data Interface or typed IPC. Native links Velox, Arrow/Flight, the Lance
Rust FFI, Protobuf, OpenTelemetry C++, CRoaring and simdjson. Native code never
imports Java-side services; Worker JVM code supplies lifecycle, policy, secrets
and governed provider networking.

Velox vectors are the only task-local execution representation; Arrow is the
interop and wire contract. Share buffers with explicit ownership, preserve
semantics in necessary conversions, and measure copy bytes and CPU per boundary
before making performance claims. IPC, compression, TLS and network transport are
not zero-copy. Private page payloads and unmeasured row upcalls are migration gaps,
not compliant alternative paths.

### Native Targets

`pangrova-worker-native/src/` is grouped by CMake OBJECT target into one shared
library. Includes are rooted at `src/`; tests mirror the layout under `test/`.
Protocol sources are generated from `pangrova-protocol`.

| Target | Directory | Responsibility |
|---|---|---|
| `pangrova_native_jni_objects` | `jni/` | JNI entry points, reference ownership, JVM callbacks, runtime initialization |
| `pangrova_native_command_objects` | `command/` | Typed command dispatch, request binding, handle ownership, Flight services |
| `pangrova_native_core_objects` | `engine/` | Velox execution and plan conversion, driver, shuffle, memory, columnar, results, write, split, merge/delete, cache, observability |
| `pangrova_native_capability_objects` | `capabilities/` | Capability catalog, function registry and artifact retrieval |
| `pangrova_native_index_objects` | `indexing/` | Bloom/hint pruning, BM25 and retrieval fusion |
| `pangrova_native_vector_objects` | `vector/` | Distance functions and ANN indexes (HNSW, IVF-PQ, DiskANN) |
| `pangrova_native_udf_objects` | `udf/` | Native UDF registry and JVM scalar callbacks |
| `pangrova_native_lance_objects` | `lance/` | Lance batch reader/writer over `lance-ffi/` |

### Native Dependency Rules

| Component | Allowed dependencies |
|---|---|
| `jni/` | `command/`, execution resources, JVM callbacks |
| `command/` | Protocol, execution, capabilities, Lance dataset commands |
| `engine/velox/` | Velox, Arrow, Protobuf, `engine/columnar/`, capability/index/vector registries; the named seams below |
| `engine/executor/` | `engine/{velox,memory,shuffle,result,observability,write,lance}/`; `capabilities/artifact/ArtifactWorkReportState.h` as task-local report state |
| `engine/shuffle/` | Arrow Flight, `engine/velox/serde/` VectorSerde boundary, `engine/{columnar,memory,observability}/` |
| `engine/memory/` | Velox memory, Arrow allocation, `engine/observability/`, JVM allocation upcall |
| `engine/columnar/` | Arrow, `engine/memory/` |
| `engine/{merge,delete,split}/` | `engine/velox/connector/`, `engine/memory/` |
| `engine/result/` | Arrow Flight, Velox output buffers, Protocol, `engine/{columnar,memory}/`, shuffle ticket-security APIs only |
| `engine/observability/` | OpenTelemetry only; no business logic |
| `engine/compat/` | Build-time platform/ABI adapters; never semantic or execution fallback |
| `capabilities/` | Velox, Protocol, `engine/velox/`, `indexing/`, `vector/` |
| `indexing/` | Velox, `engine/{observability,split}/`, `capabilities/artifact/` |
| `vector/` | Arrow, `engine/{memory,velox}/`, `capabilities/artifact/` |
| `udf/` | Velox, `command/resource/` handle registry, `engine/` |
| `lance/` | Lance C FFI, Arrow, `engine/observability/`, `capabilities/` |

Red lines:

- Only `jni/` defines `Java_*`/`JNI_On*`; the export maps list exactly those symbols.
- `jni.h` is included only by `jni/` and the upcall adapters
  `engine/memory/PangrovaAllocationListener.h`, `engine/shuffle/ShuffleEgressExecutor.h`,
  `engine/write/NativeWriteExecutor.h`, `engine/velox/runtime/NativeEngineLifecycle.h`
  and `udf/JvmScalarFunction.h`.
- No local include cycles and no new edges inside a cyclic component group; remove a
  resolved edge from `check-native-boundaries.sh` in the same change. Inspect edges
  with `python3 .pangrova/scripts/check-native-includes.py pangrova-worker-native/src --components`.
- `engine/` reaches `capabilities/artifact/` or `indexing/` only through these seams:
  the `engine/velox/plan/` artifact, retrieval-merge and generated-values operators and
  their `Substrait*` conversions; `engine/velox/runner/VeloxTaskRunner` →
  `indexing/index/IndexHintFilter`; `engine/velox/filter/DynamicFilterConverter` →
  `indexing/index/FileIndexFilter`; and the artifact report state in
  `engine/executor/driver/{TaskExecutor,PlanExecutionState}.h`. Removing a seam means
  moving its contract to an engine-owned port.

## Module Dependency Policy

Cross-module imports are default-deny: they must be permitted by the Maven graph,
the source's `allowedImports` and the target's `exports`/`friendExports`.

- A separately published `pangrova-<domain>-api` owns calls, plugin factories and
  extension contracts under `.api`. Core consumes contracts, replaceable providers
  implement them, and Server composes processes and selects implementations
  explicitly. An API used inside one family with one implementation stays with it.
- Do not recreate the retired central `pangrova-spi`, generic
  `persistence-api/jdbc/memory` artifacts or cross-domain aggregate stores.
- Store ports live in `org.pangrova.persistence.<domain>.api`, backends in
  `org.pangrova.persistence.<domain>.<backend>`. JDBC cooperation lives in
  `persistence-jdbc-api`: participants borrow the caller connection and never
  commit, roll back or close it.
- Providers never import Core, Metadata or process assembly. Core never constructs a
  backend statically or falls back to an in-memory store or permissive default.
- A backend move preserves authority, lock order, transaction ownership, codec bytes
  and SQL; semantic changes need their own tests.
- Schema DDL lives only in `pangrova-schema-migrations` (`db/migration` and the
  private Shuffle schema, each with its own Flyway history). Providers validate the
  schema and never issue DDL. Resource-only artifacts declare `resourceOnly` and
  `resourceRoots` and contain no Java.
- Domain contracts never depend on Protocol. `QueryId` and `ExecutionAnchor` belong
  to Execution API; wire-only types such as `AiTaskLifecycle` stay in Protocol.
- The Coordinator production classpath excludes the Worker artifact and JNI.
- Known coupling recorded as debt in the ADR is not permission for new coupling.

### Domain Invariants

- **Metadata/Semantic/AI.** `metadata-catalog` owns technical identity, schema,
  version, binding and statistics; business concepts and Semantic Views belong to
  `pangrova-semantic`; grounding/evaluation state to `ai-control`. Metadata imports
  neither. Graph and RDF/OWL types stay in semantic adapter packages. Projections and
  indexes are rebuildable, never authority. AI-produced SQL or actions pass grounding,
  semantic compilation, Planner validation and a Security decision; LLM output never
  authorizes execution.
- **Metadata persistence.** Metadata API/SPI lives only in `metadata-api`; Jackson
  stays out of domain contracts. Server selects the PostgreSQL providers. Providers
  durably accept immutable requests before effects; pending and unknown outcomes are
  terminal for the invocation and are confirmed with the original operation/run ID.
  Atomic projection validates the original `CatalogDeploymentIdentity` under the
  catalog row lock and commits projection, outbox and receipt together. Only
  source-certified coverage advances sync progress; wall time is never a cursor.
  Overlapping scans obtain a durable observation epoch before the first external
  read; only authoritative point reads establish contents or absence.
- **Security.** Security Access excludes persistence implementations and Testing Kit
  from production. Deferred evaluation receives current-authority readers and the
  publication lock through constructors. Security Memory provides only the transient
  Tenant store; Role/Grant/Mask fixtures belong to Testing Kit. Security owns
  `CurrentModelAuthority`; AI Control implements it and Metadata consumes it without
  importing AI Control.
- **Filesystem.** Filesystem API carries no provider or cloud SDK, even for tests.
  Providers never depend on Worker, File Core or JDBC stores; callers keep
  authorization, SQL and budgets. Publication copies and verifies staging before an
  atomic no-replace link. COPY sources require exact S3 versions; OSS implements only
  content publication. SDK clients receive non-owning credential views. Mutable-version
  S3 deletes require conditional delete and are never retried unconditionally.
- **Generation.** Composition roots use `generation.submission` factories and pass
  `generation.api` values; `generation.publication` is not exported. Codec owns
  canonical bytes and format selection; unknown formats fail closed. Generation never
  depends on Coordinator, Planner, persistence implementations or Security runtime,
  and does not own Dynamic Table/MV lifecycles. Authority views are process-local
  evidence; providers re-establish authority per operation. Partition preparation
  callbacks run before locks are released, perform no network I/O, and roll back the
  claim on failure.
- **Query and coordination.** Server or test assembly selects Query and Coordination
  backends. HA binds one execution-anchor backend before startup; task completion
  requires an explicitly selected write journal. In-memory leases provide no
  inter-process exclusion. Query owns session and stage completion; Execution Core
  owns `QueryExecutionScope` and binds it at dispatch. Placement shares the
  topology-owned cache-affinity router and never imports Query split lease managers.
- **AI Control.** The process root supplies the real
  `GenerationJobSubmissionTransactions`; AI Control has no production or test-jar
  dependency on Metadata, and cross-domain authority tests live in Testing Integration.

Gates: `ModuleDependencyPropertyTest` (imports, exports, providers, split packages,
baseline), `PackageDependencyAcyclicityPropertyTest` (no main-source package cycles)
and `.pangrova/scripts/check-module-manifest.sh` (baseline freshness, ownership).
Generated bytecode and reflective edges need their own connectivity tests.

## DML Execution Routing

| Operation | Path |
|---|---|
| `SELECT` | Native scan/filter/projection through stage/task dispatch |
| `INSERT VALUES`, static `INSERT OVERWRITE` | Native typed `ValuesRel` → Worker write → fenced Coordinator commit |
| `INSERT SELECT` | Native read → Worker write → fenced Coordinator commit |
| `DELETE`, `DELETE WHERE` | Native protected mutation predicate → surviving rows → fenced overwrite |
| `UPDATE`, `MERGE` | Native distributed transform via `NativeDmlCompileFunction` → atomic commit |
| DDL | Java catalog metadata API (`DdlExecutor`) |

All relational DML goes planner → v2 task → Worker/Native through
`PlanExecutionFactory.executeDml()`. The Coordinator validates, authorizes, binds,
admits, schedules, fences and commits metadata, but never builds a `Page`, opens a
`ConnectorPageSink` or retains DML rows. Workers return `finishWithMetadata()`
fragments; the Coordinator performs one `ConnectorMetadata.finishDistributedWrite()`
commit fenced by `WriteCommitFencer` and journaled in `WriteCommitJournal`. DELETE
never bypasses policy compilation, write admission or the journal, even when a
connector supports metadata-only deletes. Row policies restrict matches but never
drop physical rows a replacement write must keep; masks protect predicates and
assignments, not survivor columns.

## Wiring and Connectivity

- Every production-used SPI has a wired implementation; experimental contracts are
  not advertised as runnable.
- Every implementation is wired by constructor injection or `ServiceLoader`.
- Every Java `native` method has a C++ implementation in the export map.
- No orphan proto messages or unread configuration keys.

For execution-affecting changes, verify the applicable chain:
SQL → Parser → Planner → Protocol → Worker/JNI → Native → Arrow result → Client.
Check that callers use the wiring and tests exercise the intended path; E2E tests
own the full chain.
