# Architecture

> 2026-11-01：目标架构由 [领域模块化 ADR](../../docs/architecture/domain-modularization-adr.md) 替代。统一领域 API/SPI 制品，保留 Core，Provider/Persistence/Server 分离。下文旧模块名记录实施现状或历史约束；与 ADR 冲突的合并 owner、中央 SPI 目标不再适用。

Pangrova is a **hybrid C++ / Java distributed OLAP engine** using the JNI bridge pattern.
Java handles coordination and planning; C++ handles high-performance vectorized execution.

## Java Side (Maven, JDK 17)

| Module | Responsibility |
|--------|---------------|
| `pangrova-common` | Shared utilities and base types |
| `pangrova-types` | Connector-independent engine logical types, signatures, declarations and Java scalar/row values; Common/JDK only |
| `pangrova-batch/pangrova-batch-api` | SDK-independent block/page values and synchronous readable-batch ownership contract; Types/Common only |
| `pangrova-batch/pangrova-batch-arrow` | Arrow decoding, stream validation, detached values and borrowed projections; owns readers, borrows allocators |
| `pangrova-config` | Configuration management |
| `pangrova-indexing-api` | Index descriptors, data, evaluation and pruning contracts; API and provider SPI share one artifact |
| `pangrova-indexing/pangrova-indexing-planner-adapter` | Calcite vector recognition and Native retrieval planning bindings; depends on Planner and Indexing API, never Indexing Core or its storage providers |
| `pangrova-ai/pangrova-ai-api` | AI model/capability/invocation values, provider extensions and physical request resource ports; configuration parsing remains in AI Control, invocation state and ownership in AI Worker |
| `pangrova-function/pangrova-function-api` | Function metadata and table argument/result values in `.api`; scalar, aggregate, table, factory and callback extension contracts in `.spi`; Common/Types only |
| `pangrova-coordination/pangrova-coordination-api` | Membership, heartbeat and lease snapshots plus maintenance leadership callbacks; Common/JDK only |
| `pangrova-filesystem/pangrova-filesystem-api` | Common/JDK-only blob/object transfer, locations, immutable profiles and provider lifecycle contracts |
| `pangrova-filesystem/pangrova-filesystem-local` | Confined local blob/object transfer, durable staging and cooperating-process mutation exclusion |
| `pangrova-filesystem/pangrova-filesystem-s3` | S3 blob/object transfer, cancellation, SDK clients and presigners |
| `pangrova-filesystem/pangrova-filesystem-oss` | OSS blob publication, owned SDK/transport and encryption; OSS object/COPY support remains pending |
| `pangrova-persistence/pangrova-persistence-generation-api` | Generation storage ports, facts and errors without JDBC participants; File, Table, Query, Security and Coordination have their own persistence API artifacts |
| `pangrova-persistence/pangrova-persistence-generation-postgresql` | Existing Generation PostgreSQL job/page/exchange storage and current-authority evidence; no domain Core, Protocol or schema initialization owner |
| `pangrova-persistence/pangrova-persistence-coordination-api` | Membership, lease and exact-query reservation ports plus quorum provider SPI in one artifact; Common and Coordination API only |
| `pangrova-persistence/pangrova-persistence-coordination-postgresql` | PostgreSQL membership, lease and admission implementations over a borrowed DataSource; canonical schema initialization only, no provider DDL |
| `pangrova-persistence/pangrova-persistence-coordination-memory` | Process-local membership and lease implementations; no production Memory admission implementation; Core must receive explicit contracts |
| `pangrova-materialization/pangrova-materialization-core` | Unified Dynamic Table / Materialized View domain kernel: definitions, generations, attempts, publication protocol, scheduling, and extension ports |
| `pangrova-materialization/pangrova-materialization-store-jdbc` | Relational materialization repositories for deployments that externalize state; never imported by the kernel |
| `pangrova-shuffle/pangrova-shuffle-api` | Common/JDK-only registration, partition, handle, capability, fenced attempt/channel identity and lifecycle contracts; generic provider/session remain pending; no Native executor handles in this API |
| `pangrova-shuffle/pangrova-shuffle-native-adapter` | Native serving commands over a borrowed guarded task command channel; no Worker Core dependency, executor ownership, or generic Provider semantics |
| `pangrova-shuffle/pangrova-shuffle-pangrova` | Actual Pangrova remote client, egress, reads, credentials and connection lifecycle; no Shuffle Core/Worker/JNI/Coordinator dependency; provider read parameters are separate from access; generic session replacement remains pending |
| `pangrova-shuffle/pangrova-shuffle-local` | Exact-attempt, framed local spool storage over unified SPI; no Core/Protocol/Native dependency; exchange publication and Native Local integration remain pending |
| `pangrova-shuffle/pangrova-shuffle-core` | Engine-side exchange orchestration; no ownership of `.api`/`.spi` packages, filesystem storage or no-op publication prototypes |
| `pangrova-execution/pangrova-execution-api` | Common/JDK-only task stage, purpose and preparation deadline values |
| `pangrova-execution/pangrova-execution-core` | Canonical task/options/split binding, exact current authority and lease identity/deadline binding; no Coordinator, Planner, Worker or backend dependency |
| `pangrova-security/pangrova-security-codec` | Shared canonical deferred authority encoding and digest; no database, transaction, current permission lookup or runtime resource ownership |
| `pangrova-shuffle/pangrova-shuffle-service` | Deployable remote shuffle transport and durable spool service; depends on Shuffle API and canonical Protocol codecs, not Shuffle Core |
| `pangrova-parser/pangrova-parser-api` | SQL AST, dialect/diagnostic/recovery values, parsing API and statement extension SPI; Common/Types only, no ANTLR/cache/runtime |
| `pangrova-parser/pangrova-parser-core` | ANTLR generation, AST construction, strict/recovery parsing, formatting, source-location registry, type syntax parsing and extension registration |
| `pangrova-planner/pangrova-planner-api` | SDK-free planning intent, result origins, lineage/statistics values, metadata observation and adaptive callback contracts; Common/JDK only |
| `pangrova-planner/pangrova-planner-core` | Calcite logical/physical planning, authorization-aware rewrites, fragmentation, cost/statistics algorithms and Substrait conversion; implementation state stays outside API |
| `pangrova-protocol` | Network protocol layer (gRPC / Arrow Flight) |
| `pangrova-observability/pangrova-observability-api` | SDK-free observability API/SPI, execution correlation capture/activation and profiling contracts; Common only; query events belong to Query API |
| `pangrova-observability/pangrova-observability-core` | MDC propagation and profiling delegation; query event dispatcher belongs to Query Core; no Query or OpenTelemetry SDK dependency |
| `pangrova-observability/pangrova-observability-noop` | Explicit disabled telemetry implementations; unsupported profiling rejects, never fabricates success |
| `pangrova-observability/pangrova-observability-sdk` | OpenTelemetry providers, SDK configuration, and transport adapters |
| `pangrova-generation/pangrova-generation-api` | Immutable job/page/graph/exchange/output/publication values and validation; no runtime, codecs, storage ports or default implementation |
| `pangrova-generation/pangrova-generation-codec` | Bounded canonical graph/exchange and generated-column catalog property encoding, strict source split decoding; no Core, persistence ports, providers or query runtime |
| `pangrova-generation/pangrova-generation-core` | Construction, frozen execution templates, exact partition binding/resource budgets, generic published-generation read binding and publication routing; shared values belong to Generation API and storage ports to Persistence API; no Coordinator, Planner, provider or Security runtime dependency |
| `pangrova-scheduler/pangrova-scheduler-api` | Common/JDK-only placement/admission/spill-budget values and errors, with strategy/admission/control/health extension contracts; no Proto or runtime implementation |
| `pangrova-scheduler/pangrova-scheduler-core` | Admission, placement eligibility/locality, exact model inventory resolution, speculation and retry orchestration; ranking is injected through Scheduler SPI; no production dependency on builtin strategies; concurrency persistence belongs to Persistence |
| `pangrova-scheduler/pangrova-scheduler-strategies-builtin` | Resource-free load-balanced ranking and performance scoring; depends only on Scheduler API; Server constructs the typed factory |
| `pangrova-query/pangrova-query-api` | QuerySession, pure StatementCategory, write authority request/resolver contracts, query state/failure/submission observations, Query-owned events, immutable dispatch values, cleanup and reader-pin contracts; Common and Transport/Connector/Security/Observability API values; no Client/Planner/Coordinator/provider dependency |
| `pangrova-query/pangrova-query-core` | Pre-allocation write receipt/declaration validation, pure AST classification and exact table/CALL binding (Parser API only), query state and issued Connector scope, terminal cleanup and retention, event dispatch, audit/profiling, stage state, immutable execution scope and retrieval intents, timeout scheduling, dispatch values and reader-pin renewal; stores belong to Persistence and process/provider assembly to Server |
| `pangrova-operations` | DDL/DML operation execution |
| `pangrova-coordination/pangrova-coordination-core` | Control plane only: parse, bind, authorize, admit, schedule, fence, validate terminal metadata and audit |
| `pangrova-server/pangrova-server-coordinator` | Coordinator process entry, role composition, HTTP/JDBC server, readiness and shutdown; consumes domain implementations without a production dependency on Worker Core or Native Bridge |
| `pangrova-coordination/pangrova-coordination-zookeeper` | Optional quorum provider over Persistence Coordination API/SPI and Coordination values; never depends on the Coordinator process artifact |
| `pangrova-worker/pangrova-worker-core` | Worker node process, JNI loading entry, task scheduling |
| `pangrova-schema-migrations` | Resource-only canonical PostgreSQL migration sequence; preserves historical names, versions and bytes; no Java code or runtime providers |
| `pangrova-metadata/pangrova-metadata-api` | Unified Metadata API/SPI, deployment/projection/schema/session/resolution/event values; Common, Connector API and Table API only |
| `pangrova-metadata/pangrova-metadata-codec` | Shared table metadata and canonical JSON encoding for caches and persistence; no Core or database implementation |
| `pangrova-metadata/pangrova-metadata-core` | Catalog resolution, synchronization, caching and remaining cross-domain adapters; residual workflow/assembly/backend extraction is pending |
| `pangrova-persistence/pangrova-persistence-catalog-api` | Catalog deployment/history, projection/sync, dependency-graph and invalidation-maintenance ports; no JDBC or Core dependency |
| `pangrova-persistence/pangrova-persistence-catalog-postgresql` | Existing deployment/projection transactions and durable invalidation polling over borrowed DataSource/cache; no Metadata Core dependency |
| `pangrova-table/pangrova-table-api` | Immutable identity/ownership/observation/rename/maintenance/statistics values and policy authority calling port; Common/Connector API only |
| `pangrova-table/pangrova-table-core` | Table control, policy binding, maintenance and statistics over Persistence contracts; maintenance defaults belong to Core; backends are selected by Metadata/Server assembly |
| `pangrova-file/pangrova-file-api` | File namespace and access values plus audit, current-authority and delegated-operation ports; Common, Filesystem API and Security API only |
| `pangrova-file/pangrova-file-core` | File authorization/URL issuance and directory refresh; consumes persistence ports and injected File clients, never the Worker adapter or a JDBC implementation |
| `pangrova-file/pangrova-file-worker-adapter` | HTTP/Jackson implementation of delegated listing, version-conditional deletion and signed-URL exchange; no Worker Core or File Core dependency |
| `pangrova-ai/pangrova-ai-control` | Governed model/evaluation/feature state and the atomic catalog-export association adapter; consumes Generation contracts without exposing this dependency through AI API |
| `pangrova-ai/pangrova-ai-worker` | Optional Worker-side AI execution plane: governed provider invocation, AI SQL functions and ServiceLoader runtime adapters |
| `pangrova-connector/pangrova-connector-api` | Unified Connector API/SPI and values; only Common, Types and Batch API; no SDK, runtime or persistence |
| `pangrova-connector/pangrova-connector-runtime` | Plugin discovery/lifecycle, bounded fixed splits, shared Connector error/cache helpers and CUSTOM_BINARY codec; no central SPI dependency |
| `pangrova-persistence/pangrova-persistence-file-api` | File space/profile revision, directory attempt, COPY claim and atomic source-read admission contracts; no JDBC or backend runtime; control-plane only, not a Worker Core dependency |
| `pangrova-persistence/pangrova-persistence-file-postgresql` | Existing PostgreSQL File authority, directory refresh and COPY admission; current authorization and execution fences remain transactional |
| `pangrova-persistence/pangrova-persistence-table-api` | Table control/observation, maintenance, statistics and governance-binding ports plus storage error; Common and Table API only |
| `pangrova-persistence/pangrova-persistence-table-postgresql` | Existing Table SQL/codec and operation transactions; authorization mutation gate stays explicit, canonical schema remains external |
| `pangrova-persistence/pangrova-persistence-table-memory` | Existing process-local Table stores only; generic Persistence Memory artifact is retired |
| `pangrova-persistence/pangrova-persistence-query-api` | Query audit, journal, anchor, quarantine, lineage and distributed write intent/outbox contracts; no JDBC connection or backend |
| `pangrova-persistence/pangrova-persistence-query-postgresql` | Query journal, execution anchor/fence, quarantine, lineage outbox and event SQL; consumes JDBC cooperation contracts, never generic backend implementations; canonical schema is required |
| `pangrova-persistence/pangrova-persistence-query-memory` | Existing process-local audit, journal, execution anchor and quarantine implementations; no durable or inter-process guarantees |
| `pangrova-indexing/pangrova-indexing-codec` | Index predicate codec, common index-file envelope/CRC and payload serializer registry; depends only on contracts and Common |
| `pangrova-connector-*` | Data source connector providers (Iceberg / Paimon / Lance) |
| `pangrova-standalone` | Single-node standalone deployment |
| `pangrova-security` | Aggregator only; no code artifact |
| `pangrova-security/pangrova-security-api` | Security call/extension contracts, immutable identity/policy/tenant/audit/secret/crypto values; Common and Connector API only; no SDK, event bus or JDBC |
| `pangrova-security/pangrova-security-core` | Authentication orchestration, policy rendering, tenant services, audit emitters, secret routing/templates and local policy events; deferred evaluation uses injected JDBC cooperation ports; no production persistence backend or test-fixture dependency |
| `pangrova-security/pangrova-security-auth-{builtin,ldap,oauth2}` | Independently packaged existing authentication implementations; OAuth2 owns JJWT, Jackson and JWKS cache |
| `pangrova-security/pangrova-security-policy-builtin` | Current-authority RBAC against mandatory role/grant ports; no Security Core dependency |
| `pangrova-security/pangrova-security-secret-{env,file}` | Existing environment and confined-file secret resolution |
| `pangrova-security/pangrova-security-kms-aws` | AWS client/configuration and KMS implementation; no SDK types in KeyManager |
| `pangrova-persistence/pangrova-persistence-security-api` | Role/grant, audit, mask, row-policy and tenant store ports |
| `pangrova-persistence/pangrova-persistence-security-postgresql` | Existing security SQL readers/stores/codecs/authorization gate; canonical Tenant implementation; all security schema initialization belongs to Schema Migrations |
| `pangrova-persistence/pangrova-persistence-security-memory` | Existing process-local Tenant store only; Role/Grant/Mask fixtures belong to Testing Kit |
| `pangrova-persistence/pangrova-persistence-jdbc-api` | Caller-owned Query/Generation fences, current permission/model authority, Generation inputs/submission/publication collaboration; implementations never commit, roll back or close the caller connection |
| `pangrova-streaming` | Streaming query support, CDC formats/writers, JDBC checkpoint/offset/watermark stores and the `coordinator` stream scheduler/DDL/Kafka adapter |
| `pangrova-governance` | Catalog/tag/classification, lineage providers, quality store/executors and the commit-blocking quality gate |
| `pangrova-indexing/pangrova-indexing-core` | Index metadata/store, generation/publication/GC persistence, planner extension and build/evaluation orchestration; concrete strategies and payload serializers live in builtin; JDBC/planner separation remains pending |
| `pangrova-indexing/pangrova-indexing-strategies-builtin` | Nine Java strategies, their payload serializers, normalization and Bloom preparation; no Core dependency |
| `pangrova-transport/pangrova-transport-client` | Existing REST/HTTP and Arrow/Flight result client, codec and ownership implementation (`org.pangrova.transport.client`); further API/codec separation remains planned |
| `pangrova-transport/pangrova-transport-flight` | Role-independent Flight server, batch stream adaptation and signed ticket validation (`org.pangrova.transport.flight`); no Worker or Coordinator implementation dependency |
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

The authoritative target is `docs/architecture/domain-modularization-adr.md`.
Each domain has one `pangrova-<domain>-api` artifact containing its `.api` and actual
`.spi` contracts. Core consumes contracts; replaceable Providers consume contracts;
Server owns process composition. Do not create new central SPI contracts or new
cross-domain aggregate persistence implementations. The central `pangrova-spi` artifact
and namespace are retired. Generic `persistence-api/jdbc/memory` artifacts are also
retired; remaining domain stores and cross-domain assembly are incremental debt.
Do not restore generic persistence containers when extracting remaining backends.

Store ports belong to `org.pangrova.persistence.<domain>.api`; implementations belong
to `org.pangrova.persistence.<domain>.<backend>`. JDBC-only cooperation belongs to
`persistence-jdbc-api`, with caller-owned connections. New provider artifacts must not
import Core, Metadata or process assembly. A backend move must preserve authority,
lock order, transaction ownership, codec bytes and SQL; semantic changes require their
own explicit tests. No historical data migration or dual-write tooling is required.

Security Core's production closure must exclude both persistence implementations and
Testing Kit. Deferred policy evaluation receives current-authority readers and the
publication lock through constructors. Do not restore static backend selection in Core.
The existing Metadata transaction adapter still selects PostgreSQL collaborators until
its complete atomic workflow moves to the workflow Provider and Server composition.
Role/Grant/Mask memory fixtures belong to Testing Kit; only the already wired transient
Tenant implementation belongs to the Security Memory provider. It provides no restart,
distributed exclusion or durability guarantee. Tenant uses only `tenants/tenant_user_memberships`, initialized by the canonical migration
bundle. Do not restore normalized parallel Tenant tables or Store-owned Tenant DDL.
Policy/audit tables and all authority triggers are also initialized by the canonical migration bundle.
Security providers may verify readiness but must not issue schema DDL.

Generic filesystem operations belong to Filesystem; authorization, SQL, transfer budgets
and domain orchestration remain with callers. Filesystem providers consume Filesystem
API without domain stores. Domain contracts must not depend on Protocol: wire-specific
`AiTaskLifecycle` stays in Protocol, while `QueryId` belongs to Query API's identity
package. Assembly selects real implementations explicitly.

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

Generation-job composition roots use `org.pangrova.generation.core.submission` factories and
pass `org.pangrova.generation.api` values to consumers. The routing implementation
in `org.pangrova.generation.core.publication` is not exported. Built-in wire output format selection
belongs to Generation Codec under `.format`; its immutable role/layout values belong to
Generation API. PostgreSQL validation consumes Codec without a Core dependency. Unknown formats
still fail closed. Storage ports belong
to `org.pangrova.persistence.generation.api`, with caller-owned JDBC cooperation
explicitly under `transaction.jdbc`. Actual PostgreSQL providers belong to
`org.pangrova.persistence.generation.postgresql`, selected by application assembly.
Capability identity ID/version/signature and optional required implementation build comparison
belongs to Protocol `CapabilityIdentities`. It does not discover models, authorize actors or
select Workers. Scheduler Core owns exact model inventory resolution; callers supply active
inventories. AI Control owns embedding catalog deployment fences. Retrieval binding consumes
only its vector/reranker plans or artifact/merge intents, never the enclosing dispatch context.

Generation Codec owns canonical graph/exchange bytes and source split decoding. Persistence JDBC
uses Codec directly; it must not acquire a Core dependency merely for serialization. Generation
Core owns construction/routing and generic immutable publication binding under
`org.pangrova.generation.core.publication.read`. Retrieval intent, ANN eligibility and exact-plan
selection belong to the query/planner consumer, never the generic publication reader. Core must
never depend on Coordinator, Planner, Persistence implementations or Security runtime, including
through its test classpath. Generation API values do
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

Query persistence backend selection belongs to Server or explicit test assembly.
HA requires its execution-anchor backend at construction; process authority must
bind the identical instance before startup and cannot replace it. Task status
completion requires an explicitly selected write journal; Core must not silently
create an in-memory journal. PostgreSQL write journal construction validates the
canonical schema without issuing DDL. Metadata still owns existing PostgreSQL
workflow adapters pending their extraction; this is not an exemption for Query or
Coordination Core to import the concrete Query providers.

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

### Query execution ownership

Query Core owns `stage.QueryExecutionScope` and `stage.binding` intent values. The scope
borrows Execution Core's live/durable authority; it must not depend on Coordinator or select a Provider.
Split lookup consumes an actual QuerySession, pinned snapshots and write mode; query identity comes
from that same session. Plan serialization consumes output names rather than the entire scope.
Coordinator placement/read/shuffle evidence adapters remain explicit integration boundaries until their
full domain ports are separated; do not restore a dependency from a lower-level runtime to the DAG entry package.
Placement consumes the shared cache-affinity router directly. Split lease management borrows that
same topology-owned router; placement must not import Query split lease managers.


Metadata API/SPI belongs exclusively to `pangrova-metadata-api`. Persistence Catalog
ports depend on those contracts, never the reverse. Shared metadata encoding belongs
to `metadata-codec`; do not expose Jackson types in domain contracts. CatalogRegistry
uses deployment authority only. Historical revisions require an explicit implementation;
absence is not a default for unsupported history. Metadata cleanup borrows an invalidation
maintenance port, while Server owns the selected PostgreSQL poller lifecycle. Metadata Core
must not depend on Catalog PostgreSQL in production; its cross-domain integration tests may
use the real Provider. Remaining cross-domain workflows and other backend dependencies are
tracked debt, not permission to restore Catalog backend coupling.

### Metadata synchronization persistence

`MetadataSyncRepository` in Catalog Persistence API owns deployment-scoped source checkpoints and completion receipts. `PostgresMetadataSyncRepository` is selected explicitly by Server on the same metadata DataSource; Core must not create it. Diagnostic completion wall time must never become an incremental cursor. Only source-certified coverage can advance progress, atomically with a run receipt and deployment/checkpoint comparison. An unknown COMMIT must be confirmed by its original run ID before retry. These guarantees concern completion persistence; projection writes additionally use the typed atomic port below. They do not imply scan-wide source-version consistency.

### Catalog deployment and projection ports

The composite `MetadataStore`/`PostgresMetadataStore` have been removed. Deployment consumers use `CatalogDeploymentRepository`; synchronization additionally borrows read-only `TableProjectionRepository`, `MetadataProjectionTransactions`, and `MetadataSyncRepository`. Server selects the independent PostgreSQL providers against its owned DataSource. The `.postgresql.transaction` package is internal to the Catalog PostgreSQL artifact and is not a consumer export. Table/schema repositories are read-only. The atomic projection port requires the original
`metadata.api.catalog.CatalogDeploymentIdentity`; PostgreSQL validates it under the Catalog
row lock and commits projection, outbox and operation receipt together. Do not look up a
new current identity to authorize work from an old handle. Unknown commits are confirmed
using the original operation ID, never a fresh retry ID. SQL participants borrow the
coordinator's connection. Per-mutation deployment fencing does not imply a whole-scan
transaction, source-version ordering or a distributed scan lease.

Metadata projection and completion providers must durably accept immutable requests before executing
effects. New IDs cannot bypass a pending Catalog operation. Sync Core must recover pending completion
and projection work before loading a source cursor or observing a new batch, including after restart.
Pending and unknown outcomes are terminal for the current invocation. Obsolete intents may be rejected
only under the current deployment fence; receipt absence is never proof of failure. These per-operation
guarantees do not imply a scan lease, source-version ordering or scan-wide exactly-once execution.
