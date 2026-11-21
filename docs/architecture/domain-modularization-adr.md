---
title: "Domain Modularization ADR"
description: "Accepted decisions for domain contracts, implementations, and process assembly"
---

# ADR: Separation of Domain Contracts, Implementations, and Process Assembly

Decision date: 2026-11-01. The architecture is accepted and being implemented;
repository-wide release acceptance remains to be completed.

This ADR supersedes the earlier target decisions for a central SPI, merged domain
owners, and standalone SPI artifacts. Phase boundaries and exit conditions are in
"Implementation Order and Exit Conditions" below; historical execution records
remain in Git history and CI records and are not proof of completion for this
round. `.pangrova/modules.yaml` governs the module boundaries actually in effect;
unimplemented targets must not be written as current state.

## Decision

- When a domain has only one artifact, its source and POM live directly in the
  domain directory. A contract with a single implementation whose callers are all
  in the domain shares that implementation's artifact. Contracts shared by multiple
  implementations are released separately; a separate artifact is also kept when
  external callers need to avoid pulling in an implementation.
- Domain values, calling contracts, extension interfaces, and strongly typed
  factories uniformly use `.api.<capability>` packages. Provider JARs register
  implementations in `META-INF/services/<factory fully-qualified class name>`.
  Service discovery, configuration validation, and instance creation are
  independently verifiable. Paimon's Hive factory selects an implementation by
  identifier, accepts a typed CatalogContext, and returns a Catalog managed by the
  caller. Pangrova's AI and CDC formats follow the same approach, with each
  capability declaring its return value and resource ownership.
  [HiveCatalogFactory](https://github.com/apache/paimon/blob/master/paimon-hive/paimon-hive-catalog/src/main/java/org/apache/paimon/hive/HiveCatalogFactory.java) · [HiveCatalog](https://github.com/apache/paimon/blob/master/paimon-hive/paimon-hive-catalog/src/main/java/org/apache/paimon/hive/HiveCatalog.java)
- Parser, Batch, File, Planner, Query, Semantic, Table, and Worker each target a
  single JAR built directly from the domain root. Batch's Arrow SDK is an optional
  dependency, so value-type callers can load independently. File's access service
  consumes exact-lookup and refresh contracts, while persistence implementations
  own transactions and storage.
- Clients have three entry points: HTTP SDK, CLI, and JDBC. Common's
  `org.pangrova.common.client` organizes shared contracts by query, result,
  metadata, session, version, and error. Result endpoints and protocol values
  belong to result, and Common does not depend on Transport.
- Common's `org.pangrova.common.json` exclusively owns the JSON mapper, persistence
  encoding, and pure-JDK string escaping. Jackson databind and JSR-310 are optional
  dependencies in Common; artifacts that call the mapper declare the required
  dependencies explicitly, and client DTOs depend only on Jackson annotations.
  The HTTP SDK owns real REST requests, credentials, and retries; Transport Client
  owns result transport and Arrow decoding. The CLI consumes QueryClient directly,
  and the HTTP classes implement that interface.
- QueryId and ExecutionAnchor identify cross-protocol queries and cross-query/
  background-task execution ownership, respectively, and belong to Execution API.
  Query publishes lifecycle, audit, and log-replay contracts. Pre-submission
  validation and binding that depend on concrete task requests, generated-column
  protocols, and Native runtime capability are implemented by Execution Binding.
- Table publishes the operations required by ownership, statistics, maintenance,
  and governance binding. Table Persistence API independently owns backend
  factories and a set of repository instances belonging to the same authority.
  The Worker single JAR contains both task control and JNI, and Server Worker owns
  process assembly.
- Function's single contract artifact is built directly from
  `pangrova-function/pom.xml`. AI's multiple implementations share
  `pangrova-ai-api`; `pangrova-ai-openai` and `pangrova-ai-qwen` each publish real
  factories, and `pangrova-ai-http` reuses governed HTTP and compatible chat
  encoding. Server selects providers through runtime dependencies, and Worker and
  Control production code depend only on domain contracts.
- Standalone process artifacts are named by actual responsibility. Packages may be
  named directly after business capabilities, for example HTTP request handling in
  `http.request` and lineage in `lineage`. A new layer must explain its callers and
  the state or resources it owns. Providers own technical implementations; Server
  owns process assembly and lifecycle; backends and SDKs are isolated by domain
  contracts.
- Java named enums, exceptions, and pure constant classes enter `.enums`,
  `.exception`, and `.constants` in their owning artifact. Implementation-side
  state types remain in the corresponding implementation artifact, and the
  original lifecycle object still owns state transitions. Source wire values and
  JNI exception changes update the service descriptor, native class names, and
  distribution tests together. Implementation selection uses factory identifiers;
  enum names do not carry provider registration.
- The Planner implementation generates executable Protocol/Substrait fragments, so
  the implementation layer explicitly depends on Protocol. Planner's public
  contracts keep a minimal classpath; an independent classloader covers its public
  signatures. The module manifest no longer supports temporarily allowing
  undeclared edges through milestones.
- Base types belong to Types and batch data to Batch. They must be usable by the
  engine without going through Connector.
- Materialization calling and extension contracts are published uniformly by
  `pangrova-materialization-api`. Domain values and service ports live in
  `.api.domain` and `.api.port`. Extension points are split into `.api` by
  definition, policy, refresh, and rewrite; duplicate-ID checks, completeness
  validation, and policy selection of the registry belong to Materialization
  Lifecycle. Persistence commands and published-version read ports are owned by
  `pangrova-persistence-materialization-api`. Server constructs the PostgreSQL
  implementation from the same data source, and each statement reuses these two
  instances.
- Query events, statistics observations, and listener extension contracts belong
  to Query API; in-process dispatch belongs to Query. Observability API does not
  depend on Query in reverse; event observation owns no query state, persistence
  commit, or replay guarantee.
- Query exclusively owns query state transitions, the actual Connector operation
  scope, terminal physical cleanup, and history retention. Coordinator orchestrators
  borrow these internal implementations; waiting for a result does not grant the
  caller the right to complete internal Futures of the state machine. Logical
  terminal state, Native write completion, publication completion, and physical
  drain are recorded separately; one completion flag does not replace them.
- All query writes share Query API's WriteAuthorityRequest / WriteAuthorityResolver;
  ordinary writes and generated-column writes both validate the real current
  authority receipt through Execution Binding's WriteAuthorityAdmission before
  allocation. Server fixes the authority implementation at construction time and
  cannot replace it through a runtime setter; writes are explicitly rejected when
  it is not configured. The generated-column Catalog property codec belongs to
  Generation Codec, while the Proto wire types remain in Protocol. Current
  authority transactions and commit fencing do not change because contracts move;
  the synchronous resolver does not claim independent cancellation or timeout
  capability.
- StatementCategory is a pure observation value of Query API; AST classification,
  exact target resolution, and CALL parameter binding belong to Query and depend
  only on Parser API and value contracts. The authority entry and execution use the
  same binding and do not resolve the target separately. Classification is
  exhaustive over built-in statement types; classification, registration, and
  actual support status are verified separately, and a QUERY default must not hide
  omissions.
- Connector API explicitly distinguishes CREATE, IF_NOT_EXISTS, and
  CREATE_OR_REPLACE view creation modes, and the implementation owns atomic
  conflict decisions. Security API's ViewAccessControl defines the current
  CREATE_VIEW/ALTER/DROP checks; create authority does not imply overwrite
  authority. Existing Iceberg views are in-memory state of the catalog instance
  and do not claim restart persistence.
- Tenant-membership SQL belongs to the Server entry adapter; atomic membership
  authorization and writes belong to Security Persistence. It must check the
  current OWNER/ADMIN of the exact ACTIVE tenant and must not trust historical
  session roles or modify allowedSchemas to represent membership. Membership
  authorization does not support cross-tenant transfer or privileged-role
  revocation; SQL has no NOOP authentication bypass.
- Domain bindings receive only their actual inputs: retrieval binding receives
  vector/reranker plans, artifact resolution receives artifact/merge intents, and
  neither depends in reverse on the full Query/DAG Context. Exact model matching
  belongs to Scheduler Policy and consumes the capability inventory the caller has
  confirmed active; Embedding Catalog deployment fence validation belongs to AI
  Control. Reject when model identity, service limits, or implementation are not
  unique; never choose by "latest". Protocol owns only the structural comparison of
  capability wire identity.
- Query API holds query sessions and DAG terminal notifications; QueryExecutionScope,
  which binds live/durable authority, wire write configuration, and retrieval
  intent, belongs to Execution Binding and does not depend on the Coordinator
  scheduling entry. Fragment queries receive only the real session, snapshot
  selection, and write mode; serialization receives only column names. Control-plane
  Worker adapters, generated-result reads, and shuffle persistence evidence stay
  within their capability boundaries and must not couple back to the DAG entry.
  Cache-affinity routing belongs to placement, and selectors borrow the same
  topology instance directly; routing must not be obtained through the Query
  fragment lease manager.
- QuerySession belongs to Query API and borrows the actual query authority and an
  immutable tracing snapshot. Capture and activation contracts belong to
  Observability API in the same artifact, and Server injects the SDK implementation
  explicitly; Session does not select a provider implicitly, and tenant/property
  enrichment must preserve all trusted execution fields.
- Parser API owns AST and parse/recovery diagnostic contracts; the ANTLR tree,
  mutable position registry, cache, and type-string parsing stay in Core.
  Extensions delegate through API parsers and do not receive implementation
  classes. Core's detailed recovery tree explicitly belongs to implementation
  tooling.
- Isolated capabilities are closed according to real demand: SQL GRANT/REVOKE is
  adapted by Server to Security's atomic persistence ports; PostgreSQL completes
  current checks, delegation chains, revisions, and audit receipts in the same
  authority gate/transaction. Parser AST presence is not evidence of support; the
  corresponding SQL, real backend, assembly, and Native/HTTP validation must be
  recorded together.
- Persistence is split by domain ports and actual PostgreSQL/Memory backends. The
  JDBC collaboration protocol and support are isolated separately; cross-domain
  transactions are owned by one strongly typed workflow coordinator that holds the
  connection and commit authority.
- Coordination registration, lease, and query concurrency quota ports belong
  uniformly to Persistence Coordination API, with the real quorum extension in the
  same artifact's `.api.provider`. PostgreSQL and Memory are independent artifacts,
  Server selects the implementation, and WorkerManager must receive an explicit
  Registry; Core no longer constructs Memory implicitly. PostgreSQL quota tables
  are created by the single schema initializer, and providers may only validate,
  never rebuild or reset active authority. Memory guarantees only in-process state;
  test quota fakes do not become production providers.
- Metadata owns Catalog deployment, parsing, projection, synchronization, and
  invalidation; Table owns engine table identity, ownership, observation,
  registration, rename, maintenance, and statistics. External catalogs still own
  external table facts.
- One fact, state machine, and resource have exactly one authoritative owner.
  Copying state, weakening CAS, fencing, authority checks, atomic commit, or
  failure handling on the grounds of module splitting is forbidden.
- Acquiring a reader pin for a published generation must hold the actual query
  authority's operation permit; the query lifecycle renews and releases it through
  the same lease service. Logical terminal state does not equal physical drain, and
  renewal continues between them. Executable generations must not be exposed when
  the authority or lease service is missing; optional materialized-view rewrites
  may reject candidates and read the original source table. Metadata discovery does
  not create queries or leases early. Leases cannot promise unlimited protection
  after a process pause or storage outage longer than the lease term; recovery and
  lost-lease fencing require separate validation.
- Coordinator assembles the control plane and Worker assembles the execution plane.
  Coordinator does not depend on Worker or the JNI execution runtime; Standalone
  reuses both role assemblies and introduces no new microservice. Worker's
  standalone and embedded entries are published by `pangrova-server-worker`, both
  share `WorkerConfiguration`, and both install the production Catalog runtime.
  The three process entries share `pangrova-server-bootstrap` to load role
  configuration. If Standalone fails to start the Worker or register a shutdown
  hook, it releases started roles in reverse acquisition order and preserves the
  primary exception.
- Plugin factories are strongly typed and validate ID, compatibility, role,
  capability, and scope at startup. Missing or failed plugins are rejected
  explicitly; there is no silent switch to Memory. Plugin updates require a
  restart and do not support hot unload.
- AQE skew detection is injected through Shuffle API's SkewDetector, and Server
  selects the builtin implementation. Planner only adapts real statistics and
  decisions, does not copy detection algorithms, and does not depend on Shuffle
  implementation artifacts. Hot-partition identification, parallelism heuristics,
  and actual range splits are distinct capabilities and cannot substitute for each
  other in support claims.
- Shuffle unifies the control contract; the Worker's JNI package manages Native
  bindings, and Direct data transfer happens inside C++. It explicitly expresses
  incarnation, exchange generation, producer/consumer attempts, seal, commit
  receipt, cancellation, backpressure, and buffer ownership. Fault-tolerance
  guarantees must be confirmed jointly by capability and deployment.
- Celeborn is a future provider. The abstraction is validated with the existing
  Direct, Local, and in-house remote implementations; there is no empty Celeborn
  module and no claimed integration.
- Native keeps Java control, JNI bridging, C++20 execution, and a single shared
  library distribution.
- The new project modifies all callers directly, retains no old-FQN forwarding
  shells, and builds no data migration, dual-write, or legacy compatibility. Empty
  database initialization resources belong to schema-migrations. PostgreSQL's
  public and shuffle schemas maintain separate version histories; Shuffle Service
  loads the same resource artifact as a runtime dependency, and Coordinator
  recovery tests use the same two real schemas.

Public interfaces and factories uniformly belong to `.api`. Implementation
packages and classes directly state the operations, data, or resources they own.
Delete layers that only forward calls; merge duplicate implementations into the
type actually called. HTTP, Flight, JNI, OpenTelemetry, and database transactions
are named by concrete protocol or capability. Interfaces must explain the origin of
credentials, caller-visible errors, and resource-release order.

## Actual Package Boundaries of Flat Domains

Each of these domains publishes one JAR from its root POM.
`ModuleDependencyPropertyTest` checks the root path, artifacts, main source, and
submodule declarations. Source dependency direction is still checked inside the
domain.

| Artifact | Responsibilities within the same JAR | Calls across the boundary |
|---|---|---|
| Parser | `api.ast` is organized by expression, statement family, and type. `parse/build/format/diagnostic` consume these types. | Callers depend on parse results and AST; ANTLR tree construction details stay inside the implementation. |
| Batch | `api.block/page/reader` describes data access. `arrow` owns the Arrow representation, stream validation, and value conversion after detaching from buffers. | Callers that only read generic interfaces can load the contracts in an independent classloader. |
| File | `api.space/profile/authority` describes the read operations required for access. `access/directory` performs authorization and directory management, and `worker` implements delegated access. | PostgreSQL may load the same File JAR, but its source may reference only File's `.api`. Server injects the repository. |
| Planner | `api.fragment/metadata/statistics` exposes planning inputs and outputs. `analysis/validation/optimization/cost/substrait` analyze, validate, optimize, estimate, and serialize. | Coordinator borrows Planner to prepare plans. Workers read the protocol and need not load Planner. |
| Query | `api.submission/session/lifecycle/journal` describes sessions, state, and replay. `lifecycle/stage/dispatch/recovery/retention` execute business flows. | Query owns state transitions. HTTP response mapping, persistence, and execution processes each consume the corresponding contract. |
| Semantic | `api.identity/version/validation/publication` defines identity, immutable versions, validation results, and publication constraints. `validation` validates definitions. | The backend owns atomic writes of versions and published pointers, and the version contract retains CAS results. |
| Table | `api.ownership/observation/rename/maintenance/statistics/policy` separates table identity, maintenance operations, and bindings. Implementations organize calls by same-named capability. | Backend transactions and repository selection belong to Persistence and Server. |
| Worker | `task.state` owns task state constraints, `task.report` owns status reporting and acknowledgement, `execution` executes tasks, and `jni` manages the JNI boundary. | Bootstrap holds the production status-reporting client, and TaskManager borrows it. Failed startup and normal shutdown both release that client after task exit. |

## Rationale for the Seven Multi-Artifact Families

The paths below come from production dependencies. If the first and last paths
were combined into one JAR, the middle artifact would depend in reverse on the
merged JAR. Entry points, replaceable implementations, and actual compile
dependencies are all considered.

| Family | Artifact boundaries | Why the boundary is retained |
|---|---|---|
| Scheduler | Scheduling contracts, scheduling flow, builtin placement implementation | Query uses placement contracts directly. The scheduling flow needs Query, quotas, and cluster state. Server loads the configured strategy through `WorkerSelectionStrategyFactory`. Only the builtin load-balanced factory ships with the package. |
| Streaming | CDC and update contracts, streaming runtime, format and write implementations, Worker-JVM mutation backend | Both PostgreSQL and Coordinator consume streaming contracts. The runtime owns recovery, checkpoints, and watermarks. Canal, Debezium, and Maxwell each have service-descriptor entries. The Worker backend consumes the runtime's exact-range ingestor and the Worker task SPI; it never publishes a catalog snapshot. Writer runtime entry points and capabilities are accepted separately. |
| Cache | `cache-api`, `cache-memory`, `cache-redis` | Cache API constrains concurrent population by group and entry generation, and the two providers discover and close independently. Metadata borrows the contract to hold rebuildable projections, while Connector Catalog and the PostgreSQL outbox retain their own authority. Server Coordinator's Redis dependency is installed only at runtime. |
| Metadata | Catalog contracts, Catalog and projection runtime, table projection encoding | `metadata-catalog → persistence-catalog-api → metadata-api`. Catalog PostgreSQL and cross-domain Workflow PostgreSQL need the same table projection encoding, and neither needs to create a Catalog registry. General Jackson configuration belongs to Common. |
| Materialization | Definition and extension contracts, materialization flow, builtin refresh strategies | `materialization-lifecycle → persistence-materialization-api → materialization-api`. The flow owns attempts and publication order. All three refresh strategies consume the same snapshot input, and results are still published by the same materialization flow. |
| Indexing | Index contracts, build and lifecycle, query predicate encoding, builtin strategies | `indexing-lifecycle → persistence-indexing-api → indexing-api`. Iceberg and Protocol use the contracts. The build flow owns attempts, and neither strategy factories nor encoders need to create a build coordinator; the builtin artifact registers nine `IndexStrategyProvider` implementations. |
| Generation | Generation identity and values, shared encoding, commit and publication flow | `generation-workflow → persistence-generation-api → generation-api`. Iceberg, Paimon, Planner, and PostgreSQL all use the encoding directly. The commit and publication flow selects existing generations, binds inputs, and validates commit results. |
| Execution | Execution identity and deadlines, pre-submission task binding | `execution-binding → generation-api → execution-api`, with another path returning identity contracts through Protocol. Query, persistence, and wire can load identity contracts separately. The other artifact is responsible only for pre-submission task requests, options, splits, authority, and lease binding. |

Helper code that truly serves a single implementation is maintained in the same
package as its callers. For process artifacts that already have a set of capability
packages, do not create another `core` under each capability package, and do not
produce one JAR per operation in the tables above. Renames or merges must preserve
the verifiable boundaries of these call paths and ports.

## Trade-offs Relative to Upstream Sources

- Iceberg's build declares API, Core, Arrow, Parquet, cloud backends, and their
  bundles separately. Pangrova follows the same dependency direction so callers
  compile against contracts and the distribution layer decides whether to carry
  implementation SDKs. Whether each domain splits into JARs is still determined by
  actual calls and lifecycle. [Iceberg build definition](https://github.com/apache/iceberg/blob/main/settings.gradle)
- Paimon's `AbstractCatalog` organizes table operations at the Catalog layer using
  separate file access, schema management, and table commit components. Pangrova
  assigns external table snapshots and commits to Connector and local control-plane
  records to domain-specific Persistence. Materialization commands, candidate
  generations, and published heads share atomic operations, and a new backend must
  validate their concurrency and replay semantics. [Paimon Catalog implementation](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java)
- SeaTunnel declares factory identifiers and configuration validation in Factory,
  with data-source types and discovery entry points in TableSourceFactory. Paimon's
  HiveCatalogFactory accepts CatalogContext and returns contract Catalog. Pangrova
  likewise declares strongly typed factories by capability, with Server selecting
  and validating and the backend owning the SDK instance and its close.
  [SeaTunnel Factory](https://github.com/apache/seatunnel/blob/dev/seatunnel-api/src/main/java/org/apache/seatunnel/api/table/factory/Factory.java) · [SeaTunnel TableSourceFactory](https://github.com/apache/seatunnel/blob/dev/seatunnel-api/src/main/java/org/apache/seatunnel/api/table/factory/TableSourceFactory.java) · [Paimon HiveCatalogFactory](https://github.com/apache/paimon/blob/master/paimon-hive/paimon-hive-catalog/src/main/java/org/apache/paimon/hive/HiveCatalogFactory.java)
- Trino reads role configuration in `ServerMainModule.setup`, installs
  CoordinatorModule or WorkerModule, and then establishes shared bindings. Here
  Module is an in-process assembly unit. Pangrova keeps role selection and common
  lifecycle in Server and separately constrains JNI loading scope with Maven
  dependencies and real JAR checks. [Trino ServerMainModule](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/ServerMainModule.java)

### Cache Ownership of SQL Text and Parse Extensions

Common's `SqlText` retains literals, quoted identifiers, comment bodies, and line
comment end positions. Parser uses it to collapse whitespace between built-in
syntax tokens. Planner's diagnostic baseline also folds the case of ASCII letters
outside quotes; both retain their own cache and identity semantics.

After extensions are registered, Parser uses the registry snapshot version and the
original SQL as the key. Parser selection, parsing, and cache writes belong to the
same snapshot; after an extension is replaced or unregistered, an older parse,
even if it finishes later, can only write under the old-version key. Cache capacity
limits apply equally to old-version entries.

## Determining and Handling Cycles

First choose the graph and analysis scope, then define edges. In the artifact
graph, `A → B` means building A with Maven requires B; in the package graph, A's
production source references a type from B; in the wait graph, A's current
operation must wait for B to satisfy a condition before continuing. In the **same
graph**, going from A along dependency edges through other nodes and back to A
constitutes a cycle, including indirect paths such as `A → B → C → A`. List all
mutually reachable nodes by strongly connected component (SCC), and provide at
least one closed path with the source of each edge. A file that directly includes
itself also counts as a cycle; references between classes in the same package are
reviewed within the package.

A review must also state phases and conditions. For example, when Workers report
completion, a bidirectional message flow can be drawn by role; a lifecycle wait
cycle exists only when both operations must wait for each other in the same
reachable state and neither can make independent progress. Investigating such a
problem requires writing down the specific wait condition, who triggers it, and
whether the trigger can run at that moment. Timeout termination can release
resources but cannot make a startup flow that originally waited on itself succeed.

Calling interfaces and extension interfaces both belong to the domain's
`.api.<capability>`. A single implementation used only within the domain ships
with the domain artifact. Contracts shared by multiple implementations are
released separately. Before merging, collapse candidate artifacts into one node
and check every path that returns through external artifacts; after merging,
continue checking packages inside the JAR. If a calling package returns an SPI
type and the SPI also accepts request types from the calling package, first find
the minimal contract each side consumes. For example, when the reader side needs
only batch data, the batch data and its ownership are defined by the flat Batch
module, and the read interface and factory reference it separately. If they truly
are the same capability and always evolve together, the related interfaces may be
placed in that capability package. Merging Core, Provider, or Server requires
separate justification.

| Object of inspection | Decision and handling |
|---|---|
| Maven artifacts | Check the whole reactor build order under actually enabled profiles. Track production dependencies, test dependencies, classifiers, and build plugins separately. If Maven must build the other side first, include it in the build graph. An aggregator parent POM listing submodules does not constitute a production-code dependency. |
| Java production packages | Check capability packages across distribution artifacts and within the same JAR. Two or more mutually reachable packages form one strongly connected component. Locate edges with real type references, including method signatures, generics, annotations, factory return values, and fully-qualified references. Collaboration among multiple classes in one package is judged by responsibility. |
| Plugins and runtime roles | Server may install providers, and providers are called through domain interfaces. Validate the factory's classloader, service descriptor, and role distribution classpath, and check whether Core still looks up providers at runtime by implementation class name. |
| Native | Validate CMake build order for each target. The Native gate parses literal includes in repository source; any file cycle is reported with per-edge line numbers and fails. Group by the source directory corresponding to each OBJECT target; command, jni, engine, capabilities, indexing, vector, udf, and lance currently belong to the same strongly connected component. For example, command references JNI's RuntimeInitializer, and JNI references command's FlightServerRegistry. Use `python3 .pangrova/scripts/check-native-includes.py pangrova-worker-native/src --components` to export edges and evidence. Component source dependencies and CMake target build order are accepted separately. |
| Runtime lifecycle | Draw "must wait for the other to complete" as wait edges. A must not wait for B ready while B waits for A ready, and a lock-holding callback must not make the other side wait for the same lock. Server may create instances first, then register callbacks, and only expose access after capability confirmation. Each resource has exactly one closer; borrowers stop using it first and the owner releases it. Verify with startup failure, cancellation, concurrency, and reverse-shutdown tests. |

### Which Edge to Cut

**The same rule applies between APIs.** If `A-api → B-api → A-api`, mark the public
signatures on each edge that introduce the dependency. First determine who
interprets the request, return value, error, or callback, and who has the authority
to change its semantics. Only values with consistent identity, constraints, and
lifecycle belong to the same contract. If B needs to report the result of A's
processing, B may declare the completion-notification interface it actually calls,
with A providing the implementation and the assembly entry passing the instance.
B then knows only its own notification contract at compile time. If the
notification is part of a persistence commit, also check the transaction
requirements below.

**Assembly must also be orderable.** If A's constructor needs a started B and B's
constructor needs a started A, the review must state when each first actually uses
the other. Construction, subscription callbacks, and exposing traffic can be
phased; the assembly entry must allow business callbacks only after subscriptions
complete, and on shutdown must stop new callbacks, wait for in-flight calls to
exit, and then release resources used by the callbacks. If two objects must jointly
decide the same state transition during construction, return that transition to a
single owner. Wrapping references in `Supplier` still requires validating the
first-fetch order.

1. **Find the semantic owner first.** Record who owns the value's identity and
   validation, who decides state transitions, who commits transactions, and which
   operations each side actually needs. First remove leftover imports, incorrect
   scopes, or stale dependency declarations. The same pair of modules may have
   multiple independent paths; check each one.
2. **Shared values return to the smallest owner that defines their semantics.**
   Execution leases across Query, Generation, and the execution protocol belong to
   Execution. Protocol may reference identity contracts, and the query state
   machine need not enter Worker with wire DTOs. Values that look similar but have
   different lifecycles or scopes are still owned separately.
3. **The domain defines the required operations, and implementations depend on the
   contract.** File's authorization service needs to look up spaces by name or ID
   and read the current profile. File declares two read contracts,
   `FileSpaceLookup` and `CurrentStorageProfile`. Persistence's `FileSpaceRegistry`
   and `StorageProfileRegistry` extend them respectively, and Server injects the
   actual instances. The compile dependency points from Persistence to File, and at
   runtime File can call the repository through the interface. The semantics of
   historical revisions, conflicts, and authorization failures must be preserved.
4. **Place multi-party orchestration with the real caller.** When two domains
   together complete an operation, the side that owns the business flow orchestrates
   it. Cross-domain atomic writes use a strongly typed workflow that preserves the
   same connection, lock order, fence, and commit owner. Server performs assembly,
   and the workflow implementation owns transaction commit.
5. **Determine artifacts by independence.** If two packages jointly maintain the
   same state machine, share the same lifecycle, and have no independent callers,
   the related behavior can be returned to the capability package that owns the
   state machine, and the forwarding layer deleted. Merging Maven modules alone
   still requires checking the internal package graph. Multiple implementations
   share an independent contract, with factories in the domain's `.api` discovered
   through service descriptors. Do not hide dependencies by moving them into
   Common, static setters, or bidirectional lookups.
6. **Place backend integration tests where both sides can be depended on.** If
   Table's test uses a real Memory backend and that backend implements Table
   interfaces, put this test group in Memory or Integration Tests. Preserve the
   original assertions, real objects, and failure coverage. Methods read by
   production code are provided by their production dependencies.

**When transactions are involved, first decide whether the callback can be
deferred.** For example, Metadata projections and invalidation outbox must be
written by PostgreSQL in the same transaction, while notification delivery can
happen after commit. If writing to the outbox is changed to an in-process
asynchronous event, a process that exits after commit will lose the notification.
Even if this split eliminates a Java package cycle, it does not satisfy acceptance.
If two workflows lock A then B and lock B then A respectively, first confirm
whether the two paths can hold locks concurrently; for reachable wait cycles,
unify the acquisition order, preserve the authority and fence recheck after
acquiring locks, and validate the contention result with two independent
transactions.

### Merge Conditions

Native components eliminate cycles incrementally along directed edges. The eight
existing components belong to one strongly connected component; comparing only
component count or member count would miss new reverse dependencies inside the
component. `check-native-includes.py` checks every directed edge inside the
component against the known scope. Introducing a new component cycle, routing back
through a new component, or adding a cycle edge inside the old component is blocked
by the existing Native boundary gate, which reports source locations. Independent
components may add one-way edges that match responsibilities. When eliminating the
last circular reference between components, remove the corresponding record from
the gate in the same batch and rescan; deleting only one include still requires
checking other callers in the same direction.

Each migration batch is submitted by the developer modifying the dependency as a
reviewable record listing at least **the graph kind, the closed path and per-edge
sources, the semantic owner, the edge to delete, the replacement dependency
direction, and the validation commands and results**. A strongly connected
component may contain multiple cycles; after cutting the example cycle, recompute
components on the whole graph. Cross-domain transactions must also validate the
original transaction boundary, and lifecycle changes must validate startup failure,
cancellation, and resource release.

- **Directly block merge**: the enabled Maven reactor cannot be ordered, Java
  production packages form a multi-package cycle, repository-native source has a
  file include cycle, or processes must wait on each other to complete startup,
  cancellation, or shutdown. Even when the graph is acyclic, a one-way dependency
  that violates a role boundary is rejected separately, for example a Coordinator
  artifact carrying a Worker execution implementation.
- **Known Native component coupling**: when touching an existing component cycle,
  record the relevant component edges and source locations before and after the
  change, and move types, reverse calls, or register implementations from entry
  points according to the semantic owner. New or expanded component cycles must be
  eliminated first. When the target split completes, recheck the components
  involved; mark a component decoupled only after mutually reachable paths
  disappear from the component graph. Lance and task commands both validate the
  repository path, Catalog, and tenant through
  `engine/resource/WorkerStorageBinding`. The Native include gate blocks the
  removed direct `lance → command`, `lance → jni`, `engine → command`, and
  `udf → command` edges. The Lance schema-fingerprint command borrows the memory
  governor provided by the command entry. JNI binds the execution error-reporting
  contract once when creating the JVM function callback. Velox operator statistics
  are encoded in `engine/velox/runner/TaskStatsEncoding`. The executor binds
  `TaskFailureReporter` together with the positive handle, and HandleRegistry
  revalidates the handle when reporting errors. The JNI entry passes cleanup
  operations for the same registry to the engine, withdrawing tasks before closing
  pools and threads on restart, exit, or initialization failure. Indirect paths
  such as `engine → jni → command` still exist.
- **Allowed bidirectional interaction**: requests and responses, progress reports,
  and completion notifications may flow in both directions. At compile time both
  sides depend on stable contracts; startup and shutdown check wait order
  separately. ServiceLoader, reflective class names, deferred construction, event
  buses, `optional`, or `test` scope cannot replace validation of the real
  dependency direction.

Before flattening, treat the artifacts to be merged as one node on the existing
dependency graph. If a path exists from the candidate merge artifact through an
artifact still released independently and back to the merged set, the merge will
create a cycle. For example, merging Metadata Catalog and API would turn
`metadata-catalog → persistence-catalog-api → metadata-api` into
`metadata → persistence-catalog-api → metadata`. In that case, keep the
independent contract or first remove that dependency according to semantic
ownership and recheck. Direct dependencies inside the merged set disappear from the
artifact graph, but packages inside the same JAR still need individual checks.

Before changing POMs, a read-only precheck can be run on candidate leaf artifacts.
Metadata's two candidate artifacts still have an independent path returning through
codec:

```bash
.pangrova/scripts/generate-module-baseline.py --check-merge pangrova-metadata-catalog pangrova-metadata-api
```

The current output is `metadata-catalog → metadata-codec → metadata-api`, and both
edges are compile. The tool searches explicit dependencies in local POMs
(including test and runtime) for the shortest path that leaves and returns to the
merged set, printing each edge's scope and POM and returning 1 when found. Thus
even after eliminating the path returning through Persistence, this flattening
would still create a cycle. The precheck covers direct declarations in the current
source tree. Inheritance, profiles, plugins, generated code, and runtime class
loading are still checked by the gates below.

**When flattening multiple domains in the same batch, collapse all candidate
groups together.** For example, A's implementation uses B's API and B's
implementation uses A's API. Flattening either domain alone keeps the build
orderable, but flattening both yields `A → B → A`. Repeat `--check-merge` for the
same command, with each group listing the leaf modules to merge into one artifact.
The tool searches the merged graph for a cycle through the candidate groups and
prints the original POM edges forming the cycle. Checking only one group at a time
misses this dependency.

- Provide dependency paths before and after the change and the specific types
  involved. Strongly connected components containing two or more packages in the
  main-source package graph must be reduced to zero. Adjusting `allowedImports`,
  `friendExports`, or regenerating the baseline can only record justified one-way
  edges.
- Validate the reactor for the default and affected distribution profiles. Run
  `PackageDependencyAcyclicityPropertyTest` on the default reactor main source.
  The report lists one actual closed path for each cyclic strongly connected
  component and the source location of every edge inside the component.
  `ModuleDependencyPropertyTest` separately validates package ownership, allowed
  edges, and service registrations. Check the actual JARs and single-role runtime
  classpath to validate reflection, generated code, and JNI entry points.
- Execute at least one real call and the corresponding failure scenario for each
  changed path. Transactional paths validate failure rollback and lost-owner
  rejection; resource boundaries validate failure cleanup and exactly-once close.
  Record the actual result for each path, with build, package-graph, and runtime
  evidence reported separately.

From the repository root, the distribution-profile build order, production-package
cycle detection, and the gate's own regression cases can be rechecked:

```bash
mvn -B -ntp -Pnative-runtime,full-regression validate
mvn -B -ntp -pl :pangrova-testing-kit -am \
  -Dtest=PackageDependencyGraphTest,PackageDependencyAcyclicityPropertyTest,ModuleDependencyPropertyTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The package-graph report is at
`pangrova-testing/pangrova-testing-kit/target/package-dependency-cycles.txt`.
Acceptance requires the report to list the actual number of scanned packages and
`Cyclic components: 0`. Maven `validate` checks the reactor order for the current
profile; full `verify` and role startup tests are still executed before release.

**Review example.** The derived kind used for lineage was once declared by a
nested enum in Query, and Governance API had to load Query to reference that value.
Governance interprets this vocabulary, so it now belongs to
`governance.api.enums.ColumnDerivationKind`. Planner's Calcite column lineage,
Query's column record, and Governance's dataset column record use the same value.
Planner and Query each depend one-way on Governance API; Coordinator can pass the
derived kind directly. Query execution, state transitions, and replay remain owned
by Query. When reviewing similar changes, the commit record should list the
original references, the new direction, the semantic owner, and the corresponding
gate or behavior test.

**Shuffle's bidirectional runtime calls.** Coordinator scheduling uses
ShuffleTaskAccess and RecoverableShuffleControl. The Pangrova remote provider uses
ShuffleLifecycleEvidence from the same contract artifact to record registration and
terminal receipts. Server injects DurableStageShuffleEvidence into the provider, and
evidence writes follow Generation's transaction boundary. Each side needs to
reference only shuffle-api; switching backends does not require placing the
coordinator or persistence implementation in the plugin classloader.

**Worker's memory callback.** `NativeRuntime` accepts the `AllocationListener`
defined by the memory package. Previously the no-argument constructor of
`NativeSpillController` bound `NativeRuntime::nativeRequestReclaim` itself,
creating `worker.jni → worker.memory → worker.jni`. Worker's
`NativeRuntimeBootstrap` now injects the reclaim operation into the Spill
controller. JNI can continue to notify the memory arbiter to release accounting,
and the memory arbiter can request real Native reclamation; the production package
retains only `worker.jni → worker.memory`. Check the package graph, reclaimed byte
counts, and the quota released by the arbiter separately.

**Coordinator and Worker.** Coordinator dispatches tasks and Workers report status;
network messages can exist in both directions at the same time. Task identity and
attempt are owned by Execution contracts, and message structure is owned by
Protocol. Both ends depend on contracts and assemble transport implementations in
their own Servers. During independent startup, Coordinator waiting for a Worker to
come online is a scheduling condition; a startup wait cycle exists only if both
wait for the other to be ready before opening registration entry points. Role
integration tests must cover different startup orders, delayed Worker registration,
and exit.

Every build and package edge can be exported with
`.pangrova/scripts/generate-module-baseline.py --graph-dir target/architecture`.
module-dependencies.dot retains scopes declared in local POMs, and
package-ownership.dot lists all modules and their main-source packages. The build
order after profile activation and real role distribution artifacts are validated
separately.

## Implementation Order and Exit Conditions

| Phase | Scope | Exit condition |
| --- | --- | --- |
| P0 | Reconcile worktree, modules, FQN, capabilities, resources, and transaction ledger | Every existing module has an owner; current state and target can be verified separately |
| P1 | Types, Batch, Execution contracts, and Common cleanup | Base contracts load with a minimal classpath; the general model is independent of Connector |
| P2 | Server, Transport, Plugin, and Standalone assembly | The two roles' dependencies are isolated; three startup modes, failure cleanup, and shutdown are validated |
| P3 | Unified API/SPI artifacts across domains, migrating packages and callers | Actual JARs, service registration, and plugin loading agree; old classes and split packages are zero |
| P4 | Split Persistence ports and existing backends by domain | Empty-database initialization belongs to Schema Migrations; backend contracts and concurrency checks pass |
| P5 | Metadata, Table, Generation, Materialization state and transactions | Validate current authority, CAS, fence, outbox, unknown commit, and recovery |
| P6 | Parser, Planner, Protocol, Query, Scheduler division | The full query entry-to-execution path passes; Protocol owns only wire semantics |
| P7 | Unified abstraction for Direct, Local, and in-house Shuffle | Three real backends validate attempt, receipt, cancellation, backpressure, and fault guarantees |
| P8 | Remaining domains and replaceable providers | Claimed capabilities are reachable, explicitly assembled, and resources close by scope |
| P9 | Worker, Native, clients, test infrastructure | JNI, cancellation, result transport, and independent multi-Worker validation pass |
| P10 | Cleanup and release | Full regression, per-class and capability review, actual distribution-artifact inspection, and same-environment performance comparison complete |

Each migration batch covers the complete capability closure: production source,
tests, POM, service registration, reflective class names, configuration,
classloading policy, manifest, and documentation are updated together. Pure
structural moves and behavior changes are validated separately. A successful
compile or a complete directory tree does not constitute phase completion.

## Validation

Run affected modules first, then expand to consumers and the full reactor. The
minimal classpath of shared contracts, public signatures, module acyclicity,
unique package owners, plugin class identity, and old-class cleanup in real JARs
must all be validated. JNI/protocol/Native behavior changes run the full chain per
build-pipeline; memory-ownership or concurrency changes add ASan. Finally run
`mvn -q -Pfull-regression verify`.

Persistence validation includes empty databases, CAS contention, idempotent
conflicts, lost owners, authority changes, related-entity atomicity, unknown
commits, recovery, and outbox. Shuffle validation includes multiple
producers/partitions, empty partitions, retries and speculative isolation, unknown
commits, active-read and cleanup races, cancellation, bounded queues, descriptors,
authorization, and Native buffers. Fake implementations must not replace real
providers or end-to-end evidence.

Validation records the actual commands, exit status, first failure, fix,
environment blockers, and unfinished items. Earlier passing results are not
inherited by a later change; unsupported capabilities are marked explicitly, and
the existence of a class or interface must not be used to claim support.

## Implemented Execution Base and Shared Encoding Boundaries

`pangrova-execution-api` owns SDK-free task stage/purpose/deadline;
`pangrova-execution-binding` owns canonical request/option/split binding,
authority context, and exact lease identity. Inputs no longer depend on Planner
StageNode, and Core does not depend on Coordinator, Worker, or PostgreSQL
implementations. This boundary does not take over scheduling, authority
transactions, lease renewal, or Native resources.

`pangrova-shuffle-api` owns Common/JDK-only domain contracts; Planner and
Operations depend only on that contract artifact. The former ShuffleTransport
implemented only Native service binding and has been removed from the general API.
NativeShuffleProducerRegistration lives in the `org.pangrova.worker.shuffle`
package of `pangrova-worker` and registers producers through the task owner's
protected command channel using a borrowed executor handle. The C++
FlightServerRegistry holds the process-shared listening port. The Worker task
owner drains active reads and writes, unregisters the producer, and then releases
the executor. Direct bytes remain in C++.
`org.pangrova.shuffle.api.worker.ShuffleByteEgress` in the unified artifact
defines real batch delivery, producer completion, and abort callbacks, used by the
assembled remote implementation. Batch completion does not equal producer commit,
and producer commit does not equal exchange publication. Caller buffers are
borrowed only during the synchronous callback; rowCount is not a batch sequence;
task and query IDs only index the authorized complete attempt binding.
Pangrova remote delivery is assembled by factories from both roles. Workers
connect to JNI through strongly typed contracts in `pangrova-shuffle-native`, and
Coordinator requests registration, publication, and authorization through Shuffle
API. `pangrova-shuffle-pangrova` owns the client, egress, reads, mTLS, and
connection quotas. Server installs the implementation in role runtime packages.
The former onPartitionCommitted and DefaultShuffleByteEgress have been deleted.

Setting `coordinator.shuffle.mode` and `worker.shuffle.mode` to `LOCAL` enables
Native local-disk exchange. The scheduler selects one Worker for a DAG with an
exchange and checks that Worker's admission domain, incarnation, tenant admission,
and operator capabilities each time. Production tasks carry `MEMORY_AND_DISK`; the
Flight service forcibly writes batches to anonymous disk segments by producer
handle, using process-shared disk, file, and transport quotas. After all upstream
stages complete, downstream reads with a partition ticket issued by that process,
and the Worker verifies the process's real Flight address before establishing a
connection. After read completion or cancellation, the Native registration and
reader release data and handles. LOCAL rejects speculative execution, replacement
attempts, Worker-generation recovery, and persisted publication receipts.

Java LocalFsSpoolStore provides explicit attempt segments and cleanup for Java
callers. Generation can store versioned allocation descriptors for any provider;
the Pangrova remote implementation currently encodes and recovers them. Celeborn
integration is follow-up work.

The new `pangrova-security-codec` is an independent boundary for actually shared
encoding infrastructure: it exclusively owns the canonical encoding and digest of
DeferredAuthorizationScope, depends on Security/Connector API and Jackson, and does
not depend on the database or runtime Core. Encoding was moved verbatim out of the
PostgreSQL Provider; Security API does not take on complex serialization, and
transactions and current authority checks remain with the original owner.

Concrete semantics, callers, and unfinished boundaries are governed by the current
code, tests, and `.pangrova/modules.yaml`.


## Shuffle Independent Capabilities and Effective Guarantees

`org.pangrova.shuffle.api.capability` in the unified API artifact owns
ShuffleCapability and ShuffleCapabilityDescriptor. Consumer retries, producer
retries, speculative-attempt isolation, readability after a compute Worker exits,
readability after a storage Worker failure, Coordinator session recovery,
blocking/pipelined reads, partition/range reads, retention, and cleanup are
declared separately; there are no implicit capabilities, and none are inferred
from storage medium or PUSH/PULL. The effective guarantee can only be the
intersection of the implementation, the deployment, and the evidence of this
exchange. The descriptor is an immutable value and cannot serve as a commit
receipt, current authority, reader lease, or scale-down approval. There is no
legacy-type compatibility shell.

The failure entry of the former FTE policy helper is explicitly a consumer
failure, avoiding treating input re-readability as safe replay of producer output.
It and the retry validator have not entered the production DAG and do not
constitute a task-level FTE guarantee that has shipped. The former
SpoolingExchangeManager has been retired. Local spool files alone can confirm only
the integrity of a single attempt and partition. The production retry owner remains
Stage/RetryStageHandler; test helpers must not be assembled in parallel as a
second authority. Future general session/receipt support must connect to that real
owner and validate deployment and per-exchange state.


## Local File Storage Primitive Boundaries

`pangrova-shuffle-local` owns the real LocalFs file implementation and the sole
SpoolStoreFactory service registration; SpoolStore/Reader/Writer/Factory/Context
belong to `org.pangrova.shuffle.api.spool` in the unified API artifact. Core's Java
page entry `SpoolBackedExchangeManager.open` receives the assembler's classloader,
enumerates factories, and creates the backend by the configured exact ID. Missing,
blank, and duplicate IDs fail before creating a Store. The Manager releases the
Store on close. Core has only a test dependency on the Local backend.

Reads must provide an immutable SpoolReadSelection that explicitly selects
producer → attempt; selecting the maximum attempt, truncating long to int, or
treating a missing selected segment as empty data is forbidden. The file format has
magic/version, per-batch boundaries and SHA-256, an end record, and total-count
checks; one read covers at most one bounded batch. Write completion completes only
one attempt/partition segment and does not equal exchange publication. Concurrent
writes to the same attempt and overwriting a completed segment are rejected.

The directory has a single OS file-lock owner; cleanup is rejected while read/write
resources are alive and may be retried after close. Store close handles its
resources before releasing the directory, preserving and propagating exceptions.
The new project does not read old raw concatenated files and builds no data
migration or dual-write tooling. Files landing on disk, atomic moves, and reopening
files do not constitute node-failure, session-recovery, or scale-down guarantees.

Java spool exchange explicitly receives a selector and accepts only the batch
formats and blocking read mode it supports; a failed read is not converted into a
normal EOF, and an aborted sink is not allowed to finish as a false success. This
component is still not integrated with Native, and storage keys still lack the
complete application incarnation/exchange generation; a complete Local Provider
support claim requires general session, receipt, asynchronous bounded data plane,
and Native adaptation to be completed. Existing lifecycle-prototype and inventory
assembly gaps do not disappear because the file implementation was split out.

### In-house Shuffle Provider and Shared Encoding Boundaries

In REMOTE mode, Server Coordinator discovers the configured factory through
`ShuffleCoordinatorFactory`. The factory validates configuration and creates a
process-specific `ShuffleCoordinatorSession`. QueryExecutionBootstrap injects the
session contract into the DAG, and Server closes the session after stopping the
query. PangrovaShuffleCoordinator registers through the provider's own service
descriptor, and the Server runtime distribution installs that implementation. Its
registration, publication, cleanup, task grant, gRPC channel, mTLS, and response
validation all belong to `pangrova-shuffle-pangrova`. The DAG requests short-lived
authorization for the bound Worker incarnation and attempt through
`ShuffleTaskAccess`. Each grant is independently concurrent, and shutdown first
drains accepted grants. A backend implementing `RecoverableShuffleControl` can
receive the `ShuffleLifecycleEvidence` journal for that execution. Generation
persists the validated immutable receipt and the original credential-free response
under the current owner and allocation. Callback interfaces are published with
Shuffle API; Provider and Coordination Control depend only on the shared contract
and have no compile dependency on each other.

The actual neutral identities of ExchangeTaskAttempt, ExchangeAttemptFence, and
ExchangeChannelContext belong to `.api.identity` in the unified Shuffle API;
identities are not copied, fields are not modified, and fencing validation is not
changed. The shared producer-set and partition-manifest digest algorithms belong to
Protocol's `.shuffle.codec`, used together by the client, server, and Coordinator
while keeping encoded bytes unchanged. These canonical wire encodings are not
placed in the minimal API and do not require providers to depend on business Core.

The Pangrova remote reader receives PangrovaPartitionRead from its own
implementation artifact, containing the selected channel and the expected server
publication evidence. Credentials are renewed at batch boundaries. Each source's
gRPC cancellation scope includes renewal and partition-read requests; shutdown
first cancels both request kinds, then waits for reads to exit and RPCs to finish,
and finally returns the channel lease. The producer cancellation scope also covers
writes, renewal, and commit acknowledgement, and waits for active calls to exit
after cancellation. Canceling a commit RPC preserves the unknown-commit-result
semantics. The Worker is responsible for Native input binding.
The Worker selects `NativeShuffleWorkerFactory` through `worker.shuffle.provider`
and passes only the selected factory's configuration prefix. The factory validates
parameters and opens a session, and the Worker verifies the capabilities actually
enabled by the deployment before publishing resources. Production and read pools
for each Worker generation share total channel and endpoint quotas.
NativeShuffleReaders open the corresponding partition from the validated external
binding, and the remote implementation selects its endpoint and validates
publication evidence. NativeExchangeAttempts uniformly encode and decode execution
owner, attempt fence, and Worker incarnation between Worker and Provider. The
Worker production artifact compiles only against Shuffle contracts and the Native
wire boundary, and Server Worker installs the selected implementation in the actual
distribution. The Worker reports the actual mode and Provider ID during
authenticated registration, and Coordinator validates the deployment before the
member can participate in scheduling. WorkerSnapshot writes deployment information
and the process incarnation into Memory or PostgreSQL, and takeover, recovery, and
heartbeat rebuild continue to validate them.

PangrovaPartitionRead is an in-process read request. Coordinator publication uses
`ShufflePublication` to specify exact query, stage, generation, and allocation;
stale requests are rejected before reading the cache or calling the RPC.
`ShuffleCommitReceipt` stores the confirmed logical producer-set digest plus the
last sequence number, row count, byte count, and content digest of each output
partition. A stage clears old receipts on a new generation. Blocking exchange
scheduling releases downstream only after the upstream receipt is available, and
the read binding captures routing, owner, and receipt under the same lock. Each
remote read binding carries the allocation, epoch, logical producer-set digest, and
the last sequence number, row count, byte count, and manifest SHA-256 of the
specified output partition. Java and Native check evidence completeness at
admission. The Worker checks the published upper bound before delivering a chunk to
Native and verifies the Coordinator receipt, service terminal frame, and locally
recomputed manifest before acknowledging completion to Native. Empty partitions
also require a complete receipt and terminal frame.
Transport failures or content corruption in remote reads are classified through
Shuffle API's `exception.ShuffleReadException`, and the Pangrova provider
constructs backend errors in its own `client.exception` package. Native wire
channel-quota errors produce a retryable execution-protocol diagnostic in
`nativewire.exception`; the Worker reports the actual channel count and limit
through that type.

The current remote service confirms the logical producer set and cannot yet verify
the caller-selected speculative attempt, so assembly rejects enabling it together
with speculative execution. Direct and Local each use their original attempt
bindings. Shuffle API read and write lifecycle interfaces use only domain types.
Current task transport and the Native wire still express endpoints, commit
addresses, and short-lived authorization. Providers using other transport layouts
must implement the corresponding task binding and explicit producer-attempt
selection.

The persistence journal accepts an allocation ID, provider ID, positive integer
version, and an immutable payload of at most 1 MiB. Generation stores this envelope
plus the shared execution epoch, producer set, and partition count. The
PangrovaAllocationCodec v1 encodes only the allocation ID, commit address, and exact
partition range; recovery rejects credential fields, unknown fields, range
conflicts, or non-canonical bytes. Coordinator uses the configured control endpoint
to request a finalize grant for the existing allocation under the current owner's
fence, then requests the current access credential and compares the original
descriptor. When preparing partitions, the actual read/write binding verifies the
registration record through the same provider encoder in the transaction that
acquires the current authority and partition lock in PostgreSQL.

### Pangrova Remote Partition Reads and Cleanup

The service registers a read only after validating the current consumer, read
authorization, partition range, and finalized receipt. The PostgreSQL row lock on
the same query serializes read-pin creation and cleanup admission. The earlier of
the authorization and request deadlines determines the readable deadline, and
batch boundaries check both the current execution owner and the registered pin.
The protection window additionally covers a single object-storage GET timeout so
that I/O already started near the deadline has time to drain.

While an active pin exists, cleanup from any service instance receives a retryable
precondition failure. A scope already in CLEANING rejects new reads. Backpressure
pauses retain the pin; send completion, cancellation, and failure release it, but
release happens only after the in-progress I/O finishes. After a service process
crash, the database deadline allows cleanup to continue. A partition whose metadata
ends early must not return a terminal frame. This mechanism protects one
time-bounded authorized read; provider fault tolerance and speculative execution
are still admitted separately per factory declaration.

### Retracting False Local Publication and Inventory Safety Inference

SpoolingExchangeManager and LocalSpillLifecycleManager had no production callers
and could not derive whole-exchange publication from a single file. This round
deleted both duplicate prototypes and their false-commit tests. The real Local
Provider's I/O, attempt selection, ServiceLoader, close, and reader-cleanup
protections were retained and validated. Native LOCAL reuses the Worker-level disk
exchange with signed tickets. Persisted session/receipt and reads after node exit
remain unsupported capabilities.

When the DAG lacks an external lifecycle, it uses an explicit
UnavailableExternalShuffleLifecycle: registration and commit fail, and cleanup
performs no action solely because no external allocation exists. Direct Native
task/output resources are still cleaned by their original owner. ShuffleHandle
LOCAL does not use an empty endpoint to represent successful registration.
Publication requests for unknown stages and stale generations are rejected, and
empty output partitions are explicitly recorded in the receipt.

ShuffleDataInventory cancels the default scale-down decision based on lists and
retention; the concrete owner must provide complete and fenced current evidence.
InMemoryShuffleDataInventory remains in Coordination test source, simulates manual
diagnostic records, and returns UNKNOWN even when empty, marked consumed, or on
persistent media. Safe scale-down still requires Worker incarnation, admission
closure, complete execution/reader inventory, and proof valid during removal; the
current WorkerManager task-count drain logic cannot yet replace these requirements.


### Worker Registration Identity Revocation Boundary

Coordination API owns WorkerRegistrationIdentity (logical ID, admission domain,
incarnation), and Persistence's WorkerRegistry defines atomic unregistration by
that identity. PostgreSQL implements it with three conditions in one DELETE, and
Memory compares in the same per-key compute; unconditional logical-ID deletion or
read-then-delete must not return. HTTP wire uses the existing
WorkerExecutionIdentity, converted by Coordinator, without introducing Protocol
into Persistence API.

WorkerRegistrationClient's scope is one process registration attempt: it binds the
original identity before the first send and reuses that identity for retries and
the actual shutdown DELETE. WorkerManager's local conditional removal uses the
original observation, and the backend separately protects different incarnations.
This does not replace complete safe scale-down, persisted admission state, or
physical resource drain; the existing Shuffle inventory gaps continue to be
implemented.


### Worker Heartbeat and Auto-Repair Identity Ownership

WorkerRegistry heartbeat must carry the complete WorkerRegistrationIdentity and
return the atomically conditionally updated WorkerSnapshot or empty. Core must not
publish local success before persisting, nor infer sender identity from the
logical ID. Missing and replaced cases are expressed through Coordination API's
WorkerHeartbeatStatus and mapped to responses by the HTTP entry.

Initial registration and auto-repair are separate: the client fixes its process
identity before the first send, and subsequent requests may only perform
conditional refresh. PostgreSQL validates identity inside one conditional
ON CONFLICT write, and Memory uses a single-key compute; read-then-unconditional-upsert
is forbidden. A Worker explicitly marked replaced permanently closes new
admission/publication through the existing TaskAdmissionRegistry, and Sender does
not auto-recover. Closing admission does not take over cancellation, Native
handles, output readers, or shutdown resource ownership, nor does it prove drain.

Global ordering of the first unconditional registration, persisted
DECOMMISSIONING, readiness/admission state integration, and exact inventory remain
follow-up work; safe scale-down and a complete Shuffle lifecycle are not claimed.


### Metadata Shared Encoding Boundary

Metadata API/SPI share an artifact, and domain values do not depend on persistence
ports. Catalog Persistence API depends on Metadata API; the PostgreSQL Provider
does not depend on Metadata Catalog. The former table metadata encoding was reused
by both the cache and PostgreSQL paths, so `pangrova-metadata-codec` owns
`TableMetadataSerializer`; general JSON read/write directly reuses Common's
`CanonicalJsonMapper`. Deployment-history reads must not default to returning
"does not exist"; an unsupported history query must be rejected explicitly and
must not substitute current for a historical revision. Invalidation cleanup is
injected into the domain scheduler through a narrowed port, and Server owns the
lifecycle of the real provider. This boundary does not mean the remaining Metadata
cross-domain workflows, the Table default backend, and Governance ownership are
complete.

### Domain Ports for Metadata Synchronization Progress

Synchronization progress and diagnostics have been extracted from the composite
MetadataStore into Catalog Persistence's `synchronization` contract, and the
PostgreSQL Provider is explicitly assembled by Server. Progress is bound to the
deployment incarnation/revision and may advance only within the complete coverage
boundary declared by the source; local end times and diagnostic history do not
constitute progress. The boundaries of atomic completion, idempotent receipts, and
unknown-commit acknowledgement are constrained by the current Catalog Persistence
implementation and tests. Splitting projection transactions, the remaining
composite Store, and the Federation SPI remains follow-up work.

### Catalog Deployment and Projection Composite Store Retirement

The composite MetadataStore and PostgresMetadataStore have been retired with no
forwarding shell. Deployment, table projection, schema projection, and
synchronization progress are injected through independent ports; Server selects
the PostgreSQL Provider. Domain Core does not depend on backend helper classes, and
the transaction-support package is restricted by the manifest to artifact-internal
use. The original SQL and per-operation outbox atomicity are preserved; a
conditional projection transaction for a full scan remains follow-up work and
cannot be inferred as complete from this responsibility split.

### Binary scalar and heap batch ownership

Types owns scalar binary snapshots; construction and raw-value access must not
expose the owned byte array. SQL NULL uses the existing NullValue representation,
while an empty binary value is valid. Batch API owns NativePage as an encoded heap
snapshot, not a JNI handle. New payload reads fail after close; previously returned
copies and size/format metadata remain valid. Consumers that need only the encoded
size use the non-copying size query. These guarantees do not imply Native zero-copy
buffers or completed Shuffle buffer-lease integration.


### Metadata Projection Write Transactions and Receipts

`TableProjectionRepository` and `SchemaProjectionRepository` are narrowed to
read-only ports. Writes go uniformly through `MetadataProjectionTransactions`,
carrying the full Catalog deployment identity of the original handle and the
operation ID. PostgreSQL validates the deployment, reads the prior value and
tenant impact, updates the projection, and writes the invalidation event and
receipt under the same row lock and transaction. Same ID with same content returns
the original receipt; different content is rejected; an unknown commit is confirmed
through the original ID, and the ID must not be regenerated for blind retries. V108
is the unified empty-database initialization resource, with no historical data
migration. Server selects this provider, and Core has no JDBC/SDK leakage. These
constraints supersede the historical state above that "per-operation conditional
projection transactions are not yet implemented", but whole-round scanning, source
version, parallel synchronization under the same deployment, and lifecycle remain
incomplete.

### Metadata durable request recovery

Catalog Persistence now durably accepts immutable projection and completion intents
before execution. The original IDs are recoverable by Catalog across
service/provider recreation; Metadata Catalog must recover pending work before
observing a new source batch. New IDs cannot bypass pending operations. Obsolete
requests are rejected under the current deployment fence.
Projection/outbox/receipt and checkpoint/run receipt retain their separate atomic
owners. This delivers per-operation recovery. The following decision extends its
guarantees to competing observations.

### Federation Same-Deployment Observation Order

V115 persists the observation epoch on the deployment row. Each full round,
incremental round, and actual push callback first restores the original intent and
then starts observation under the same deployment row lock. Sampling uses the
current round's epoch. When updating projections or committing synchronization
progress, the server validates the deployment and epoch; previous intents must be
processed under their original IDs before a new epoch may be issued.
Already-persisted per-operation receipts can still be acknowledged under a new
epoch or deployment. This ensures that when any two Coordinators scan
overlappingly, the earlier scan cannot commit new operations or advance the
checkpoint after a newer observation.

The incremental coverage interval feeds the checkpoint, and table-level state comes
from independent authoritative point reads. A push deletion rechecks whether the
source table has been rebuilt. Cache misses, timeouts, insufficient authority, and
unsupported capabilities cannot constitute proof of absence. Incremental
enumeration permits partial visibility, and omitted tables or schemas do not
trigger cleanup. When the external source lacks a unified snapshot or single-object
version, it is impossible to claim that the entire Catalog is published under one
source version, nor to make a cross-system atomic promise for source reads and
local commits. If these two guarantees are needed later, the source must provide
verifiable read boundaries and versions. Underlying source I/O during
cancellation, push-subscription startup/exit, event recovery, and log retention
each still require their own capability and lifecycle validation.
