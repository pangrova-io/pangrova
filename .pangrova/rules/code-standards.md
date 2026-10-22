# Code Standards

## Naming constraints

These are design conventions. SPI annotation checks are enforced in CI; naming rules are enforced via code review.

### P0 — Framework name isolation (C1)
Class, package, and proto names must not leak framework names into modules that should be framework-agnostic:
- **`Velox`**: banned in ALL Java source. C++ allowed only for direct Velox SPI implementations (`VeloxTaskRunner`, `VeloxPlanConverter`, `VeloxMemoryGovernor`, `PangrovaVeloxConnector`, `PangrovaVeloxDataSource`, `VeloxBatchReader`).
- **`Arrow`**: banned in `spi`, `common`, `resource`, `observability`, `client-*`, `worker` (Java), `coordinator`, `planner`, `parser`, `metadata`, `governance`, `security`, `resilience`, `streaming`, `operations`. Allowed in `worker-native` (C++), `protocol`, `shuffle`.
- **`Calcite`/`RelNode`/`RexNode`/`SqlNode`**: banned in `coordinator`, `worker`, `spi`, `protocol`. Allowed only in `planner`.
- **`Flight`**: banned in `spi`, `common`, `protocol`, `client-*`, `coordinator`.
- **`Iceberg`/`Paimon`**: banned in all except their own connector module.
- **`Presto`**: banned everywhere. **`RSS`/`Rss`**: banned everywhere.

### P0 — Other naming rules
- **C2**: Fully qualified class names must be unique across modules. Simple names
  may repeat in distinct owner packages; use a capability qualifier when imports
  would otherwise be ambiguous. Keep SPI and implementation roles clear.
- **C9**: Proto/JSON field names must not contain banned framework names.
- **C10**: Banned package segments: `jaxrs/`, `rss/`, `dpp/`, `aqe/`, `sop/`, `rls/`, `dt/`.

### P1 — Naming precision
- **C3 abbreviation ban**: `Rss`, `Dpp`, `Aqe`, `MV`/`Mv` (prefix), `Dt` (prefix/pkg), `HA`/`ha` (prefix/pkg), `Sop`, `Repl`, `rls` (pkg), `Ipc` (class/method/field — use `Batch`/`Page`/`Stream`). Allowed: `DAG`, `CTE`, `RBAC`, `CDC`, `SQL`, `HTTP`, `gRPC`, `IPC` (comments only), `JNI`, `UDF`, `TLS`, `JDBC`, `LDAP`, `OAuth2`, `DTO`, `SPI`, `API`, `REST`, `ACL`.
- **C4**: No `Impl` suffix — use descriptive names.
- **C5**: Implementation class must not reuse the SPI interface's simple name.
- **C11**: Established pattern suffixes (`Adapter`, `Bridge`, `Proxy`, `Decorator`,
  `Facade`, `Mediator`, `Visitor`, `Observer`, `Strategy`) are allowed when they
  accurately describe the class's responsibility. Prefer a domain-specific name
  when a pattern label adds no information.
- **C12**: Functional interface names must not contain multiple verbs joined by `And`/`Or`/`Then`.
- **C13**: Names must not expose internal delegation targets to callers (`PageSink*` in a class whose callers don't know about PageSink).
- **C14**: `*Lookup` = key→single-value; `*Fetcher` = remote I/O; `*Resolver` = key→complex-result; `*Provider` = lazy/factory. Use the suffix matching actual behavior.

### P2
- **C6**: `*Manager`/`*Helper`/`*Util` only if one-sentence responsibility.
- **C7**: No `Simple*`/`Legacy*` in SPI.
- **C8**: No `NoOp*` in SPI — belongs in impl modules.

## Java package architecture

These rules apply immediately to new or moved code. Existing violations must be
tracked as a ratcheted baseline: they may decrease, never increase. The target
ownership map is defined in `docs/architecture/long-term-modular-architecture.md`.

### Ownership and visibility

- Every code-bearing Maven leaf owns one or more explicit package roots. Roots
  owned by different leaves MUST NOT be equal or prefix-overlap.
- Review package cohesion by capability, consumers, and dependency direction.
  File count alone must not force package splits or maintain historical split
  quotas. AST nodes, protocol types, and extension-point families may be numerous
  while sharing one responsibility.
- A multi-leaf product family root (for example `org.pangrova.worker`,
  `org.pangrova.shuffle`, `org.pangrova.connector`, or
  `org.pangrova.testing`) MUST NOT be claimed by more than one leaf. Sibling
  leaves use non-overlapping roots such as `.runtime`, `.service`, or
  `.testkit`.
- Domain API, Core, Store, Assembly, and Server are logical package boundaries
  inside one owner artifact by default. They become separate artifacts only
  for an independently published contract, dependency/classloader isolation,
  a process/distribution boundary, or JNI/native packaging. Directory symmetry
  and a single implementation are not sufficient reasons to split.
- A merged owner artifact MUST keep adapter-only libraries non-transitive
  (`optional` or app-supplied runtime dependencies) and prove that every
  exported API compiles, links, and loads on its declared minimal classpath.
  JDBC pools/drivers, Flyway, Curator, cloud SDKs, JWT implementations, and
  provider SDKs must never arrive transitively at an API-only consumer.
- Cross-module imports are default-deny. They must be allowed by the Maven
  dependency graph, the source module's per-target `allowedImports`, and the
  target module's `exports` or `friendExports` entry in
  `.pangrova/modules.yaml`.
- Cross-bounded-context code may depend only on a registered shared primitive
  library, API, SPI, protocol, or producer-owned event package. Importing
  another context's `.core`,
  `.internal`, `.store`, `.bootstrap`, `.endpoint`, or `.server` package is
  forbidden. The same dependency direction must be enforced between packages
  inside a merged owner artifact. A separate adapter may use only its
  explicitly listed friend port.
- A generic `event-api` artifact is forbidden. `org.pangrova.common.event` may
  contain only the stable envelope, identity, timestamp, schema/version, and
  minimal publication primitives; event payloads remain in the producing
  owner's exported capability package.
- `pangrova-metadata` owns technical object identity, schema, version, binding,
  and statistics references only. Business concepts, terms, metrics,
  relationships, semantic views, ontologies, and their publication state MUST
  be owned by `pangrova-semantic`; AI grounding/evaluation state MUST be owned
  by `pangrova-ai-control`. Metadata MUST NOT import either implementation.
- `pangrova-semantic` is one owner artifact with internal model, relationship,
  ontology, compiler, projection, adapter, and JDBC-store package boundaries.
  A generic `pangrova-graph` artifact is forbidden. RDF/SKOS/OWL/SHACL,
  reasoner, and graph-database types may occur only in semantic adapter
  packages and MUST NOT leak into exported API, Planner, Query, Execution, JNI,
  or Connector SPI signatures.
- Metadata dependency graphs, Semantic knowledge graphs, Governance lineage
  graphs, and Indexing/AI retrieval graphs are distinct facts with distinct
  owners. A projection or index is rebuildable and MUST NOT become the
  authority for semantic definitions, permissions, or publication pointers.
- AI-produced SQL or actions MUST pass through a versioned grounding snapshot,
  deterministic semantic compilation, Planner validation, Security decision,
  and the applicable approval/action policy. LLM output is never an execution
  authorization and AI Worker MUST NOT persist semantic or ontology authority.
- A capability/runtime leaf MUST NOT depend on a process artifact's assembly,
  server, resource, or bootstrap packages. A same-family host port may be used
  only as a `provided` dependency through a declared friend export; promote it
  to a dedicated SPI artifact when it gains external compatibility or a second
  independent provider family.
- Split packages are forbidden across main, test fixture, generated, shaded,
  and plugin runtime artifacts. Every code-bearing JAR must have a stable,
  unique `Automatic-Module-Name`.

### Package structure and naming

- Under API and Core roots, organize by business capability first and technical
  role second, for example `metadata.api.catalog`,
  `query.core.lifecycle.policy`, and `metadata.store.jdbc.catalog`.
- Do not create horizontal catch-all packages named `service`, `manager`,
  `util`, `impl`, `model`, or `store`. `model` may only be a local subgroup
  below a named capability; `store.jdbc` is allowed only as a registered
  adapter root.
- Framework or technology names belong only at an adapter boundary, such as
  `connector.iceberg`, `coordination.zookeeper`, `store.jdbc`, or
  `planner.substrait`; they must not appear in domain or public API packages.
- `internal` is documentation, not a Java access boundary. Architecture tests
  must reject imports of internal packages from any other leaf.
- Every owned root and every exported/friend-exported package must contain a
  `package-info.java` documenting its owner, stability, allowed consumers, and
  prohibited responsibilities.
- Tests inside a production leaf may mirror that leaf's package to exercise
  package-private behavior. Reusable test utilities belong under an exclusive
  testing root such as `org.pangrova.testing.testkit`; they must not create a
  split package with production code.
- `pangrova-common` must remain dependency-minimal: no Jetty/Jersey, Jackson,
  YAML parser, JDBC/Flyway, Curator, cloud SDK, table-format SDK, or provider
  SDK. Such dependencies belong to Config, wire/serde adapters, owner stores,
  plugins, or process assembly.

## Quality gates

- Java source file ≤ 800 lines. Method/constructor ≤ 100 nonblank,
  noncomment lines. Cyclomatic complexity ≤ 15, measured by Checkstyle's Java
  parser. Do not maintain a second regex-based approximation of these metrics.
- Keep nesting shallow. Checkstyle nesting limits are `NestedIfDepth=3`,
  `NestedForDepth=3`, and `NestedTryDepth=2`. Review mixed control-flow nesting
  and extract coherent operations when the combined structure obscures intent.
- Method/constructor parameters ≤ 10. Review constructors with more than 6
  dependencies for mixed responsibilities; introduce a parameter/dependency
  object only when its members form a cohesive concept.
- Review interfaces above 12 methods for unrelated consumer responsibilities.
  Enums and constant sets have no arbitrary value-count limit; keep values in
  their owning domain and group documentation by meaning.
- Repeated domain logic should be shared when it has the same meaning and change
  lifecycle. A third occurrence prompts review, not automatic extraction.
- Size ceilings are backstops, not targets. Keep one clear responsibility per
  class, explicit control flow, and meaningful method names. Extract coherent
  operations; do not add forwarding-only classes, generic context bags, compressed
  statements, or extra indirection just to satisfy a metric.
- Comments explain behavior, invariants and tradeoffs. Link issues or designs when
  useful; do not require removing provenance merely to satisfy a naming rule.

## Git & PR conventions

Prefer [Conventional Commits](https://www.conventionalcommits.org/):
```
<type>(<scope>): <description>
```
- **Types**: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`
- **Scope**: module name (`parser`, `worker`, `coordinator`, `native`, etc.)
- Use descriptive branches; no required prefix or commit-message language gate.
- **PR checklist**: see `.github/PULL_REQUEST_TEMPLATE.md`

## Dependency management

- **Version properties**: All dependencies MUST use version properties from the parent pom (`${arrow.version}`, `${calcite.version}`, etc.). Hardcoded versions are rejected.
- **Core dependency conflicts** — special care required when upgrading: Calcite, Arrow, Iceberg, Substrait, Protobuf, Netty, Jackson, SLF4J, gRPC, Guava.
- **Scope rules**:
  - Test-only deps: `<scope>test</scope>`
  - Runtime-only deps: `<scope>runtime</scope>`
  - Provided by container: `<scope>provided</scope>`
- **No duplicate functionality**: before adding a dep, verify it doesn't overlap with existing libs.
- **BOM alignment**: use `<dependencyManagement>` import for multi-artifact libs (Arrow, Netty, Jackson).

## Logging

- **Format**: structured `key=value` pairs (not free-form sentences)
- **Required context** on critical paths: `traceId`, `queryId`, `taskId`
- **Level strategy**:
  - `ERROR` — requires human intervention, non-recoverable in current request
  - `WARN` — auto-recovered but unexpected; may need future attention
  - `INFO` — state transitions, lifecycle events, request boundaries
  - `DEBUG` — diagnostic detail for development/troubleshooting
- **Banned**: `e.printStackTrace()`, `System.out.println()`, `System.err.println()`
- **Logger creation**: `private static final Logger LOG = LoggerFactory.getLogger(ClassName.class);`
- **Exception logging**: `LOG.error("action failed", e)` — never swallow the exception object

## Code standards

- **Language**: identifiers, code comments and log messages use English. Commit
  messages must clearly describe the change; language alone is not a gate.
- **Constructor injection only** — no Spring DI annotations (`@Autowired`, `@Inject`).
- **YAML config only** — config classes must be immutable.
- **Arrow/off-heap**: always `try-with-resources`. No cross-thread pointer passing without ownership transfer.
- **Contract-first**: params/returns must be domain model or DTO, not `Object`/`Map<String,Object>`.
- **DTO/protocol/config**: immutable (final fields, no setters, builder or record).
- **Thread safety**: cross-thread data must be immutable, volatile, or properly synchronized. Shared mutable state requires `@ThreadSafe`/`@NotThreadSafe`/`@GuardedBy` annotations. Named thread pools with explicit owner and shutdown path. `ScheduledExecutorService` → `setRemoveOnCancelPolicy(true)`. No locks during I/O.
- **Error handling**: no exception swallowing; cross-module errors need `ErrorCode` + context; async exceptions must surface. Structured logging (`key=value`); critical paths include `traceId`/`queryId`/`taskId`.
- **No fallback chains**: `try A; catch → try B; catch → try C` is banned unless each branch handles a genuinely distinct failure mode. One canonical path, no silent degradation.
- **No backward compatibility shims**: if a new API replaces an old one, remove the old one. No `@Deprecated` forwarding stubs, no version-sniffing branches, no feature flags for dead code paths.
- **Deprecated code cleanup**: migrate affected callers when replacing an owned API.
  Third-party deprecations do not require unrelated repository-wide migration.
  Any necessary suppression must be narrow and explain the migration constraint.
- **Dead code policy**: commented-out code, unreachable methods, unused imports, unused fields → DELETE immediately. Version control preserves history. "Keeping for future use" is banned.

## JNI bridge rules

- **Bidirectional**: Java→Native (pull) and Native→Java (callback/upcall) are both allowed.
- Native→Java upcall rules: cache `JavaVM*` in `JNI_OnLoad`; cache `jclass` (GlobalRef) and `jmethodID` at init time; native threads must `AttachCurrentThread`/`DetachCurrentThread` (or RAII guard); all `jobject`/`jclass` held beyond a single JNI call must be GlobalRef with explicit `DeleteGlobalRef`; always check `ExceptionCheck()` after `CallXxxMethod`.
- Native may perform network I/O to other native workers via Arrow Flight for shuffle/exchange.

### Native library loading
- Single loading path via classpath resource: `META-INF/native/{os}/{arch}/lib{name}.{dylib|so|dll}`.
- `NativeLibraryLoader.load()`: idempotent, thread-safe, extract from classpath → temp file → `System.load()`.
- **Forbidden in production code**: `System.loadLibrary()`, `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`, fallback chains. Dev-time `-Djava.library.path` is acceptable for local test runs before packaging.
- Production dylib: statically link all shared deps. Dev dylib: dynamic deps via CMake `BUILD_RPATH`.

## C++ conventions (worker-native)

### Naming
- Files: `PascalCase.h/.cpp`. Namespaces: `pangrova::*`.
- Classes: `PascalCase`. Functions/methods: `camelCase`. Constants: `kPascalCase`.
- Test files: `<Component>Test.cpp`. Test suites: `<Component>Test`.
- Private member variables: `snake_case_` (trailing underscore).

### Quality gates
- File ≤ 800 lines. Function ≤ 100 lines, ≤ 10 branches.
- Constructor params ≤ 6. No functions with > 4 indent levels.
- No God files (>5 unrelated responsibilities).

### Memory safety (P0 — violations are correctness bugs)
- **RAII everywhere**: no raw `new`/`delete`, no `malloc`/`free`.
- **Smart pointers**: `std::unique_ptr` for ownership, `std::shared_ptr` for shared ownership. No `shared_ptr` cycles (use `weak_ptr`).
- **No raw `.get()` ownership transfer**: if you call `.get()`, the smart pointer must outlive the raw pointer's usage.
- **Arrow buffers**: always in scoped guards. No cross-thread buffer passing without explicit ownership transfer.
- **GlobalRef lifecycle**: `NewGlobalRef`/`DeleteGlobalRef` in matching scope (constructor/destructor or RAII wrapper).
- **Move semantics**: move large objects instead of copying. No unnecessary copies of vectors, strings, or buffers.

### Thread safety (P0)
- Shared mutable state must use `std::mutex` or `std::atomic`.
- No lock-during-I/O: never hold a mutex while doing network or disk I/O.
- Lock ordering: document if multiple mutexes are needed; always acquire in consistent order.
- `std::shared_mutex` for read-heavy shared state.

### Exception safety (P0)
- C++ exceptions MUST NOT cross the JNI boundary. All JNI entry points must `catch (...)` and convert to Java exceptions.
- Use strong exception guarantee where possible: either complete successfully or leave state unchanged.
- Resource cleanup on exceptions: rely on RAII destructors, not manual cleanup in catch blocks.

### Error handling
- Use `Status`/`Result<T>` pattern for operations that can fail. No error codes silently ignored.
- `PANGROVA_CHECK` / `PANGROVA_RETURN_NOT_OK` macros for status propagation.
- No empty catch blocks. No `catch (...) {}` except at JNI boundary (where it converts to Java exception).

### Include hygiene
- `#pragma once` as the first directive in every header.
- Minimal includes: forward-declare where possible.
- Include order: own header → project headers → third-party headers → system headers.
- No transitive dependency on Velox headers in JNI-facing headers (keep JNI layer thin).

### Const correctness
- `const` on methods that don't modify state.
- `const&` for parameters that are not modified.
- `const` on local variables that are not reassigned.

### Protobuf conventions
- Files: `snake_case.proto`. Messages: `PascalCase`. Fields: `snake_case`.
  Execution boundary package: `pangrova.protocol.v2` (see `architecture.md`).
- Changes to `.proto` files MUST regenerate code on BOTH sides (Java + C++).
- Wire format stability: no renumbering of existing fields.

### Arrow C Data Interface
- Use Arrow C Data Interface for cross-boundary (JNI) transfer.
- Zero-copy when possible; document any copy with justification.
- Schema alignment: Java and C++ sides must agree on column types, names, and nullability.

## See also

- **CI gates**: see `ci-gates.md` for CI enforcement scripts and quality thresholds
- **Architecture**: see `architecture.md` for dependency and execution boundaries
- **Development**: see `development.md` for build, test, and workflow guidance
