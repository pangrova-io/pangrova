# Code Standards

Naming rules are enforced in review; SPI annotations, Checkstyle and SpotBugs are
enforced by CI.

## Naming

### P0

- Framework names appear in Java type, package and protobuf names only inside the
  artifact that integrates that framework: Arrow/Flight in Batch, Transport, Shuffle,
  Protocol and the observability SDK; Calcite (`RelNode`, `RexNode`, `SqlNode`) in
  Planner; Iceberg/Paimon/Lance in their connector. `Velox` never appears in Java
  names; in C++ only direct Velox integrations use it. `Presto` and `Rss` are banned
  everywhere.
- Fully qualified class names are unique across modules. Simple names may repeat in
  distinct owner packages; add a capability qualifier when imports would be ambiguous.
- Banned package segments: `jaxrs`, `rss`, `dpp`, `aqe`, `sop`, `rls`, `dt`.

### P1

- Packages and types are nouns; methods are verbs. SQL AST types may be named by
  statement keyword (`GrantPrivilegeStatement`). Keep SDK/protocol spelling at an
  interoperability boundary.
- Banned abbreviations: `Rss`, `Dpp`, `Aqe`, `Mv`/`MV` and `Dt` prefixes, `HA`/`ha`
  prefixes or packages, `Sop`, `Repl`, `rls`, and `Ipc` in identifiers (use `Batch`,
  `Page` or `Stream`). Allowed: `DAG`, `CTE`, `RBAC`, `CDC`, `SQL`, `HTTP`, `gRPC`,
  `JNI`, `UDF`, `TLS`, `JDBC`, `LDAP`, `OAuth2`, `DTO`, `SPI`, `API`, `REST`, `ACL`.
- No `Impl` suffix, and an implementation never reuses its interface's simple name.
  Name a class for the contract it implements or the resource it owns; Pangrova-owned
  types avoid `Adapter` and `Bridge` suffixes.
- Functional interface names contain one verb (no `And`/`Or`/`Then` chains) and do not
  expose internal delegation targets.
- Suffixes match behavior: `*Lookup` key→value, `*Fetcher` remote I/O, `*Resolver`
  key→complex result, `*Provider` lazy factory. `*Manager`/`*Helper`/`*Util` need a
  one-sentence responsibility.
- SPI packages contain no `Simple*`, `Legacy*` or `NoOp*` types.

## Packages and Artifacts

New or moved code follows these rules immediately; existing violations may only
decrease. Ownership details are in `architecture.md` and `.pangrova/modules.yaml`.

- Each code-bearing leaf owns explicit package roots that never equal or
  prefix-overlap another leaf's; a multi-leaf family root (`org.pangrova.worker`,
  `.shuffle`, `.connector`, `.testing`) is claimed by one leaf at most.
- Split packages are forbidden across main, test-fixture, generated, shaded and plugin
  artifacts. Every code-bearing JAR has a unique `Automatic-Module-Name`.
- Create an artifact only for a real boundary: a published contract, replaceable
  implementation, SDK or classloader isolation, resource lifecycle, process or JNI
  packaging, or reused codec/test infrastructure. No empty modules, aggregators
  without children, or symmetry-only splits.
- Contract artifacts compile, link and load on their declared minimal classpath. JDBC
  pools and drivers, Flyway, Curator, cloud/provider SDKs and JWT libraries never reach
  an API-only consumer transitively.
- Cross-context code depends only on shared primitives, APIs, SPIs, Protocol or
  producer-owned event packages, never another context's `.internal`, `.store`,
  `.bootstrap`, `.endpoint` or `.server` packages. The same direction applies between
  packages inside a merged artifact. `org.pangrova.common.event` holds only envelope,
  identity, timestamp, version and publication primitives; there is no `event-api`.
- Capability leaves never depend on a process artifact's assembly, server or bootstrap
  packages; a same-family host port is a `provided` friend export.
- Organize by business capability, then technical role (`metadata.api.catalog`,
  `persistence.catalog.postgresql.dependency`). No horizontal `service`, `manager`,
  `util`, `impl`, `model` or `store` packages; `model` only below a named capability.
  Collapse intermediate `core`/`impl`/`adapter`/`provider` packages without own types.
- Technology names belong only in adapter packages (`connector.iceberg`,
  `coordination.zookeeper`, `planner.substrait`).
- No `package-info.java`, placeholder packages or empty directories after moves.
- Tests may mirror their leaf's package. Reusable test utilities live under an
  exclusive root such as `org.pangrova.testing.testkit`.
- `pangrova-common` stays dependency-minimal: no Jetty/Jersey, YAML, JDBC/Flyway,
  Curator, cloud, table-format or provider SDKs. Its JSON helpers use optional Jackson
  databind and JSR-310; modules calling them declare both.

## Structure

Checkstyle reports file length 800, effective method length 100, cyclomatic
complexity 15 and parameter count 10 as warnings; nesting depth (if 3, for 3, try 2)
is an error. The same limits guide C++ review. Review constructors with more than 6
dependencies and interfaces with more than 12 methods for mixed responsibilities.
Thresholds are review signals: extract coherent operations, never forwarding classes,
generic context bags or compressed statements to satisfy a metric. Share logic only
when it has the same meaning and change lifecycle.

## Java

- Identifiers, comments and logs are English. Comments explain behavior, invariants
  and tradeoffs.
- Constructor injection only; no `@Autowired`/`@Inject`. Configuration is YAML-backed
  and immutable; DTO, protocol and config types are records or final-field builders.
- Contracts use domain types or DTOs, never `Object` or `Map<String, Object>`.
- Off-heap and Arrow resources use try-with-resources; pointers cross threads only
  with ownership transfer.
- Shared mutable state is immutable, volatile or synchronized and annotated with
  `@ThreadSafe`, `@NotThreadSafe` or `@GuardedBy`. Thread pools are named with an owner
  and a shutdown path; `ScheduledThreadPoolExecutor` sets
  `setRemoveOnCancelPolicy(true)`. No locks held during I/O.
- Never swallow exceptions; cross-module errors carry an `ErrorCode` and context, and
  async failures surface.
- One canonical path: no `try A → catch → try B` fallback chains unless each branch
  handles a distinct failure mode, and no compatibility shims, `@Deprecated`
  forwarders, version sniffing or flags for dead paths. Migrate callers of a replaced
  owned API; suppress third-party deprecations narrowly with the reason.
- Delete dead code: commented-out code, unreachable methods, unused imports, fields
  and includes. `UnsupportedOperationException` stubs need a tracking issue on the
  method or its `@ExperimentalImplementation` owner. When deleting a feature, remove
  its registrations, callers, tests, configuration and documentation.

### Logging

- Use `private static final Logger LOG = LoggerFactory.getLogger(Owner.class);` with
  structured `key=value` messages; critical paths carry `traceId`, `queryId`, `taskId`.
- `ERROR` needs human action; `WARN` is recovered but unexpected; `INFO` marks state
  transitions and request boundaries; `DEBUG` is diagnostic detail.
- Pass the exception object (`LOG.error("action failed", e)`). Production code never
  uses `printStackTrace()` or `System.out/err`.

### Dependencies

- Versions come from parent POM properties; BOMs align Arrow, Netty and Jackson.
- Use `test`, `runtime` and `provided` scopes precisely, and do not add a library that
  duplicates existing functionality.
- Treat Calcite, Arrow, Iceberg, Substrait, Protobuf, Netty, Jackson, SLF4J, gRPC and
  Guava upgrades as compatibility changes.

## JNI

- Load the library only through `NativeLibraryLoader.load()` from
  `META-INF/native/{os}/{arch}/`. Production code never uses `System.loadLibrary()`,
  library-path environment variables or loader fallbacks; `-Djava.library.path` is
  acceptable only for local tests before packaging.
- Production libraries link dependencies statically; development builds use CMake
  `BUILD_RPATH`.
- Upcalls cache `JavaVM*` in `JNI_OnLoad` and `jclass` (GlobalRef) and `jmethodID` at
  initialization, attach native threads with an RAII guard, release every GlobalRef
  explicitly, and check `ExceptionCheck()` after each `Call*Method`.
- C++ exceptions never cross JNI: entry points catch and convert to Java exceptions.
- Data crosses through the Arrow C Data Interface; Java and C++ agree on column types,
  names and nullability, and every copy is justified.

## C++

- Files and classes `PascalCase`, functions `camelCase`, constants `kPascalCase`,
  private members `snake_case_`, namespaces `pangrova::*`, tests `<Component>Test.cpp`
  with GoogleTest (`TEST_P` for data-driven cases).
- RAII only: no raw `new`/`delete` or `malloc`/`free`; `unique_ptr` for ownership,
  `shared_ptr` without cycles, and a raw `.get()` pointer never outlives its owner.
  Move large values instead of copying.
- Shared state uses `std::mutex`, `std::shared_mutex` or `std::atomic` with a
  documented lock order; never hold a lock during I/O.
- Report failures with exceptions or `VELOX_CHECK*`/`VELOX_USER_CHECK*`/
  `ARROW_RETURN_NOT_OK`; never ignore a status. Empty `catch (...)` is allowed only at
  JNI entry points that convert the error. Prefer the strong exception guarantee.
- Headers start with `#pragma once`, forward-declare where possible and order includes
  own → project → third-party → system. JNI-facing headers do not pull in Velox.
- Apply `const` to non-mutating methods, read-only parameters and unchanged locals.

## Protobuf

- `snake_case.proto` files, `PascalCase` messages, `snake_case` fields; the execution
  package is `pangrova.protocol.v2`.
- Regenerate Java and C++ in the same change; never renumber existing fields or
  hand-edit generated sources.

## Commits and PRs

Prefer Conventional Commits, `<type>(<scope>): <description>` with types `feat`,
`fix`, `docs`, `refactor`, `test`, `chore`, `perf` and a module scope. No branch
prefix or message-language gate applies. PRs use `.github/PULL_REQUEST_TEMPLATE.md`.
