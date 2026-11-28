# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Distributed SQL query engine with Calcite optimization and Velox vectorized execution
- Apache Arrow zero-copy columnar data transfer (Arrow C Data Interface for JNI)
- Arrow Flight (gRPC) shuffle data plane between workers
- Substrait plan exchange format between Java coordinator and C++ workers
- Connector SPI with Iceberg and Paimon connector implementations
- Read-write separation: C++ scans via Velox, Java transactional writes via connector SDK
- Multi-active coordinator HA with leader election and lease-based failover
- Fault-tolerant stage scheduling with retry budgets and speculative execution
- Adaptive query execution (AQE): join strategy switching, partition coalescing, skew handling
- Dynamic partition pruning and runtime filter generation (bloom + min/max)
- Security framework: JWT/LDAP/OAuth2 authentication, RBAC authorization, column masking, row filtering
- CDC streaming with Kafka consumer, watermark tracking, and incremental write
- Vector search indexing (HNSW, IVFPQ) with native index evaluation
- Observability: OpenTelemetry tracing, Prometheus metrics, structured logging
- Resource management: memory arbitration, tenant resource tracking, spill management
- JDBC driver and REST client SDK with retry, failover, and connection pooling
- Governance: data quality rules, lineage tracking, audit logging
- Native library loading via classpath resource extraction (platform-specific bundles)
- Comprehensive testing: E2E with TestContainers, property-based tests with jqwik, TPC-H/DS benchmarks

### Changed
- **BREAKING** Common Java utilities now use responsibility-specific packages. JDBC helpers
  use `jdbc.datasource`, `jdbc.migration` and `jdbc.operation` while retaining the Maven
  coordinates and automatic module name. Recompile external Java plugins against the
  matching Coordinator and Worker release.
- **BREAKING** Java parser contracts now live in `org.pangrova.parser.api.syntax`,
  and the connector index capability interface is `ConnectorIndexCapabilities`.
  Update Java imports before recompiling extensions for this release.
- Generated-value sidecars now use one nullable segment-v2 contract and matching
  Java/Native build/overlay capability version 2.0.0. Readers reject old formats;
  sidecars must be rebuilt, not silently reinterpreted. Final SQL/distributed
  acceptance is tracked in [`docs/getting-started/roadmap.md`](docs/getting-started/roadmap.md).
- **BREAKING** `SecurityStatementHandler.executeGrant` / `executeRevoke` now take a
  `SecurityStatementHandler.GrantTarget` record instead of four positional
  `objectType/catalog/schema/table` strings. The four values always travel together and are
  consumed in that order by `PrivilegeGrant` and `GrantStore.revokeGrants`, so passing them
  individually only invited argument-order mistakes.
- Build quality config moved from `build-tools/` to `config/` (`config/checkstyle/`,
  `config/spotbugs/`); resolved via `${maven.multiModuleProjectDirectory}`, which
  requires the `.mvn/` marker directory at the repository root
- Checkstyle `Header` module removed: it referenced a `java.header` file that never
  existed in the repository, so the module could not initialize. Per-file Apache
  headers are now explicitly optional — the root `LICENSE`/`NOTICE` are authoritative.
  Rationale for this and every other rule relaxation is recorded in the comment block
  at the top of `config/checkstyle/checkstyle.xml`
- SPI stability annotations added to core connector interfaces (ConnectorMetadata, ConnectorPageSource, ConnectorPageSink, ConnectorSplitManager, ConnectorPageSourceProvider, ConnectorPageSinkProvider, TaskExecutor)
- RetryPolicy jitter factor widened from 25% to 50% to reduce request clustering
- HTTP metrics renamed to follow Prometheus/OTel conventions (unit suffixes)
- CMake -Werror enabled by default for first-party C++ sources
- ConnectionPoolConfig now wired to HttpClient executor pool sizing
- `PackageStructurePropertyTest` now enumerates every package exhaustively instead of sampling
  10 at random, and scans nested modules (`pangrova-connector/*`, `pangrova-shuffle/*`, …) that the
  top-level-only module scan skipped entirely. Coverage went from 10 sampled to 402 packages.
  The three packages this exposed (`org.pangrova.worker.ai.function`,
  `org.pangrova.indexing.engine.vector`, `org.pangrova.planner.rule`) are pinned to their current
  file counts via `TRACKED_OVERSIZE_PACKAGES` rather than exempted, so they cannot grow further
  without a deliberate decision.

### Removed
- Removed the incomplete Celeborn adapter, its reflective coordinator factory, ServiceLoader
  registration, and container test. The adapter had no Worker-side production wiring and the test
  did not prove that query data traversed Celeborn. The generic shuffle SPI and canonical external
  shuffle protocol remain available for a fully wired implementation.

### Fixed
- Carry the Coordinator internal authentication secret and opt-in speculation
  setting from YAML into the production bootstrap; reject blank internal secrets.
  Use consumed environment names and distinct advertised hosts in multi-process
  test clusters instead of silently defaulting Worker identity and connection settings.
- Close the Coordinator-owned metadata invalidation poller before its JDBC pool,
  including startup-failure cleanup, instead of leaving its scheduler alive.
- Preserve the submitting principal and resolved tenant on Coordinator query
  completion/failure audit events without retaining tokens or session-property maps.
- Classify Coordinator tenant quota rejection as HTTP 429 (`TENANT-0001`,
  `USER_ERROR`) rather than an internal HTTP 500, without fabricating a reset time.
- Preserve generated-value NULL validity and exact physical row locators through
  Native immutable publication and scan overlay; enforce dense materialization
  grants before allocating the output rather than exposing a zero embedding.
- Checkstyle gate was silently dead and is now enforced. The config referenced a `Header`
  module whose `headerFile` did not exist, so every module aborted during config
  initialization and CI's `mvn checkstyle:check` step failed for a reason unrelated to code
  style — meaning no rule in `config/checkstyle/checkstyle.xml` had ever actually run. With
  the config repaired the gate reports 0 violations across all 37 modules. The 4,599
  violations this exposed were handled in three ways, recorded in
  `config/checkstyle/suppressions.xml`:
  - 2,810 came from one deliberate house style (compact single-line members such as
    `@Override public int getFetchSize() { return 0; }`). The four rules that reject it were
    relaxed rather than rewriting 2,810 sites; the rationale is in the "deliberate
    departures" block at the top of `checkstyle.xml`.
  - ~500 mechanical violations were fixed outright, and those rules are now at zero:
    `LineLength`, `WhitespaceAround`, `OneStatementPerLine`, `UnusedImports`,
    `AvoidStarImport`, `Indentation`, `LeftCurly`, `EmptyLineSeparator`,
    `MissingSwitchDefault`, `MultipleVariableDeclarations`, `OperatorWrap`, `MethodName`,
    `ConstantName`, `JavadocType`, `JavadocStyle`, `AnonInnerLength`. Notably, 22 files with
    inline fully-qualified type names were converted to real imports instead of merely being
    wrapped at 120 columns.
  - The remaining 1,280 are frozen in the new `§5` baseline of `suppressions.xml`
    (`MagicNumber` 504, `MissingJavadocMethod` 426, `CyclomaticComplexity` 152,
    `ParameterNumber` 61, `HiddenField` 53, `MethodLength` 36, `FileLength` 25,
    `BanThreadSleep` 23). Every entry is scoped to a single file, so this is a ratchet and
    not an amnesty: the same violation in any unlisted file still fails the build, and the
    list can only shrink. Each rule group documents what the violations are, why a bulk fix
    was rejected, and the intended convergence direction.
- `HighAvailabilityCoordinationService` no longer returns a 41-line anonymous
  `ElectionScheduler.Callback`; the implementation is a named `SchedulerCallback` inner class,
  which keeps the CRITICAL lease-renewal-failure alarm readable rather than buried in a
  nested initializer
- Three `Thread.sleep` call sites whose `catch` blocks did nothing but re-assert the interrupt
  flag (`LineageEventEmitter`, `BatchStreamProducer`, `OutputBufferResultSink`) now delegate to
  the canonical `ThreadUtils.sleepQuietly`, removing three duplicated helpers. The other 23
  sites were each read and left alone: they perform real work after re-asserting the interrupt
  (throw a domain exception, return a documented sentinel, break the loop, set a shutdown
  flag), so routing them through a quiet helper would have discarded behaviour
- StageDAG cycle detection via DFS prevents query-level deadlocks
- Stage retry now cancels old tasks before launching replacements
- CoordinatorResultBuffer returns error code instead of throwing on buffer full
- StageFragmenter parallelism configurable (was hardcoded 256MB/128 max)
- JNI GetStringUTFChars null check in JniEntryPoints.cpp
- Arrow release callback verification in ColumnarDataExporter.cpp
- NativeWriteCommitTracker timeout-based cleanup for orphaned write state
- Task cancel verification polling to detect zombie native tasks
- Session fixation protection via post-authentication session ID regeneration
- LDAP password stored as char[] with secure clearing
- RBAC wildcard grant audit logging
- Column masking exemption audit logging
- SQL quality rule injection prevention via whitelist enforcement
- REST API input sanitization for namespace/table names
- HeartbeatSender graceful shutdown awaits in-flight heartbeat
- SpillManager disk space check before spill operations
- ShuffleDiskSpiller double-close protection
- CoordinatorTaskTracker worker-query reverse index for O(1) lookup
- Iceberg partition writer close-all-on-error with suppressed exceptions
- NativeWriteExecutor shared allocator to reduce GC pressure
- QueryResultCache L1 capacity limit to prevent unbounded growth
- FaultTolerantStageScheduler retry counter reset on stage success
- StageDAG upstream FINISHED state verification before downstream launch
- Shuffle cleanup failure metric counter for storage leak detection
- TaskDescriptor attempt generation ID for stale task rejection
- PangrovaCoordinator ownership transfer retry with failure collection
- MemoryArbitrator stale allocation watchdog
- PangrovaArbitrator OOM handling: abort task instead of process crash
- StructuredLogger real key=value structured output methods
- DiskANN strategy lookup no longer aborts index-engine startup when the native runtime is
  absent. `ServiceLoader.findFirst()` raises `ServiceConfigurationError` when the provider is
  registered but its static initializer cannot link `libpangrova_native`; that error escaped
  `orElse(null)` and cascaded into an `IllegalStateException` from `IndexEngineRegistry`, taking
  down coordinator startup on any JVM-only build. DiskANN is optional, so the failure is now
  logged and the strategy degrades to unavailable.

### Security
- JwtDecoder recursive nesting depth limit (max 10) to prevent stack overflow DoS
- C++ stack protector (-fstack-protector-strong) enabled
- FlightShuffleServer lingering demuxer auto-cleanup on client disconnect
- NativeCommandDispatcher ERROR logging for unknown commands

---

## Version Notes

- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Features to be removed in future
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security-related fixes
