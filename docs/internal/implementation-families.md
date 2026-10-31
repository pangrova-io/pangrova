# Implementation Family Ledger

This ledger records every extension family that has more than one candidate
implementation, multiple competing contracts, or a descriptor without a consumer.
It exists because `.pangrova/modules.yaml` checks ownership and edges, but cannot
decide whether a registered provider is actually reached at runtime.

Regenerate the provider facts from `.pangrova/module-baseline.yaml`
(`providers:` per artifact) and `.pangrova/scripts/generate-module-baseline.py`.
A row must be updated in the same change that adds, moves, wires, or deletes an
implementation. `ModuleDependencyPropertyTest` fails when declared providers and
descriptors diverge.

Status vocabulary:

- **wired** — a production `ServiceLoader` site (or explicit construction) exists
  on a reachable path.
- **test-only** — a provider or descriptor exists only in test resources.
- **descriptor-only** — a `META-INF/services` entry exists with no production
  `ServiceLoader` call.
- **zero-provider** — a production loader exists but no provider is registered.
- **zero-impl** — the contract has no production implementor at all.
- **unwired** — a loader exists, but no production caller reaches it.

## Current verification note (2026-10-31)

The empty `pangrova-observability-core` artifact has been removed: Query audit/event
persistence already lives in Query and the leaf contained only package documentation.
Its reactor, dependency-management, consumer dependencies and ownership entry were
removed together. API/SDK remain. Historical milestone notes below describe migration
steps, not proof that all subsequent cleanup is complete. See
[the current execution plan](project-restructure-execution-plan.md) for package inventory,
remaining boundary gaps and verified build results.

## 1. ServiceLoader families

| Contract | Location | Providers | Production loader / consumer | Status | Owner |
|---|---|---|---|---|---|
| `org.pangrova.connector.spi.ConnectorFactory` | `pangrova-connector-spi` | `IcebergConnectorFactory`, `PaimonConnectorFactory`, `LanceConnectorFactory` | `ConnectorFactoryRegistry` (runtime) via `WorkerCatalogRuntime`, `PluginConnectorHandleFactory`, `PluginManager` | wired | M3 done |
| `org.pangrova.function.api.registry.FunctionFactory` | `pangrova-function` | `AiFunctionFactory` | `NativeFunctionBootstrap` | wired | M2 done |
| `org.pangrova.function.api.runtime.FunctionCallbackScope` | `pangrova-function` | `WorkerAiCallbackScope` | `NativeFunctionBootstrap` | wired | M12 R done |
| `org.pangrova.function.api.runtime.FunctionFailureProjector` | `pangrova-function` | `AiFunctionFailureProjector` | `NativeFunctionBootstrap` | wired | M12 R done |
| `org.pangrova.indexing.api.IndexStrategyProvider` | `pangrova-indexing-api` | 9 providers under `indexing.engine.*` | `IndexEngineRegistry` | wired | M1 |
| `org.pangrova.indexing.api.eval.IndexFileEvaluatorFactory` | `pangrova-indexing-api` | `DefaultIndexFileEvaluatorFactory` | `IndexEvalSplitFilter` | wired | M1 |
| `org.pangrova.shuffle.spool.SpoolStoreFactory` | `pangrova-shuffle-core` | `LocalFsSpoolStoreFactory` | `SpoolingExchangeManager` | wired | M7 |
| `org.pangrova.observability.api.Context$TaskWrapping` | `pangrova-observability-api` | `sdk.logging.provider.mdc.MdcTaskWrapping` | `TaskWrappingLoader` | wired | M2 done |
| ~~`org.pangrova.shuffle.api.ShuffleManagerFactory`~~ | deleted in M7 | — | — | the resolver and all three factory providers had no production caller; the provider SPI, descriptor, resolver and factory classes were deleted rather than kept as an unreachable selection path | M7 done |
| `java.sql.Driver` | JDK contract | `PangrovaDriver` | JDBC `DriverManager` | wired | — |
| `org.pangrova.connector.spi.index.FileSystemAccessor` | `pangrova-connector-spi` | `indexing.fs.LocalFileSystemAccessor` | none; accessors are constructed by connectors | direct construction; descriptor removed in M1 | — |
| `org.pangrova.observability.api.LoggingProvider` | `pangrova-observability-api` | `sdk.logging.provider.otel.OtelLoggingProvider`, `api.noop.NoOpLoggingProvider` | none; bootstraps construct providers directly | direct construction; descriptor removed in M1 | — |
| `org.pangrova.security.api.secret.SecretResolver` | `pangrova-security-api` | worker test resource (`TestSecretResolver`); `EnvironmentSecretProvider`, security `EnvSecretProvider`/`FileSecretProvider` | `AiFunctionFactory`, `WorkerCatalogRuntime`, coordinator wiring | AI fallback is the only implicit provider | M2 done |

### Decisions

- **M1 (done)** — The `FileSystemAccessor` and `LoggingProvider` descriptors were
  deleted. Nothing loads them; direct construction is the real contract. Introduce
  a loader before re-adding a descriptor.
- **M7 (done)** — `ShuffleProviderResolver` was exercised only by tests. No
  production path selected a `ShuffleProvider` (coordinator spool paths use
  `SpoolStoreFactory`, not this SPI), so the resolver, its test, the three
  `ShuffleManagerFactory` providers, `ShuffleContext`, `ShuffleProvider`,
  `DefaultShuffleProvider`, the `ShuffleManagerFactory` descriptor and its
  `modules.yaml` provider entry were deleted. `ShuffleDurability`,
  `ShuffleCapabilities` and `ShuffleTransportMode` remain because scheduler and
  retry-policy code consume them.
- **M2 (done)** — Observability is split by dependency weight. `pangrova-observability-api`
  (`org.pangrova.observability.api.{noop,profiling,event}`) owns null objects and the
  profiling/event API surface and depends only on `pangrova-common`. `pangrova-observability-sdk`
  (`org.pangrova.observability.sdk.{metrics,tracing,logging,profiling,alerting,config}`)
  owns OpenTelemetry providers (`*.provider.otel`), transport adapters
  (`tracing.adapter.{grpc,http,nativebridge}`), MDC propagation, and native metric bridges
  (`metrics.nativebridge`). Engine modules depend on the API for no-ops and on the SDK only
  when assembling telemetry. Query audit/event/profile persistence moved to
  `pangrova-query` in M7: `query.api.audit` owns `QueryAuditService`/`QueryAuditRecord`,
  `query.store.jdbc.audit` owns `JdbcAuditStore` (`query_audit_records`), the event store
  lives in `query.store.jdbc.event.JdbcQueryEventListener` (schema
  `V102__query_event_store.sql`), and `query.core.profiling.QueryProfileService` reads
  audit records. The legacy `pangrova-observability` leaf keeps only alerting/logging/
  metrics/tracing leftovers and is deleted when those find owners. Domain metric
  definitions still in `sdk.metrics` (`CoordinatorMetrics`, `WorkerMetrics`,
  `ShuffleMetrics`, `CacheMetricsPublisher`, `StandardMetricsRegistration`,
  `HttpMetricsCollector`) move to their owners in M7.
- **M2 (done)** — Heavy provider implementations are isolated from the low-level
  packages they used to share: `common.json.provider.jackson.JsonMapperFactory`,
  `common.http.adapter.jetty.EmbeddedHttpServerFactory`,
  `resilience.provider.resilience4j.{ResilienceRegistry,ResilientCallDecorator}`, and
  `resilience.config.provider.yaml.ResilienceConfigLoader`. `YamlConfigLoader` stays in
  `org.pangrova.config` because config owns option loading and has no competing YAML
  implementation; direct construction remains the contract (no descriptor).
- **M2 (done)** — Table policy adapters no longer reach into catalog registration.
  `metadata.control.TablePolicyAuthority` is the table-owned port for stable
  identity, lifecycle state, catalog deployment currency, and governance bindings;
  `metadata.service.RegistryTablePolicyAuthority` adapts the live registry.
  `metadata.adapter.governance.TableControlQualityRuleStore`,
  `metadata.adapter.security.TableControlColumnMaskStore`, and
  `metadata.service.TableControlRowPolicyStore` consume the port, so the
  governance/security adapters stay free of `CatalogRegistry` or
  `TableControlService`; the row-policy adapter moves with the M8 Table slice.
- **M2 (done, no change)** — Planner context is already an immutable value:
  `planner.PlannerContext` and `planner.cache.QueryContext` are records, so the
  M2 requirement needs no new port.
- **M2 (done)** — Secret resolution now has one contract in
  `org.pangrova.security.api.secret` (`SecretResolver`, `SecretProvider`,
  `SecretReference`, `SecretTemplates`) owned by `pangrova-security`; the former
  lightweight `pangrova-security-api` leaf was folded back into that artifact. AI and connector keep consumer adapters
  (`EnvironmentSecretProvider`, `EnvironmentConnectorSecretResolver`) and there is
  no separate AI descriptor contract. The security implementation registry and
  ENV/FILE providers live under `security.core.secret`. See section 3.

## 2. Zero-provider extension points

| Contract | Location | Loader | Registered providers | Owner |
|---|---|---|---|---|
| `org.pangrova.connector.spi.FederatedCatalogFactory` | `pangrova-connector-spi` | `PluginFederationHandleFactory` | none | M3 |
| `org.pangrova.ai.worker.governance.observer.AiInvocationObserver` | `pangrova-ai-worker` | `CompositeAiInvocationObserver` | none | M11 done |
| `org.pangrova.connector.spi.ConnectorSecretResolver` | `pangrova-connector-spi` | `WorkerCatalogRuntime` | none; ENV adapter is the built-in fallback | M2 done (consumer adapter delegates to `security.api.secret`) |
| `org.pangrova.parser.spi.StatementParser` | `pangrova-parser` | `StatementParserRegistry` | none (built-in registry entries only) | M4 |

Each entry must end in a formal decision: implement and wire it, or delete the
contract and its loader. "Future extension" is not an acceptable resting state.

## 3. Duplicate contract families

| Responsibility | Competing types | Consumers | Owner |
|---|---|---|---|
| Secret resolution | `org.pangrova.security.api.secret.SecretResolver` (single contract); consumer adapters `org.pangrova.connector.spi.ConnectorSecretResolver` and worker `EnvironmentSecretProvider` / `EnvironmentConnectorSecretResolver` | AI provider registry, worker catalog runtime, security wiring | M2 done |
| Tenant registry | `security.core.tenant.InMemoryTenantStore`, `security.store.jdbc.PersistentTenantStore`, `metadata.tenant.JdbcTenantStore` | coordinator security/governance bootstrap | M6 split done; two-schema convergence deferred to a separate S |
| Column masks | Contract `security.api.mask.ColumnMaskStore`; providers `security.core.mask.provider.empty`, `security.core.mask.provider.memory`, `security.store.jdbc.PersistentColumnMaskStore`; metadata projection `metadata.adapter.security.TableControlColumnMaskStore` | governance + security policy chains | M6 done |
| Watermarks | `streaming.store.memory.InMemoryWatermarkStore`, `streaming.store.jdbc.watermark.JdbcWatermarkStore` (table watermark), `streaming.store.jdbc.watermark.JdbcWatermarkPositionStore` (source position) | streaming scheduler, CDC pipelines | M9 done: two facts retained because table-watermark and source-position keys differ |
| Table control stores | `metadata.control.InMemory*` vs `metadata.control.Jdbc*` (four pairs) | coordinator DDL and maintenance | M1 packaging, M8 contract |
| L2 cache | `metadata.core.cache.DisabledL2CacheStore`, `metadata.core.cache.InMemoryL2CacheStore`, `coordinator.query.cache.RedisL2CacheStore` | query cache | M6 packaging done; Redis stays coordinator-owned |

M1 only moves the in-memory and JDBC halves into matching provider subpackages.
Contract unification happens in the owning milestone; until then both
implementations must stay selectable and behavior must not change.

## 4. Zero-implementation SPIs

| Contract | Location | Consumers | Owner |
|---|---|---|---|
| `PolicyEvaluator` | `pangrova-materialization/pangrova-materialization-core/extension` | `MaterializationExtensionRegistry` | M6 |
| `MaterializationKindHandler` | `pangrova-materialization/pangrova-materialization-core/extension` | `MaterializationExtensionRegistry` | M6 |

`@Stable` contracts with no production implementor must be downgraded to
`@Experimental` (enforced by `.pangrova/scripts/check-spi-stability.sh`) or
deleted. The M6 entries are registry extension ports; their milestone must either
provide a real handler or remove the port from the published surface.

## 5. Packaging families for M1

Pure moves, no behavior change:

| Family | Current | Target |
|---|---|---|
| Shuffle factories | `coordinator.query.execution.dispatch.shuffle` | **M1 done**: `shuffle.provider.{nativeengine,spool}` |
| Shuffle spool | `shuffle.spool` | **M1 done**: contracts stay in `shuffle.spool`; `LocalFs*` moved to `shuffle.spool.localfs` |
| Index engines | `indexing.engine.<kind>` | already one package per kind; keep descriptor imports aligned |
| Index evaluation | `indexing.eval` | **M1 done**: `DefaultIndexFileEvaluator*` moved to `indexing.eval.defaults` |
| TableControl stores | `metadata.control` | **M1 done**: eight classes split into `metadata.control.memory` and `metadata.control.jdbc` |
| AI provider | `worker.ai.provider` | **M1 done**: secret providers in `provider.env`; governed decorators and request builder in `provider.governed`; OpenAI dialects in `inference.provider.openai`, `embedding.provider.openai`, `rerank.provider.openai`. **M2 done**: `EnvironmentSecretResolver` became `EnvironmentSecretProvider` implementing the shared contract |
| Connector secret | `worker.jni.write.secret` | **M1 done**: `EnvironmentConnectorSecretResolver` moved to `worker.jni.write.secret.env`. **M12 done**: bridge packages live at `org.pangrova.worker.nativebridge.jni.write.secret.env` |
| Shuffle providers | `coordinator...dispatch.shuffle` | **M1 done**: factories and lifecycle managers moved to `shuffle.provider.*`; descriptor moved to shuffle-core |
| AI governance | `worker.ai.governance` | **M1 done**: authorizers stay in `governance`; terminal observers moved to `governance.observer` |

Parent packages must not retain unrelated concrete providers once a provider
subpackage exists; the M1 gate rejects a family-empty root or a split package.

## 6. Follow-up obligations

- Every deletion above removes its descriptor in the same commit.
- `ModuleDependencyPropertyTest.serviceProvidersMatchDeclaredFamilies` is the
  executable check; update `.pangrova/modules.yaml` `providers:` with the move.
- `docs/internal/implementation-families.md` is updated in the same PR as any
  status change; stale "wired" rows are a review defect.

## 7. M3 connector SPI migration

**M3a done (2026-10-27).** Connector contracts no longer live in `pangrova-spi`.

- New leaf `pangrova-connector-spi` (`org.pangrova.connector.spi`, 39th leaf) owns the former
  `org.pangrova.spi.connector` tree, the connector index-eval contracts (`index.predicate`,
  `IndexEvalResult`, `DataFileStatistics`/`FileColumnStatistics`), the write-commit journal
  family (`coordination.*`, because those records carry connector metadata), and the
  materialization source contract (`materialization.*`, because `Connector` returns it).
  It depends on `pangrova-common` only.
- Shared contracts moved out of `pangrova-spi`: `org.pangrova.spi.annotation` →
  `org.pangrova.common.api.annotation` (keeps SPI `Experimental.reason()` distinct from
  `common.annotation.Experimental`), `SessionContext` → `common.session`,
  `CopyFilePublication`/`CopyFileCommitBinding` → `common.file`.
- Transitional edge `pangrova-spi` → `pangrova-connector-spi` existed for the remaining legacy
  contracts `index.eval.IndexEvaluatedSplit` and `index.eval.IndexFileEvaluator`. The M9
  indexing slice moved them to their owners (`pangrova-indexing-api` and
  `pangrova-connector-spi`), so the edge disappeared.
- **M15d done (2026-10-28).** The `pangrova-spi` GAV, its directory, every POM dependency,
  manifest entry and CODEOWNERS line were deleted; zero production imports and zero GAV
  references remain. `PackageStructurePropertyTest.legacyModulePackagesAreGone` now fails on
  any `org.pangrova.spi`/`org.pangrova.resource`/`org.pangrova.resilience` declaration or
  import, and `check-spi-{stability,annotations}.sh` derive their roots from the
  manifest-declared `-api`/`-spi` artifacts instead of the removed tree.
- **M15 done (2026-10-28).** Zero-implementation SPIs were deleted rather than relocated:
  `spi.cache` (`DistributedCacheStore`/`CacheKey`/...), `spi.storage.StorageProfileResolver`,
  `spi.execution.accelerator`, `spi.execution.udf`, `spi.resource.classification`,
  `spi.resource.guard`, `spi.resource.scaling`, `spi.security.credential`. Each had no
  production import, no ServiceLoader descriptor and no provider; keeping them advertised
  extension points that could never load. Their unit tests were removed with them.
- Service descriptors were renamed to
  `META-INF/services/org.pangrova.connector.spi.ConnectorFactory` in Iceberg/Paimon/Lance;
  `.pangrova/modules.yaml` providers and both `check-spi-*.sh` roots now cover
  `pangrova-connector-spi`.
- Remaining M3 work: extract `pangrova-connector-runtime`
  (discovery/classloader/lifecycle/activation) from coordinator/worker wiring and prove
  classloader isolation plus real discovery in the connector contract tests.

**M3b done (2026-10-27).** Connector plugin runtime is its own leaf.

- New leaf `pangrova-connector-runtime` (`org.pangrova.connector.runtime`, 40th leaf) with
  `classloader` (`ConnectorClassLoaderFactory`, `SharedPackagePolicy`, `PluginClassLoader`,
  `DefaultConnectorClassLoaderFactory`), `discovery` (`ConnectorFactoryRegistry`), and
  `lifecycle` (`PluginManager`). It depends on `pangrova-connector-spi` and `pangrova-common`.
- `ConnectorFactoryRegistry.discover(ClassLoader)` is now the single ServiceLoader entry point:
  both `PluginConnectorHandleFactory` (coordinator) and `WorkerCatalogRuntime` (worker) use it,
  and duplicate or blank connector names fail closed at discovery instead of overwriting.
- `DefaultConnectorClassLoaderFactory` now actually applies the resolved
  `SharedPackagePolicy` shared-package set to each `PluginClassLoader`; previously the policy
  was computed but ignored. `SharedPackagePolicy.SPI_PACKAGES` includes
  `org.pangrova.connector.spi` alongside the legacy `org.pangrova.spi`.
- Moved with their tests out of `pangrova-coordination/pangrova-coordination-core/.../plugin` (package removed).
  `org.pangrova.coordinator.plugin` no longer exists.
- `connector-runtime` capability/activation subpackages stay uncreated until real code exists;
  capability descriptors remain in `connector-spi` (`ConnectorMetadata`, `NativeIndexCapability`,
  `MaterializationCapabilities`) per the no-empty-shell rule.
- M3 exit evidence: Iceberg TCK (9 tests), SPI write/replay contracts, commit-status and
  idempotent-commit tests pass; `PluginConnectorHandleFactoryTest` discovers iceberg/paimon/lance;
  zero `org.pangrova.spi.connector` sources/descriptors remain.
