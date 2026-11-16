---
title: "Module Refactor Disposition Ledger"
description: "Maintains verified production calls, class ownership, and transaction and resource boundaries"
---

# Module Refactor Disposition Ledger

This table records classes that have been traced through production entry points.
The module manifest records reactor-wide dependencies and package ownership but
cannot prove that an interface is wired or that a path supports recovery. Modules
that have not been reviewed class by class remain listed as pending review.

Comparing the 2026-10-24 `b51114d0` baseline (before the first Coordination
authority split) with the current worktree can export the original path, target
path, artifact, and FQN of every Java production top-level and member type. The
command includes uncommitted new source. Name matching is only a review lead;
splits, merges, runtime calls, and test evidence are recorded in the tables below.
The JDK 17 parser reads actual declarations, and auxiliary classes, member enums,
and records in the same file each get an independent entry. Local classes and
anonymous implementations are reviewed with the method that contains them.

```bash
python3 .pangrova/scripts/export-class-inventory.py --base b51114d0 --output target/architecture/class-dispositions.tsv
```

## Acceptance Scope of the 16 Requirements (2026-11-14)

This review covered 153 Maven projects in the worktree, including 130 code or
resource artifacts and 23 aggregator projects. The table records verified
repository structure and behavior that still requires distribution validation
separately.

| Requirement | Verified evidence | Pending acceptance |
|---|---|---|
| 1, 2 Single-submodule and local contracts | The POM scan found no single-submodule aggregator directories. All 28 existing `-api` artifacts have direct non-test dependencies declared by artifacts outside the domain. The section "Reviewing Contract Artifacts by Actual Callers" below records callers and their semantics. | Out-of-domain dependencies show current consumers; further merging still requires checking package-level responsibilities and cycles. |
| 3 Multiple implementations and factories | Production service descriptors contain 24 service contracts and 49 registrations across 30 artifacts. AI has OpenAI and Qwen factories, and Connector can load from a process plugin directory. The former TableWriter batch-write hierarchy had no production factory, consumer entry, or close owner and was retired together with the two writers. | The section "Process Service Registration and Actual Calls" below lists registration, selection, resources, and validation scope item by item. Java spool, in-memory table implementations, and vendor online environments are accepted within their actual support scope. |
| 4–7 Flat domains, contract names, clients | Parser, Batch, File, Planner, Query, Semantic, Table, and Worker are each built directly by the domain POM. Java contracts use `.api`. The actual Client sub-artifacts are HTTP, CLI, and JDBC. | JDBC cancellation and CLI timeouts have run in the two-Worker distribution. Later client resource-reclamation changes must be revalidated with the corresponding artifacts. |
| 8, 9 Shared client contracts and JSON | Shared client contracts live in `common.client`, and JSON lives in `common.json`; the former standalone artifacts were removed from the POMs. | Recheck runtime dependencies and old classes in the final distribution. |
| 10, 14 Seven focus domains and reference designs | Scheduler splits by policy/strategies, Streaming by pipeline/source/format/writer, Metadata by catalog/codec, Materialization and Indexing by lifecycle/strategies, Generation by workflow/codec, and Execution by binding. The external contracts of all seven are released independently. The ADR records relevant Paimon, Iceberg, Trino, and SeaTunnel source references and trade-offs. No `-core` Maven artifact remains in the repository. | Validate these boundaries in same-source, role-assembled distribution regression. |
| 11, 12 Package responsibilities, naming, and isolated capabilities | The package graph is accepted by real Java references. Catalog ALTER/DESCRIBE SQL dispatch, versioning, and authority were added to the worktree. Two identical `ThreadSafe` declarations were found and unified into the Common concurrency package. The unused `TransportClientConfig` in Coordinator that pointed to a nonexistent client was removed. | `USE BRANCH` has been exercised from the client connection through to the actual Worker read. The sections below record authorization, rejection, and measurements for two connectors. |
| 13 Shared utilities | File signatures, result tickets, Flight, Shuffle, AI tenant derivation, and SQL masking all use independent HMAC-SHA256 instances created by Common. Each domain owns signed content, key derivation, encoding, and verification. | Snapshot `a6b4ec203` passed 117 gates and consumer tests in a 106-project closure. The independent signature commit `a11a56341` passed 78 items in a 102-project closure with zero skips. Final distribution regression must still verify same-source artifacts. |
| 15 Enums, exceptions, constants | The naming/declaration gate uses the JDK AST. It covers default-reactor production source roots, nested declarations, direct exception parents, and pure constant holder classes. | Type ownership and acyclicity across 922 packages passed on snapshot `a6b4ec203`. Later declaration changes are rechecked by the same gate. |
| 16 Parallel delivery and production acceptance | Role integration, Native, and client validation use isolated directories. macOS arm64 Release and ASan each passed 4333 items, and the first CLI/JDBC two-Worker cancellation run was verified. | Same-source full regression, Linux distribution artifacts, the final classpath, and the complete performance comparison still require independent evidence. |

In PostgreSQL regression, the Metadata test first stops the database at V73 to write
old data and then runs migrations to V112, failing because the old record lacks a
source provider. The new project initializes from an empty database. Streaming
PostgreSQL now uses the current creation interface to register a stream with an
explicit source provider, write offsets, and commit receipts, then delete and
recreate under the same name. Deletion clears the cache, the publication evidence
of the old checkpoint remains readable, and the recreated instance uses an
independent checkpoint. The epoch-fencing and state-round-trip property tests moved
back to Streaming PostgreSQL with it, and Metadata removed three test
dependencies. Against real PostgreSQL, this provider passed 17 state-repository
tests and 5 property tests with zero skips.

As of this review, requirements 1–15 have structural, gate, and measured evidence;
requirement 16 still lacks Linux amd64 role distribution artifacts, the full ASan
run, the final performance/resource comparison, and one same-source all-green
full-gate run. Historical SPI exceptions decreased from 7 to 5, each with a
production entry point or independent-library evidence. These numbers correspond to
the repository-wide modularization requirements.

| ID | Remaining delivery and acceptance evidence |
|---|---|
| 3 | Configuration, factories, role assembly, calls, and close for replaceable capabilities are unified through strongly typed factories/SPIs; `SpiWiringVerificationTest` (6 items) and `ServiceLoaderReachabilityTest` (6 items) pass on the final source; the two TableWriter implementations were retired as a whole because they had no production factory or consumer entry. |
| 11 | After factories moved into providers, 910 Java production packages and 5,219 cross-package references have zero cyclic components; all 5 historical SPI reachability baselines have production entry points or independent-library evidence; 12 SPI validations pass on the final source. |
| 12 | Package names, class names, and responsibility review are recorded in this ledger; mechanical scans confirm no util/utils packages outside common, no `-core` or spi packages, exceptions/constants/enums in dedicated packages, and no single-submodule parent modules; the 5 historical exceptions each have entry-point or independent-library evidence. |
| 13 | All shared utilities belong to Common, and module-specific utilities are packaged under common by module; non-common util/utils count is 0; shared implementations such as signatures create instances uniformly in Common, while domains retain content, key derivation, encoding, and verification responsibilities. |
| 16 | Same-source full Maven, Native ASan, Linux amd64 role distribution artifacts, and the final performance/resource comparison are still not all green. The macOS Release snapshot `c61dc7785` passed 4335/4335. The ASan full run of Native Shuffle mutual TLS in that round had a handshake failure; the same ASan case passed 24 isolated runs, and the root cause is not yet confirmed. Fixes for index authority tests and cold-cache traversal were pushed (`fd262a4c2`), and the original 11 PostgreSQL cases plus 6 cache cases passed revalidation. |

## November 14 Release and Resume Record

Remote `refeactor-modules` already contains the following changes. Unfinished
worktree work is preserved on `codex/modularization-checkpoint-20261114`. That
branch is for handoff; validation status is recorded separately in the table below.

| Scope | Existing evidence | Remaining acceptance |
|---|---|---|
| Query and SQL text | `009fac5c4` removed duplicate column derivation from 31 SQL handlers; 48 cases passed. `b85743ea4` fixed collisions between filtered and unfiltered queries caused by line comments, and the issue where a replaced extension could still hit the old AST. The final Parser suite (92 items) and Planner baseline tests (12 items) passed. | Revalidate the main SQL path with the final distribution. |
| Metadata and multiple implementations | In `842848058`, two Coordinators using the same PostgreSQL register the observation version before source I/O. An old observation can neither overwrite a new projection nor advance the checkpoint or write the outbox. 24 failure and concurrency tests passed. Independent Cache and Federation candidates were validated, SPI historical reachability exceptions dropped from 25 to 7, and the related candidate passed 71 tests in a targeted regression of 147 projects. | After integrating the new Cache, lease factory, and Metadata fence, rerun role discovery, authority change, and recovery, and dispose of the remaining 7 items individually. |
| Worker and clients | `abccb48c7` sends HashJoin build key domains to the scan columns of the same task. `208471094` fixed JDBC cancellation, close, and localized column labels; 242 items passed. `60b972fd4` distinguishes result notices from Native release receipts; the macOS two-Worker plus PostgreSQL client regression passed. `e7f2c2f1b` provides raw performance and resource records for 24 SQL statements, 60 Native tasks, and 309 OS samples. | Filtering for the two-sided exchange candidate happens at the receiver; real results and upstream read volume still need validation. Linux amd64 images and failure-recovery cases still need to run. |
| Items 11–13 | The Java integration snapshot has 914 packages and no cycles. SQL parsing and diagnostic baselines share text normalization. The worktree also converged SHA-256 format checks from 33 production consumers into Common and fixed default Locale dependencies in several modules. | Complete the remaining per-class responsibility review by caller, and run the summary, vendor protocol, and Turkish-locale regressions. |
| Item 16 | Early macOS Release snapshot 4335/4335. The corresponding full ASan run had one real Shuffle mTLS handshake failure; the original case passed 24 isolated repetitions. | The worktree now explicitly requires an exclusive gRPC listening port. The original implementation also passed the single-port-conflict case, so that case cannot establish the handshake root cause. Rerun Release, ASan, Java/Native, and each role distribution with the new binaries, and complete same-source full regression. |

At the end of this round, the Cache and Coordination factory integration candidate
`5cf0d2fa0` completed a targeted regression of 148 Maven projects. Coordinator lease
initialization (5 items), backend selection and failure behavior (5), SPI assembly
(5), and ServiceLoader reachability (6) all passed. This candidate is based on
source before the Metadata observation-version change. The integration candidate
`991b7b07c`, which includes the observation version, Cache, and remaining
reachability-exception changes, is still being validated.

The full ASan run of the macOS arm64 source snapshot `c61dc7785` has finished. Of
4335 items, 4334 passed; the only failure was
`NativeExchangeDataPlaneE2ETest.MutualTlsTraversesNativeConsumer`, where the client
connection phase returned `SSL_ERROR_SSL: wrong version number`. Release on the
same snapshot passed 4335 items. The exclusive-listening-port change is in a later
checkpoint and must be revalidated with rebuilt binaries. The Linux image is still
being built; real container restart cases await a matching immutable image ID.

To resume, create an independent worktree from the checkpoint and first run
`.pangrova/scripts/check-module-manifest.sh`. Under JDK 17, real PostgreSQL, and the
Native and Iceberg prerequisites required by the repository, run
`mvn -Pnative-runtime,full-regression -fae verify`. Linux restart tests consume the
immutable ID of the built image, verify its OCI source revision, and validate the
real journal and task receipts. Passing items from different source snapshots are
kept separately.

## November 14 Additional Validation

- The current source (`85e48e618`) production package graph scanned 910 packages
  and 5,219 cross-package references with zero cyclic components. The SPI
  historical reachability baseline is now 5 items, each with per-item production
  entry points or independent-library evidence.
- This round resumed from `67d0f9100` to `9ecbb6aaa`: added SPI stability
  annotations and Common annotation dependencies for Cache/Connector; fixed the
  proto golden, the documentation class-reference gate, and the protocol
  descriptor; the cache/metadata candidate `1295489229` reached BUILD SUCCESS for 8
  test classes; retired the TableWriter batch-write pipeline and
  `streaming-writers-builtin`, which had no production factory or consumer entry,
  reducing SPI historical exemptions to 5; moved BranchSelection ownership to
  Query API; fixed latent Coordination/Server assertions and the single
  DAG-completion-authority scenario; refreshed the TPC-DS q08 golden plan to a
  consistent plan across 5 cold JVMs.
- Same-source Java validation: 142 of 152 reactor projects passed (including
  Planner, Connector Lance, all Metadata/Cache, and Client); in the
  Worker/Coordination/Server/Standalone chain restored by `-Pnative-runtime`,
  Coordination (2610 items) and Server (647 items) passed after fixes. The
  remaining 2218 failures in integration modules are all explicit capabilities or
  distribution prerequisites described below.
- Same-source Native: macOS arm64 Release CTest with recursive CTEs, spatial
  functions, and GEOGRAPHY constructors passed 4336/4336 (627.18 seconds, EXIT=0),
  library SHA-256
  `3c43f95f5c4d8af0c6473a109b46add7b4f11fad9b1f2de1126f411cad21c9fa`; the
  `-Pnative-runtime` classifier installed from the same staged bytes and verified
  the same SHA-256; the new `pangrova_native_spatial_objects` and
  `parseSubstraitPlan` parsing budget both entered that same-source build.
- The recursive CTE execution chain is implemented: Substrait has no RepeatUnion,
  so `RecursiveCteExpander` expands a top-level `WITH RECURSIVE ... UNION ALL`
  before parsing into an anchor plus 15 recursive levels; each level is an ordinary
  query that follows the full Native path, and the final CTE is the UNION ALL of all
  levels; `UNION` (deduplication), SEARCH, CYCLE, and multiple recursive CTEs are
  rejected with explicit planning errors. Nested clauses exceed protobuf's default
  100-level nesting, so Java `SubstraitPlanCodec` and Native `parseSubstraitPlan`
  uniformly use a bounded recursion budget of 1024. `AdvancedQueryFeaturesE2ETest$WithRecursive`
  passed 2/2 on a real Iceberg table (sequence 5 rows, hierarchy 4 rows), and 7
  Planner tests passed (including Substrait parsing). Spatial functions are
  implemented by Native `st_geomfromtext`, `st_geographyfromtext` (WKT
  POINT/POLYGON to WKB, supporting multiple rings and holes), and
  `st_within`/`st_intersects`/`st_contains`; GEOGRAPHY and GEOMETRY share the WKB
  representation and are evaluated in planar coordinates, and geodesic semantics
  are explicitly documented as unimplemented. `SpatialFunctionsE2ETest` passed 4/4
  against real Iceberg tables and WKB columns. The Native boundary gate now
  registers `spatial` as an independent source group: 418 files, 878 local
  includes, zero cycles. Both retain explicit rejection: non-POINT/POLYGON
  geometries, empty/unclosed rings, and SEARCH/CYCLE do not produce fabricated
  results.
- Same-source full Java (`fa689d223`): all Java modules in the 152-project reactor
  passed; the final source ran shared E2E in CI form (`-Pfull-regression`, real
  AI/FILE fixtures, local stage, retaining the `native-runtime-owner` exclusion)
  for 2233 items with 1 failure, 1 error, and 2 skips. The only error was
  `CoordinatorRestartChaosTest` missing `pangrova.e2e.image`; the only failure was
  `NativeAnnRecallE2ETest` random top-K boundary ties, which passed 3/3 on isolated
  rerun and, after switching to a query point with a unique boundary, passed 3/3
  again. The 5 independent-process cases passed directly in the shared run; the
  eight isolated owner selections separately passed 18/18 in their own JVMs. The
  error-handling base now cancels the native task before waiting up to two seconds
  for terminal state; the full ErrorHandlingTest (15 items) and cancellation/
  admission sequence (16 items) both passed. `GovernedFileSqlE2ETest` 6/6 (real
  MinIO/S3 and local capability key), `LocalGovernedFileSqlE2ETest` 4/4,
  `IndependentLocalProcessesE2ETest` 1/1, `IndependentClientCancellationE2ETest`
  1/1, `IndependentLocalDiskShuffleE2ETest` 1/1,
  `IndependentSpeculativeExecutionE2ETest` 10/10, and
  `IndependentAiProcessesE2ETest` 1/1 all passed on local staging; the staging
  consists of `deploy/docker/stage-role-classpaths.py`, the same-source Release
  dylib (SHA-256
  `3c43f95f5c4d8af0c6473a109b46add7b4f11fad9b1f2de1126f411cad21c9fa`), and 1105
  `runtime.sha256` entries. The independent AI case's audit assertions read the
  production `query_audit_records` table.
- After the final Native library
  `3c43f95f5c4d8af0c6473a109b46add7b4f11fad9b1f2de1126f411cad21c9fa` was rebound
  to local staging, the classifier JAR, worker JAR, and staging native entry
  SHA-256 values matched, and `IndependentLocalProcessesE2ETest` passed 1/1 (126.9
  seconds).
- The eight isolated `native-runtime-owner` selections passed in separate JVMs per
  CI Step 7: WorkerBootstrap 5, NativeExchangeTransport 4,
  WorkerShuffleChannelAdmission 1, NativeShuffleEgress 1, NativeAiFailureProjection
  1, WorkerMembershipAdmission 1, CopySourceReadHttp 1, and LocalGovernedFile 4, for
  18 items and 0 failures; they used the `-Pnative-runtime` classifier, real
  AI/FILE fixtures, and local staging.
- 2026-11-15 final integration: `refeactor-modules` fast-forwarded from `8b5ff2b71`
  to `bf8032b00` (27 validated commits) with no conflicts and no force-push; that
  commit equals the resume branch HEAD. The only open items are requirement 16's
  Linux amd64 OCI image/container restart cases, the full ASan run, and the final
  performance/resource comparison, all retained as release gates. After renames,
  the manifest, SPI annotation/stability, Native boundary/JNI, dead stubs, E2E
  truthfulness, device paths, and documentation class-reference gates were green
  again.
- Multi-implementation SPI audit: all 15 multi-provider interfaces are published as
  strongly typed Factory/Provider/Catalog contracts and discovered through
  ServiceLoader, including Connector 3, IndexStrategy 9, RefreshStrategy 3,
  CdcFormat 3, BlobStorage 3, Authenticator 3, Coordination 2, Secret 2, AI
  Provider/Model 2, Lineage 2, Cache 2, ObjectStore 2, Scheduler 2, and TableStore
  2; no multi-implementation capability constructed explicitly by callers was
  found.
- Naming cleanup: removed the last two main-source interfaces named with an `Spi`
  suffix — former `TableMaintenanceSpi` is now `TableMaintenance`, and former
  Former `ControlledTableMaintenanceSpi` is now `CoordinatorTableMaintenance`; 7 test
  classes for the Streaming/Coordination maintenance, retention-policy, and
  scheduling paths passed, and the documentation class-reference gate passed 676
  items.
- Same-source integration items still failing: `CoordinatorRestartChaosTest`
  requires a Linux amd64 OCI image via `pangrova.e2e.image`;
  `SharedAiProviderDeploymentIntegrationTest` passed 4/4, and its hosts assertion
  now queries the IPv4 mapping the container actually installs (inside the image,
  IPv6 DNS answers return before IPv4 hosts, so the old `getent hosts` observation
  could not see the mapping). The full ASan run has not executed: the existing
  archived ASan Velox provenance records the old patch hash
  `6799535a1bdf87ce9af3f559546dbbe10ef244c210f3a19f52cf6fbf56bbeadb`, while the
  current patch is
  `7242efda7c67823843cd45444254147a054b95f101c0de9c4edd28322f448372`; same-source
  ASan requires rebuilding ASan Velox. The performance/resource comparison recorded
  24 SQL statements, 60 Native tasks, 309 OS samples, and the Shuffle read and JNI
  paths; those samples come from the previous release and the incremental
  comparison on the final binaries has not been rerun separately.
- The former TableWriter batch-write pipeline is retired. TableWriter, WriteRecord,
  WriteContext, DataFileSink, WriteMode, TableFormat, and the Lake, merge-tree, and
  incremental writers plus StreamingWriter, CdcConsumer, LateDataRouter, and
  StreamingWiring had only test construction sites, with no production factory,
  dependency injection, or consumer entry. The real CDC path remains
  CdcPipelineLoop → CdcTwoPhaseCommitter → ConnectorPageSinkCdcWriter; SQL writes
  remain Worker NativeWriteExecutor and ConnectorPageSink. Modules, service lists,
  public documentation, SPI baselines, and regression assertions were updated
  together.
- The TPC-DS q08 plan difference is only the removal of a redundant SEARCH on the
  customer_address side implied by an INNER equi-join and 20 ZIP constant domains.
  Five cold JVMs produced byte-identical plans, the join still retains the full
  constant domain, and the golden plan was refreshed according to that reviewed
  difference. Duplicate, NULL, prefix, and non-matching samples in the same query
  domain confirmed consistent IN-list and equi-join decisions.
- Lance's 48 items require explicitly passing the same-source
  `pangrova_lance_fixture_tool`. The Native source built from the main worktree
  matches HEAD file by file (`src`, `cmake`, and `CMakeLists.txt`); passing its
  absolute path yielded 48/48, and JNI/SDK use the same source artifacts.
- The AI model registry hands vendor instances implementing `AutoCloseable` to the
  version wrapper for release. Concurrent closes wait on the same result, and close
  exceptions still reach the caller. Pushed as `0ed4ff20b`.
- Vector retrieval retains the full sorted pagination path for positive OFFSET;
  zero OFFSET can still use a qualifying index. Pushed as `7a374091f`.
- Merge-join cost and Substrait generation share one key-order decision. The sort
  can be omitted on a side only when both left and right inputs already satisfy
  ascending order with NULLS FIRST. 47 Java cases and 145 Native cases passed.
  Pushed as `94e71d6b8`.
- Implementation maturity is marked with
  `@ExperimentalImplementation(trackingIssue=...)`, while API stability retains
  `@Experimental`. The JDK AST gate scans concrete method bodies in production
  source, including default methods of API artifacts. 7 gate counterexamples and
  boundary cases plus 28 CI policy cases passed. Pushed as `f522eceab`.
- Binary SQL literals require two ASCII hex characters per byte at the Parser
  entry, and recovery diagnostics preserve line and column. Coordinator DML decodes
  with the same value type. Pushed as `ca083e4ab`.
- Snapshot `43e081c4` passed the two `SqlPrivilegeAuthorityE2ETest` items. It
  verified that GRANT takes effect, REVOKE is rejected afterwards, change receipts,
  and rejection of unauthenticated requests across PostgreSQL, authenticated HTTP,
  and the Native query chain. The first isolated run stopped at shared fixture
  initialization because the prebuilt AI test image ID was not passed. After adding
  the image ID, both items ran with zero skips. The snapshot's full regression is
  recorded separately.

Catalog incremental synchronization exits directly when it receives cancellation
from the data source. The scan thread checks for interruption at startup, retry
backoff, change or table traversal, and completion-record writes, and preserves the
interrupt flag. Already committed projections retain their original receipts.
Pushed as `8ea8b1f09`. The 44 cases covering PostgreSQL recovery and cursor tests
passed, and the new case verified that the source is called once, no full scan
starts, and no completion is recorded.

File auditing was pushed as `98bb40d37`. 103 validations across 14 categories
passed, and 31 overlapping cases ran separately after the merge. Catalog DDL and
revision leases were pushed as `1ceb37abe`: 66 tests passed, and an independent
candidate ran 6 additional real Catalog SQL cases. Reader pins for query and
refresh attempts were pushed as `c0b61905d`: 41 tests passed, and an independent
candidate ran 10 additional real dynamic-table SQL cases. Both SQL groups used the
macOS arm64 library matching the release baseline's JNI constructor, SHA-256
`6eab152f31b1f305763a87cd14d77baad01959fb4fef2a00a91a03859bc85e7d`.

## Catalog DDL, Query Resources, and Branch Selection

| Capability | Actual entry, authority, and validation |
|---|---|
| Catalog configuration and authorization | SQL CREATE, DROP, ALTER, and DESCRIBE are registered by the same task group. Access checks require the current identity, and ALTER uses a dedicated CATALOG grant. ALTER creates and validates a candidate Connector before publication, then commits current, history, and invalidation using the PostgreSQL incarnation and predecessor revision. An unconfirmed commit preserves the persisted facts, and the node can reconcile again. DESCRIBE returns only name, category, provider, status, and revision. CREATE/ALTER SQL report text is stripped of properties before query-event generation, audit, lineage, and journal recording. |
| Catalog resources | QueryConnectorAuthority acquires the actual metadata and its CatalogHandle once. Early planning, incremental scanning, and metadata query scopes share this version until the query's drain and cleanup complete. The old revision closes after all readers release it, and release failures can be retried. A commit listener for the same incarnation is registered on the new handle before replacement. |
| Branch selection | `USE BRANCH` validates the current SELECT authority, tenant, table, and readable branch, then returns `branch_selection`. The HTTP Client shared by JDBC/CLI maintains the selection per connection. The next query authorizes again using the fully qualified table name, resolves the real branch head, and pins the snapshot. MAIN clears the current table's selection, and explicit VERSION AS OF takes precedence. The independent AST rewriter and its in-process branch map were removed from Planner. |
| Measured | After isolated compilation under JDK 17, Catalog authority, PG, resource, and Planner tests passed 90/90, and Catalog HTTP SQL passed 6/6; branch authority, tenant, resolution, dual-client, and protocol checks passed 44/44. Iceberg branch and tag SQL with real Workers and Connectors passed 14/14, and Paimon time-travel SQL passed 5/5. Paimon branch snapshot IDs cannot be located globally, so USE is explicitly rejected even when a real branch exists; TAG and explicit snapshot each pass. |

## Connector Factories, Index File Access, and Table Policy Repository

| Capability | Entry, owner, and failure behavior | Validation entry |
|---|---|---|
| Connector factory v2 | The three production factories explicitly publish version, role, and configuration validation. Only one declaration of a factory name is allowed within one discovery scope, and both roles use the same directory description. REST configuration validation runs before creating SDK connections. | `ConnectorFactoryRegistryTest`, `PluginConnectorHandleFactoryTest`, `LanceFactoryWiringTest` |
| Iceberg index files | The factory creates an independent FileIO, and the Connector takes over after successful construction. Index accessors and column readers borrow it; callers close open streams, and the Connector closes FileIO and Catalog after the pin timer stops. Directory enumeration returns only direct child files and raises an error when the backend lacks enumeration capability. | `IcebergConnectorShutdownTest`, `IndexGcIcebergPinIntegrationTest` |
| Paimon index deletion | The production accessor uses the SDK's non-recursive delete. If deletion returns false and the target still exists, an error is reported to the caller; a target that has already disappeared may be replayed idempotently. | `PaimonFileSystemAccessorTest` |
| Table policy binding | Former `TableControlGovernanceBindingStore` is merged into `table.api.policy.TablePolicyRepository`. The `policy` packages of PostgreSQL and Memory each own implementations. The Binder uses the atomic upsert return value directly to confirm the binding ID, first creator, and creation time. The workflow reuses the same query when reading the policy in the same transaction. | `PostgresTablePolicyRepositoryTest`, `InMemoryTablePolicyRepositoryTest`, `TableControlPolicyBinderTest` |
| JDBC primary exception | The transaction owner in each domain still decides commit, rollback, and close order. `JdbcSupport.rollbackAfterFailure` uniformly preserves SQL, driver, or resource exceptions during rollback; Query journal conflict handling still requires actual rollback confirmation. | `JdbcRollbackTest`, `JdbcTransactionsTest`, `JdbcQueryStateJournalIntegrationTest` |
| Worker observability and Secret factories | Task status reporting uses the role-assembled MetricsProvider. A real OTLP gRPC request validates deployment resource attributes. ENV/FILE factories reject duplicate prefixes or incompatible versions before creating any provider. | `RemoteTaskStatusReporterTracePropagationTest`, `TelemetryInitializerTest`, `SecretResolverRegistryTest` |

## Shuffle Server Reads and Cleanup Contention

The Direct data path runs in the Native execution side, and Local's SpoolStoreFactory
provides file read/write selected by attempt. Pangrova remote Coordinator and Worker
are discovered by their own service descriptors; both factories declare blocking
partition reads, while explicit attempt selection and speculative isolation are
still unwired.

Discovery previously entered CLEANING based only on a write reservation. An
in-progress partition read could have its objects deleted by another instance while
the authorization was still valid. ShuffleServiceEngine now validates the existing
authorization and registers a read pin through PostgreSQL, and ShuffleReadPump
releases it after asynchronous transfer completes. Independent service instances
share the same query lock and deadline, and expired pins are deleted with the
query. ReadPump also refuses to send a terminal frame before reaching the last
committed batch. Transactions, failure paths, and runtime evidence are covered by
DurableShuffleIntegrationTest and ShuffleAccessRenewalIntegrationTest.

On 2026-11-14, a 48-item reactor ran under JDK 17 in an isolated build directory;
all 20 server cases passed with no skips. Twelve DurableShuffleIntegrationTest
cases covered real PostgreSQL, MinIO, and the gRPC thread pool, attempted cleanup
from a second service instance during a read, and verified rejection until the read
protection drained after a crash. Six authorization-renewal tests and two gRPC
capacity tests passed at the same time. The first isolated build lacked the same
batch of moved exception types; after synchronizing the complete service source and
schema initialization resources, the rebuild passed.

## Catalog Projection and Control-Plane Persistence Dependencies

`persistence.workflow.postgresql.projection.ProjectionRows` and
`persistence.catalog.postgresql.projection.ProjectionRows` each held identical SQL.
`CatalogProjectionSql` now owns table-projection read, upsert, delete, and schema
observation inside the Catalog PostgreSQL artifact. It borrows connections; read
repositories own their own connections, and the projection workflow owns the
original transaction, deployment revision lock, and outbox commit. Commit
`01193e208` passed 58 cases, including 36 PostgreSQL workflow, failure, and
invalidation cases, 2 contract cases, and 20 architecture cases.

Catalog and Security PostgreSQL classes for the control plane are used directly
only in integration tests. Both dependencies are now test scope, and Server
Coordinator continues to assemble them explicitly. Domain production-dependency
closure checks now cover all domains, single-artifact contracts, and every
Persistence backend, producing the complete violation path along actual POM compile
and runtime edges. Backend-internal workflows may still borrow other PostgreSQL
implementations participating in the same transaction. Commit `6731af8ad` passed
21 architecture cases plus 6 generation-read and query-failure-cleanup cases using
real PostgreSQL.

Commit `23d98de2b` checks the unique ownership of own Java packages, module names,
and plugin registrations during role packaging. The same JAR may contain multiple
versioned implementations of one class. The macOS Native classifier gained an
explicit module name. Nine launcher and packaging tests passed, and the real role
JARs were checked with the repackaged Native classifier. Standalone has 424 JARs
and 862 Pangrova packages; Coordinator has 366 JARs and 795 packages; Worker has
309 JARs and 426 packages. The three roles have 44, 34, and 18 Pangrova service
registrations respectively, all without duplicates.

## Query Audit Assembly and SQL Execution Responsibilities

`QueryAuditEmitter` moved from Coordination to `org.pangrova.query.audit`. It
borrows the security-event write interface and the query terminal-state record
port; Server owns the asynchronous sender and its close responsibility. Assembly
previously checked only the security-event sender, so configuring only query
history storage skipped writes entirely. The two outputs can now be selected
independently, and terminal records retain the submitter, tenant, and diagnostic
SQL provided by QueryManager. End-of-query calls still go through
`QueryTerminalEvents → QueryAuditDispatcher`. The public `QUERY_CANCELLED` event
was also found to have no production emitter. The cancellation branch now submits
that event and writes a query-history record with success=false. Cancellation
reason and task physical release are still reported separately by the execution
side.

After item-by-item review of Coordinator's MERGE path, the unused
former `MergeExpressionEvaluator` was deleted. Actual MERGE is compiled and distributed
through `PlanExecutionFactory.executeDml`; the deleted local implementation treated
two NULLs as equal. Another former `PartialResultMetadata` with no registration, caller,
or wire consumer was also deleted. SQL currently declares no external
`PARTIAL_RESULT_OK` missing-partition result contract.

Normal client submissions keep the original JSON field set. A successful
`USE BRANCH` submission carries the table and branch selection, restoring MAIN
carries the table and clear flag, and status polling does not replay session
modifications. Snapshot `bb70dde59`'s full run first failed two Common response
structure assertions; after fixing field ownership, JSON omission for ordinary
queries, and branch/clear JSON round-tripping, all 331 Common items on the new
snapshot passed.

## CDC Runtime Metrics

Previously `CdcPipelineMetrics` only declared metrics. Coordinator now passes the
selected MetricsProvider through StreamingBootstrap and StreamScheduler to the
running CdcPipelineLoop. SourceReader declares an optional current lag; Kafka
aggregates the offset distance of actually assigned partitions in the poll thread.
OTel reads sampled values in the collection thread. The data point is skipped when
no partition exists or any partition lacks an observation. The process no longer
needs the two CDC-private NoOp enums.

Each started loop owns two gauges tagged with the stream. If the second
registration fails, the first is reclaimed; the loop releases them in reverse order
on recovery failure, exit, or cancellation, and queued cancellations have not yet
created gauges. Counters distinguish new polls, retried writes, and source offset
acknowledgements; an acknowledged batch retries only the watermark when watermark
storage fails. Complete metric names and units are in
[Streaming runtime metrics](./streaming.md#81-metrics). Kafka, PostgreSQL, CDC loop,
MetricsScope, and the real OTel in-memory exporter are validated by
KafkaCdcSourceReaderIntegrationTest, CdcPipelineExactlyOnceTest,
CdcPipelineLoopTest, MetricsScopeTest, and MetricAttributesOverloadTest.

## Process Configuration, Index Publication, and Error Ownership

| Former entry | New ownership and real path | Validation scope |
|---|---|---|
| Both roles built Observability configuration separately, and Coordinator always returned empty | Server Coordinator and Server Worker both construct the SDK's `ObservabilityConfig` from deployment configuration. Sampling, compression, resources, credentials, and TLS are validated in one configuration object. The process owns the SDK lifecycle, and Worker terminal task reporting records delivery failures through the same MetricsProvider | 53 configuration and SDK point tests passed. Role assembly, network export, and distribution regression are validated separately |
| Former `StateStoreFactory`, global in Config, and `state-store.type` | Deleted the static registry with no production registration or caller and the configuration key that could not change the backend. Deployment templates removed the key. Coordinator assembles the existing persistence implementation through domain ports, and Metadata JDBC configuration still points to the actually accessed data source | Build reference scans, configuration-read integration tests, role startup |
| The separate former `PublicationMode` in each of the two PostgreSQL index publishers was deleted | `IndexPublicationMode` defines descriptor conditions for initial builds and online replacement. Each workflow locks and validates the staging generation inside its own original transaction, and the original current authority, epoch, idempotency, and secondary fence execute at commit time | Publication tests for PostgreSQL scalar indexes and generation jobs |
| `JournalEventSql.PayloadConflict` and `QueryLineageFactSql.PayloadConflict` | The query PostgreSQL artifacts own immutable payload conflicts for journal and lineage respectively. A journal conflict carries typed event identity, and the queue may deterministically reject the event only after a successful rollback | Journal conflicts, rollback failures, query startup recovery |
| Remote Shuffle, Iceberg notifications, and expression errors | In-house Shuffle Service errors moved into the server-side exception, preserving gRPC failure-type mapping. Iceberg commit-notification exceptions carry the original cause, and read expressions that cannot be pushed down are independently owned by the Iceberg Provider | Server authorization and reads, commit replay, expression translation |

On 2026-11-14 under JDK 17, 49 distinct cases passed with no skips. These included
14 direct loop cases, 9 PostgreSQL commit and recovery cases, 4 real Kafka cases,
11 OTel dimension and metric-scope cases, 10 scheduling-lifecycle cases, and 1
Coordinator leadership-switch case. The first role build shared the protocol output
directory with another Maven process, and javac hit NoSuchFileException while
reading generated classes; after running a 133-item reactor in isolated source and
output directories, it passed.

## Materialization

`MaterializationReadPin` uses Execution API's `QueryId`, and
`MaterializationAttemptExecutor` uses this domain's `AttemptId`. Persistence ports
accept only `ReaderId` values typed `QUERY` or `REFRESH_ATTEMPT`. V68
empty-database initialization uses `(generation_id, reader_kind, reader_id)` as the
primary key and indexes `(reader_kind, reader_id)` for renewal and release. Expiry
cleanup and GC still check the validity period of all readers. PostgreSQL performs
renewal, targeted release, batch release, and expiry cleanup on two rows with the
same UUID, and the GC case verifies that a live refresh pin independently protects a
retired generation. Independent validation covered 38 tests for Repository, GC,
Incremental, and query integration, 3 tests for the minimal-classpath schema
artifact, and all 10 DynamicTableMaterializationE2ETest tests with real Coordinator,
Worker, and Iceberg.

Class names in the table share the `org.pangrova.` prefix, and artifact names share
the `pangrova-` prefix.

| Former class | Disposition and actual target | Production calls, facts, and resources | Validation |
|---|---|---|---|
| Former `materialization.port.MaterializationRepository` | Moved to `persistence.materialization.api.lifecycle.MaterializationRepository` in `persistence-materialization-api` | SQL handlers call the command kernel through `MaterializationKernelFactory`. Server creates `JdbcMaterializationRepository` and borrows the Metadata data source. Registration, candidate generations, publication, outbox, and pin remain same-transaction operations | `JdbcMaterializationRepositoryTest`, dynamic-table and materialized-view SQL E2E |
| Former `materialization.port.MaterializationCatalog` | Moved to `persistence.materialization.api.publication.MaterializationCatalog` in `persistence-materialization-api` | `PlannerWiring` and statement kernels borrow the same read port created by `CoordinatorBootstrap`; published heads and registry epochs come from the repository | `CoordinatorBootstrapTest`, `MaterializationReadPinIntegrationTest` |
| Former `materialization.port.EventOutbox` | Removed the orphaned write interface | No production implementation or caller. Publication and state updates already carry events in repository methods, and the PostgreSQL implementation owns transaction commit | Repository tests, materialization SQL E2E |
| Former `materialization.extension.MaterializationExtensionRegistry` | Moved to `materialization.extension.MaterializationExtensionRegistry` in `materialization-lifecycle` | Server creates the registry, and command kernels borrow the same instance to select strategies. The registry holds an immutable extension set and has no independent persistence or close action | `MaterializationExtensionRegistryTest`, `MaterializationArchitectureTest` |
| Former `coordinator.materialization.MaterializationExtensions` and `server.coordinator.assembly.materialization.MaterializationExtensions` | Removed hard-coded assembly. Server's `MaterializationDdlRegistration` calls `MaterializationExtensionRegistry.discover` and loads the three `RefreshStrategyFactory` implementations from the builtin service descriptor | Exactly one shared pure strategy per declared kind; DDL creation of the first generation requires FULL, automatic incremental refresh prefers PARTITION_REPLACE and then APPEND_ONLY. Explicit selection still matches declared capabilities first. `RefreshStrategyDiscoveryTest`, `MaterializationServiceIncrementalTest`, `PartitionReplaceRefreshStrategyTest` |
| Former `coordinator.materialization.MaterializationDdlRegistration` | Moved to `server.coordinator.assembly.materialization.MaterializationDdlRegistration` in `server-coordinator` | `QueryExecutionBootstrap` installs the handler into the statement registry and injects the repository, read projections, and strategies. Command kernels only borrow these objects | `DynamicTableMaterializationE2ETest`, `MaterializedViewMaterializationE2ETest` |
| Former `coordinator.materialization.MaterializationKernelFactory` | Retained in `coordination-control` and changed to inject persistence ports and the extension registry | Each command binds its session, authority, query fence, and clock. A native commit-validation dependency based on the data source remains, pending extraction of the corresponding workflow | `CoordinatorBootstrapTest`, materialization read pin and SQL E2E |

## Direct Shuffle and Index Builds

| Former class | Disposition and actual target | Production calls, facts, and resources | Validation |
|---|---|---|---|
| Former `worker.shuffle.NativeShuffleTransport` → `NativeShuffleProducerRegistration` | Published by the flattened `worker` in package `worker.shuffle` | `WorkerTaskBootstrap` injects the protected JNI command channel, `TaskLifecycleManager` registers each executor's producer, and `TaskResourceReleaser` unregisters it. The C++ FlightServerRegistry holds the process-shared service. Data flow stays in C++; the Worker still manages release of the Native executor handle | `NativeShuffleProducerRegistrationIsolationTest`, `NativeShuffleProducerRegistrationTest` with the real Native library |
| Former `coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager` and former `ShuffleProtocolAdapter` | Moved to `shuffle.pangrova.client.coordinator.PangrovaShuffleCoordinator` and package-private `PangrovaShuffleControlProtocol` in `shuffle-pangrova` | Server discovers ShuffleCoordinatorFactory by the configured ID and injects the corresponding session into the DAG. The provider exclusively owns registration, read/write grants, finalize validation, cleanup, gRPC channel, and close. The DAG uses only domain contracts | `PangrovaShuffleCoordinatorTest`, `ShuffleLifecycleOperationsTest`, `NativeExchangeTransportExternalIntegrationTest` |
| Former `coordinator.query.execution.dispatch.shuffle.generation.ShuffleLifecycleEvidence` | Moved to `shuffle.api.coordinator` in `shuffle-api`, with `RecoverableShuffleControl` and `ShuffleTaskAccess` added | Coordinator provides Generation-persisted journal. After provider acknowledgement it passes the immutable receipt and original credential-free response; the database still validates owner, allocation, generation, and producer set | `ShuffleContractIsolationTest`, `GenerationExchangeEvidenceTest`, `PangrovaShuffleCoordinatorTest` |
| Worker directly created RemoteShuffleClientPool and RemoteShuffleByteEgress | NativeShuffleWorkerFactory and NativeShuffleReaders in `shuffle-native`; PangrovaShuffleWorkerFactory in `shuffle-pangrova` | Server Worker installs the provider. WorkerTaskBootstrap selects the factory and validates session capabilities. Reads and writes borrow channel quotas from the same generation. WorkerTaskOwnership closes acquired resources on partial startup failure and transfers close ownership to NativeExchangeTransport, NativeTaskExecutor, and TaskManager on success | WorkerTaskOwnershipTest, NativeShuffleEgressContractIntegrationTest, RemoteShuffleChannelAdmissionTest |
| shuffle_mode in registration requests | Added shuffle_provider, Coordination API's WorkerShuffleDeployment, and schema initialization V113 | The Worker publishes the actually installed deployment during authenticated registration. WorkerManager validates registration, re-registration, and failover takeover. Memory and PostgreSQL bind the deployment to the process incarnation, and after recovery only a matching process may participate in scheduling | WorkerResourceVersionCheckTest, WorkerManagerRegistryTest, JdbcWorkerRegistryPostgresTest |
| Former `shuffle.pangrova.client.RemoteShuffleClientException` and the former Worker backend exception classification | Pangrova's `client.exception.RemoteShuffleClientException`, unified API's `exception.ShuffleReadException`, and `nativewire.exception.ShuffleChannelAdmissionException` | The Worker maps data corruption and channel-quota rejection to execution errors. It needs only API and Native wire types. Server installs the backend by role; the errors themselves have no persistent state or close action | NativeExchangeTransportExternalIntegrationTest, RemoteShuffleChannelAdmissionTest, TaskCompletionMonitor |
| Former `indexing.build.IndexBuildCoordinator` | Retained in `indexing-lifecycle` | `IndexDdlExecutor` and the incremental refresh path submit builds. Each submission creates a separate attempt identity that identifies both the database claim and the physical object directory. The creator closes the borrowed executor | Concurrency takeover and late-write tests in `IndexBuildClaimLifecycleTest` |
| Former `indexing.build.IndexFileBuilder` | Retained in `indexing-lifecycle` | Coordinator passes the attempt; the builder borrows column readers and storage accessors, computes checksums, and writes to the exact path returned by the mapping | `IndexBuildClaimLifecycleTest` |
| Former `indexing.storage.IndexStorageLayout` | Retained in `indexing-lifecycle` | `IndexFileBuilder` generates paths from generation, attempt UUID, and source file identity. Reads and cleanup of registered objects use the saved mapping | `IndexStorageLayoutTest`, `IndexBuildClaimLifecycleTest` |
| Former `indexing.lifecycle.IndexLifecycleListener` | Removed the unwired class | The former implementation inferred objects to delete from logical data file names. Production `IndexGarbageCollector` reads registered physical paths from the repository | Checked calls and assembly before deletion; real attempt isolation is covered by concurrency tests |

Boundaries still to review include recovery when an index object is written but the
mapping commit crashes, and how generations that have written partial mappings
recover. Direct Shuffle start/stop tests cover Native control and are not evidence
of publication receipts, retries, and recovery for Local, the in-house remote
backend, or Celeborn.

Shuffle transfer direction, storage medium, retention guarantees, and provider
capability enums are published by `shuffle.api.enums`. Data read modes and
partition merge policies are declared by `shuffle.exchange.enums`. No production
entry currently enables partition merging. The remote service declares ticket
operations in `shuffle.security.enums` and request failure categories in
`shuffle.server.enums`. Enum types retain their original values, publishing
artifacts, and package visibility.

On macOS arm64, `IndependentLocalDiskShuffleE2ETest` used a frozen Coordinator and
two independent Worker JVMs to create a real Iceberg table, write it in two batches,
and execute a self-join with aggregation. Results were 196 and 784. At least two
completion-stage reports came from the same Worker incarnation, with attempt 0. The
source commit is `16ca580103b74eba1130c509ab1e21d71f9f8281`, Release Native SHA-256
is `6eab152f31b1f305763a87cd14d77baad01959fb4fef2a00a91a03859bc85e7d`, and the
1096-item distribution manifest SHA-256 is
`3b63390ba1476e02c5bd45afa687d10e0135e06ac25de1f4b84de2566e7bcc11`. The test
passed 1/1 in 484.2 seconds. After independently linking Release production and test
objects, all 19 `ShuffleBufferStoreTest` cases passed, including forcing local disk
writes while memory is available, releasing files after a read, wake-up on
cancellation, and quota reclamation. Local data reads depend on the selected compute
process staying alive; cross-Worker recovery is outside this mode's guarantee.

Native's Lance dataset control and task request parsing now share
`engine/resource/WorkerStorageBinding` to validate tenant, Catalog identity, and
repository path. The direct `lance → command` edge was removed. Velox statistics
encoding is owned by `engine/velox/runner/TaskStatsEncoding`, and one idle include
was removed. Two real references to `engine → command` remain: cleaning execution
handles on session exit and locating the original handle when reporting task
errors. Component direct edges decreased from 31 to 30, and the original eight
components still form one strongly connected component. The file graph contains
414 source files, 874 local references, and zero file cycles. The native boundary
script and its 6 tests passed. Thirteen affected translation units compiled on
macOS arm64 Release and relinked the single shared library; 86/86 passed with real
PostgreSQL/Lance fixtures, including historical snapshot writes, repository
out-of-bounds rejection, and operator statistics.

The next batch removed those two `engine → command` references. Velox task
failures and asynchronous result-stream failures return through
`TaskFailureReporter` to the HandleRegistry bound to the handle, and every report
reacquires the active executor while the final diagnostic remains owned by
TaskLifecycle. JNI's initialization, restart, rollback, and exit entries provide
cleanup for the same registry, and NativeEngineLifecycle calls it under lock before
reclaiming runtime resources. Component direct dependencies decreased from 30 to
29, and the gate also rejects the original two reverse direct edges. The source
graph is 415 files, 875 local references, and zero file cycles. Indirect paths
through JNI and UDF still make the eight components mutually reachable.

JNI then injected the same reporting contract into the immutable JVM function
callback object. Function execution still writes validated AI errors into the task
through the request-bound handle. Lance schema-fingerprint computation receives the
current memory governor from the command entry, preserving the order of validating
the request and tenant before allocating the memory pool. Removed `udf → command`
and `lance → jni`, and added a one-way `udf → engine` reporting-contract reference.
The current graph has 28 component edges, 415 files, 874 includes, and zero file
cycles; the eight components remain mutually reachable. Repository gate regression
tests passed 8/8.

On macOS arm64, the CMake Release single shared library validated four real cases:
Worker capability handshake 1, AI callback registration 1, and Lance control routing
2. Routing checks authentication, tenant authority, stale epochs, duplicate
fingerprints, and fingerprint changes after a Schema column-name change; real
write/read-back is validated through public SQL. The first attempt to place the
AI test that owns the Native lifecycle and the shared SQL cluster in one JVM failed
with duplicate registration; after rerunning in separate JVMs per the
full-regression convention, both exited successfully. An intermediate batch had 31
Native cases covering execution-handle registration and restart, late result-stream
errors, and preservation of the first production failure. On the same source and
PostgreSQL/Lance fixtures, Release CTest passed 4333/4333 (318.53 seconds) and ASan
CTest passed 4333/4333 (1492.29 seconds). Both per-item logs passed sanitizer
diagnostic scans. ASan used a validated Folly build with LeakSanitizer disabled per
macOS runtime conventions.

## Shuffle Persistence Allocation Evidence

| Former class and responsibility | Current class and artifact | Entry, state, and validation |
|---|---|---|
| A single address in `ShuffleLifecycleEvidence.Allocation` and credential-bearing `registered/finalized` handles | `ShuffleProviderDescriptor` in `shuffle-api` and the journal Allocation | The journal accepts only allocation ID, provider ID, version, and up to 1 MiB of private bytes, plus the publication receipt and credential-free response. |
| `GenerationExchangeRegistration.Address/Endpoint` | Generation API retains logical exchange, stage, epoch, cohort, original owner, and descriptor. PangrovaAllocationCodec exclusively owns remote routing encode/decode. | The registration golden of GenerationExchangeEvidenceCodec and the publication golden referencing its SHA-256 were updated together. PostgresGenerationStageExchangeStore rejects conflicting provider, version, or bytes in the original authority and owner transaction. |
| Address translation in `DurableStageShuffleEvidence` and Generation routing comparison in `StageExchangeAccessValidator` | Coordinator validates the complete cohort before journal recovery. External task transport compares the actual read/write binding with the original allocation through the session's `ExternalShuffleDescriptorEncoder` in the partition claim transaction. | Dynamic authority is regranted by the actual service. Cached allocations or publication receipts also require validation against the current process owner. Real PostgreSQL takeover tests verify the old owner is rejected, the wrong generation is rejected, and the first owner is unchanged. |

This batch's independent JDK 17 compilation and first 87 tests passed. The final
45/45 revalidation covered adjusted gRPC recovery, lost-owner caching, codec, and
golden; the suite spanning the real journal, producer, and PostgreSQL takeover
passed 8/8. Persistent receipts for Direct, Local, and future Celeborn each require
their own production implementation and corresponding validation.

## Metadata Federation

| Former class | Disposition and actual target | Calls, state, and resources | Validation |
|---|---|---|---|
| Former `metadata.federation.FederatedCatalogFactory` | Moved to `metadata.api.catalog.FederatedCatalogFactory` in `metadata-api` | External factories register under this service name. Each creation returns an instance closed by the corresponding Catalog handle | `ServiceLoaderReachabilityTest`, `FederationProviderDiscoveryTest` |
| Former `coordinator.metadata.PluginFederationHandleFactory` | Moved to `metadata.catalog.federation.PluginFederationHandleFactory` in `metadata-catalog` | Server injects the Catalog registry after construction; startup discovers factories, rejects duplicate or empty IDs, and resolves the configured secret reference each time a Catalog is created | `PluginFederationHandleFactoryTest`, `FederationProviderDiscoveryTest` |
| Former `metadata.federation.FederationCatalogHandle` | Moved to `metadata.catalog.federation.FederationCatalogHandle` in Metadata Catalog | The registry owns deployment, retirement, and close; periodic synchronization borrows the external Catalog through CatalogHandle.federationSource(); external facts remain owned by the provider | `MetadataBorrowedOwnershipIntegrationTest` |
| Former `metadata.federation.FederationConnectorMetadata` | Moved to `metadata.catalog.federation.FederationConnectorMetadata` in Metadata Catalog | Forwards namespace, table, column, and statistics reads to the external Catalog. Cache-decorator tests use explicitly injected instances, while the production handle uses the instance returned by the provider | `MetadataAbstractionLayerAuditTest`, `CachingFederatedCatalogTest` |
| Former `metadata.federation.FederationTableHandle` and `metadata.federation.FederationColumnHandle` | Moved to the `metadata.catalog.federation` package of Metadata Catalog with class names unchanged | Carry only external Catalog table and column identity, with no connection or file ownership | `CatalogHandleAndStatusProtocolConsistencyTest`, `CachingFederatedCatalogTest` |

The repository currently ships no Federation backend factory. The released Iceberg,
Paimon, and Lance implementations use Connector factories; Federation test factories
validate only discovery, rejection behavior, and adapter boundaries.

## Worker Process and Standalone

| Former class | Disposition and actual target | Calls, state, and resources | Validation |
|---|---|---|---|
| Former `worker.bootstrap.PangrovaWorkerMain` | Entry moved to `server.worker.bootstrap.PangrovaWorkerMain` in `server-worker`; configuration conversion extracted to `server.worker.bootstrap.WorkerConfiguration` in the same artifact | The Worker start script loads the role JAR; the entry validates configuration and installs the Catalog runtime and shutdown hook; WorkerBootstrap owns execution resources | `PangrovaWorkerMainTest`, `WorkerPortConfigurationTest`, script classpath tests |
| Former `worker.embedding.EmbeddedWorker` | Moved to `server.worker.embedding.EmbeddedWorker` in `server-worker` | Standalone passes the actual Coordinator HTTP port. Configuration uses the same converter and installs the production Catalog runtime; Standalone closes the Worker before the Coordinator | `EmbeddedWorkerTest` |

Worker Core still contains phased assembly of execution components. On macOS
arm64, `PhysicalTwoWorkerShuffleE2ETest` passed join and global Top-K across two
Workers, with the second Worker in an independent JVM. Full Standalone startup,
independent-process failure recovery, and Linux container role distribution are
validated separately.

## Server Bootstrap and Index Mapping Transactions

| Former class or method | Disposition and actual target | Production calls, facts, and resources | Validation |
|---|---|---|---|
| `loadPangrovaConfiguration` and `parseConfigPath` in the three role Mains | Merged into `server.bootstrap.ServerConfigurationLoader` in `server-bootstrap` | Coordinator, Worker, and Standalone pass their default configuration resources before constructing services, and log redaction preserves original file-path semantics | `ServerConfigurationLoaderTest`, configuration and validation tests for the two independent Mains |
| Former `server.coordinator.bootstrap.CoordinatorResourceRelease` | Merged into `common.lifecycle.ResourceCloser.closeAll` | Coordinator, Standalone, and Streaming specify the release order of owned resources, execute all close actions, and avoid self-suppression of the same repeated exception | `ResourceCloserFailureTest` |
| Former `indexing.build.BuildFileMapping` and `indexing.core.build.BuildMappingPersister` | Original artifacts retained; the same attempt identity is passed to persistence ports | CREATE INDEX and incremental refresh share the converter. Mixed attempts in one batch are rejected before commit | `IndexBuildClaimLifecycleTest`, `JdbcIndexMappingPublicationFenceTest` |
| Former `persistence.indexing.postgresql.metadata.JdbcIndexMappingWriteFence` | Locks the exact claim in the original transaction | The same connection first locks the descriptor and required staging rows, then locks the index, generation, and owner-consistent claim, writes the whole mapping batch, and commits. An old heartbeat does not authorize later writes | `JdbcIndexMappingPublicationFenceTest` covers takeover after a successful heartbeat, publication conflicts, and batch-write rollback |

## Reviewing Contract Artifacts by Actual Callers

This review counts, by top-level domain directory, the number of other domain
artifacts whose Java main source actually references the contract artifact.
Same-domain dependencies, test code, and dependencies introduced only transitively
by Maven are excluded. Reflection discovery and external plugins require separate
review. Counts indicate whether a contract is used only inside its domain; they do
not represent implementation count and do not prove end-to-end validation.

| Contract artifact (prefix `pangrova-` omitted) | Out-of-domain consumer artifacts | Example production consumer artifacts |
|---|---:|---|
| `ai-api` | 4 | `coordination-control`, `persistence-ai-postgresql` |
| `connector-api` | 36 | `coordination-control`, `execution-binding` |
| `coordination-api` | 4 | `persistence-coordination-api`, `persistence-coordination-memory` |
| `execution-api` | 14 | `coordination-control`, `generation-api` |
| `filesystem-api` | 7 | `ai-worker`, `coordination-control` |
| `function-api` | 4 | `ai-worker`, `planner` |
| `generation-api` | 9 | `ai-control`, `coordination-control` |
| `governance-api` | 5 | `coordination-control`, `metadata-catalog` |
| `indexing-api` | 7 | `connector-iceberg`, `coordination-control` |
| `materialization-api` | 4 | `coordination-control`, `persistence-materialization-api` |
| `metadata-api` | 8 | `coordination-control`, `persistence-catalog-api` |
| `observability-api` | 10 | `coordination-control`, `execution-binding` |
| `persistence-catalog-api` | 4 | `coordination-control`, `metadata-catalog` |
| `persistence-coordination-api` | 5 | `coordination-control`, `coordination-zookeeper` |
| `persistence-file-api` | 3 | `coordination-control`, `server-coordinator` |
| `persistence-generation-api` | 5 | `coordination-control`, `execution-binding` |
| `persistence-indexing-api` | 3 | `coordination-control`, `indexing-lifecycle` |
| `persistence-jdbc-api` | 4 | `coordination-control`, `metadata-catalog` |
| `persistence-materialization-api` | 3 | `coordination-control`, `materialization-lifecycle` |
| `persistence-query-api` | 6 | `connector-iceberg`, `connector-paimon` |
| `persistence-security-api` | 8 | `coordination-control`, `governance-runtime` |
| `persistence-table-api` | 3 | `coordination-control`, `metadata-catalog` |
| `persistence-workflow-api` | 3 | `coordination-control`, `metadata-catalog` |
| `scheduler-api` | 3 | `coordination-control`, `query` |
| `security-api` | 29 | `ai-control`, `ai-worker` |
| `shuffle-api` | 7 | `coordination-control`, `execution-binding` |
| `streaming-api` | 3 | `coordination-control`, `persistence-streaming-postgresql` |

Client has three entries: CLI, JDBC, and HTTP. The 19 production types of the
former Client Core were moved into Common by calling responsibility. ResultEndpoint
and ResultProtocol, which cross process boundaries, belong together in
`common.client.result`. The HTTP SDK creates connections, CLI and JDBC borrow the
SDK, and Common declares query operations and result values.

Function was previously the only domain directory with a single submodule. Source
moved up to `pangrova-function/src`, and the root reactor builds the
`pangrova-function-api` JAR directly from that directory. AI function factories and
Native callbacks continue to depend on the same cross-domain contract.

| Former class or entry | Current artifact and responsibility | Runtime boundary and validation |
|---|---|---|
| Vendor branches in `ai.worker.provider.ConfiguredAiProviderRegistry` | `ai-worker` retains model, secret resolution, concurrent creation, and close ownership; instances come from `ai.api.provider.AiProviderFactory` in `ai-api` | `ConfiguredAiProviderRegistryTest` and `ConfiguredAiMultimodalProviderRegistryTest` cover versions, secrets, and failures |
| Former `metadata.service.TableControlWriteAuthorities` | Deleted; Coordinator assembles the PostgreSQL workflow and `MetadataTableControlPolicyProjections` through `CoordinatorTableWriteAuthority` | Generation and Query accept the injected `TableControlWriteAuthority` in the caller's transaction, preserving lock, tenant, table, and model validation order |
| Former `MetadataTableControlPolicyEvaluator` / former `TableControlPolicyEvaluator` | Deleted the forwarding interface and implementation; `PostgresTableControlWriteAuthority` uses Security Access's `DeferredAuthorization` and `DeferredPolicyReader` | Metadata Catalog production dependencies retain only workflow API, and foreground and deferred authorization share row-policy and column-mask projections for stable table identity |
| Former `ai.control.config.AiProviderDialectValidator` | OpenAI validation moved to `ai.provider.openai.AiProviderDialectValidator` in `ai-openai`; `AiProviderFactories` in `ai-control` discovers factories and rejects duplicate dialects | The registry uses the same factory batch to validate models and create model clients. General endpoint, egress, and capability limits are still validated by the control plane |
| Former `ai.provider.openai.inference.OpenAiCompatibleInferenceProvider` | Moved to `ai.http.chat.OpenAiCompatibleInferenceProvider` in `ai-http`; manages OpenAI-compatible chat calls, governed HTTP, batching, and admission | The OpenAI and Qwen providers reuse the same transport implementation and do not depend on each other |
| Former `ai.qwen.QwenProviderFactory` | The `ai-qwen` production service descriptor installs the factory, supporting TEXT inference on the Model Studio compatible interface | `QwenProviderRegistryTest` verifies deployment name, request, and result through a real local HTTP round trip and validates rejection paths and capabilities; online calls with an Alibaba Cloud account remain to be verified |
| `parser.api.ast.statement.privilege.GrantPrivilegeStatement` and `RevokePrivilegeStatement` | Retained; the grammar builds the AST through the parser, and Server's `PrivilegeStatementTasks` connects current authority and persistence, with authorization facts owned by Security | `SqlPrivilegeAuthorityE2ETest` covers authenticated HTTP submission, receipt, Native read authority after grant/revoke, and rejection of unauthorized access |

## Common Stateless Utilities and Consumption Boundaries

| Former class or entry | Target and call sites | Contract and validation |
|---|---|---|
| Former `org.pangrova.materialization.api.domain.Require`, `org.pangrova.materialization.api.refresh.RequireExtension`, and equivalent private validations in Connector, Generation, Persistence, and other artifacts | Merged into `org.pangrova.common.util.Preconditions`. Callers pass the field name and preserve the original string, exception type, and message. Optional-null rules in Semantic and AI are separate from required fields | `PreconditionsTest` verifies Unicode whitespace, preserved leading/trailing spaces, null, and field names. Generation publication, refresh attempts, and AI Blob timeout code share strict positive Duration validation |
| Numeric accessors of `CdcRow` | Reuse `org.pangrova.common.util.NumberConversions`, with callers specifying the return value for missing or non-numeric input | `NumberConversionsTest`, `CdcEnvelopeFieldEvolutionTest`. Paimon type writes still perform strict conversion and continue to fail on illegal values |
| Ordered Map snapshots in `CdcRow`, `TypedRow`, `ColumnValue.MapValue`, `SplitData`, `DataSplitProtoCodec`, `ShuffleStageStatsReport`, and `NativeMetricsPayload` | Unified into `org.pangrova.common.util.OrderedMaps.immutableCopy`, producing an independent, shallow, unmodifiable snapshot each time | `OrderedMapsTest`, `ColumnValueNestedNullTest` check input order, null, and external mutation. SQL row order and wire format are defined by the calling domain |
| `metadataPathComponent` in the HTTP Client and `CatalogDeploymentClient.encode` in Worker | Unified into `org.pangrova.common.util.UriComponents.encode`. Spaces in Worker paths use %20 and literal plus signs use %2B | `JdbcMetadataProtocolTest`, `CatalogDeploymentClientUriTest` use an HTTP peer to check paths, query parameters, and UTF-8 encoding |
| Former `org.pangrova.coordinator.http.QueryPathUtils` | Deleted the class and its only self-test. Actual state, cancel, profile, and profile JSON paths are bound directly by `QueryResource` through JAX-RS queryId path parameters | Checked production references, reflective class names, and resource registration. `QueryCancelProtocolTest` verifies actual cancel requests and return values |
| Former `org.pangrova.metadata.codec.json.MetadataJsonMapper` | Seven static forwarding methods deleted. PostgreSQL Catalog, Workflow, Indexing, and Coordinator use `org.pangrova.common.json.CanonicalJsonMapper` directly. Coordination Control and Indexing PostgreSQL removed direct Metadata Codec dependencies used only for forwarding | `CanonicalJsonMapperTest` verifies identical time representation, generics, bytes, tree ownership, and errors. Metadata Codec's `TableMetadataSerializer` continues to serve cache and PostgreSQL table models |
| Domain Codecs for Indexing, Generation, and Security plus JDBC Support | Retained original artifacts. They own index byte formats, Generation persistence evidence and replay limits, authority-scope envelopes, and connection-pool and JDBC resource lifecycle respectively | Stateless helper extraction did not move their commit authority, data ownership, or resource-close responsibilities. Shared utilities are used along existing dependency edges to Common |

This batch compiled changed source in a temporary JDK 17 output directory and ran
144 JUnit tests independently; all passed, including a real Generation publication
test against PostgreSQL 16. During the first expanded validation, the temporary
classpath mixed in old Semantic/Generation test fixtures and duplicate service
descriptors. After recompiling the current source and removing the old Provider
classpath, the results above were obtained. Common and Jackson annotations also
loaded 21 Client contract types, and a fixed encoding regression ran with full
Jackson dependencies.

## Semantic View

| Former class | Disposition and actual target | Calls, transactions, and resources | Validation |
|---|---|---|---|
| Former `coordinator.semantic.SemanticViewStatementService` | Creation, idempotency checks, source authorization, validation and reconciliation, and publication moved to `semantic.version.SemanticViewService` in the flat `pangrova-semantic` artifact | Server injects the Store and AccessControl into one domain service, and three SQL tasks call it directly through `semantic.api.version.SemanticViewOperations` in the same artifact. The PostgreSQL Store updates the active version and publication epoch in one transaction; the service borrows the repository and has no connection or publication cache | `SemanticViewSqlIntegrationTest`, `JdbcSemanticViewStoreTest` |
| Session and name resolution from the same former class | Moved to `coordinator.query.execution.dispatch.task.semantic.SemanticViewSqlScope` in the same package as the SQL task | The entry takes tenant, default catalog, and schema from the authenticated session and passes the complete identity to the domain service. SQL AST and QuerySession stay on the calling side | No-tenant rejection, cross-tenant isolation, and SQL dispatch cases in `SemanticViewSqlIntegrationTest` |

## Materialization and Streaming Submodule Review

| Artifact | Entry, state, and reason for retention |
|---|---|
| `materialization-api` | Defines attempt, generation, refresh context, persisted refresh type, refresh policy, and factory contracts for Planner, Persistence, and Coordinator |
| `materialization-core` | One definition, attempt, publication, recovery, and retention flow shares the same ports and identity; registration, first-generation creation, incremental capability decision, and plan validation share one registry |
| `materialization-strategies-builtin` | The three algorithms without external resources are released independently of Core. Server keeps only a runtime dependency. Changing service-file order does not change explicit or automatic strategy priority |
| `streaming-api` | CDC envelope, source, converter and factory, checkpoint, commit, and watermark ports are used by the production control plane and PostgreSQL |
| `streaming-core` | Pipeline read, decode, write, checkpoint, and watermark are coordinated by the same scheduling lifecycle; format implementations are discovered through services |
| `streaming-source-kafka`, `streaming-formats-builtin` | The Kafka SDK and JSON decoding formats are packaged separately; concrete production and test entries are in the Streaming source and format sections of this document |

This batch passed 64 strategy discovery, registry, refresh service, proof, and
attempt tests. It also compiled all production source of this family under JDK 17
and ran 6 package-dependency architecture tests that verify the dependency direction
of domain values, calling ports, and extension contracts.

## Indexing Core and JVM Strategies

| Former location | Current ownership | Calls and ownership | Validation |
|---|---|---|---|
| Former `indexing.build`, `indexing.core.build`, `indexing.metrics.IndexBuildMetrics` | Same artifact `indexing.build` | DDL and maintenance reuse the same build coordinator and attempt, mapping commits continue to use the current PostgreSQL claim; Core borrows executors and storage ports | `IndexBuildClaimLifecycleTest`, `JdbcIndexMappingPublicationFenceTest` |
| Former `indexing.eval`, `indexing.core.evaluation` | `indexing.evaluation` | JVM data preparation, circuit breaking, and evaluation counters belong to one capability package; the Java evaluation service is still not wired into actual Worker split pruning | `IndexEvaluationPreparationTest`, `IndexEvalCircuitBreakerTest` |
| Former `indexing.storage`, `indexing.fs`, `indexing.metrics.IndexCacheMetrics` | `indexing.storage` | Data accessors, cache, cache metrics, and attempt-specific object paths are centralized; file accessors are borrowed for a single build or read | `IndexStorageLayoutTest`, `IndexCacheDataFileIndexTest` |
| Former `indexing.store`, `indexing.planner.vector`, `indexing.config` | `indexing.evolution`, `indexing.planner.vector`, and `indexing.config` respectively | Schema changes update the descriptor through persistence ports; planning only recognizes vector capabilities and binds plans; Server constructs configuration | `VectorSearchRewriterCastTest`, full Indexing main-source compilation |
| Former `indexing.registry.IndexEngineRegistry` and `indexing.builtin.codec.BuiltinIndexSerializers` | Registry moved to `indexing.strategy`; manual serializer list deleted | Each discovered `IndexStrategyProvider` supplies its own payload codec, assembled once by Server and used for actual builds and reads/writes. Builtin supplies nine registrations on Server's runtime classpath | `IndexStrategyDiscoveryTest`, `IndexCodecGoldenTest`, `IndexTypeStableIdTest` |

## Governance Lineage Backend

Process assembly of the former `governance.lineage.LineageBackendConfiguration`
moved to Server Coordinator. `governance.api.lineage.LineageBackendFactory` is
implemented by Log and OpenLineage HTTP, with service descriptors published together
with Governance Providers. Factory discovery does not open an HTTP client; the
selected factory creates transport only after outbound-mode and outbox-requirement
checks pass. Query owns persisted facts; Governance's emitter performs actual
sending, acknowledgement, and drain before closing transport. Startup rejection and
real HTTP suites separately cover factory selection, PostgreSQL send confirmation,
and process takeover. The authorized query executor required by SQL quality checks
is still not assembled on this path.

## Scheduler Decision Inputs, Retry Budgets, and Generation Publication Routing

| Former class or package | Current artifact and package | Production entry, state, and validation |
|---|---|---|
| Former `scheduler.api.placement.{ClusterSnapshot,SchedulingWorkerSnapshot,ControlLoopAction}` | `.api.control` in `scheduler-api` | These types express one observation of the whole cluster and proposed actions. The experimental `SchedulerControlLoop` interface and empty-action implementation have independent tests; currently no Coordinator timer executes these actions and no control-loop configuration key is active |
| Former `scheduler.api.placement.{RetryBudget,FaultTolerancePolicy,NativeFaultPolicy,TaskDuration}` | The first two moved to `.api.retry`, Native fault policy to `.api.health`, and task duration to `.api.speculation` | `NativeCrashHandler` handles faults, and slow-task identification uses `TaskDuration`. `RetryBudget` has independent policy tests; retry counts for failed online tasks are maintained by Query's Stage |
| Former `scheduler.core.speculation.{SpeculativeWorkerInventory,SpeculativeQueryInventory,SpeculativeAttemptCanceller}` | `.api.speculation` in `scheduler-api` | Coordinator provides Worker/Query observation and cancellation entries. `SpeculativeTaskRegistry` and `CoordinatorSpeculativeAttemptDispatcher` with Protobuf requests or execution reports stay in the Core execution-adapter layer. Scheduler API loads independently without Protocol or Calcite |
| Lineage and attempt caching in `SpeculativeTaskLauncher` | Deleted duplicate cache; added `scheduler.api.retry.TaskRetryAdmission` | `QueryBootstrap → DAGLifecycleController → Stage` validates replaceability, holds lineage budget and slots, and registers terminal routing before dispatch. Wrong queries, no available replacement node, and exhausted budget consume no quota. Actual dispatch failure rolls back the slot, count, and route. Canceling a losing speculative attempt is separate from failed-retry admission |
| `scheduler.api.placement.WorkerSelectionStrategyFactory` and builtin algorithms | Factory and placement values remain in Scheduler API, candidate eligibility and scheduling in Core, and algorithms and service descriptors in `scheduler-strategies-builtin` | Coordinator's `coordinator.query.placement-provider` finds a version-compatible, uniquely identified factory through `QuerySchedulingWiring`. Role assembly and tenant-selection cases cover this path |
| Former `generation.core.submission.GenerationPublicationFactory` and `IndexingBootstrap.artifactPublicationStores` | Deleted forwarding and duplicate routing; uses `generation.publication.GenerationPublicationRouter` | Server maps the four output kinds (vector index, text index, generated column values, tabular export) to three PostgreSQL participants once, and `DurableGenerationPublisher` calls the selected storage ports within the existing claim and publication-confirmation scope. Unit tests check the four dispatches, rejection of unknown types, delayed retry on failure, occupied claims, and stale-claim takeover; database tests check real empty-table publication |
| `generation-api`, `generation-codec`, `generation-core` | Retained all three existing artifacts | API is referenced by Planner, Persistence, and scheduling; Codec implements value and graph encoding shared across readers; Core drives commit, execution, and publication. PostgreSQL routing is added in Server, contracts stay independent, and Core need not depend on providers in reverse |

Independent JDK 17 compilation covered the Coordinator assembly entry and SQL
end-to-end fixtures. Nineteen targeted test classes ran 158 items, including
PostgreSQL 16 container publication, Stage/DAG retry contention and rollback,
speculative cancellation, strategy discovery, and query ownership checks. These
counts cover the targeted suites that executed.

The macOS arm64 remote partition-read comparison observed persisted Arrow bytes,
mTLS response frames, and read time. Three alternating rounds produced 192 valid
samples. Environment, quantiles, throughput, reproduction steps, and the raw CSV
are in
[Remote Shuffle partition read measurement](../testing/shuffle-read-comparison.md).

The former Worker ShuffleRequestRouter and its self-tests were removed. The old Java
registry and plaintext ticket codec were called only by their own tests. Actual
Direct Flight routing is registered by the C++ FlightShuffleServer with leased
handles using signed tickets, and the shared port is released at runtime shutdown.

## Security Secrets and AI Function Resources

Coordinator, compute Workers, and AI share prefix registration and discovery from
`pangrova-security-secrets`. The Coordinator distribution includes ENV and FILE,
and the standard Worker includes ENV. The role classpath determines available
backends. `SecretResolverRegistry` maintains the unique prefix-to-implementation
mapping and rejects unknown prefixes and duplicate registrations.
`CoordinatorSecretDiscoveryTest` validates resolution through role assembly. Worker
Catalog passes the tenant scope, AI looks up the reference configured by the model,
and ENV rejects empty values. AI function registration, production resource
initialization, and close are serialized by the same `AiFunctionFactory`. Calls
after close are rejected before creating files, threads, or model clients, and
repeated close releases only once.

## Top-Level Types of AI, Security, Governance, and Connector

The 61 top-level types in these four families are classified into `.enums`,
`.exception`, and `.constants` of their owning artifacts. They include
`connector.api.enums.WriteMode`,
`security.api.exception.AuthorizationDeniedException`,
`ai.control.enums.EvaluationRunState`, and
`connector.api.constants.ConnectorFactoryContract`. Nested exception causes, states,
and constants used only in their own class remain defined by the original class.
Filesystem and the existing cache capability packages have no comparable top-level
types to move.

After further review of Connector public signatures, 18 public nested enums moved to
`connector.api.enums`. Read bindings and table declarations, snapshot references,
Native write allocations and commit results, materialization source errors, column
statistics comparison, Streaming writes, and storage sampling each have independent
types. Iceberg and Paimon system-table kinds remain in their providers' `.enums`,
preserving supported SQL suffixes and rejection behavior. Enum values and order on
SQL and the protocol remain compatible. Connector's 5 public exceptions are still
published by `connector.api.exception`. Iceberg notification failures are used only
inside the commit implementation and are not published to plugins. The factory
contract version is owned by `ConnectorFactoryContract`; handle encoding versions,
empty-snapshot prefixes, and unknown-statistics sentinels continue to be validated
by their corresponding types. The review included multi-line fully-qualified names,
static imports, reflection, and service descriptors. Point recompilation covered all
Connector family production source and affected callers, 467 compilation units in
total. All 135 tests for Connector public semantics passed, including real Iceberg
tables, Paimon materialization sources, PostgreSQL authority, and Lance SDK
fixtures.

Connector classloader-isolation tests package host contracts together with plugin
code, check public signatures and Class identity of nested types, and verify that
Common utilities and Protocol HTTP DTOs are loaded by the plugin itself.

## Quality, Lineage, and File Backends

| Former path | Current disposition and validation entry |
|---|---|
| Former `QualityCheckService`, `GovernanceStatementHandler`, and rules executed by the pre-write check | Programmatic entries and scheduled tasks use `GovernanceStatementHandler`. Shared `QualityRuleEvaluation` with the pre-write check validates rule, table, severity, and row count. Actual audit and write rejection are covered by `WriteQualityGateTest`, `CronSchedulerTest`, and others. |
| Former `GovernanceQueryRunner` | Structured reads carry a stable table UUID and tenant principal. PG SQL tests cover expressions; production still lacks an authorized scheduled read channel and candidate write post-image. |
| `LineageBackend.isAvailable()` | Both existing implementations previously returned true unconditionally; they now express delivery success through send acknowledgement. All implementations explicitly take on the close contract. LOG has no private external resources, and the HTTP client is reclaimed by the sender after drain. |
| Environment-variable parsing and hard-coded file backends in Worker | The shared secret registry connects actual Catalog and AI, and `BlobStorageFactory` provides the FILE publication path for LOCAL, S3, and OSS; `ObjectStoreAccessorProvider` provides LOCAL and S3 file spaces. Worker separately requires `ManagedBlobStorageFactory` for COPY target backends. It guarantees per-request budgets, visible cancellation, and verifiable physical backend identity. Only S3 is currently implemented; Local and OSS are rejected before opening the target. COPY exact-version sources continue to use S3. 35 targeted cases passed, covering actual Worker mTLS requests, S3 physical identity, rejection of unsupported backends, duplicate configuration, lease close, and partial startup failure. |
| Former `EmbeddingProviderLookup` and static catalog forwarding | Removed the unused null path from the former; model advertisement is read directly by `WorkerAiModelCatalog` from installed factory model definitions. The configuration registry owns creation, caching, and close during execution. |

## Public Representations of Metadata, Table, and Connectors

| Former responsibility or class | Current entry and owner | Validation |
|---|---|---|
| The five forwarding methods of Metadata's RegistryTablePolicyAuthority | CatalogRegistry binds the unique TableControlPolicyBinder at construction. It implements TablePolicyAuthority directly. Row policy, column mask, and quality rules borrow the same instance. TableControlService decides the stable ID, current deployment, and ACTIVE admission, and the PostgreSQL workflow continues to own transactions | TableControlRowPolicyStoreTest, TableControlQualityRuleStoreTest, MetadataTagOperationsTest, and TableControlPolicyBinderTest cover rename, recreation under the same name, and rejection of stale authority |
| Row-policy storage, transactional policy projection, and DefaultConnectorSession in metadata.service | Row policy and column mask are projected by stable ID in metadata.security. Connector sessions are constructed in metadata.connector; tests verify target catalog/schema, tenant propagation, and token isolation. The test asserting that cache keys use the authorized-session digest lives beside the package cache key | MetadataResolutionCacheKeyTest, DefaultConnectorSessionTest; 32 items passed together with the previous row |
| MetadataCleanupConfig with no assembly reference | The three YAML fields listed by the former class have no loading entry. The invalidation log is cleaned by the maintenance leader through PostgreSQL cleanup, and L1 is managed by the cache's own TTL. Deleted the inactive configuration declarations | ModuleInvalidationLifecycleTest checks leadership rotation and close |
| Resolved-table cache in the former `DefaultMetadataService` and `MetadataResolutionCacheKey` | `metadata.resolution.CatalogTableResolution` owns the cache isolated by authenticated session, controlled table-version validation, observed-table TTL, and post-DDL invalidation. DDL reads metadata from Connector using the current authenticated session. SQL and REST calls still go through MetadataService | DefaultMetadataServiceResolveTest, DefaultMetadataServiceCacheTest, DefaultMetadataServiceAlterTableTest, and MetadataResolutionCacheKeyTest passed 12 items in total |
| PostgreSQL transaction assembly, publication candidates, and transaction authority in `metadata.generation` | `persistence.workflow.postgresql.generation`'s PostgresGenerationStores, PostgresGenerationPublicationCandidates, and PostgresGenerationTransactionAuthority combine current authority, fence, and Generation participants. Metadata production code removed dependencies on Query, Generation, and Security PostgreSQL implementations | 30 PostgreSQL Generation storage and fencing items and 4 lock items passed. The full cross-domain commit integration group is validated with full-regression |
| HTTP endpoints, filters, and StageFileUrlService in Coordination | `server.coordinator.http` owns Jersey registration and entry points. Query, Catalog, Copy, and File resources and filters are split downward from `http.endpoint`. `http.request` independently owns request limits and in-process idempotency keys, so endpoints need not import assembly packages | After migration, FileUrlServiceTest, RequestLimitControlIsolationTest, CatalogRestResourceTest, QueryStatusResourceTest, and InternalAuthFilterPropertyTest passed 41 items. The first review found a five-package cycle; after adjusting the request package, 826 production packages were scanned with zero cyclic components |
| Same-format snapshot tag naming in Iceberg and Paimon | Common's connector.SnapshotPinTags owns UTF-8 encoding of persisted tag names and the snapshot suffix. Each connector creates and deletes tags in its own Catalog and validates snapshot-tag consistency | Commit d10e8c49: SnapshotPinTagsTest, IcebergSnapshotPinProviderTest, two SnapshotPinLifecycleTest groups, and two architecture gates passed 39 items |
| Java FuzzerCoordinator.NativeFuzzerRunner | The current distribution has no implementation, service registration, or JNI entry. The executable run-operator-fuzzer.sh calls the C++ reference runner with DuckDB for SQL-generation self-checks. Java differential execution calling Pangrova Native is unimplemented | Reviewed the Java interface, CMake entry, runner, and script within their reachable scopes |

### Replaceable Federation Metadata Cache

The former `metadata.api.cache.L2CacheStore` allowed unconditional overwrite, Memory
ignored per-entry TTL, and Server never installed it. `cache.api.CacheStore` now
returns the group and entry generation from the same atomic observation for both hits
and misses. The Memory and Redis factories are registered by two separate JARs, and
`TieredMetadataCache` populates only while the generation still matches. A Redis
instance owns its own closable connection pool, and old payloads and inactive
generation keys expire with bounds.

Production queries cache table projections through the Federation handle, keyed by
catalog, schema, table, incarnation, and revision. Synchronization and health checks
borrow the same underlying source directly, and read authority for external schemas
remains with the connector. MetadataBootstrap closes the registry and dispatcher
before closing the store; on creation failure, Server releases stores not yet
transferred. The PG outbox propagates committed invalidations to each Coordinator.
The earlier claim that Redis Pub/Sub provides cross-node consistency was corrected.
Configuration, network failure, close, cross-namespace, same-name directory
recreation, cache population, and deletion races are validated separately. In an
isolated worktree, 36 Metadata tests, 2 federation factory tests, 1 real Redis
container test, and 9 Server configuration and PostgreSQL invalidation-progress
tests passed. Redis ran two independent clients concurrently for 12 rounds of
invalidation versus stale-population races. `jdeps` shows Cache API depends only on
`java.base` and Common's annotation package, with no third-party dependencies; only
two of the 3 JARs, the providers, each publish one factory registration.

### Catalog Metadata Calls and Query Scope

The call entry of `metadata.service.DefaultMetadataService` moved to
`metadata.catalog.CatalogMetadata`. Planner enumerates catalogs through
`MetadataService`, and Coordinator assembly injects the same registry into the
Calcite schema. Query execution opens catalog scopes with the actual tenant session
through `metadata.query.MetadataQueryScopes`. This class holds exactly the same
Connector authority, partial-failure results, and counters as the request process.
After cleanup failure it keeps the instance, allowing callers holding the authority
to retry. The method that looked up authority by string query ID and released it,
used only in tests, was deleted. The corresponding tests now verify that a different
authority with the same ID is rejected, that release is impossible before wiring is
stopped, and that repeated release can be replayed.

Column types, authenticated sessions, and stable table UUIDs in the same deployment
are resolved in `metadata.governance` and handed to Governance's instance-local
binding. Catalog registration, resolution, and DDL keep their original call order.
The package-naming gate's only permanent exception for `metadata.service` was
removed; a private worktree scan of 911 main-source packages found zero cyclic
components. 101 tests passed for column tags, Catalog DDL, query transactions and
cleanup, Calcite caching, and DDL invalidation notifications using PostgreSQL and
the Paimon SDK (Metadata 44, Planner 2, Coordination 55). DDL assertions target real
`TagChangedEvent` and persistent bindings to stable table IDs. A cleanly packaged
Metadata Catalog contains 47 classes, and the removed `metadata.service` package has
no entries in the JAR.

## Execution, Query Terminal State, and Coordinator Metadata Progress

| Former location | Current ownership | Calls and validation |
|---|---|---|
| Former `coordinator.query.execution.dispatch.dag.StageCompletionCallback` | `query.api.stage.StageCompletionCallback` in `pangrova-query` | Query manages logical tasks, stages, and the final DAG result. Production `DAGDispatchEngine` explicitly determines whether the query is complete and delivers the terminal state. The implementation that defaulted to true was deleted, and JVM compilation now requires every implementation to state readiness explicitly. The port uses only JDK and `QueryFailure` |
| Former `coordinator.query.task.TaskReportCompletion` | Retained the report-receiving side and merged duplicate terminal projections in normal, background, and speculative paths | A validated report provides query, state, and four error fields, and the receiver selects the original logical task slot for the winning attempt. The receiving layer still owns fence, journal, write fragment, and drain; Query API does not accept `TaskStatusReport` or REST errors. `TaskStatusReceiverWriteCommitTest`, `SpeculativeExecutionWiringTest` observe actual order and results |
| Former `execution.core.stage.QueryExecutionScope` and `execution.core.stage.binding` | Retained Execution Binding and corrected rule documentation that had pointed to Query Core | Binds QuerySession plus the session or durable task authority, write configuration, and retrieval/rerank intent. Moving it into Query would make Execution Binding's StageExecutionContext depend on Query itself in reverse. Execution API publishes identity, deadlines, and stage/purpose separately and holds no flow state |
| Former `coordinator.coordination.InvalidationProgressRegistryAdapter` | Changed to Server's `bootstrap.CoordinatorInvalidationProgress` | Server selects the JDBC or quorum member registry once; HA and invalidation-log reads use the same registry. The query role reads its own committed watermark after registration, then starts polling. An independent maintenance role observes only the peer floor and cleans up to the lower of its locally consumed position and the minimum watermark of online nodes. It rereads the lower bound when the maintenance lease is reacquired |
| `PostgresInvalidationPoller.setInvalidationProgressRegistry` | Deleted the late-binding entry; the constructor binds the query member or maintenance observer once | On restart, registry read failure propagates to the startup layer and the operator may retry after recovery; a read error does not overwrite database progress with 0. A temporary reporting failure during polling still allows local invalidation to proceed and is reported again in a later round; the PostgreSQL reporting side passes SQL failures to the caller |

The real-PostgreSQL `CoordinatorInvalidationProgressTest` writes two committed
invalidation entries to the log; a recovered query member keeping the first
watermark evicts only the second catalog. The maintenance test has a second process
take over and release the actual database lease; after this process reacquires a
higher epoch, it deletes only entries acknowledged by lagging members.
`JdbcInvalidationPollerTest` also covers first registration, recovery after cursor
read failure, batched consumption, and unknown peer watermarks.

Re-registration with the same ID updates only member information. ZooKeeper uses
znode version CAS to preserve watermarks reported concurrently; Memory preserves
the watermark within the same map update, and PostgreSQL's registration upsert
preserves the persisted value. If the member record has been deleted, the new record
starts with its own watermark.

Independent JDK 17 validation totaled 196 items. Task reporting, changed catalog
write fixtures, and Execution Core passed 121; invalidation log, backend failure,
and maintenance lease passed 51; real embedded ZooKeeper and Memory member tests
passed 23; Execution API isolated-classpath reflection passed 1. The first write
fixture explicitly threw when deleting a catalog; it now deletes by exact deployment
identity and preserves history required by queries, and the three write-test groups
passed on rerun.

### Parser Public Enums and SQL Structure

`parser.api.enums` owns 17 public types moved out of AST nodes plus three public
enums used by dialect and keyword parsing.

| Former declaration | Target type |
|---|---|
| `Literal.LiteralType`, `BinaryExpression.Operator`, `TruthTestExpression.TruthValue` | `LiteralType`, `BinaryExpressionOperator`, `TruthValue` |
| `SortItem.SortDirection`, `SortItem.NullOrdering` | `SortDirection`, `NullOrdering` |
| `WindowSpec.FrameType`, `WindowSpec.FrameBoundType` | `WindowFrameType`, `WindowFrameBoundType` |
| `JoinClause.JoinType`, `SetQuery.SetOperatorType`, `CteQuery.SearchOrder` | `SqlJoinType`, `SetOperatorType`, `CteSearchOrder` |
| `QueryHint.HintType`, `TableSample.SamplingMethod`, `TimeTravelSpec.Type` | `QueryHintType`, `TableSamplingMethod`, `TimeTravelType` |
| `TableFunctionRef.TableFunctionType`, `UnpivotClause.NullHandling` | `TableFunctionType`, `UnpivotNullHandling` |
| `DataTypeSpec.FileType.FileStorage`, `AutoEmbeddingColumnSpec.EmbeddingMode` | `FileStorageMode`, `EmbeddingMode` |
| `SqlDialect.QuotingStyle`, `SqlDialect.CaseSensitivity`, `ReservedWordPolicy.Category` | `IdentifierQuotingStyle`, `IdentifierCaseSensitivity`, `KeywordCategory` |

Parser builders, formatters, Planner, and Coordinator statement executors use the
corresponding SQL enums. The 20 constant groups match commit
`16ca580103b74eba1130c509ab1e21d71f9f8281` in names, counts, and declaration order.
Calcite and Protobuf types continue to be handled by conversion methods at their own
boundaries. A private 131-module reactor compiled production and test code. Parser
(670), Planner (1530), HTTP client (115), and JDBC (237) ran independently, for 2552
items passed. The Parser JAR physically contains all 20 new classes, and none of the
20 moved member classes is packaged.

### Planner Public Vocabulary and Generated Plans

| Former declaration | Consumers and ownership |
|---|---|
| `RuntimeFilter.FilterType`, `DynamicFilter.Type` | Connector API's `DynamicFilterType` spans Planner MIN/MAX, Bloom filters, the Coordinator registry, split pruning, and Worker RPC. REST still transmits the original strings, and Protobuf uses existing typed payloads. |
| `AutoEmbeddingColumnDescriptor.EmbeddingMode` | Reads SYNC/ASYNC from DDL through Parser's `EmbeddingMode`. Planner continues to own text or FILE input classification. |
| `ColumnDerivation.DerivationType` | Calcite expression classification and Query audit share Governance's `ColumnDerivationKind`. Planner depends directly on Governance API, which references only Common. |
| Public enums in SQL writes, stages, and query hints | `PlannedDmlOperation` represents Calcite-planned INSERT/DELETE/UPDATE/MERGE. Worker JNI write-operation encoding is owned by Worker. Stage modes, terminal purpose, distribution, and baseline comparison results belong to Planner; call hints use retrieval kind, score direction, and AI call type under `planner.api.enums`. |
| `FilterExpressionParser.FilterParseException`, `MaskExpressionParser.MaskParseException` | The two security-expression parse errors are declared by `planner.exception`, preserving original messages, causes, and type-based catching. |
| Report versions in the four generation tasks and source-encoding aliases for persisted generated columns | BM25, vector, tabular export, and generated-value plans use `planner.constants.ArtifactBuildReport.SCHEMA_VERSION`. Native conversion validates version 2. Inline generation expressions, persisted plans, and Coordinator publication share `GeneratedColumnSourceEncoding` names and the U+001F separator. |

All 16 former member classes are confirmed absent from the four actual JARs, and
their replacements were verified. A 128-module private reactor compiled
successfully. 197 Connector contract tests and 51 Coordinator binding, lineage, and
filter tests passed. One of Planner's first 1530 items found that an isolated
classloader lacked the new Governance contract; after the fix that test passed,
and it also verifies that Governance implementations, Calcite, Protocol, Arrow,
JDBC, and Worker execution classes are not visible.

### Transport / Client / Protocol and Runtime Role Distribution Boundaries

The six sub-artifacts of Client and Transport contain no production type, package,
or service registration named Bridge or Adapter. In actual calls, the former
`org.pangrova.client.http.ResultStreamFetcher` only forwarded to
`FlightResultFetcher`, translated exceptions, and passed close requests; its only
caller was `PangrovaRestClient`. The wrapper was deleted. The SDK now owns the
Flight reader directly, translates connection or server errors at both the batch
and streaming public entries, and preserves error codes, server status, and the
original exception. Client close still seals the HTTP entry first, then closes
Flight and HTTP resources.

`BatchStreamProducer` implements Arrow's `FlightProducer` on the Flight Server side,
owns ticket validation and batch-stream delivery, and remains in Transport Flight.
Both secure and insecure listeners use the same producer. The 142 existing tests in
Client HTTP and Transport Flight passed in an independent JDK 17 reactor. New
coverage has the TLS-requiring client read a plaintext endpoint; both read entries
preserve `TRANSPORT_TLS_REQUIRED` and the original Transport exception, and the four
test items passed.

On 2026-11-14, macOS arm64, JDK 17, commit
`32b3d02b36b205bf958ccda9771589c27eb595d2`'s same-batch distribution ran
`IndependentClientCancellationE2ETest`. One Coordinator, two independent Native
Workers, PostgreSQL, and LDAP used real network ports. The distribution manifest
SHA-256 is
`66cc08c640c9d5d0619b52f7cf2705a391f99e4be61feaf26937ccb311ce2a41`, and Native is
`1197e854373d45f6ee137cd6c495fba67bdc26252517b6014c3c497f74c22392`. The 22 classes
and service-descriptor entries on the JDBC runtime classpath were checked one by one
against the frozen JARs. The first run revealed that a task report resent by a
previous query also hit the test barrier. After excluding the already observed query
and allowing reports only after public HTTP confirmed the same query was canceled,
the retest passed one item with zero skips in 100.8 seconds. The two scenarios were
triggered by JDBC `Statement.cancel()` and a 15-second deadline in the packaged CLI;
afterwards the same JDBC connection and a new CLI process both retrieved result 42
over gRPC TLS. JDBC buffers received Arrow data before creating the ResultSet. Run
instructions are in [independent process testing](../contributing/testing.md).

A later JDBC lifecycle fix covers cancellation after the server has created a query
but before the client receives the submit response. Each Statement publishes the
query ID of its request; Connection close first cancels and closes its Statements,
then releases the HTTP client. The JDBC suite of 241 items passed in an independent
Maven run. Coordinator query, DAG, and speculative attempts share one exact
cancellation-retry queue. `TaskCancelRetrierTest` passed 11 items by disconnecting
the actual HTTP connection, verifying the release receipt of the same attempt, and
reclaiming the queue. `TaskIdentityRegistryDrainContractTest` passed 11 items,
covering late result notices for retired read attempts, genuine Native release
receipts, and rejection with wrong authority or when write artifacts are present.
All runs had zero skips.

In the macOS distribution copy of `32b3d02b` above, the same source tree plus this
batch's cancellation and receipt revisions recompiled 11 source units and updated
the Protocol, Coordination, Server Coordinator, and Worker JARs. The new role
manifest is
`780f9d7734b48d25dd3c331bad915024c44513a4919924551804cbbd500622e3`, with Native
keeping the hash above. The independent-process E2E delayed the first FINISHED
until the actual resend from the same Worker arrived, checked that the PostgreSQL
tenant concurrency slot changed from occupied to idle, and then delivered the late
notice. Coordinator returned `RETIRED_RESULT_NOTICE` for that unreleased notice and
`CONTINUE` for the genuine Native release receipt. JDBC and the packaged CLI both
recovered queries and TLS reads; one suite item passed with zero skips in 123.9
seconds. An earlier test that intercepted all retries prevented a query still
holding a dispatch operation from retiring before the barrier opened and was changed
to delay only the original report. This is targeted validation of the four changed
JARs; a final repository-wide distribution still requires rebuilding and checking
the manifest.

The cancellation-audit retest updated the Query artifact's `QueryAuditEmitter` on
the previous role manifest, removed the old class from Coordination, and recompiled
the Dispatcher and the QueryManager that calls its factory. The new manifest SHA-256
is
`9cb36ae9e40843933bfc3faa0db1f9b994b315c5602b28c5c0c2c749489b0362`, with the Native
hash unchanged. The same independent-process test passed one item with zero skips in
108.2 seconds. JDBC and CLI each queried PostgreSQL by the actual query ID to verify
the historical original SQL, LDAP user, resolved tenant, completion time, and failure
flag, and checked one submission and one cancellation in the security log in order.
The first assertion required the submit audit to carry a tenant; in practice,
submission precedes tenant resolution in the admission phase. The retest separately
validated the authenticated user and SQL at submission and the resolved tenant and
cancellation result at terminal state. It then used a real Arrow IPC UUID ResultSet
under a Turkish default Locale to verify column-name reads; columns `ID` and `iD`
both returned original values. The independent JDBC suite passed 242 items with zero
skips.


| Former responsibility or call | Ownership and disposition | Evidence |
|---|---|---|
| JDBC metadata directly read Substrait vector elements and nullability | Transport Client's `ColumnDescriptor` interprets the wire description, JDBC maps Java array types and nullability flags to JDBC return values, and Protocol becomes a test-scope dependency of that module | The independently packaged JDBC JAR was checked class by class with no Protocol or Substrait type references |
| Generate role distribution manifests from the Maven runtime graph | `deploy/docker/stage-role-classpaths.py` checks main classes inside JARs, role-forbidden classes, and Native payloads, including multi-version entries; launchers start with the role's complete manifest | A macOS ARM64 private reactor install and staging produced 358 JARs for Coordinator, 306 for Worker, and 416 for Standalone; three independent JDK 17 processes verified role main classes and Planner and Native Bridge visibility |
| Child Workers in physical shuffle tests used the Surefire test classpath | Distribution validation accepts `-Dpangrova.e2e.worker.classpath`, taken from `bin/pangrova-classpath.sh`'s `pangrova_runtime_classpath worker`; unpackaged Maven reactors keep the test classpath | The Worker classifier in that snapshot had the same SHA-256 as the osx/aarch64 Native library from the same CMake build; Linux images and cross-Worker execution are separate validations |
| Query Submit pre-stored queryId by bare key | Server HTTP binds SQL and complete session attributes to the authenticated principal and key ownership. A successful save returns the original complete receipt; unregistered rejection permits retry; registered or unconfirmable failure stores an error carrying queryId | `IdempotencyKeyRegistryTest` covers concurrency and expiry, and `QuerySubmissionIdempotencyE2ETest` covers real identity, HTTP 409, drain retry, and registered failure; pending unified regression |

## Ownership of Directory Reads, Table DDL, Column Tags, and Stream State

| Former location | Production caller and current ownership | Validation status |
|---|---|---|
| Eight production types in Coordination's `metadata.read` | All moved into Planner's `core.metadata.catalog`. MetadataSchemaBuilder and LazyMetadataSchema construct Calcite structures, and MetadataResolvedTable retains the selected column declarations. PlanningReadBindings rechecks snapshot/range and cache against the current table identity, and CatalogStatisticsProvider and row types use the same binding. The authenticated session, snapshot selection, and incremental selection belong to Planner with the binding. The concrete registry implements Metadata's CatalogReadAccess, TableControlService implements TableReadAuthority, and TableControlStatisticsService implements CurrentTableStatistics | Single-module type inference and cache cases moved to Planner. Real Connector, unavailable-catalog, and SQL-chain cases continue to use downstream integration fixtures. Pending unified regression |
| CREATE, ALTER, DROP, and column-declaration conversion in DefaultMetadataService | Metadata's core.ddl.CatalogTableDdl owns ALTER mutual exclusion, connector DDL, table-identity validation, cache invalidation, and event dispatch. core.resolution.CatalogTableResolution owns the read cache. Each MetadataService instance owns one set of read caches and a DDL executor | DefaultMetadataServiceControlTest, DefaultMetadataServiceAlterTableTest, and MetadataColumnDeclarationsTest pending unified regression |
| MetadataTagOperations | Governance's core.classification.ColumnTagBindings binds the captured stable table identity through TablePolicyBindings and replays tags. Metadata resolves actual columns from Connector in the current user session; business execution does not load Metadata in reverse | PostgreSQL tag, rename, and stale-authority cases pending unified regression |
| governance-providers published logging, the HTTP SDK, and the quality executor together | The two lineage backends each register LineageBackendFactory in their own JAR. The logging backend depends on governance contracts and SLF4J; the OpenLineage backend owns the SDK and HTTP client. The SQL quality executor is published separately; rejection when the authorized executor is missing belongs to the governance flow | Duplicate-identity rejection at startup, Marquez, and HTTP cases pending unified regression |
| Two in-process stores in Streaming Core | persistence-streaming-memory owns CDC registration and watermarks. Server instantiates it on explicitly chosen in-process-state paths. Streaming Pipeline production source uses only read/write ports | WatermarkTrackerTest, StreamingGovernanceChainTest, and PostgreSQL recovery cases pending unified regression |
| Two timer task groups in ModuleIntegrationBootstrap | Server's CatalogMaintenance exclusively owns timers for catalog synchronization and invalidation-log cleanup. The coordinator installs tasks when it gains leadership, cancels timers when leadership is lost, and waits for executors to exit and closes the poller on shutdown | ModuleInvalidationLifecycleTest observes polling threads and lease rotation with real PostgreSQL; pending unified regression after the split |

### Actual Boundaries of Streaming Sources, Formats, and Writers

The source ID of `CREATE STREAM ... FROM <provider>` travels through AST, SQL
printing, StreamState, PostgreSQL, and recovery scheduling. Server discovers
`CdcSourceFactory` once, and DDL and Scheduler share the registry. DDL first checks
source attributes and format, and Scheduler creates an exclusive reader from the
persisted source and immutable checkpoint ID. The Kafka provider owns the SDK, topic,
group, rebalance, seek, commit, and close on partial initialization failures. Core
and API have no Kafka SDK compile dependency. The retired `KafkaStreamSourceReader`
implemented only the removed `StreamSourceReader` and could skip unconsumed records
when a poll was truncated; the production `KafkaCdcSourceReader` retains
per-partition fallback and recovery seek.

The three JSON formats share Jackson and registration and are packaged in
`streaming-formats-builtin`. The Lake, merge-tree, and incremental writers in the
former `streaming-writers-builtin` had only test construction sites, with no
production factory, dependency injection, close owner, or real consumer entry; after
verifying public documentation and replacement paths, the writer artifact and the
former `TableWriter` batch-write hierarchy were retired together. Coordinator CDC
writes continue to use `ConnectorPageSinkCdcWriter`, and SQL writes use Worker
`NativeWriteExecutor` and `ConnectorPageSink`.

Final unified validation must cover `StreamDdlRoundTripPropertyTest`,
`StreamSchedulerLifecycleTest`, `CdcSourceRegistryTest`,
`KafkaCdcSourceReaderIntegrationTest`, `StreamingKafkaIdentityIntegrationTest`, and
PostgreSQL StreamState access. Real Kafka delivery is an existing source; new
provider capabilities are determined by their own real implementation and
validation.

## Table Ownership Changes and Metadata Reads

| Former class and responsibility | Actual callers and ownership | State, release, and validation |
|---|---|---|
| `TableControlService.rename/completeRename` | The DDL handler calls `org.pangrova.table.rename.TableRenames` directly. `begin` writes the original registration as RENAMING, and `complete` validates the target physical identity before activating the registration. | Shares the same TableControlStore, preserving revision checks and rename intent. Versioned final state and observation records are still committed in one write. Existing Memory/PG and DDL tests participate in unified regression. |
| `TableControlService.observeVersion/markUnavailable` | Periodic observers, ANALYZE, and controlled-table maintenance call `org.pangrova.table.observation.TableVersionObservations` directly. | Observers update only the captured registration version. Unreachable catalogs and confirmed missing tables have different results. TableRenames and observations both borrow the same TableCatalogAuthority and backend. |
| `CatalogTableResolution` | Metadata reads obtain the catalog deployment identity and table ownership view before accessing Connector and recheck both after the read. | Cache keys include catalog incarnation and revision. External OBSERVED tables are resolved again after catalog recreation. Cases covering ADOPT and same-name recreation during reads were added to DefaultMetadataServiceControlTest, pending unified execution. |
| Metadata timer tasks in `ModuleIntegrationBootstrap` | Server Coordinator owns `CatalogMaintenance`, scheduling synchronization and invalidation cleanup by leadership term. | `stop` cancels scheduling, and `close` waits for real exit before releasing the borrowed poller. A release failure still allows other independent resources to be attempted, keeping the failed owner for retry. |

### Scheduler, Generation, Materialization, Execution, and Indexing Top-Level Values and Exceptions

Each row corresponds to a package in one artifact. The following 26 top-level enums,
6 exceptions, and 1 pure constant class migrate FQNs while preserving enum constant
names, declaration order, error codes, and encoded values. Implementations and tests
compile against the new FQNs. Scheduler Core continues to own queue weights, and
Indexing Core continues to own schema-change classification.

| Target package in the existing artifact | Classes |
|---|---|
| `org.pangrova.execution.api.enums` | `TaskPurpose` |
| `org.pangrova.generation.api.constants` | `GenerationOutputTypeIds` |
| `org.pangrova.generation.api.enums` | `GenerationEmptyPartitionPolicy`, `GenerationExecutionKind`, `GenerationJobState`, `GenerationOutputScope`, `GenerationPageKind` |
| `org.pangrova.generation.api.exception` | `GenerationExecutionOwnerLostException` |
| `org.pangrova.indexing.api.enums` | `DistanceMetric`, `IndexState`, `IndexType` |
| `org.pangrova.indexing.api.exception` | `IndexAlreadyExistsException` |
| `org.pangrova.indexing.enums` | `SchemaChangeType` |
| `org.pangrova.materialization.api.enums` | `DependencyKind`, `MaterializationAction`, `MaterializationEventType`, `MaterializationKind`, `PinState`, `RefreshMode`, `ScheduleState`, `SecurityMode`, `TriggerKind` |
| `org.pangrova.materialization.api.exception` | `MaterializationException` |
| `org.pangrova.scheduler.api.enums` | `AdmissionVerdict`, `ControlLoopAction`, `ErrorCategory`, `FaultTolerancePolicy`, `HealthState`, `NativeFaultPolicy` |
| `org.pangrova.scheduler.api.exception` | `CircuitBreakerOpenException`, `NoAvailableWorkerException`, `TaskDispatchException` |
| `org.pangrova.scheduler.enums` | `QueueType` |

The former `RefreshMode.accepts(RefreshKind)` pointed the enum back to the business
definition package. `RefreshKind.satisfies(RefreshMode)` now performs the same
full/incremental decision. The former `MaterializationException.notFound(MaterializationId)`
accepted a business object but used only its canonicalName; callers now extract the
canonicalName, and the exception package depends on base error types.
TaskDispatchException uses the ErrorCategory enum in the same artifact. Reviewing
source of the packages above in these five domains found only this same-domain
exception → enums production import.

Named values of StrategyId, the wire version of IndexSerializer, and field defaults
of RetryBudget and SpeculationConfig remain with the types that own their validation
or encoding semantics. Conversion tables for materialization objects, attempts, and
generations, and refresh kinds, are now uniquely owned by ObjectState, AttemptState,
GenerationState, and RefreshKind in materialization.api.enums. PostgreSQL reads and
writes persisted tags directly, and unknown tags still return storage-corruption
errors. StrategyId lives in materialization.api.identity. Package-local and
method-level helper errors keep restricted visibility. Independent JDK 17
compilation covered 337 production source files of these five domains and related
storage APIs; no hard-coded native class names of migrated types were found in JNI,
Proto, or the library. Role regression and final-distribution old-class checks await
a clean build.

### Common Types of Coordination, Server, Transport, Client, Operations, Standalone, and Testing

| Target package in the artifact | Types |
|---|---|
| `org.pangrova.coordination.api.enums` | `WorkerHeartbeatStatus`, `CoordinatorRole` |
| `org.pangrova.coordinator.exception` | `NativeDatasetControlRpcException`, `UnregisteredWorkerException` |
| `org.pangrova.operations.enums` | `JfrState` |
| `org.pangrova.operations.exception` | `DiagnosticException` |
| `org.pangrova.server.coordinator.enums` | `AuditWriterConfig`, `CoordinatorRole` |
| `org.pangrova.testing.integration.enums` | `FaultType` |
| `org.pangrova.testing.integration.exception` | `FaultInjectionException`, `SqlExecutionException` |
| `org.pangrova.transport.client.exception` | `TransportConnectionException`, `TransportException`, `TransportServerException` |
| `org.pangrova.transport.flight.exception` | `TicketValidationException` |

The two CoordinatorRole types own election state and process configuration
respectively, preserving their semantics and artifacts. None of the migrated
top-level values or exceptions depends in reverse on business packages of the
artifact. SqlExecutionException stores the error text provided by the runner and
preserves the original SQL and message; audit enum external logging/JDBC types were
previously Javadoc-only references, and the assembly package now imports the
concrete implementation. The HTTP client's public authentication scheme moved from
ClientAuthentication.Scheme to client.http.enums.AuthenticationScheme, preserving
BEARER and BASIC values and credential encoding. The CLI's QueryTimeoutException and
ResultDecodeException moved into the artifact's unique client.cli.exception package.
The SQL shell, batch decoder, and two caller test groups reference the new types. To
support cross-package access within the artifact, the two types, constructors, and
timeout-information accessors are public and marked `@Internal` for their CLI
implementation scope. Standalone has no public candidates. Four static fields of
TestObservability hold test tracing, logging, metrics instances, and filters and are
disposed of as test fixtures. Configuration class defaults remain with the same
validating type.

The three-process `IndependentLocalProcesses` uses frozen Coordinator/Worker
classpaths respectively, and the two Workers each own a PID, HTTP/Flight endpoints,
and a Native runtime. The SHA-256 manifest includes the role manifests themselves,
the corresponding role JARs, and the original flat/CLI inventory. SQL, CLI, and fault
injection share this assembly. This suite contains parallel real child JVMs and runs
serially with other suites that hold the same Native runtime, test library, or
storage container. The existing `PhysicalTwoWorkerShuffleE2ETest` instead places one
Worker and the test Coordinator in one JVM, with the second Worker in an independent
JVM.

Validation boundary: independent JDK 17 compilation of migrated public classes,
frozen-artifact verification, three-process assembly, and SQL-exception consumers;
final unified regression verifies real child processes and resource release.

## Authorization Entries and Transactions in Security, AI, and Governance

The former class names in the table below use `b51114d0` as the baseline. All
package names omit the `org.pangrova.` prefix. The test column lists assertions that
can reproduce the row independently; delivery validation distinguishes actual runs
from source review.

| Former class and disposition | Current artifact and production class | Calls, authority facts, and resources | Corresponding tests |
|---|---|---|---|
| Former `coordinator.http.endpoint.AuthenticationFilter` moved; former `security.authn.AuthenticationManager` moved with the authentication chain | `server.coordinator.http.endpoint.AuthenticationFilter` in `server-coordinator`; `security.authentication.AuthenticationManager` in `security-access` | The Jersey filter distinguishes public health checks, internal routes, and user requests. External requests set the identity attribute of the current request after authentication. Internal routes require an explicit internal token. Server's `SecurityBootstrap` validates bypass declarations of factories and instances, reclaims instances on creation failure, and closes the manager and authenticators at shutdown. | `AuthenticationFilterInternalTokenIsolationTest`, `AuthenticatorFactoryDiscoveryTest`, `SignedOidcSecurityBootstrapIntegrationTest`, `LdapAuthenticatorTest` |
| Former `security.authz.DynamicRbacAccessControl` moved into the policy implementation. Authorization changes and replay were split into dedicated persistence ports | `security.policy.builtin.authorization.DynamicRbacAccessControl` in `security-policy-builtin`; `persistence.security.postgresql.grant.JdbcPrivilegeMutationRepository` in `persistence-security-postgresql` | Server reads roles at startup and checks backend availability for authorization. Query checks read current roles, permissions, row policies, and column masks. GRANT/REVOKE management SQL enters a repository-owned transaction, checks the current executor's authority after locking, commits grant rows and the request receipt together, and callers can confirm replay using the request identity. | `PrivilegeSqlExecutionTest`, `JdbcPrivilegeMutationRepositoryTest` |
| Former `worker.ai.governance.ConfigurableAiInvocationAuthorizer`, `worker.ai.context.GovernedAiBindings`, and `worker.ai.invocation.AiInvocationService` moved to the Worker-side AI runtime | `ai.worker.governance.ConfigurableAiInvocationAuthorizer`, `ai.worker.context.GovernedAiBindings`, `ai.worker.invocation.AiInvocationService`, and `AiTaskBudget` in `ai-worker` | `AiFunctionFactory` installs the invocation authorizer. It denies by default when unconfigured, and enabling the builtin authenticated mode requires model, capability, and permission allowlists. Actual callbacks validate calls against the approved Task/Attempt and OperatorBinding, and budget registration and cancellation belong to the task lifecycle. The configuration authorizer and task binding perform distinct checks, and both run before outbound calls. | `ConfigurableAiInvocationAuthorizerTest`, `GovernedProviderTest`, `AiPhysicalInvocationAdmissionTest` |
| Added model current-authority collaboration interfaces and PostgreSQL implementations at publication time | `persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority` in `persistence-ai-postgresql`; `persistence.workflow.postgresql.generation.PostgresGenerationTransactionAuthority` in `persistence-workflow-postgresql` | Coordinator injects the same-connection direct role reader. The Generation publication workflow acquires the authority lock first, rereads tenant, table, and model authority in the same request transaction, and compares model incarnation, revision, and deployment signature. The model reader borrows the connection, and closing and committing the transaction belong to the upper workflow. | `ModelRevocationAuthorityIntegrationTest`, `QueryOnlyWriteAuthorityIntegrationTest` |
| Former `metadata.service.MetadataTagOperations` split; tag operations moved to Governance and column resolution stayed in Metadata | `governance.runtime.classification.ColumnTagBindings` in `governance-runtime`; `persistence.table.postgresql.policy.PostgresTablePolicyRepository` in `persistence-table-postgresql` | Metadata resolves the actual columns and current table identity. Governance validates PII type, persists the binding to the stable table UUID through Table's policy port, and then notifies the tag projection. Replay checks the current catalog, and a renamed table keeps its original identity. | `MetadataTagOperationsTest` |
| Former `governance.quality.QualityCheckService` deleted; former `governance.quality.rule.GovernanceStatementHandler` moved; domain-shared result validation extracted | `governance.runtime.quality.GovernanceStatementHandler`, `QualityRuleEvaluation`, and `RuleBasedWriteQualityGate` in `governance-runtime`; `governance.quality.executor.sql.SqlQualityRuleExecutor` in `governance-quality-sql` | Scheduled and programmatic entries obtain rules from the rule repository; the executor binds results to rule ID, table UUID, severity, and row count, and the pre-write entry handles execution failures by rule severity. SQL read requests carry the table identity, and execution identity explicitly carries principal and tenant. Production scheduled SQL reads currently lack a re-verifiable durable principal, and pre-write lacks a visible view of candidate commit data; both remain non-executable. | `GovernanceStatementHandlerTest`, `WriteQualityGateTest`, `QualityPredicateSqlIntegrationTest`, `QualityAuthorityMigrationIntegrationTest` |

On 2026-11-14, source for 16 test classes in the table rows was compiled under JDK
17, and JUnit with PostgreSQL ran independently: 109 items passed with 0 skips.
Authority-change failures included losing commit acknowledgement during execution,
audit write failure, third-party revocation of delegated authority, and cascading
revocation by grant option. The model test actually deleted and recreated roles
before rechecking deferred authorization; Worker retests covered missing Native
context, impersonating another operator, and wake-up on cancellation. OIDC used a
test issuer signature and checked audience and service interruption. LDAP binding
and errors were validated against a test directory context; interoperability with a
real LDAP server and production certificate chain has not run. The Worker
authorization tests above made no requests to commercial AI vendors. Production
support status for scheduled SQL and post-write views is shown in the table above.

## Index Persistence Ports and Garbage-Collection Protection

A single PostgreSQL repository provides nine operation groups. Descriptor caching
and stuck-build recovery use `IndexDescriptors`, build-output validation uses
`IndexBuildGenerations`, mapping writes use `IndexFileMappings`, and generation
publication uses `IndexPublications`. The commit-log poller uses `IndexCommitLog`;
shared circuit-breaker state uses `IndexBreakerStates`. `IndexCleanup`,
`IndexCacheEvents`, and `IndexFileStatistics` define cleanup, cross-process
invalidation, and statistics operations respectively. Consumers that need multiple
capabilities and Server inject the same repository instance through
`IndexMetadataStore`. Database connections, publication transactions, and the claim
lock for mapping writes remain owned by the original PostgreSQL implementation.

Unimplemented persistence operations explicitly reject calls. Cleanup candidates
with creation-time protection include only mappings older than the cutoff.
Status-based pagination orders by update time and index ID.
`IndexPersistenceGuaranteesTest` covers missing implementations and cleanup time
boundaries, and `IndexPersistenceApiIsolationTest` checks independent contract
loading.


## 2026-11-14 macOS Two-Worker SQL Sampling

On the same macOS arm64 host, JDK 17 and Release JNI built from source were used to
compare two frozen distribution manifests. Historical Java source is commit
`ec51db67b183fd8af2495c4ab099b4837c6e7ee9`, and this round's Java source is worktree
digest `69b7aea5dd0fc63b6cd48d1fd9d5ee7ed288038cc8839517c4385ed997ecae3e`. The
subsequent index-file fingerprint change came after this sampling. The historical
version used artifacts actually present in that commit's Dockerfile and their
runtime dependencies. Its manifest also referenced `pangrova-file-worker-adapter`,
which was deleted in that commit, and the test assembled the File reactor as it
existed then.

Fresh Coordinator, two independent Workers, PostgreSQL, and LDAP were started in
the order historical, current, current, historical. Each round wrote three data
files into an Iceberg JDBC Catalog, each with three rows of 4, 5, and 5. Constant
SQL and full-table sum alternated, each warmed up three times and sampled twelve
times. All 96 formal queries had independent query IDs, and the public CLI verified
a single-row result of 42, a FINISHED terminal state, consumed results, and gRPC
TLS. Timing started before launching the CLI subprocess and covered CLI completion
and the parent process's query-state validation.

| Workload | Samples per version | Historical median / P95 (ms) | Current median / P95 (ms) |
|---|---:|---:|---:|
| `SELECT 21 + 21` | 24 | 3994.75 / 7686.60 | 3527.89 / 5314.47 |
| `SELECT SUM(id)` | 24 | 5065.55 / 12416.22 | 4651.81 / 6293.05 |

RSS of each subprocess was sampled per second after the first formal sample
completed. Maximum values for historical and current versions were Coordinator
389.3 / 393.0 MiB, per Worker 187.5 / 196.0 MiB, and per CLI 135.8 / 141.5 MiB. RSS
includes heap, Native memory, and shared mappings. JNI copy volume, network bytes,
and wait-queue duration were not measured separately. A Linux Native build and
PostgreSQL tests were running in the background, so the timing distribution
describes only these two frozen manifests on the shared host.

The runtime manifests of the historical and current versions have SHA-256
`ccec52b0a3cea4c7b38559f65d0c5b14d969c4c3658753e3c0081159667ef46d` and
`b3788bd0c2df8c08f23b3389ace39b3b11058595553a5418e435cc2306a8ee11`; Release JNI
digests are
`6cfa0b80749bcb0d322948d394bbd9aebe36f843f5c7071c903a19bc00f0682f` and
`75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929`.

[Reproduction commands and per-round distribution](../testing/speculative-process-e2e.md#measuring-public-sql-latency-across-two-builds) · [Raw query records, RSS, and digest](https://github.com/pangrova-io/pangrova/tree/main/pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-process)

### Two-Worker Resource, Reclamation, and Shuffle Backpressure Supplement

The resource comparison reused the macOS machine, two source versions, distribution
manifests, JNI, and three-file Iceberg data described above. Four rounds ran in the
order historical, current, current, historical. Each round warmed each SQL statement
once and then measured three times, for 24 formal queries. After the client received
42, the sampler waited until all Worker tasks observed for the same query returned
Native owner-released receipts and checked in PostgreSQL that the tenant concurrency
slot was zero before accepting the sample. All four rounds shut down processes,
connections, and containers normally. Latency below is the median from client
validation completion to observing those conditions, including database read cost.

| SQL / version | Tasks per query | Max Velox operator memory reservation (bytes) | Release-observation latency ms | Worker status HTTP request/response total bytes (per query) |
|---|---:|---:|---:|---:|
| Constant / historical | 1 | 9,280 | 73.7 | 4,102–4,105 / 213 |
| Constant / current | 1 | 9,280 | 138.0 | 4,104–4,110 / 213 |
| Iceberg SUM / historical | 4 | 75,648 | 148.6 | 16,316–16,363 / 639 |
| Iceberg SUM / current | 4 | 75,648 | 126.0 | 16,332–16,350 / 639 |

Final statistics for all 60 actual tasks were readable, Velox reported consumer wait
of 0 ms and spill of 0 bytes. The single-row results of both SQL statements produced
no observable millisecond-level blocking. Request and response bodies were counted
by the HTTP relay and cover only the task-status endpoint.

macOS libproc sampled RSS, threads, open descriptors, and socket descriptors of the
same role JVM, and JDK 17 `jstat -gc` sampled heap usage independently. In total 309
OS observations were saved. The comparison window runs from the end of each round's
first formal query to just before completion of the last, excluding startup and exit,
leaving 90 samples with heap data. Values below are the maximum observed per metric
for a single process of each type; thread and socket counts include the service
process's own background work.

| Version / role | Samples | Heap MiB | RSS MiB | Threads | File descriptors | Socket descriptors |
|---|---:|---:|---:|---:|---:|---:|
| Historical / Coordinator | 14 | 201.2 | 348.6 | 110 | 546 | 19 |
| Current / Coordinator | 16 | 213.8 | 257.1 | 109 | 442 | 17 |
| Historical / single Worker | 28 | 96.3 | 179.3 | 115 | 533 | 16 |
| Current / single Worker | 32 | 95.2 | 188.8 | 123 | 377 | 15 |

Two additional rounds used JVMTI NativeMethodBind to read copied array lengths when
the actual task-status, statistics, and release JNI methods returned. Both rounds
created the same table and executed two constant queries and two Iceberg sums, and
both Worker PIDs were checked against role logs. Historical release method: 14 calls
/ 6,224 bytes; status method: 186 calls / 85,680 bytes. Current release method: 14
calls / 6,232 bytes; status method: 318 calls / 127,521 bytes. Neither round called
the separate statistics method. Poll counts depend on process residency. This
observation covers return copies of three Native methods from Worker startup to exit;
other JNI interfaces, total process Native memory, and host network traffic were not
measured.

A repeatable slow-reader load was also built against the same version's production
`ShuffleBufferStore`. Capacity was one record, residency budget 0 bytes, and the
first batch occupied 65,536 bytes and one actual spill file. The second producer
called the store, and the consumer released the first record after at least 40 ms.
The historical version returned a capacity error 12/12 times with a median call
duration of 0.012 ms; the current version waited for the consumer to return quota
12/12 times, fully replaying the second 65,536 bytes with a median of 44.12 ms and a
range of 40.96–45.75 ms. After 24 runs, record count, spill bytes, and file count all
returned to zero. This measurement reused the same C++ toolchain and SDK and compiled
the real storage source and headers of both versions separately. It covers storage-
layer backpressure alone, while the SQL measurement covers the complete path
including the client.

The four SQL rounds, two JNI rounds, and storage-layer comparison each used
independent processes. The concurrent Linux Native build and partially overlapping
other isolated SQL processes affect timing. These resource samples delineate per-path
cost and release behavior. Historical query latency, remote Shuffle throughput, and
this batch's data retain their original sampling scopes. [Raw CSV](https://github.com/pangrova-io/pangrova/tree/main/pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-resources) · [Reproduction commands](../testing/speculative-process-e2e.md#measuring-public-sql-latency-across-two-builds) · [Remote Shuffle throughput](../testing/shuffle-read-comparison.md)

## 2026-11-14 macOS Distribution Role Artifact Verification

After Corretto 17 installed 149 reactor modules on Apple M2, 363 JARs for
Coordinator, 309 for Worker, and 421 for Standalone were frozen in Maven runtime
dependency order. Three independent JVMs used production launchers to select their
classpaths. Coordinator loaded `SqlCompiler`; loading WorkerMain and `NativeRuntime`
returned ClassNotFoundException. Worker loaded WorkerMain and `NativeRuntime`;
loading CoordinatorMain and `SqlCompiler` returned ClassNotFoundException.
Standalone loaded all four types plus its own main method. The Pangrova main artifact
in each role matched the corresponding JAR in this batch's target byte for byte.

The two Native resources in Worker and Standalone came from the Worker JAR and the
platform classifier respectively, and both matched this batch's Release dylib
SHA-256 `75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929`. The
1,096-record runtime.sha256 digest is
`4d052d60150412c320eae6b97f8d3f1e9f0716ad4360cc805dc55b2c3e69aed3`. This
validation covers macOS role artifacts, classloading, and Native byte consistency.
[Role freezing and recheck commands](../testing/speculative-process-e2e.md)

## Database Validation of CDC and Metadata Invalidation Chains

Former `CdcCheckpointDatabase` has been deleted. `CdcPipelineExactlyOnceTest` and
`TwoPhaseCommitCoordinatorTest` each own an independent PostgreSQL schema initialized
by the formal migrations, and each case clears that class's checkpoint table. The
old-row failure case first initializes the isolated schema to V72, writes a
representative row, applies formal V73, and verifies rejection of the new bad row via
PostgreSQL error code 23514. In `CdcEosChaosIntegrationTest`, the actual Coordinator
and JDBC Store hold PREPARED, commit, and recovery state, Toxiproxy interrupts the
database connection, and the test sink independently records unpublished attempts.
JDK 17, PostgreSQL 16, and Toxiproxy 2.12.0 ran these 44 items for real; all passed.

`MetadataSyncPropagationE2ETest`, `MultiCoordinatorArchE2ETest`, and
`MultiCoordinatorRoleRoutingE2ETest` borrow the SQL test cluster's PostgreSQL, each
initializing an independent schema, with formal V42 providing the commit-order
trigger. The cross-connection case places the same table in the read-side cache,
submits an invalidation message, and waits for the independent poller to actually
evict its cache entry. The three classes ran 35 items with JNI, HTTP SQL, Iceberg
candidate, and real model fixtures on macOS arm64; all passed. One case that directly
wrote the invalidation log was then replaced with a production projection-transaction
replay test and passed separately, verifying the persisted table, receipt, and one
invalidation record. Cache validation with two Coordinators used independent
connections and pollers within a shared test process.

## Query Registration, Task Identity, and Materialization Commands

| Former entry | Disposition and call path | State, resources, and validation |
|---|---|---|
| Submit, recovery, cleanup, and drain in `coordinator.query.QueryManager` | Both submission and recovery reserve the query ID in `query.lifecycle.QueryRegistry` first and publish the instance after authority initialization succeeds. Retention removes expired queries through the same registry. | A registration failure releases the reservation; a published query holds a drain count until physical cleanup completes; the same state machine releases only once. `QueryRegistryTest` covers duplicate submission during initialization, reading a half-initialized instance, retry after initialization failure, and repeated completion. |
| `coordinator.scheduler.RemoteTaskDispatcher.bindControlIdentity` | Task and Shuffle identity binding moved to `execution.binding.TaskControlBinding`. Dispatcher still checks current authority, Worker routing, HTTP timeout, and retries before sending. | Bound tasks and Shuffle must match the same plan epoch and execution owner. Encapsulation, conflict rejection, epoch catch-up, COPY TLS, and HTTP tests passed 24/24. |
| Former `materialization.usecase.MaterializationService` | Definition planning and replay validation belong to `MaterializationDefinitionPlanning`, CAS schedule updates to `MaterializationSchedules`, and cascading deletion to `MaterializationRemoval`. Commands still use the same repository, validating management authority for all objects to delete before updating them one by one. | CAS retry, authority, source pin, definition replay, refresh, post-publication reclamation, and recovery tests passed 66/66. |
| Leadership callbacks maintained by `server.coordinator.bootstrap.ModuleIntegrationBootstrap` | If any step of an election startup fails, revoke that term and stop already installed services in reverse order; when leadership is lost, continue attempting every stop action. | `ModuleInvalidationLifecycleTest` injects scheduler rejection, checks that the next term can start again, and that repeated notifications do not accumulate tasks. |

Joint validation of query registration, terminal state, read pins, recovery, index
persistence guarantees, and cleanup boundaries passed 77/77 under JDK 17 and
PostgreSQL. This number does not include independent-process acceptance of role
distribution artifacts.

## Worker Task Registration, Cancellation, and Catalog Operation Scope

| Baseline responsibility | Production entry and current class | Facts, resources, and failure semantics |
|---|---|---|
| Task table and local ID counters in `worker.task.TaskManager` | Worker HTTP → `TaskManager` → `TaskSubmissionHandler` → `WorkerTaskRegistry` | The registry exclusively owns local resource IDs and corresponding `TaskInfo`, and concurrent publication checks capacity in the same critical section. Records that acquired resources but failed to roll back may exceed the ordinary admission limit until actual cleanup completes. Exact-attempt admission/replay receipts are held separately by `TaskAdmissionRegistry` and remain queryable after resource release. |
| Cancellation and reclamation in `TaskSubmissionHandler` | Exact seal, task retirement, and Worker-internal cancellation enter the existing `TaskLifecycleManager` through `WorkerTaskCancellation` | The same `TaskInfo` continues to perform terminal CAS. Failure to cancel Native propagates the failure; other tasks of the same query can still receive cancellation, and the failed attempt remains in the query index for retry. Receiving a cancellation means only that the request was accepted; complete exit still requires a Native receipt. |
| Global countdown latch for drain | `WorkerShutdownSequence` → `TaskManager` → `TaskDrainExecutor` | After rejecting new records, wait for terminal state by exact local ID. Repeated notifications, concurrent drains, and already removed terminal tasks are handled by actual task state. On timeout or waiting-thread interruption, forcibly cancel attempts still active and preserve the interrupt flag. |
| Former `TaskRetryExecutor`, former `TaskRetryConfig`, and former `TransientFailureClassifier` | Retired Worker startup created them and recorded an enabled object with no callers plus orphaned unit tests. Real failure rescheduling is coordinated by Coordinator's `QueryBootstrap`, Scheduler's `SpeculativeTaskLauncher.retryFailedTask`, and `TaskRetryAdmission` | New attempt allocation and budget remain on the control plane. Workers report actual failures and do not resend writes by reusing the original attempt. |
| Version caching and task-output validation in `worker.catalog.WorkerCatalogRuntime` | `WorkerCatalogRegistry` holds the connector keyed by tenant, catalog, incarnation, revision, and fingerprint; `WorkerCatalogRuntime` binds task output; `CatalogControlLease` limits the lifetime of control operations | The registry owns single initialization, LRU, lease counts, and retry after close failure. The hot-cache entry validates the SHA-256 algorithm and digest length; control operations additionally require the output-embedded deployment to exactly match the request. Each task holds its own session and an output handle validated by the shared codec. `NativeWriteAdmission` prevents release from racing the current Native write call. |

On 2026-11-14 under JDK 17, 484 source units of the Worker family and integration
callers were compiled in an isolated output directory. Task submission, JNI,
memory/result release, cancellation, and admission groups passed 82 items. Then 33
items including drain races and 33 items including retry after real Native
cancellation rejection passed, with overlap between the two rerun groups and the
first. Another 9 items of the real authenticated Coordinator, Paimon/Lance, and JNI
Catalog lifecycle group passed, covering 1024 active leases at capacity, LRU,
concurrent creation, close versus deployment-pull races, wrong fingerprint
algorithms, and cross-deployment output rejection. These results are from macOS
arm64.

### Scalar Index Pruning and CDC Watermarks

| Former responsibility | Current ownership and call path | Boundary and validation |
|---|---|---|
| Coordinator's former `FilterPredicateExtractor` | Planner traces the column ordinal of Filter inputs and parses Calcite predicates. Direct projections may rename and reorder. Computations, aggregations, windows, and LIMIT that cannot prove pushdown safety terminate that condition's pushdown. | Real Calcite plans validate same-name shadowing, expression columns, and predicates around LIMIT. Dynamic IN members must not be lost. |
| Coordinator's former `IndexSplitFilter` and `filter` package | `core.pruning` in `indexing-lifecycle` owns split pruning, published-generation selection, L1 concurrency, and L2 hints. Coordinator borrows the Catalog query capability bound to the current session. Server selects filters and persistence implementations. | PostgreSQL reads the active manifest and mappings in the same REPEATABLE READ transaction. A test publishing a new generation between two batch reads verifies the original batch stays consistent and later requests see the new publication. The exact query snapshot is compared with the manifest, allowing reuse of immutable files from an earlier source. |
| Former `IndexHint` and `IndexPredicateAnalyzer` | Each hint carries the generation read at analysis time. Deduplication uses the full equality relation of index, generation, type, and predicate. | The query entry reads the exact generation through the publication port. Newly created, unpublished, superseded, and snapshot-conflicting hints are checked separately; a split is retained when no qualifying hint exists. This read grants no cleanup lease. |
| Former `WatermarkTracker`, `CdcPipelineLoop` | The pipeline loads committed watermarks on recovery. Each local observation instance of the Tracker owns watermark, latest event, and state; the persistence port owns the monotonic position. | A late old write can update only the detached observation instance after completion. Recovery does not count as a new event. Watermark, pipeline, scheduling, and PostgreSQL tests passed 30 items. |

## SQL DDL, Catalog Schema, and Split Planning

| Former code and operation | Current class, artifact, and production call | Authority and boundary |
|---|---|---|
| `coordinator.query.statement.DdlExecutor.declaredTableMetadata`, same-package `DdlTypeMapper`, and `GeneratedColumnDefinitionCompiler` | `planner.metadata.TableDeclarationCompiler` in `pangrova-planner` compiles parsed CREATE TABLE into ConnectorTableMetadata. Its same-package components parse nested fields, FILE, VECTOR precision, and generated-column constraints. ALTER type mapping reuses DdlTypeMapper. | Planner uses existing Parser, Connector API, Generation Codec, and Protocol dependencies. The operation has no connection or state. The DDL executor holds the session, calls the real Connector to create, registers table identity under the same deployment, and triggers invalidation after success. |
| Two entries of `coordinator.metadata.MetadataBootstrap.buildCalciteSchema` | Server's `PlannerWiring` passes the same session's `PlanningReadBindings` to Planner's `MetadataSchemaBuilder` and `CatalogStatisticsProvider`. Integration fixtures use the same entry. | Identity, tenant, and the selected snapshot enter Schema and Statistics with the query. CatalogRegistry and the Connector Metadata lease held by the query retain their own close responsibilities. |
| Former `coordinator.indexing.IndexHintGenerator` | Planner's `PlanSplitSourceExtractor` returns `ScanPredicates` for the whole query and for each split. Coordinator's `IndexAwareSplitFetcher` requests READY indexes with the real tenant and columns, then calls Indexing's `IndexPredicateAnalyzer` and `IndexSplitFilter`. | Counting happens before per-table deduplication. A single-scan condition must not be propagated even when the same table appears twice. Authorization and cancellation exceptions continue to propagate. The old entry's overloads with STALE inclusion and file-count-based coverage estimation have no production callers; coverage analysis remains in Indexing. The online scalar path selects READY indexes. |
| Direct Calcite root reads in `DispatchContextEnricher` | `indexing.planner.vector.VectorSearchRewriter`, which receives `PlannedQuery`, identifies sort shape using the existing Indexing → Planner compile dependency. The query-execution side continues to provide tenant-aware index lookup and the bound snapshot. | Coordinator production source needs no Calcite types. Vector index descriptions and READY state belong to Indexing. Planner needs no index implementation reference. |
| Calcite dialect rendering in `coordinator.governance.QualityReadSql` | The former calls came only from PostgreSQL tests of governance predicates and quality reads, and the class moved into the original artifact's test source. | Real governance rules are handled by Governance's assembled execution port. Runtime support scope is described in data-governance.md. |

Remaining DDL is divided item by item by operation. SQL tasks and StatementDispatcher
handle sessions, admission, SQL authorization, and result rows. Catalog create,
alter, drop, and describe enter CatalogRegistry and Metadata services.
Ownership, ADOPT/UNLINK, rename, and maintenance policies for CREATE/DROP/ALTER TABLE
are issued to Table through TableControlDdlOperations. Column tags and masking keep
the original Governance/Security and persistence collaboration, and query-cache
invalidation and optional index evolution still trigger after a successful change.
CTAS plan derivation and the production write/register steps are retained separately.
SQL entries for index DDL, materialization, views, and models call their own domains;
merging them into Planner would introduce reverse dependencies on control flow and
authority writes.

The former DdlExecutor's generic SET recorded raw values and returned empty success.
The explicit SetSessionTask now returns PARSER_UNSUPPORTED_FEATURE. SQL diagnostics
mask such raw assignments in records, audit, and query history. Request-level
configuration and connection-local receipts for USE BRANCH use their existing
protocols.

On macOS arm64 under JDK 17, the production entries above and their test callers were
compiled in an isolated directory. The targeted Planner/DDL/Schema/quality-predicate
group passed 86 items, including real PostgreSQL, Paimon, and Calcite. Real Iceberg,
PostgreSQL, JNI Worker, and Coordinator also ran SnapshotBoundIndexE2ETest and
IndexJoinE2ETest: 8 items passed, with no skips in either group. SET, HTTP error
classification, and diagnostic rendering passed 16 items; the real endpoint returned
HTTP 400 / PARSER-0002, QUERY_SUBMITTED logs for all four assignment forms showed
`SET [redacted]`, and no test values appeared in records.

## File Authorization and Worker Operation Audit

| Former entry | Real responsibility and disposition |
|---|---|
| `file.api.audit.FileOperationAudit.fileOperation` and `coordinator.query.audit.FileSpaceAuditEmitter.fileOperation` | The former call happened after ticket signing. FileGrantAudit emits FILE_ACCESS_GRANTED through the security domain's AuditEventWriter, and FileOperation explicitly distinguishes HEAD, LIST, READ, WRITE, and DELETE. Unrecognized input is rejected at ticket signing or Worker decode. |
| Former `coordinator.enums.FileSpaceMutation` | This enum only selected audit events and has been deleted. The SQL entry explicitly selects the event by Volume or Stage after a genuinely successful namespace operation. File-attribute changes continue to use the existing sealed operations and PostgreSQL atomic updates of file.api.space.FileSpaceMutation. |
| Former `worker.file.FileSpaceFileResource` | POST head and HTTP HEAD produce FILE_METADATA_READ; LIST is acknowledged per page; PUT and multipart complete are acknowledged after the provider returns; GET is acknowledged after the requested byte count matches, writing out, flushing, and closing the source stream. DELETE requires a successful provider return. Authentication or version failure, cancellation, and partial stream writes produce no success event. Object storage does not share a transaction with the audit sink. |
| Standalone and embedded entries of `server.worker` | The existing LoggingAuditEventWriter is passed to the Worker bootstrap. The Worker holds only the Security API writer and borrows it, keeping the Worker decoupled from Coordinator and JDBC. Production structured logs are Worker-side completion evidence; Coordinator uses its existing logs or composite backend. |

Independent JDK 17 compilation and File/Worker/authority/HTTP tests on macOS arm64
passed 91 items, including real PostgreSQL audit queries, MinIO S3 multipart, HTTP
expiry, and disconnect. After finally switching namespace events to the required
writer as well, 43 source files were compiled and 13 related PostgreSQL, URL, and
authority tests passed on rerun; the two groups overlap and neither had skips. PG
cases checked 6 ticket signings and 7 actual operations respectively, covering an
independent HEAD, Range GET, client write failure, and conditional delete failure for
the same READ ticket. Presigned URLs for direct storage recorded the actual expiry in
the receipt.

## Table Creation, Tag Projection, and Runtime Naming

| Entry | Responsibility and validation |
|---|---|
| CREATE TABLE from SQL and Metadata | Both entries pass the deployment of the Catalog handle that actually executes CREATE to TableRegistrations. A registration failure can record an orphan object only in that deployment. When a catalog is recreated, a table name is taken by another identity, or a pending rename exists, the current identity is preserved. PostgreSQL retires the old record, inserts the new UUID, updates the name, and writes the invalidation event in one transaction. The persisted revision is used as the concurrency token, preserving database time precision. Six test groups totaling 127 items passed, including rollback after an enforced INSERT conflict and Catalog replacement during an external CREATE. |
| Metadata column tags and Governance | ColumnTagBindings binds the table policy repository and event source at construction. Coordinator installs listeners using the same event source and replays existing tags. Governance removes its registered listener when governance shuts down. Paimon integration builds control records, tags, masking, and metadata reads against the same PostgreSQL authority. |
| OpenTelemetry, Worker HTTP, task status, and maintenance lineage | Two duplicate trace/span ID reads were merged into CurrentSpanIds; Worker access logs read the current span, and Native execution uses the execution identity captured at submission. TaskReportCompletion routes accepted reports to the logical task. After an explicit component caller registers MaintenanceLineageListener on ObjectDependencyService, dependency writes produce lineage notifications. PhysicalTypeProjectionDeclaration validates projection rules for Substrait physical types. |

### Worker and Protocol Public Enums, Errors, and Constants

| Owner | Types and callers | Semantics and validation |
|---|---|---|
| `protocol.enums` | `ReportedTaskState` is generated from Worker task-state projection, and `DataTicketType` is used by Flight signing and verification. Retrieval merge policy is owned by Execution API, and Protocol binding references that value directly | Worker's eight execution states map to six external states. The old `protocol.execution.TaskStatus`, which only wrapped the enum and had no callers, was removed. Proto numbering and unknown-value handling are owned by generated types. Flight signing continues to use the original ticket format. |
| `worker.enums` | `BackpressureAction`, `ControlEpochFenceReason`, `TaskAdmissionReason`, `TaskCancellationStatus`, `NativeTaskFailureCategory`, `NativeMemoryEventType`, `NativeFunctionType`, and `DmlOperation` | Classification results, admission, cancellation, and quota events are handled by the original state machines. JNI decodes operations into stable `0=INSERT`, `1=OVERWRITE`, `2=DELETE` before constructing write requests; unknown codes raise an error. Worker and Native must match in the distribution. |
| `worker.exception` | `NativeQuotaSendException`, `CopyPreparationHaltedException`, `CopySourceReadRejectedException`, `TenantQuotaExceededException` | Preserve tenant identity, quota type, HTTP rejection status, and execution error category; original callers continue to distinguish timeout, cancellation, policy rejection, and backend failure. |
| `protocol.constants.BuiltInCapabilityIdentifiers` | Capability IDs, versions, format numbers, and reserved column names for Planner, Worker, and Protocol | The capability catalog continues to build references with signatures. Original field values remain consistent with the online format, and inventory and protocol descriptors are validated by real Worker handshakes. |
| `worker.constants.SpillMetricAttributes` | Domain and reason labels shared by memory events and spill observations | The recorder owns instrument and gauge lifecycle. Configuration defaults and encoding boundaries remain with the configuration or protocol types that validate and use them. |


### Connector Process Plugin Directory and Leases

| Former location and capability | Current owner, real calls, and validation |
|---|---|
| Former `org.pangrova.connector.runtime.lifecycle.PluginManager` and the Coordinator empty-URL loader | Retired. Connector Runtime's `ConnectorPluginInventory` assembles role-scoped factories once at startup. Builtin factories use the role classpath, and each external directory installation gets its own loader. Duplicate names within a role, wrong versions, missing factories, and initialization exceptions abort assembly and release resources in reverse. Plugin updates require restart. |
| Former Coordinator `PluginConnectorHandleFactory` | The process reads the directory from `connector.plugin-directory` and creates real catalog handles through `ConnectorFactory`. Coordinator closes the inventory only after CatalogRegistry closes all handles. |
| Former Worker `WorkerCatalogRuntime` and `WorkerCatalogRegistry` | Worker/Standalone assemble them through server configuration. Exact deployment revision and tenant lease belong to Worker. Close rejects new work, active leases prevent loader reclamation, and close is retried after the last lease is released. The plugin inventory stores no Catalog facts and holds no query or persistence access. |
| Validation | `ConnectorPluginInventoryTest` packages real JARs and checks isolation, role filtering, duplicate-registration rollback, resource close, and explicit rejection without a factory. `ExternalCatalogRolesIntegrationTest` uses a two-role JAR with same-named plugins and verifies that both roles create and close connectors independently through CatalogHandle and authenticated HTTP revision reads. |

This batch compiled Coordinator, Worker, and their 140 reactor projects under JDK 17
in an isolated worktree. The 34 items for the external JAR, builtin Catalog,
classloading, and embedded role configuration above all passed with no skips. The
two-role case also checked independent dependency JARs and local HTTP requests with
bearer authentication. It created two independent role inventories in one JVM and
did not execute a separate-process SQL query.


### DAG Terminal Association, Generations, and Shuffle Redispatch

| Capability | Production owner and call |
|---|---|
| Early, duplicate, cross-query, and stale reports | `TaskStatusReceiver` validates the issued attempt, and `TaskReportCompletion` forwards to the DAG. Each active DAG has its own `TaskTerminalRegistry`. Registration binds query, Stage instance, and generation; the first terminal state is delivered once, and query retirement clears early events and delivery flags. |
| Stage terminal state and retry | Register task terminal state inside the Stage lock and select the unique terminal transition, running state listeners outside the lock. The Stage owns the generation and budget, and `RetryStageHandler` validates error category and configured budget. Repeated failures, cancellation, and stale generations cannot advance the next generation. The test-only StageRetryManager, 30% failure threshold, external counter entry, and unwired TaskRetryCallback were removed. |
| Worker-death redispatch | The entry goes through `QueryBootstrap.retryTaskOnDeadWorker → SpeculativeTaskLauncher → DAGLifecycleController`. An independent task without Exchange can reserve, commit, or roll back on another Worker. Currently input read grants/tickets cannot be re-signed for a new attempt, and updating the output location of an orphaned producer is not wired. The combination is rejected when the original request or DAG contains a related Exchange. A retryable source Stage can still rebuild the whole generation while downstream is not yet scheduled. |
| Physical failure and query terminal state | Removed `CoordinatorTaskTracker → TaskFailureCallback → QueryManager`. A failed old physical attempt still performs drain, and the DAG listener triggers results or write commit based on the real terminal state. Reports without a DAG are still judged by the task tracker. Actively canceling a logical DAG wakes the finalizer and then waits for the actual Worker drain. |
| Exposed results and dispatch failure | OUTPUT locks the attempt when handing the Flight address to the result entry; an existing DML fragment and a producer generation already bound by a consumer also prohibit transparent replay. Redispatching an independent read without Exchange binds the replacement Worker, control plane, and frozen inputs first. A dispatch that has entered the network with a lost ACK keeps the replacement request and cancels the original target Worker; rollback restores the old slot and replays the first old terminal state that arrived during the exchange. |

The independent snapshot is based on `7fd172e47080af083f34be70d7f09653ab9201f1`
plus the worktree changes listed in this section. Under JDK 17 and macOS AArch64,
the native-required coordination-control targeted regression passed 152 items,
including 8 PostgreSQL container durable-report items, 3 Native/JNI/Flight output
and close items, and 7 Native/HA ownership items. The Server dead-Worker entry reran
2 items separately and passed, covering replacement slots, unrecoverable failure, and
lost-detection racing a successful terminal state. The first Native validation found
the snapshot lacked the local dynamic library; after adding the same actual
distribution bytes, the rerun passed. The Server fixture first left an unissued
logical slot waiting for physical cleanup; after fixing fixture unregistration, the
rerun passed. This batch did not validate migrating an in-flight Flight stream that a
client is reading between two independent processes; that combination rejects retry
after publishing an endpoint.

### Process Service Registration and Actual Calls

Reading file names and comment-stripped implementation lines from each artifact's
`src/main/resources/META-INF/services` yields 37 descriptors, 24 service interfaces,
and 49 registrations. The table counts by interface. Only registrations delivered by
the repository are counted. Federation plugins installed separately by users and
Native algorithms registered by C++ have their own assembly entries. Artifacts that
runtime roles share from the application classpath are discovered by role boundary.
Connector additionally supports an explicit plugin directory and child loaders.

| Service interface and entry count | Actual selection and call | Resources, failures, and validation scope |
|---|---|---|
| `java.sql.Driver` · 1 | JDBC DriverManager selects PangrovaDriver by URL, and Connection executes queries through the shared HTTP/Flight client | The caller closes the Connection, and release of the last shared lease closes the client. Independent JDBC/CLI cancellation and subsequent queries are in the client section of this ledger. |
| `AiModelCatalog` · 1 | Worker's NativeCapabilityAdvertisement reads WorkerAiModelCatalog with the role loader and generates scheduling declarations for models deployed by function plugins | Reject when configuration exists but no factory is installed or when multiple directory owners exist. The catalog describes models; actual metering belongs to the task. See WorkerBootstrapCapabilityAdvertisementTest and FunctionProviderDiscoveryTest. |
| `CoordinationProviderFactory` · 2 | CoordinatorLeaseBootstrap discovers JDBC and the installed ZooKeeper on the current role classpath. The selected factory provides both distributed leases and member registration; duplicate IDs or aliases and version mismatches are rejected before connecting to the backend. | JDBC borrows the process connection pool. ZooKeeper exclusively owns the Curator client and reclaims it on connection or initialization failure. Process exit drains leases and member consumers before closing the provider and connection pool. CoordinatorLeaseBootstrapTest, ZooKeeperDistributedLeaseTest. |
| `AiProviderFactory` · 2 | Configured model dialect; AiProviderFactories' role loader discovers OpenAI/Qwen. The Worker registry uses the same factory batch to create model clients | Each registry closes the clients it created. AiProviderFactoriesTest, QwenProviderRegistryTest, and OpenAI HTTP protocol tests cover implemented operations. Vendor public accounts were not called in this batch. |
| `CacheStoreFactory` · 2 | Coordinator's `metadata.cache.l2.provider` selects memory or Redis, and TieredMetadataCache actually reads and writes L2 | `disabled` enables only L1. When L2 is selected, all factories are validated by ID, duplicates, and version before opening. Empty, leading/trailing whitespace, or an ID occupying disabled is rejected. TieredMetadataCache closes L2. MetadataCacheDeploymentTest, RedisCacheStoreTest. |
| `ConnectorFactory` · 3 | Coordinator and Worker each own ConnectorPluginInventory and resolve Iceberg/Paimon/Lance by catalog type. External directories load as plugins | Startup rejects duplicate names, unload follows process shutdown, and Worker closes after active leases are returned. See the two-role validation of real plugin JARs in the previous section. |
| `BlobStorageFactory` · 3 | AI FILE storage configuration may select Local/S3/OSS. Worker COPY separately requires managed, exact identity, and per-request budget; only S3 is currently provided | Storage closes with the function factory or COPY runtime. BlobStorageFactoryDiscoveryTest, OssBlobStorageHttpTest, and S3/MinIO tests. OSS HTTP protocol evidence does not cover cloud accounts or regional networks. |
| `ObjectStoreAccessorProvider` · 2 | Worker file spaces adopt Local/S3, selecting the provider by StorageProfile storage kind and returning a borrowed accessor | Worker owns the provider and its cache. Duplicate, uninstalled, and post-close requests are rejected. ObjectStoreAccessorFactoryLifecycleTest, S3ObjectStoreAccessorMinioTest. OSS does not register this interface. |
| `FunctionFactory` · 1 | NativeFunctionBootstrap aggregates discovered scalar/aggregate descriptions and registers JNI; the current builtin registration is AiFunctionFactory | Closeable factories are retained before reading their configuration and functions. Duplicate names or failure enter reclamation, and callbacks are published only after successful registration. NativeFunctionBootstrapTest, FunctionProviderDiscoveryTest. |
| `FunctionCallbackScope` · 1 | NativeFunctionBootstrap binds WorkerAiCallbackScope through WorkerCallbackProviders, and NativeFunctionCallbacks installs the current task authorization before executing the real callback | Each call restores the scope on exit, and ordinary functions run when it is not installed. Duplicate installation rejects startup. WorkerAiCallbackScopeTest. |
| `FunctionFailureProjector` · 1 | NativeFunctionCallbacks requests AiFunctionFailureProjector to encode model errors; Native handles ordinary function errors normally | Optional, resource-independent singleton service. Two candidates are rejected before instantiation. NativeFunctionBootstrapTest loads two candidates with real service resources, and AiFunctionFailureProjectorTest covers error payloads. |
| `LineageBackendFactory` · 2 | Coordinator's `pangrova.lineage.backend` selects LOG or HTTP. HTTP requires a durable outbox before creation | LineageEventEmitter manages the consumer thread and backend and drains on close while confirming close results. LineageBackendConfigurationTest and OpenLineageHttpTransportTest verify HTTP writes with Marquez and PostgreSQL. |
| `IndexStrategyProvider` · 9 | IndexingBootstrap installs BTREE, BLOOM_FILTER, PREFIX, NGRAM_BLOOM_FILTER, BITMAP, HASH, SPARSE, R_TREE, and GIN through the role loader. Build, encode/decode, and computation share the same instances | Algorithms have no external resources. Duplicate types are rejected, and missing types are rejected by concrete type. IndexStrategyDiscoveryTest actually builds, encodes, decodes, and prepares each index and also checks thread-context loader changes. Vector and inverted execution belong to Native. |
| `RefreshStrategyFactory` · 3 | RefreshResolutionPolicy selects FULL, APPEND_ONLY, or PARTITION_REPLACE; MaterializationAttemptExecutor validates input, generates plans, and verifies completion credentials | No external resources; missing FULL is rejected, and strategies without an implementation such as CDC are explicitly rejected. RefreshStrategyDiscoveryTest and real plan tests for each strategy. |
| `TableStoreFactory` · 2 | Coordinator uses postgresql to create control, statistics, policy, and maintenance repositories. The memory factory is used only for an explicit in-process authority | Server owns the DataSource and repositories borrow it; the whole inventory is version-checked first. Missing PostgreSQL fails. TableStoreDiscoveryTest verifies formal assembly; InMemoryTableControlStoreTest/JdbcTableControlStoreTest test real behavior separately. |
| `AiTaskLifecycle` · 1 | NativeTaskExecutor creates one optional WorkerAiTaskLifecycle, and registration, metering, cancellation, and reclamation of the same attempt find that instance. NativeCreationOwners retains its reclaimer even after failure recovery | The process uses the existing no-quota contract when no AI implementation exists, and duplicate implementations are rejected. WorkerAiTaskLifecycle forwards metering to the AI runtime, and the executor reclaims JNI handles. NativeCreationRecoveryContractTest and the real Native Worker fixture have different evidence scopes. |
| `WorkerSelectionStrategyFactory` · 1 | `coordinator.query.placement-provider` selects load-balanced. QuerySchedulingWiring gives the strategy to TaskScheduler, which ranks the set already admitted by Worker admission | Algorithm has no external resources. Missing, duplicate, and version-mismatched factories are rejected. QuerySchedulingStrategyFactoryTest, TaskSchedulerStrategyBoundaryTest. |
| `AuthenticatorFactory` · 3 | Coordinator security configuration selects NOOP, LDAP, or OAUTH2, and the artifact enters the request authentication filter | SecurityBootstrap owns the authenticator. AuthenticatorFactoryDiscoveryTest, LdapAuthenticatorTest, OAuth2AuthenticatorTest, and AuthenticationFilterIntegrationTest verify factories, authentication protocols, and entries respectively. Unit LDAP bind stand-ins and the real LDAP used by independent distribution regression are listed separately. |
| `SecretProviderFactory` · 2 | SecretResolverRegistry in Coordinator/Worker dispatches ENV or FILE by configured reference prefix, and Catalog/AI resolve at consumption time | The product reads environment or files and holds no long-lived connections. Duplicate prefixes, unknown prefixes, and file access errors fail explicitly. EnvSecretProviderTest, FileSecretProviderTest, CoordinatorSecretDiscoveryTest. |
| `ShuffleCoordinatorFactory` · 1 | CoordinatorShuffleProviders selects pangrova for REMOTE, requires configured capabilities, and real commit selection and receipt are saved with the execution authority | The Coordinator owns the session lifecycle. PangrovaShuffleProviderIsolationTest and the publication, failure, and read-lease evidence in the previous section. Direct/Local Native selection relies on Native deployment capabilities. |
| `SpoolStoreFactory` · 1 | SpoolBackedExchangeManager.open selects local-fs by configuration, recognizes the API version, and reads completed partitions by exact producer attempt | The Java page manager closes the local spool. SpoolBackedExchangeManagerTest and LocalFsSpoolStoreTest cover real directories, empty partitions, and unknown selections. This library entry is not currently called by the process Native execution chain. |
| `NativeShuffleWorkerFactory` · 1 | WorkerTaskBootstrap validates the pangrova factory and session capabilities for REMOTE, and producers are called by JNI egress | WorkerTaskOwnership owns the session, seals new tasks, and waits for producers and readers to drain. External providers include Pangrova; Celeborn is not implemented. |
| `CdcFormatFactory` · 3 | Coordinator startup loads Canal, Debezium, and Maxwell JSON into CdcRecordConverterRegistry; StreamDdlExecutor validates SQL FORMAT, and CdcPipelineLoop calls the selected decoder | Resource-free, shareable decoders. All factory versions and identifiers are validated before creating and comparing formatId. CdcFormatDiscoveryTest actually parses three different formats, and CdcRecordConverterRegistryTest verifies that an incompatible factory cannot trigger any creation. |
| `CdcSourceFactory` · 1 | CdcSourceRegistry validates SQL SOURCE and configuration, and StreamScheduler creates an independent reader for each running Kafka stream | The pipeline owns poll/seek/commit/close. Recovery must use the exact checkpoint and topic partitions. KafkaCdcSourceReaderIntegrationTest uses a Kafka container for actual consumption and recovery. |

The baseline snapshot `e15ea186992cff75622a14fed7c61d3b4aab2d07` built 147 reactor
projects under JDK 17 in an isolated worktree; 32 targeted test classes passed 232
items with zero failures and zero skips. These include Kafka partition consumption
and recovery 4, Redis 1, MinIO 14, Marquez and PostgreSQL 4, and the OSS official SDK
against an HTTPS service fixture 16. The final Java source snapshot
`6aea9e39e72cb3b9955ff0411f9db98841b9294f` checked another 16 classes and 75 items
with zero failures and zero skips. These include PostgreSQL table repositories 12,
in-memory table repositories 4, and Native Worker creation, read, drain, and rebuild
3. The Native cases this round used the macOS arm64 dynamic library. The two batches
contain overlapping tests and are counted separately.

#### The 22 Types in the First SPI Reachability Failure

| Count | Ownership and disposition |
|---|---|
| 6 | The quality executor, dependency-graph in-memory fixture, federation manifest, synchronization record, projection record, and Catalog registry in the Metadata test JAR all come from test source. The gate filters Maven tests, native-fixtures, and query-fixtures classifiers but still loads production JARs and their service registrations. |
| 6 | TableAsset, ViewAsset, MaterializedViewAsset, FunctionAsset, ModelAsset, and VolumeAsset are all value types of the sealed Asset. The gate checks that every permitted subtype is indeed a record and continues judging the SPI by the other contracts it implements. |
| 5 | Governance in-memory audit, audit configuration, Catalog tag, column tag, and quality rule, which were only referenced by tests, moved to test source. Coordination and Server reuse of quality rules and audit configuration is provided by a test JAR containing only the referenced classes. |
| 2 | The manually populated Shuffle inventory lacking a fenced worker identity moved to test source. LineageTagPropagator, which propagated column tags by name, moved to test source together with LineageCatalog and query helper classes that supplied facts only for that example. Production authority still reads bindings persisted by stable table ID. |
| 3 | Configuration, factories, role assembly, calls, and close for replaceable capabilities are unified through strongly typed factories/SPIs; `SpiWiringVerificationTest` (6 items) and `ServiceLoaderReachabilityTest` (6 items) pass on the final source; the two TableWriter implementations were retired as a whole because they had no production factory or consumer entry. |

Of the historical exemptions, 123 classes can now be reached from formal entry points,
and another 13 FQNs no longer have corresponding production source; all were removed.
The remaining 25 historical entries stay listed separately. Existing `@DeadCode`
annotations must provide a tracking item and be unreachable; the marked #19 COPY URI
admission implementation is not counted as support. This round added no historical
exemptions and no DeadCode annotations because of its failures.

For isolated source `b9565713a25429d54258c726244f6bc0ff0e297e` under JDK 17,
SpiWiringVerificationTest (5 items) and ServiceLoaderReachabilityTest (6 items) all
passed with zero skips. The first batch of domain tests passed 177 items; the
extended gate first reported the existing COPY #19 marker, and after restoring the
repository's recognition of tracking annotations and validating their contents, the
rerun gate passed. The Metadata integration group was also explicitly enabled, and
JdbcObjectDependencyStoreIntegrationTest ran 45 items on PostgreSQL, including
concurrency, failure rollback, and graph contracts. Clean production output did not
contain the 9 moved classes, and the actual Governance tests JAR contained only the 2
repository fixtures borrowed across modules.

### Per-Item Review of the 25 Historical SPI Reachability Items and URI COPY #19

The 25 items below come from the previous reachability baseline. Role assembly,
independent-library validation, and test-only fixtures are noted separately.
PostgreSQL regression runs the complete formal migration set from an empty schema.

| Former entry | Production entry, capability, and disposition |
|---|---|
| IcebergColumnValueReader | The fixed-schema reader was referenced only by itself. The old class and SpotBugs exception were deleted, and IcebergIndexAccessors provides IcebergCatalogColumnValueReader by Catalog and FileIO. |
| InMemoryCoordinatorRegistry | Explicitly constructed in a single process, it can store members, roles, and invalidation watermarks. Persisted role assembly selects PostgreSQL or the quorum registry. The library implementation and original exemption are retained; it cannot recover another process's members. |
| CoordinatorQueryLifecycle | Embedded QueryLifecycle starts queries through QueryManager and handles unknown IDs. The Coordinator HTTP entry has its own separate authentication and authorization flow. The trusted-caller component and its exemption are retained, and bare-ID cancellation is not treated as an external entry. |
| LocalFileSystemAccessor | Constructed only by explicit local-path callers and index tests; the false automatic-discovery description was updated. Production Catalog selects a concrete Connector accessor and does not fall back to local when FileIO is missing. The exemption remains temporarily. |
| ArrayBlock, DictionaryBlock, DoubleBlock, IntBlock, LazyBlock, LongBlock, RunLengthEncodedBlock, SliceBlock (8 items) | Column-value representations provided by Batch API itself. The gate recognizes them by the Block contract and owning package; external Block implementations and the other factory interface still participate in assembly checks. BlockRegionBoundsTest checks bounds and overflow for each type. |
| StandardTypeRegistry | Independent type resolution and compatibility computation. TypeSystemPropertyTest proves round trips of supported base types and DECIMAL. The original exemption remains; extensions such as TIMESTAMP precision are judged by their respective release status. |
| Former LakeTableWriter and MergeTreeTableWriter (2 items, retired) | The two writers plus TableWriter, WriteRecord, WriteContext, DataFileSink, WriteMode, and TableFormat had only test construction sites and no production factory or consumer entry. Real CDC uses CdcWriteSink and ConnectorPageSinkCdcWriter, and SQL writes use Worker NativeWriteExecutor and ConnectorPageSink. Code, modules, service lists, and documentation were retired together, reducing SPI historical exemptions to 5. |
| CacheEvictionBroadcaster | The old queue had no heartbeat consumer, and a single drain could not deliver to all Workers. Moved to test source. Iceberg may explicitly provide a best-effort file-hint listener; the formal service still provides no cluster-wide file eviction guarantee. |
| SettableDynamicFilter | Moved to tests. Production split enumeration passes DynamicFilter.ALL. Test collectors and completion handlers without a build producer moved to tests as well; the logic that guessed build/probe by the order of two leaves was deleted. Distributed automatic pruning status is described in dynamic-filtering.md. |
| LineageQueryEventListener | Explicit QueryEventDispatcher calls can send START and terminal states to a borrowed emitter. The gate publishes for real and checks messages. Coordinator remains the sole writer of existing durable lineage. |
| JdbcQueryEventListener | After an explicit observer goes through the dispatcher, the JDBC listener is reopened to read the same terminal state. The gate verifies callable library behavior. The role does not enable split-event collection; real resource auditing does not depend on this best-effort listener. Its private test-created schema was removed. |
| MdcTaskWrapping | Wired into Coordinator's actual bounded asynchronous dispatch executor. Request log context is captured at submit and restored after each run. QueryAsyncSubmission continues to bind the execution span, query ID, and timeout cancellation. |
| Former InMemoryCheckpointStore and InMemoryQueryCheckpoint (2 items) | Both prototypes and the test-only CheckpointManager moved to test source, eliminating implicit Memory selection by a production parent object. QueryStateJournal, execution fence, and recovery state continue to be owned by the existing persistence path. |
| UnifiedMemoryLedger | Experimental memory-decision code has no physical reclamation receiver yet; the original exemption remains. WorkerResourceBootstrap creates MemoryArbitrator and NodeMemoryPool, and the Native budget comes from task authorization. Introducing a second accountant cannot prove release. |
| NoOpSchedulerControlLoop | The empty-action implementation moved into the test package of SchedulerControlLoopTest. No cluster-management action is actually enabled. |
| DefaultIndexAvailabilityPublisher | The unimplemented Worker /v1/index/load, callers, and service registration had no basis. The broadcaster, IndexAvailabilityPublisher API, and corresponding SpotBugs entry were retired. Real generation commit still requires a publication claim and the selected generation. |

**URI COPY #19**. CopyIntoParserTest covers explicit rejection of URI sources at
parse time, and SELECT sources follow their independent entry. JdbcCopyFileClaimStore
can recognize immutable sources, replay within the same execution, blocking by other
executions, and profile/digest conflicts. Atomic source-read admission and
credential-bearing Worker staging unit tests verify signatures, tenant, bytes, and
rollback. Formal SQL still cannot freeze a URI manifest and associate the claim,
target-authority recheck, and recovery with the same write publication. The issue
marker is retained, and reload generation continues to be rejected. Existing claims
and internal endpoints do not constitute user-available URI loading.

**Validation for this batch**. Isolated source `5821360ba` passed 201 items across 22
test classes, including PostgreSQL, role assembly, and two SPI gates. The later
isolated source `ad3023b53` passed 151 items in the non-Native E2E selection, and the
final literal-join and HEP-rule selection passed 33 items, covering nullable BIGINT,
column renaming, reversed equality, and inputs whose cardinality estimate points
opposite to the actual build direction. The batch's remaining 7 SPI exemptions were
listed individually in the table above; LakeTableWriter and MergeTreeTableWriter were
retired this round, leaving 5.

**Actual support scope of join filtering**. Velox production Connector's
`canAddDynamicFilter` is enabled, pushing from the actual hash build to the probe
scan with column aliases. On two Parquet sources, INNER reduced scan output from 4
rows to 2 while the join output kept duplicate keys; LEFT scan returned all 4 rows.
The published remote build was delivered by an independent test through Direct
commit, producer unregistration, seal, loopback Flight, Arrow exchange, and Native
hash join, and both scan and join counts were verified. Concrete queries on two
independent Workers, cross-stage control-plane publication of key domains to the
upstream probe, and reduced file I/O bytes each have independent validation
requirements. The official Native test runner's 44 targeted regressions passed, and
the separately executed 12 NativeExchangeDataPlaneE2ETest items all passed. The
macOS arm64 ASan/UBSan run of same-task and remote build, injection, and exchange
tests passed 18 items with no findings in the diagnostic scan. Six real Iceberg
SQL / Coordinator / Worker / JNI tests passed, covering nullable BIGINT and VALUES
with INNER, LEFT, duplicate builds, and duplicate probes. The first run stacked a
Filter on Calcite's transitive predicate derivation and timed out; after limiting
derivation for literal inputs and resolving the real key along projections, the
rerun passed. Assertions verify the probe multiset read independently, and the
generator cycles 50 keys to produce 100 rows. Validation used the previous Maven
reactor's test classpath plus the rebuilt planner, test classes, and Native dynamic
library from this round. Detailed boundaries are in
[Dynamic filters](dynamic-filtering.md).


