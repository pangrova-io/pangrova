# Table control and multimodal file planes — implementation and production qualification ledger — 2026-10-25

Status: **production qualification is incomplete**. This consolidated ledger
covers the full P0–P3 Table Control Plane plan and the M0–M5 multimodal file-plane
work against the Databricks/Snowflake alignment design as of 2026-10-25. It records
implementation, repairs, validation evidence and remaining production gates; it
is not production acceptance or a claim that either plan is complete.

Both workstreams were previously paused at the user's requested account-quota boundary
(10% remaining), after committing repairs and completing in-flight focused
validation. All file-plane changes in this checkpoint were committed and pushed
on `codex/ai-native-semantic-closure`.

Evidence below belongs to the source/runtime snapshot identified in each section.
Component tests and results from different snapshots must not be combined into a
passing final-revision gate. The final combined revision needs fresh qualification;
changes in other engine areas need their own current-source evidence.

## Resumed implementation and qualification

The catalog deployment identity gap is now implemented. CREATE, ADOPT, orphan
registration and legacy registration capture the authoritative incarnation and
revision. Admission, SQL and metadata read bindings (including cache hits),
governance bindings, statistics, observations and rename/drop completion reject
a changed deployment. The registry also compares the local handle with the
durable descriptor, so a stale peer cannot register authority under a replacement
catalog. Control-layer identity values do not depend on catalog assembly types.

Legacy observation backfill requires both catalog fields to be absent, an
explicit legacy-write allowlist entry and a matching previously registered format
UUID/location. It cannot replace an existing or partial catalog binding. CREATE
under a different confirmed deployment gets a new stable table ID; ADOPT rejects
an old binding until its LINKED registration is explicitly unlinked.

| Check | Observed result | Qualification limit |
|---|---|---|
| Catalog/control/metadata focused tests | 78 passed, including 4 real PostgreSQL deployment-binding tests; no failures, errors or skips | Includes same-UUID catalog replacement, revision changes, stale peer handles, cache rejection and guarded legacy backfill. |
| Coordinator control-plane focused tests | 70 passed across ownership DDL, maintenance, ANALYZE, SHOW and scheduled table selection | Component scope; process-level active-active qualification remains open. |
| HTTP SQL Table Control lifecycle | 7 passed with packaged Release JNI, actual SDK files, PostgreSQL and the prepared real Ollama deployment; no skips | Includes the new catalog replacement case rejecting SELECT, INSERT, DROP, ANALYZE and CALL while retaining the unchanged physical UUID and data. |
| Metadata/coordinator static analysis | Both `verify -Pstatic-analysis -DskipTests` selections passed; zero reported bug instances | Tests ran separately. The analyzer still prints nonblocking missing-lambda-symbol diagnostics. |
| Current Java reactor installation | `-Pnative-runtime -DskipTests install` passed | Compilation/package evidence; refreshes stale local CLI/shuffle artifacts that had contained older SPI classes. |
| Full metadata owner selection | 822 tests selected: 818 passed and 4 observer-fixture errors, with no skips | The fixture discarded catalog descriptors. Its persistence was repaired and all 5 observer tests then passed; this is not a single green 822-test run. |
| Architecture and lightweight gates | Package dependency acyclicity, scoped/full-owner Checkstyle, SPI annotations, E2E truthfulness, documentation references and dead-stub checks passed | The initial control/catalog package cycle was removed before acceptance; these checks do not establish runtime reachability by themselves. |
| Current Release native suite | 4,333 passed; 1 of 4,334 failed with an mTLS `wrong version number` handshake error | The same isolated case subsequently passed once and in 100 diagnostic repetitions. Its intermittent full-suite failure is unresolved and the Release gate remains open. |

The broader dependency reactor was also interrupted while a Docker exec output
stream remained open after its command exited. This is retained as an incomplete
reactor; no test was disabled or timeout raised. The final same-source regression,
sanitizers, isolated Native owners and deployment gates below remain required.

## Table Control Plane

### Root causes repaired

| Area | Root cause and resulting behavior | Commit |
|---|---|---|
| Physical deletion vectors | The reader expected a synthetic Puffin header and a 32-bit bitmap instead of Iceberg SDK bytes. It could choose the wrong blob and lose positions above 2^32. The reader now validates PFA1 framing, blob bounds/type/file identity, length, magic, CRC, bitmap ordering and cardinality; selection uses the scanned data file and preserves 64-bit positions. | `199acb4d` |
| Delete execution | Encoded vector children were treated as flat; absent equality columns could produce incomplete filtering. Decode/flatten the required children and reject unqualified equality deletes before file I/O. The public equality-read capability remains false. | `199acb4d`, `662e15e2` |
| Table identity and observations | Stale observations could update a newer control revision; cache projections omitted control identity/version; some mutation paths did not fence physical identity. Publish registry/version history atomically, enforce CAS and identity, and invalidate the corresponding projections. | `b36a83d7` |
| Reconciliation | Discovery failures could be confused with absence, and stale/orphan recovery lacked sufficient identity checks. Preserve unavailable observations, restore only matching physical identity, and quarantine replacements. | `b36a83d7` |
| Rename | Name-based identity changes could invalidate Paimon ownership or expose an incomplete transition. Reserve RENAMING, carry the stable table ID, refresh physical identity after the rename, and reject reads during an unresolved transition. | `b36a83d7` |
| Statistics | Collection could race a replacement or a control revision update. Capture and recheck identity, version and control revision before publishing ANALYZE results. | `b36a83d7` |
| Commit ambiguity | Iceberg verification could treat an unproven outcome as failure; an outer Paimon retry could republish after the SDK had committed. Preserve UNKNOWN and let the SDK own its commit retry protocol. | `662e15e2` |
| Recovery SQL | Attempt reconciliation selected commit-evidence fields without joining the evidence table. Restore the LEFT JOIN and verify the actual persisted evidence. | `960cd5fc` |
| Maintenance | CALL bypassed the control-plane admission path. Reentrant holder IDs also allowed concurrent attempts on one coordinator, and a long operation could outlive its lease. Route CALL through ownership/RBAC/policy checks, allocate a holder per attempt, renew the lease and verify ownership before execution and before reporting success. | `dd34c0a9` |
| Maintenance outcome | A rejection after connector execution had begun could be recorded as a harmless skip. Such failures now remain FAILED; loss of the lease does not imply that physical work was rolled back. | `dd34c0a9` |
| Native E2E ownership fixtures | Shuffle/COPY fixtures did not bind the real Worker process identity before admission. Bind the actual owner throughout the fixture chain. | `3850e292` |
| Overwrite regression fixture | A static-partition INSERT supplied the partition column in VALUES, violating the documented SQL contract. Supply only non-partition columns while retaining assertions that the selected partition is replaced and other partitions survive. | `abd0ddf3` |
| Lease regression fixture | A remaining-TTL assertion included client scheduling and connection latency. Assert that the database grant timestamp follows the blocking transaction boundary and retains its full duration. | `922fdf1b` |
| Rust protocol artifact drift | The local Lance archive still used generated messages without Worker execution identity. Rust dropped that field, so a real Lance INSERT produced a transaction reference whose attempt-path hash failed Java validation. Require a source/header/archive/target provenance receipt at CMake configure and build time; rebuild the Rust archive before relinking JNI. Docker now stages the SDK preparation script and patch before its Rust build. | `4f26149e` |
| Isolated E2E Native classpath | Selecting only the E2E module did not activate the Worker project's profile transitively, leaving JNI absent despite `-Pnative-runtime`. Declare the classifier dependency directly in the E2E profile. | `4f6a45d4` |

### Evidence obtained

These results refer to isolated source/runtime snapshots used during this review.
Concurrent changes to CLI packaging, AI execution and FILE/Volume/Stage were
integrated into the branch afterward. The final combined revision must be
qualified again; earlier green results must not be represented as that result.

| Check | Observed result | Qualification limit |
|---|---|---|
| Native Release suite after DV repair | 4,333 tests passed | One subsequently added factory test was absent from this compiled test binary. |
| Native deletion-vector focused suite after forcing the test target to rebuild | 30 tests passed, including multiple-blob selection with a 64-bit row offset | Complements the preceding full run; this is not a single 4,334-test full run. |
| `TableControlLifecycleE2ETest` | 6 passed through HTTP SQL, real native execution, PostgreSQL control records, Iceberg SDK files and Paimon rename | SDK external writer, not Spark/Flink. Store reconstruction, not coordinator process restart. |
| Recovery focused integration | 9 passed: HTTP lineage recovery and materialization repository | Real PostgreSQL and the pinned Marquez 0.50.0 application on a native ARM runtime. |
| `JdbcDistributedLeaseLockClockIntegrationTest` after assertion repair | 9 passed | Actual PostgreSQL row/conflict waits and delayed writes. |
| `WriteDmlTransactionE2ETest$InsertOverwrite` after SQL repair | 1 passed | Actual static partition overwrite. |
| Java static analysis | protocol, metadata, Iceberg, Paimon and coordinator passed | Before the final combined-revision rebuild. |
| Combined Java reactor | Clean-snapshot install passed, followed by install with `native-runtime` | Compilation/package evidence only. Isolated E2E subsequently exposed the transitive-profile problem above; installing Worker with the profile alone did not fix it. |
| Combined-snapshot metadata focused integration | 80 passed across control registry, policies, statistics, database lease clocks, actual HTTP lineage recovery and materialization evidence | Includes a successful Marquez rerun after reducing concurrent load; full metadata/reactor qualification remains open. |
| Combined-snapshot coordinator focused tests | 71 passed across maintenance SPI/source/execution, dispatch properties, table extraction, permissions and CALL routing | Focused coverage; does not replace process-level active-active and external-writer qualification. |
| Lance archive provenance | 4 tooling tests passed; actual stale archive rejected by both the verifier and CMake configuration; changing a protocol after configuration also blocks the next build | Rust rebuilt with a valid receipt and Release JNI/test/fixture targets relinked successfully. |
| Rebuilt Rust/JNI affected Native suite | 38 tests passed: Lance commit execution and deletion-vector reader/factory/filter | New combination; full Release and sanitizer regression still required. |
| Rebuilt Rust/JNI HTTP SQL acceptance | 9 passed: NativeDatasetControlRoute 2, TableControlLifecycle 6, static-partition overwrite 1; no skips | Confirms the formerly failing real Lance INSERT/read, actual SDK DV reads and control-plane lifecycle on the new JNI. Full deployment matrix remains open. |
| Isolated Native runtime owner suites | WorkerBootstrap 5; NativeExchangeTransportExternalIntegration 3; WorkerShuffleChannelAdmission 1; CopySourceReadHttp 1 passed | These 10 results precede the latest deletion-vector JNI build. |
| macOS ASan full suite | 4,334 tests passed in 2,308.42 seconds | Exact pre-rebuild JNI/Rust artifact; `detect_leaks=0` per macOS policy. New Rust/JNI and Linux ASan/LSan remain separate gates. |
| Broad Java and SQL E2E suites | Stopped with the failures below after their artifacts were superseded | Incomplete and not green; partial successes do not qualify the suite. |
| Canonical Linux production image | Build incomplete | A local macOS runtime cannot qualify the Linux image. |

The initial native and HTTP SQL tests using genuine SDK Puffin files failed on
the old header parser before the repair. The focused native and six-case SQL
suite subsequently passed. No capability was enabled based on synthetic data.

The broader metadata run exposed the lease test timing issue fixed above and a
Marquez startup HTTP read timeout. The latter had passed in the focused run;
resource contention is suspected, but a successful serialized rerun is still
required. The broad E2E run exposed the static-partition fixture fixed above.
The broad E2E run also found a real Lance INSERT failure caused by the stale Rust
protocol archive described above. Earlier Release and ASan results must therefore
be retained as evidence for their exact old artifact, not promoted to validation
of the rebuilt Rust/JNI combination.

The successful rebuilt Rust archive SHA-256 is
`786a171a8eb8ba416a82652c3502fe3c4fd83ea360c672bce96e1a47d367ee98`;
the relinked macOS Release JNI SHA-256 is
`4e6a3e1cacc646d3666c305092ef629d0ab850181f0b1c7313c520fe9196af38`.
The new 38-test native and 9-test SQL results belong to these artifacts. The Java
snapshot contains the combined `abd0ddf3` source, the lease-test repair
`922fdf1b`, and the isolated E2E profile repair `4f6a45d4`; later concurrent FILE
changes on the shared branch are not covered by that snapshot's results.

No timeout was raised, test disabled, or new sanitizer suppression added to make
these failures disappear.

### Remaining production blockers and unimplemented scope

| Plan area | Remaining work | Required acceptance evidence |
|---|---|---|
| P0 final regression | Complete a clean build and the affected/full Java reactor against the final combined revision; rerun failed suites and all four isolated Native owner suites with the final JNI artifact. | Matching source/artifact hashes, complete reports, no unresolved failure or skipped mandatory gate. |
| P0/P1 catalog deployment identity qualification | Registration, adoption, guarded legacy backfill and admission comparisons are implemented and covered by the resumed checkpoint above. Complete final combined-revision and process-level qualification. | Retain same-UUID catalog replacement/revision and stale-peer PostgreSQL/HTTP SQL evidence in the final regression. |
| P1 stable governance identity | `TableControlPolicyBinder` stores stable-ID bindings, while `MetadataTagOperations` and derived mask projections remain name-based. Finish the read/write projection and recovery audit; do not equate the binder's existence with end-to-end stable-ID enforcement. | ADOPT → tag/mask/quality/audit → rename → restart → UNLINK; external drop/recreate cannot inherit stale policy. |
| P1 external writers | Add/run actual Spark local Iceberg writes on MinIO with JDBC/REST catalogs. | Append, position delete, DV, overwrite and equality-delete fail-closed results compared with Spark. |
| P2 recovery | Crash/ambiguous rename currently remains RENAMING and fails closed. Durable automatic reconciliation of the physical rename and governance projections is not implemented. | Crash injection before/after physical rename and every projection step converges without granting authority to a replacement. |
| P2 lifecycle/governance integration | Complete combined-revision validation of automatic maintenance leadership, durable audit, tags/masks, write-quality gates, statistics consumed by planning, UNKNOWN recovery and orphan cleanup. | Actual SQL results and durable effects across coordinator process restart and leadership transfer. |
| P2 Paimon external writer | Add/run Flink mini-cluster fixtures for primary key, sequence groups, aggregation and partial updates. | Flink-produced data remains correct and respects MANAGED/external ownership boundaries. |
| P3 equality deletes | Full equality-delete semantics remain unimplemented/unqualified; capability must stay false. | Spark oracle for equality/position/DV mixtures, null/type/schema-evolution and snapshot/sequence applicability. |
| P3 external lineage | Complete the snapshot/audit-log reconstruction and table/column lineage depth audit. | Queryable external lineage with durable ordering and restart recovery. |
| P3 active-active | Exercise two real coordinators plus an independent writer, not only store instances or threads. | CAS, lease loss, version recheck, conflicting commits and restart tests show no duplicate/lost data or false success. |
| P3 deployment matrix | Finish canonical same-source Linux image, Linux ASan/LSan, S3/MinIO and REST/Hive/JDBC catalog qualification. | Image/source provenance and all mandatory CI/release gates pass. |
| Format coverage | Compressed Puffin footer/blob encodings are explicitly rejected. | Either implement and test them against SDK-produced files or document a deliberate supported-format limit in release qualification. |

Paimon procedures that remain unsupported must continue to fail explicitly;
they must not be advertised as successful no-ops. Their complete support/removal
matrix still needs final P3 verification.

## Multimodal file plane

### Implemented and wired

- Volume and Stage definitions, immutable identity, DDL, metadata persistence,
  grants, authorization and audit; SQL `LIST` and `REMOVE`; managed roots retain
  identity across rename and are not reused by a new object with the same name.
- Coordinator-issued short-lived, credential-free file operation descriptors;
  Worker LOCAL/S3 streaming access, metadata, listing, conditional deletion,
  ranged reads, HTTP HEAD, version conditions and provider presigned URLs.
- Stable Stage URLs resolve the immutable Stage identity and recheck current
  authorization on every GET/HEAD before redirecting to a short-lived Worker URL.
  The previously issued Worker URL remains usable until its own expiry.
- Manual Stage refresh publishes a durable directory index atomically only after
  a complete listing. Refresh attempts are fenced. A Worker-side PostgreSQL page
  reader supports bounded pages, a consistent snapshot, cancellation and deadlines.
  This page reader is not wired to a Native SQL Directory operator.
- Multipart sessions now have a signed 24-hour absolute expiry independent of
  short-lived request grants. Tokens bind the complete storage scope and path;
  renewal requires a fresh authorized grant and can continue on another Worker.
  Expired cleanup retains ownership on failure for a subsequent retry.
- LOCAL multipart parts use content-based SHA-256 ETags and atomic replacement.
  Truncated/oversized/interrupted writes preserve the previously accepted part.
  Completion checks actual bytes against the client's part ETags and sizes before
  publication; rejected validation preserves the object and retryable parts.

The behavior and API details are in
[Volumes, Stages and file access](../getting-started/catalogs/volumes-stages-files.md).

### Validation evidence

The focused Maven reactor selection completed with **91 tests, zero failures,
errors or skips**, covering the following classes against real PostgreSQL,
production HTTP resources and the packaged Native library where applicable:

- `JdbcFileSpaceRegistryTest` (12), `JdbcFileDirectoryIndexTest` (8),
  `JournalSqlOperationTest` (1), `JdbcQueryLineageOutboxIntegrationTest` (9),
  `JdbcCopyFrozenInputLineageTest` (5).
- `FileSpaceAccessServiceTest` (10), `FileUrlServiceTest` (4),
  `WorkerFileClientTest` (6), `StageDirectoryRefreshServiceTest` (3),
  `FileSpaceObjectStoreTest` (5).
- `StageFileUrlHttpIntegrationTest` (6), `FileDirectorySnapshotIntegrationTest` (8),
  `FileSpaceDdlE2ETest` (5), `FileSpaceControlPlaneContractE2ETest` (3),
  `ActivationSmokeE2ETest` (3), `SpiWiringVerificationTest` (3).

The subsequent multipart changes passed isolated JDK 17/JUnit checks using the
reactor dependency classpath, with zero failures or skips:

- `FileSpaceFileResourceTest`: 19; `S3ObjectStoreAccessorMinioTest`: 8 against a
  real MinIO container; `LocalObjectStoreAccessorTest`: 16;
  `FileSpaceHttpReadTest`: 20; `FileSpaceMultipartHttpTest`: 1.
- The new two-Worker renewal test fails against the pre-fix resource because the
  origin Worker deleted staged parts after request-grant expiry. It passes after
  the session lifetime repair.
- Three LOCAL retry/content regression cases fail against the pre-fix accessor
  and pass after the integrity repair.
- Scoped Checkstyle for both changed production classes passes. Earlier scoped
  file-plane Checkstyle, dead-stub/connectivity, SPI annotation and E2E
  truthfulness checks passed. These are not a passing repository-wide static gate.

The two HTTP Worker endpoints above run in one JVM. This establishes routing and
shared-storage behavior, not independent-process recovery or distributed qualification.

The broader default-profile metadata/coordinator/worker owner reactor was
interrupted during quota wrap-up with exit 130. Metadata completed 514 tests with
zero failures, errors or skips; this profile excludes integration-tagged metadata
tests. Earlier dependency modules also completed successfully. The run had not
reached the Coordinator and Worker owner suites. This is an incomplete reactor,
not a passing broader gate; rerun the affected owners and full release profile.
The earlier owner run stopped with two metadata errors; their root causes
(journal double cleanup and an empty migration update advancing the authorization
revision) were fixed, and the affected focused tests subsequently passed.

### Remaining work against the design

| Work package | Remaining implementation / acceptance |
|---|---|
| M0: definitions and governance | Finish the full cross-tenant, external-location privilege, audit and migration acceptance matrix; retain real database and authenticated HTTP evidence. Existing passing cases do not cover every combination. |
| M1: transfers and URLs | SQL URL functions and SQL/client PUT/GET surfaces; scoped URLs and S3 AssumeRole/STS credentials; durable multipart ownership/recovery and autonomous reclamation; S3 large-object and 100 GiB multipart acceptance. The LOCAL 5 GiB HTTP gate is recorded separately below and does not qualify these other backends or multipart sizes. |
| M2: queryable files and ingestion | Wire `DIRECTORY(@stage)` through planner → typed v2 → Worker → JNI → Native with the exact six columns; implement URI `COPY INTO`, `read_files` with the eight requested formats, external-table DDL/readers, write-through index updates and actual `FILE MANAGED` copy/publication. FILE modifiers and index/page-reader foundations are not those completed execution paths. |
| M3: Iceberg v3 and AI | Verify every requested Iceberg v3 feature and AI function against the latest implementation and real providers, including FILE authorization, modality/size limits and exact SQL semantics. Changes and tests in other workstreams are not automatically qualification of the design's full matrix. |
| M4: lifecycle and operations | Managed DROP physical-file semantics, external-file preservation, automatic refresh, events/Inventory, durable restart/checkpoint recovery, per-tenant/space byte and concurrency quotas, backlog/coverage and latency metrics, documented operational limits. Manual refresh and opportunistic multipart cleanup do not satisfy this package. |
| M5: production scale and clients | OSS/GCS/ADLS implementations and consistency tests; 100-million-entry directory qualification; real client REST/JDBC/CLI E2E; fault/restart/multi-process tests and deployment/runbook verification. |
| Release acceptance | Full affected Java/Native/JNI chain on the final source, required sanitizer checks, independent-process/multi-node failures and release full regression. Native smoke tests do not replace these gates. |

### Reproducing the large-object gate

`FileSpaceLargeObjectIntegrationTest` transfers a real 5 GiB LOCAL object through
HTTP, compares full upload/download SHA-256, checks HEAD length, verifies bytes
across the 4 GiB boundary and rejects a stale ETag. Allow at least 6 GiB of free
scratch space and several minutes; temporary files are removed by JUnit. This is
not a sparse-file or mocked byte-count check and does not qualify S3 large objects
or 100 GiB multipart.

```bash
mvn -pl pangrova-worker -am -Pfull-regression \
  -Dtest=FileSpaceLargeObjectIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The initial standalone harness passed on 2026-10-25 with a 256 MiB Java heap,
transferring and downloading all 5,368,709,120 bytes. Upload and download SHA-256
were `7535f0d34d4af8a46292dbb5958215503907cd0bd269ada090dfc59ff3896d6f`.
HEAD, the range crossing 4 GiB, and the stale-ETag 412 check passed; its temporary
object was removed. The final JUnit integration test also passed: one test,
zero failures/errors/skips, 267 seconds with a 256 MiB heap. Its position-dependent
pattern includes high offset bits, so a 32-bit range-offset wrap cannot return
matching bytes. JUnit removed the temporary 5 GiB object after the test.

## Shared production gates and continuation order

The P0–P3 and M0–M5 matrices above retain their workstream-specific acceptance
criteria. Final-source regression, artifact provenance, sanitizer validation,
process-level recovery and deployment qualification are shared release gates;
focused or historical results do not close them.

1. Freeze the next combined branch revision, relink ASan and run the full affected
   native/Java chain. Release JNI, its classifier and a new runtime manifest have
   been rebuilt; the focused SQL acceptance passed. Preserve reports and hashes
   from previous artifacts separately. Complete the interrupted file-plane
   Coordinator/Worker owner suites and the full release profile on the final
   source as well.
2. Serialize the resource-heavy Docker integrations, reproduce any remaining
   failure, and rerun the four runtime owner suites in separate JVMs.
3. Complete stable-ID governance projections and process-level qualification of
   catalog deployment fencing, then add actual crash/restart and active-active
   acceptance fixtures.
4. Implement the Native Directory execution slice and then ingestion and FILE
   managed publication, following the execution boundary below.
5. Add the Spark/Flink external-writer matrix and the equality-delete oracle.
   Complete the remaining file-plane governance, transfer, backend, scale,
   lifecycle, AI and client acceptance matrices before closing their gates.
6. Build and qualify the canonical Linux image with Linux sanitizers and the
   full release regression against the final implementation. Only then reassess
   production readiness.

### File-plane execution boundary

The next main vertical slice is the Native Directory execution path. Resolve and
authorize the immutable Stage reference in the control plane, propagate a typed
binding and publish capability only when the Worker/Native path is actually wired.
Construct the six columns in Native and verify SELECT, predicates and joins with
real directory snapshots and authorization changes. Do not return relational
query rows from Coordinator Java or register a placeholder table function.

Then connect ingestion and FILE managed writes to the existing fenced publication
mechanisms before claiming the design's first complete upload → DIRECTORY → COPY
→ FILE/AI query loop. Keep the remaining size, scale, backend and recovery gates
explicit until executed successfully.

Do not mark either original plan complete merely because targeted tests pass.
