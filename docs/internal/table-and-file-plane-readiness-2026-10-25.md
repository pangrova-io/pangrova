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

## File-session authority binding — continuation on 2026-10-26

File tickets now carry stable namespace ID, authenticated subject/tenant and the
exact storage-profile incarnation/revision returned by one current-definition
lookup. Multipart signatures and originating-Worker provider-ID checks bind these
fields together with the complete physical storage scope and path. A new grant
from another principal, tenant, namespace or profile version cannot resume the
old session even when the physical path is identical. Rename keeps the namespace
identity; DROP/re-CREATE cannot adopt it. Missing authority fields and older
session signatures fail closed. Upgrade requires fresh tickets/new sessions.

The reactor installation, focused Worker/Coordinator regressions and both owners'
static analysis passed. The corrected combined selection passed all 22 tests,
zero failures/errors/skips: `StageFileUrlHttpIntegrationTest` (7),
`FileSpaceControlPlaneContractE2ETest` (3), `TableControlLifecycleE2ETest` (9),
and `ActivationSmokeE2ETest` (3). The HTTP regression uses actual PostgreSQL RBAC,
authentication filters, Coordinator issuance and Worker LOCAL multipart bytes.
The registry correctly forbids overlapping external locations; the test exercises
rename followed by DROP/re-CREATE at that location instead of weakening the guard.
Complete Worker/Coordinator module reruns are in progress at this checkpoint.

An already issued ticket remains a bearer capability until its original deadline;
subject binding does not authenticate its holder or provide immediate revocation.
This is not durable multipart state, autonomous cleanup, UNKNOWN reconciliation,
current tenant-membership qualification or the final production regression.
No heavy-data test or native source change is included.

## Stable row-policy ownership — continuation on 2026-10-26

Production bootstrap now reads and saves authored row policies through the stable
table ID. Canonical rows survive rename without a name migration; diagnostic names
are projected from the current registry. A different table under the released name
cannot inherit or overwrite those policies, and deleting the captured original
policy ID leaves replacement policies intact. Both reserved rename names and a
replaced catalog deployment reject policy reads. Deferred artifact authorization
uses the same UUID lookup on its existing publication transaction.

V100 preserves historical name-only rows as unbound instead of guessing ownership.
An enabled unbound row fails closed until explicitly verified and rebound or
retired. Upgrading an existing populated policy table preserves expressions and
priorities, replaces the old name-only uniqueness constraint, and does not advance
the authorization revision merely for an unchanged policy migration.

The Java reactor installation passed with JDK 17 and Python 3.12. Security,
Metadata and Coordinator module test invocations passed, together with a separate
populated-policy upgrade regression and changed-owner Checkstyle. Security,
Metadata and Coordinator static analysis also passed. The focused
HTTP SQL/Native selection passed 14 tests, zero failures/errors/skips:
`TableControlLifecycleE2ETest` (9), `LanceFeatureViewExportE2ETest` (2), and
`ActivationSmokeE2ETest` (3). The fixture now shares production JDBC table ownership
between foreground SQL and background publication instead of assigning unrelated
in-memory identities. Rebuilding the query pipeline and reopening a store do not
qualify independent-process recovery. JNI sources/artifacts are unchanged.

This closes the tested table-identity path for authored row policies. Quality-rule
identity, column-schema identity, immutable directory manifests, durable multipart,
managed ingestion, active stream revocation and final production qualification
remain open; the deferred heavy environment gates remain deferred.

## Stable classification and rename-reservation checkpoint

The current continuation replaces the duplicate name-keyed `column_tags` store with
canonical `COLUMN_TAG` governance bindings. Foreground masks and deferred raw-read
publication now derive classification directly from stable-ID bindings. Production
bootstrap no longer depends on an event listener updating a second mask authority.
Authored column masks persist a table ID; retirement and delayed notifications target
only the captured original identity. Unknown historical ownership fails migration or
policy reads rather than being adopted by name.

Physical rename persists its source and operation identity and transactionally reserves
both names. Both source and target remain blocked through restart while RENAMING.
CREATE, ADOPT and governance cannot overwrite a pending rename. Connector exceptions
retain reservations because an exception or one source-path observation cannot prove
that an in-flight physical rename will never commit. Automatic crash reconciliation
remains a separate open requirement.

Real HTTP SQL validation found that the security expression parsers used the SQL
standard function table, giving `CHAR_LENGTH` INTEGER while Native requires BIGINT.
They now use the same typed operator contracts as ordinary SQL. The regression covers
all three length aliases in masks and row filters, including substring positions.
The focused final migration/control/policy verification passed: 84 tests, zero
failures/errors/skips. It includes the PostgreSQL upgrade from V64 with an existing
frozen COPY input, rename reservation across store restart, name reuse, canonical
classification without event projection, and deferred raw-read rejection. Empty
backfills now avoid issuing UPDATE statements that would unnecessarily advance the
authorization revision and revoke an unrelated existing COPY root. Reservation
maintenance touches only the affected names, with no whole-map sweep or redundant
reservation rewrites during ordinary version observation.

Security and Governance owner regressions passed, as did Planner and Worker daily
regressions. Metadata's complete post-upgrade rerun at `27d84e07` passed all 830 tests with zero
failures/errors/skips after the upgrade correction. Coordinator's first complete
run had 3,346 tests and one unbound-mask fixture error; the corrected real ADOPT /
identity-bound fixture and ownership/SDK regression passed all 35 selected tests.
Checkstyle passed for the changed Java owners. Metadata, Coordinator, Security and
Governance owner static analysis passed; the entire reactor remains unqualified.
Static analysis is not globally green: unchanged materialization reports nine
representation-exposure findings and unchanged planner materialized-view rewriting
reports two. These remain explicit gates; the reviewed Worker SDK-client classification is described below.
Native sources are unchanged in this checkpoint.

This checkpoint does not implement immutable directory Manifests/Native DIRECTORY,
durable multipart ownership/reconciliation, FILE MANAGED ingestion, active-stream
revocation, or the rest of M0–M5. It also does not qualify authored row-policy/quality
rule identity, independent-process recovery or the production deployment matrix.
The previously deferred heavy data/environment gates stay deferred.

## Paimon metadata freshness and final classification SQL evidence

The first corrected Native SQL run passed masking/rename but failed writing a new
schema under the released original name. A separate Worker's real SDK catalog held
the old cached table schema; SDK `uuid()` could simultaneously read the new table's
creation time. Authority now invalidates the SDK name cache before each fresh
metadata resolution, including writes, read binding validation, snapshots, indexes
and procedures. Exact UUID/location/schema comparisons remain required. No retry,
cache TTL extension or identity check was removed.

Two independent real SDK catalogs cover external schema evolution (old write must
fail) and rename/recreate (new write must succeed without inheriting old schema).
The complete Paimon owner passed 394 tests, zero failures/errors/skips, plus
Checkstyle. The final `TableControlLifecycleE2ETest` passed all eight tests, zero
failures/errors/skips, in 58.329 seconds through authenticated HTTP SQL, the real
planner, typed v2, Worker/JNI, Native execution and client result transport.
It verifies four PII policies, NULL/short values, query-pipeline reconstruction,
rename, old-name recreation and writes, independent policies and captured-ID drop
cleanup. Pipeline reconstruction is not independent-process crash recovery.
Native sources/artifacts did not change in this repair; the verified packaged
Release JNI SHA-256 remains
`4e6a3e1cacc646d3666c305092ef629d0ab850181f0b1c7313c520fe9196af38`.

## File delivery deadline checkpoint

Provider URL requests are capped at the authorizing grant's remaining whole seconds;
less than one second remaining is rejected before signing. The Worker returns the
bounded issue-time deadline rather than advertising a new hour-long lifetime.
Opened file reads check their original deadline before and after storage reads/skips.
Expiry closes the stream and reports an error; a storage read returning after expiry
cannot deliver that chunk or be mistaken for successful EOF.

Small-data real HTTP, LOCAL, actual S3 signing and MinIO checks passed: 51 tests,
zero failures/errors/skips, including a read that returns only after expiry. The
changed six Java owners also passed Checkstyle. Native sources are unchanged.

This is an expiry boundary, not immediate RBAC revocation: previously issued provider
URLs follow provider semantics, already transmitted bytes cannot be recalled, and an
in-flight blocked storage read relies on its backend timeout before the post-read check.
Autonomous idle-stream cancellation and live revocation still require separate work.
No heavy object/scale test was run.

## File endpoint ownership/static-analysis checkpoint

File requests defensively copy their multipart part receipts and the injected JSON
mapper configuration. External mutation cannot alter a constructed completion request
or switch a live endpoint's decoding rules. Directory snapshot initialization,
validation, release and page reads share the same connection monitor; cancellation
still uses the independently visible active statement. LOCAL staging explicitly
requires a destination parent, with no fallback path.

The 58 focused deadline/HTTP/LOCAL/real S3 signing/MinIO tests passed, with zero
failures/errors/skips. Worker Checkstyle and SpotBugs passed after these repairs.
One narrowly scoped constructor/field filter documents intentional retention of the
factory-owned S3Client: the pinned SDK declares it `@ThreadSafe`, its configuration is
built before borrowing, the accessor never returns it, and the factory owns its pool
and shutdown. This does not suppress getter exposure or unrelated findings. The other
eight reported Worker findings were repaired in source rather than excluded.
The unchanged materialization/planner findings continue to block global static analysis.

## Combined correctness checkpoint — continuation on 2026-10-26

The final combined selection passed all 30 tests, zero failures/errors/skips:
`TableControlLifecycleE2ETest` (8), `FileDirectorySnapshotIntegrationTest` (8),
`ActivationSmokeE2ETest` (3), and `ColumnMaskingE2ETest` (11). This includes actual
Native SQL and activation, plus the PostgreSQL directory component; the latter is
not Native DIRECTORY SQL. The quoted-column regression asserts the rendered
identifier `"ssn"` explicitly instead of expecting the old unsafe bare substitution.

CREATE registration and orphan registration now validate the exact control record
read for mutation. A separate preflight name check cannot protect an interleaving
rename by itself. Neither path may overwrite RENAMING or release either reserved
name; the captured metadata revision still fences later concurrent changes.
The 57 selected service/JDBC/tag regressions passed after this final guard, as did
Metadata Checkstyle/SpotBugs. The complete 830-test Metadata result recorded above
belongs to the earlier post-upgrade checkpoint; it is not relabelled as a fresh
complete reactor run after this final guard.

All remaining implementation/production gates below remain open. This checkpoint
qualifies the listed paths and does not claim M0–M5 or the entire plan complete.

## Resumed implementation and qualification

Current user-directed priority: align architectural correctness and research the
remaining design before heavy qualification. See the [2026-10-25 architecture
review](table-and-file-plane-architecture-alignment-2026-10-25.md). The 100 GiB,
hundred-million-entry, PB-scale and Linux production-environment gates are
**deferred to a dedicated test environment at the user's request**. They remain
open requirements; this checkpoint does not launch them on the development host.
Bounded correctness checks and already-running owner selections may finish.

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
| Clean metadata owner selection | 822 passed; zero failures, errors or skips; Maven exit 0 | Complete owner evidence for the catalog-fencing snapshot, following the observer-fixture repair. Does not replace the full dependency reactor or process-level qualification. |
| Clean Coordinator owner selection | 3,264 passed; zero failures, errors or skips; Maven exit 0 | Clean compiled outputs plus the actual-authority fixture repairs in `83eadc51`; the later Shuffle/Paimon artifact changes require their own affected checks. |
| Architecture and lightweight gates | Package dependency acyclicity, scoped/full-owner Checkstyle, SPI annotations, E2E truthfulness, documentation references and dead-stub checks passed | The initial control/catalog package cycle was removed before acceptance; these checks do not establish runtime reachability by themselves. |
| Current Release native suite | 4,333 passed; 1 of 4,334 failed with an mTLS `wrong version number` handshake error | The same isolated case subsequently passed once and in 100 diagnostic repetitions. Its intermittent full-suite failure is unresolved and the Release gate remains open. |
| Current macOS ASan native suite | 4,334 passed in 1,985.30 seconds; runner exit 0 | Rebuilt Rust/JNI combination, with `detect_leaks=0` per macOS policy. Linux ASan/LSan and the failed Release gate remain separate requirements. |
| Current isolated Native runtime owner suites | WorkerBootstrap 5; NativeExchangeTransportExternalIntegration 3; WorkerShuffleChannelAdmission 1; CopySourceReadHttp 1 passed, each owner in a separate JVM | All four owners also passed again after installing `a7877791` / `7c50471b` artifacts; zero failures, errors or skips. The downstream classpath contains all 651 canonical SPI classes only in the SPI JAR. |
| Coordinator fixture repairs | 61 tests passed after cleaning compiled outputs | Generated-column fixtures now persist catalog descriptors, routing retains actual Worker identity, and managed write admission registers the connector identity. The clean complete owner result is recorded above. |
| Shuffle library/executable boundary | 46 module tests passed; repeated packaging and standalone configuration-loader smoke passed | The main artifact contains only 75 service classes; `-exec.jar` contains matching bytes for all 651 canonical SPI classes. Installed thin artifact passed all four downstream Native owners (10 tests); the executable remains a separately attached artifact. |
| Paimon connector procedure matrix | 392 module tests passed, including 6 new durable-effect cases; Checkstyle and static analysis passed with zero reported bug instances | `7c50471b` wires the implemented descriptors to connector discovery. Real local SDK effects include compaction, tag retention/delete, rollback after catalog reopen, branches and orphan deletion. Unsupported operations remain unadvertised and fail without changing live rows; SQL CALL authorization is unchanged. Spark/Flink, process-level and final artifact qualification remain separate. |

The first metadata owner attempt selected 822 tests: 818 passed and 4 failed
on an observer fixture that discarded catalog descriptors. Persisting those
descriptors repaired the fixture; the later clean 822-test run passed as recorded
above. The earlier failed report is not treated as a passing run.

The broader dependency reactor was also interrupted while a Docker exec output
stream remained open after its command exited. This is retained as an incomplete
reactor; no test was disabled or timeout raised. The final same-source regression,
sanitizers, isolated Native owners and deployment gates below remain required.

The rebuilt macOS ASan JNI SHA-256 is
`712c1b7cff054908f51b434e6ef5d0ca61a941f81334719486914fc4c97aaeae`.
It uses the Rust archive identified below. The successful ASan report does not
replace the unresolved Release mTLS failure.

The broader Coordinator selection exposed fixtures that discarded durable catalog
state or substituted a synthetic Worker identity; `83eadc51` repairs those
fixtures without weakening admission checks. Cleaning also removed an orphaned
compiled test whose source is absent from the repository. The first incremental
selection remains failed; the subsequent complete clean owner result is recorded
separately above.

`a7877791` separates the Shuffle library JAR from its attached `exec` executable
and updates the service image input. Replacing the main dependency artifact with
a fat JAR had embedded duplicate SPI classes in Java consumers; incremental
packaging could then expose stale contracts ahead of the canonical SPI JAR.

### Declared deletion-vector format boundary

The release scope now explicitly supports uncompressed Puffin footers and
uncompressed DV blobs, with the limits documented in
[Storage backends](../getting-started/catalogs/storage-backends.md#iceberg-deletion-file-compatibility)
and the production sign-off checklist. Optional LZ4 footer compression is a
deliberate unsupported format boundary. Compressed `deletion-vector-v1` blobs
are prohibited by the [Iceberg Puffin specification](https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type)
and must remain rejected; they are not missing DV compression functionality.
External-writer and final-artifact acceptance remain open.

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
| P1 stable governance identity | Canonical tags and authored/derived column-mask reads now use stable table IDs, including deferred raw-read authorization and captured-ID cleanup. Authored row-policy reads now share the stable UUID in foreground/background paths. Complete quality-rule and column-schema identity and process recovery qualification; do not equate this classification checkpoint with all governance being complete. | ADOPT → tag/mask/quality/audit → rename → restart → UNLINK; external drop/recreate cannot inherit stale policy. |
| P1 external writers | Add/run actual Spark local Iceberg writes on MinIO with JDBC/REST catalogs. | Append, position delete, DV, overwrite and equality-delete fail-closed results compared with Spark. |
| P2 recovery | Crash/ambiguous rename persists the source intent and reserves both names in RENAMING; it fails closed across store restart. Durable automatic reconciliation of physical rename and governance projections is not implemented. | Crash injection before/after physical rename and every projection step converges without granting authority to a replacement. |
| P2 lifecycle/governance integration | Complete combined-revision validation of automatic maintenance leadership, durable audit, tags/masks, write-quality gates, statistics consumed by planning, UNKNOWN recovery and orphan cleanup. | Actual SQL results and durable effects across coordinator process restart and leadership transfer. |
| P2 Paimon external writer | Add/run Flink mini-cluster fixtures for primary key, sequence groups, aggregation and partial updates. | Flink-produced data remains correct and respects MANAGED/external ownership boundaries. |
| P3 equality deletes | Full equality-delete semantics remain unimplemented/unqualified; capability must stay false. | Spark oracle for equality/position/DV mixtures, null/type/schema-evolution and snapshot/sequence applicability. |
| P3 external lineage | Complete the snapshot/audit-log reconstruction and table/column lineage depth audit. | Queryable external lineage with durable ordering and restart recovery. |
| P3 active-active | Exercise two real coordinators plus an independent writer, not only store instances or threads. | CAS, lease loss, version recheck, conflicting commits and restart tests show no duplicate/lost data or false success. |
| P3 deployment matrix | Finish canonical same-source Linux image, Linux ASan/LSan, S3/MinIO and REST/Hive/JDBC catalog qualification. | Image/source provenance and all mandatory CI/release gates pass. |
| Format boundary qualification | The supported DV format is now documented as uncompressed footers and uncompressed blobs. Optional compressed footers remain unsupported; compressed DV blobs violate the specification and stay rejected. | Verify the declared limit against actual external writers and the final shipped artifact; retain explicit rejection evidence and do not advertise unrestricted Puffin encoding support. |

Paimon procedures that remain unsupported must continue to fail explicitly and
remain excluded from discovery. The current local SDK matrix is now verified
above; it does not authorize otherwise rejected SQL CALL mutations or close
external-writer and process-level qualification.

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

1. Complete architecture alignment before extending the file execution path, as
   detailed in the linked 2026-10-25 review: immutable directory generations,
   durable uploads and UNKNOWN recovery, separate authorization/ownership/read
   protection, and stable governance projections. Heavy capacity and Linux
   production-environment testing is deferred to the dedicated environment.
2. Freeze the next combined branch revision and complete the affected
   native/Java chain. Release JNI, its classifier and a new runtime manifest have
   been rebuilt; the focused SQL acceptance passed. Preserve reports and hashes
   from previous artifacts separately. Complete the remaining Worker-wide and full
   release profile gates against the final source. The clean metadata / Coordinator
   owners, macOS ASan and all four
   isolated runtime owners have now passed for the checkpoints recorded above;
   do not rerun unchanged inputs merely to accumulate green reports.
3. Reproduce and correct the unresolved Release mTLS failure and dependency-reactor
   Docker exec stream hang. Run any newly affected owner in a separate JVM;
   preserve failed evidence until a relevant correction is validated.
4. Complete stable-ID governance projections and process-level qualification of
   catalog deployment fencing, then add actual crash/restart and active-active
   acceptance fixtures.
5. Implement the Native Directory execution slice and then ingestion and FILE
   managed publication, following the execution boundary below.
6. Add the Spark/Flink external-writer matrix and the equality-delete oracle.
   Complete the remaining file-plane governance, transfer, backend, scale,
   lifecycle, AI and client acceptance matrices before closing their gates.
7. In the dedicated environment, build and qualify the canonical Linux image,
   Linux sanitizers and full release regression against the final implementation.
   Only then reassess production readiness.

### File-plane execution boundary

The next main vertical slice is the Native Directory execution path. Resolve and
authorize the immutable Stage reference in the control plane, propagate a typed
binding and publish capability only when the Worker/Native path is actually wired.
First implement the reviewed immutable generation/Manifest boundary; do not wire
the Worker JDBC reader into production SQL. Construct the six columns in Native
and verify SELECT, predicates and joins with real directory snapshots and
authorization changes. Do not return relational query rows from Coordinator Java or register a placeholder table function.

Then connect ingestion and FILE managed writes to the existing fenced publication
mechanisms before claiming the design's first complete upload → DIRECTORY → COPY
→ FILE/AI query loop. Keep the remaining size, scale, backend and recovery gates
explicit until executed successfully.

Do not mark either original plan complete merely because targeted tests pass.
