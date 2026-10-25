# Table Control Plane review and qualification ledger — 2026-10-25

Status: **production qualification is incomplete**. This ledger follows the full
P0–P3 Table Control Plane plan, not only its fail-closed P0 subset. A component
test passing does not qualify the complete deployment.

The review was paused at the user's requested account-quota boundary (10%
remaining), after committing the repairs and completing the in-flight focused
validation. No production-readiness or full-plan-completion claim is made.

## Root causes repaired

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

## Evidence obtained

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

## Remaining production blockers and unimplemented scope

| Plan area | Remaining work | Required acceptance evidence |
|---|---|---|
| P0 final regression | Complete a clean build and the affected/full Java reactor against the final combined revision; rerun failed suites and all four isolated Native owner suites with the final JNI artifact. | Matching source/artifact hashes, complete reports, no unresolved failure or skipped mandatory gate. |
| P0/P1 catalog deployment identity | `TableControlRecord` persists catalog incarnation/revision, but `TableControlService.newRecord` does not populate them from the live catalog descriptor. Complete registration/adoption/backfill wiring and all admission comparisons. | Catalog drop/recreate and revision change cannot inherit old table authority, including when the physical table UUID is unchanged. |
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

## Continuation order

1. Freeze the next combined branch revision, relink ASan and run the full affected
   native/Java chain. Release JNI, its classifier and a new runtime manifest have
   been rebuilt; the focused SQL acceptance passed. Preserve reports and hashes
   from previous artifacts separately.
2. Serialize the resource-heavy Docker integrations, reproduce any remaining
   failure, and rerun the four runtime owner suites in separate JVMs.
3. Complete catalog deployment identity and stable-ID governance projections,
   then add actual crash/restart and active-active acceptance fixtures.
4. Add the Spark/Flink external-writer matrix and the equality-delete oracle.
5. Build and qualify the canonical Linux image with Linux sanitizers and the
   full release regression. Only then reassess production readiness.

Do not replace these outstanding items with a claim that the original P0–P3
goal is complete merely because the targeted control-plane tests pass.
