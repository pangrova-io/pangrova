# PB-scale execution hardening

Date: 2026-09-05

## Scope and acceptance requirements

This plan addresses the execution-boundary and bounded-resource findings from the
current-source review. It preserves the existing Java coordinator / native worker
architecture and the user's pre-existing worktree changes. It does not equate
small-scale correctness tests with measured PB-scale capacity.

| Requirement | Acceptance criterion |
|---|---|
| REQ-001 | Connector metadata DELETE never reads and retains the table's data rows in the coordinator; read-dependent DELETE uses distributed execution. |
| REQ-002 | Production native shuffle has bounded byte usage, consumer-driven backpressure, cancellation/error propagation, and no slow-partition deadlock. |
| REQ-003 | Split discovery and delivery do not require retaining all splits in coordinator heap or fail solely because more than 100,000 splits exist; retries, index binding, completion, and cancellation remain correct. |
| REQ-004 | Partitioned windows can execute on correctly hash-partitioned workers; global windows retain their required single-task semantics. |
| REQ-005 | Distributed writes avoid unnecessary batch-wide TypedRow materialization, preserve column mapping/types/nulls, and release native/JVM resources on success and failure. |
| REQ-006 | Scale validation rejects missing/zero/invalid measurements and documents reproducible large-data, skew, backpressure, and failure-recovery acceptance. |
| REQ-007 | Execution-boundary documentation matches implemented behavior; the full native/ASan/Java/E2E/smoke chain and relevant repository gates are recorded truthfully. |

## Technical context and constraints

- Java 17, Maven 3.9+, C++20, CMake, GoogleTest, JUnit 5, jqwik.
- Existing connector SPI, Substrait/protobuf contracts, JNI lifecycle, and constructor wiring remain authoritative.
- Preserve row correctness, partition ownership, snapshot identity, write atomicity,
  resource ownership, and authenticated control identities before throughput.
- No larger caps/timeouts as substitutes for bounded processing; no silent fallback,
  disabled tests, generated-source edits, or unsupported claims of PB readiness.
- Repository rules under `.pangrova/rules/` are the constitution for this change.
- No framework or language migration is proposed; migration transformation guidelines do not apply.

## Implementation steps and tasks

### P1. DELETE execution boundary (REQ-001)

- [ ] T001 [P] Trace `PlanExecutionFactory`, `PaimonMetadata`, and connector tests; replace coordinator row-reading DELETE with a metadata-only capability decision and distributed fallback. Delete obsolete full-table row helpers and update all callers/tests.
- [ ] T002 Add regression coverage for partitioned/unpartitioned DELETE ALL, supported metadata predicates, unsupported row predicates, and propagation of genuine connector errors.

### P2. Native shuffle resource safety (REQ-002)

- [ ] T003 [P] Trace `FlightShuffleServer`, `PartitionDemuxer`, producer queues and consumers; implement a bounded production path without head-of-line deadlock, including explicit resource cleanup.
- [ ] T004 Add native tests for slow/missing consumers, skewed partitions, oversized batches, cancellation, and error/cleanup paths; exercise the production registration path.

### P3. Incremental split discovery and delivery (REQ-003)

- [ ] T005 Trace connector source laziness, `SplitFetcher`, `DAGDispatchEngine`, existing pull protocol, worker lifecycle, and native split feeding. Select and implement one complete bounded path; do not merely enable the incomplete pull flag.
- [ ] T006 Cover more than 100,000 synthetic splits, bounded residency, exact-once assignment within an attempt, retry fencing, source failures, empty sources, index binding, cancellation, and source closure.

### P4. Window distribution (REQ-004)

- [ ] T007 Trace planner exchanges and `LeafStageDispatcher`; enforce partition-key-compatible distribution before removing blanket window consolidation. Preserve global-window semantics.
- [ ] T008 Add planner/dispatcher tests and SQL result regression for multiple partitions, global windows, ordering, and multiple window groups.

### P5. Worker write batches (REQ-005)

- [ ] T009 [P] Trace `WriteOperator`, `NativeWriteExecutor`, `PageDecoder`, `WritePageProjector`, and connector sink implementations; preserve a canonical batch format and avoid avoidable per-batch object materialization.
- [ ] T010 Add contract/ownership tests for column order, complex values, nulls, empty batches, decode/write failures, and resource release; verify the actual native producer format against the Java consumer.

### P6. Validation evidence and documentation (REQ-006, REQ-007)

- [ ] T011 [P] Inspect benchmark regression checks and baseline handling; fail explicitly on invalid measurements and add deterministic script tests without fabricating baseline numbers.
- [ ] T012 Update execution-boundary documentation, changelog, and a scale acceptance matrix; distinguish code completion, executed validation, and hardware-dependent capacity certification.
- [ ] T013 Run closest checks, affected suites, and the full-chain verification below. Review changes, connectivity, and relevant repository gates.

## Testing strategy

- Application type: distributed SQL engine with JNI and connector storage boundaries.
- Critical journeys: DELETE without coordinator row reads; large split streams;
  shuffled joins with slow consumers; partitioned/global windows; distributed
  writes and atomic commit; terminal-task cancellation and recovery.
- Unit/native tests use real collaborators or small fakes, deterministic data,
  bounded synchronization, and temporary directories. No mocking frameworks or sleeps.
- Integration/E2E reuse existing PostgreSQL Testcontainers and repository test bases.
- Docker must be available for container tests. There is no H2/SQLite or disabled-test fallback.
- Browser testing is not applicable. Capacity testing requires dedicated distributed hardware and data; missing measurements remain an explicit open gate.
- Entry points: affected Maven reactor tests and CTest filters first, followed by
  the mandatory complete chain. New tests must be reachable by canonical runners.

## Full-chain verification (sequential, stop at failure)

1. Configure `dev-release` and build native with warnings-as-errors.
2. Run all native CTest tests.
3. Configure/build/test `dev-asan`.
4. Package the new native library into Java resources.
5. Verify JDK 17 / Maven; run full `mvn -q -DskipTests install`.
6. Run full `mvn -q test`.
7. Run E2E reactor tests.
8. Run smoke reactor tests.

Run dead-stub/connectivity, SPI/proto compatibility (if applicable), changed-line
coverage, and documentation gates. Never report an unexecuted check as passed.

## Requirement mapping

| Requirement | Plan items | Tasks | Implementation evidence |
|---|---|---|---|
| REQ-001 | P1 | T001, T002 | Connector DELETE implementation, coordinator routing, connector and SQL tests |
| REQ-002 | P2 | T003, T004 | Native shuffle registration/buffering and native regression tests |
| REQ-003 | P3 | T005, T006 | Connector source, coordinator dispatch, worker/native delivery, split tests |
| REQ-004 | P4 | T007, T008 | Planner distribution, dispatcher, window SQL tests |
| REQ-005 | P5 | T009, T010 | JNI writer and connector batches, type/ownership tests |
| REQ-006 | P6 | T011, T012 | Benchmark validation script tests and scale acceptance matrix |
| REQ-007 | P6 | T012, T013 | Documentation and recorded full-chain/gate results |

## Progress and verified blockers

- Planning complete; implementation and validation pending.
- Initial worktree contains 625 changed/untracked paths; preserve those changes.
- Default shell uses JDK 21; an existing JDK 17 installation is available and must be selected for Java verification.
- Docker daemon is reachable. Native release build artifacts and dependency environment exist locally.