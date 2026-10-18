# TPC-H / TPC-DS benchmark audit

Audit and implementation date: 2026-10-18.

Pangrova now has a complete, explicitly stepped **engineering workload and
measurement harness**. This is not evidence that every query executes correctly
on Pangrova, that SF100/1000 meets a latency objective, or that the system has a
certified TPC result. Those claims require actual qualified runs.

## Findings and disposition

| Finding before this change | Disposition |
|---|---|
| Benchmark directory contained only TPC-H Q1; no TPC-DS directory | Added 22 TPC-H templates and 99 TPC-DS templates / 103 executable steps, with source and SQL hashes. |
| Nightly referenced absent startup/stop/schema files | Replaced with owned Docker-context lifecycle, complete schema manifests, catalog preparation/verification and a dedicated-host job. |
| dbgen `-O` incorrectly used as an output directory; child failures hidden | Uses `DSS_PATH`, fixed distributions, individually checked subprocess results and retained failure logs. |
| TPC-DS schemas covered only three tables, inferred other types, mishandled chunks and first rows | Explicit 24-table schema, typed streaming conversion, trailer/NULL validation, exact input inventory, file/footer hashes and atomic completion. |
| Generic parallelism assumed every dsdgen table has every chunk | A real SF1 run disproved that assumption: the pinned kit omits later children for small tables. Small scales generate serially; SF100+ chunks fact parents and generates dimensions once. Returns follow their sales parent. |
| PR produced three old-format query summaries; nightly used three samples with a strict 1000-sample comparator | PR now validates workload/evidence/transport contracts. Exclusive-host ordinary runs use three windows of five full rounds. The existing strict tail comparator remains separate and unchanged. |
| Java smoke suite called 10/50-sample maxima P99 and wrote obsolete JSON | Removed its obsolete producer, report maxima honestly, compute even-sample medians correctly; all existing maximum-observation budgets remain. |
| TPC-DS helper was uncalled and could fall back to nonempty/schema checks | Removed the uncalled helper, ten-query resources, placeholder references and rounding generator. Existing eight synthetic-pattern tests remain. New full results require Spark/Trino agreement; missing results fail, agreed empty results are valid. |
| Environment/data identity absent from comparisons | Workload, data files, snapshots, SUT main references, harness, client runtime, resources, cache and configuration are compared before accepting evidence. |
| Historical SF100 zeros and SF10 aggregates appeared usable as baselines | Preserved historical files unchanged. They do not qualify; no synthetic numbers were inserted. |

The planner's pre-existing 31-query TPC-DS golden selection is not expanded by
copying more files or generating unreviewed goldens. Its 99 SQL files are not
execution coverage. The new benchmark corpus is generated from pinned templates
rather than copied from those potentially simplified planner resources.

## Standards alignment and source versions

The [TPC current-version directory](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
listed TPC-H 3.0.1 and TPC-DS 4.0.0 at audit time. Use their corresponding
specifications when assessing conformance:
[TPC-H](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf),
[TPC-DS](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v4.0.0.pdf).

The reproducibly available source kits pinned here are **community TPC-H 2.14.0
and TPC-DS 2.10.0**. Their archive/commit checksums, versions and derived schema
sources are explicit in `tools.lock.json` and `schemas/`. They must not be
relabeled current official tool releases. Moving to official, version-aligned
archives requires an explicit reviewed lock/schema/query update and fresh
qualification; no automatic version detection or silent substitution exists.

Each query retains its source-template hash and adaptations. Notable changes:
TPC-H row-count directives become LIMIT and Q15 uses a query-local CTE; TPC-DS
Netezza day arithmetic becomes ANSI intervals; Q58/Q72 ORDER BY names are
qualified, Q72's date increment becomes an interval, and reserved aliases in
Q77/Q90 are renamed. Template variants with multiple results remain multiple
steps. These adaptations require independent engine validation, not a claim of
official approval.

TPC-DS SF100 is an engineering scale; formal 4.0 scales start at SF1000. Formal
TPC-H throughput stream minima include five at SF100 and seven at SF1000;
TPC-DS throughput uses an even stream count of at least four. An engineering
concurrency sweep alone does not satisfy the other execution rules.

## Evidence collected in this implementation

- Real pinned dbgen SF1 generation and typed Parquet conversion completed:
  eight tables / 8,661,245 rows, including 6,001,215 LINEITEM and 1,500,000 ORDERS.
- Real pinned dsdgen SF1 generation and conversion completed: 24 tables /
  19,557,335 rows, including 2,880,404 STORE_SALES and 11,745,000 INVENTORY.
- These datasets passed file hashes, Parquet footer/schema validation and scale
  qualification. They are local validation datasets, not performance baselines.
- All 125 executable steps were checked against the real SF1 schemas using an
  independent DuckDB binder. This is a syntax/schema diagnostic only; it neither
  executes the full workload nor replaces the Spark/Trino reference agreement.
- Persistent Java transport checks use the production REST client and decoder,
  local HTTP fixtures and actual Flight RPC. They verify process reuse, complete
  typed values, long strings, empty results, authentication failures and timeout
  cancellation. They do not exercise the Pangrova planner/native engine.
- Linux/ARM64 with GCC 12 bootstrap built all four generator executables successfully.
  The initial DS link failure exposed legacy common-symbol assumptions; explicit
  GNU89/common linkage flags fixed it without modifying upstream source.
- JDK 17 test compilation and fresh CLI packaging succeeded after selecting the
  repository-required Python 3.12 instead of the host's Python 3.9.
- The tooling suite passed 113 tests, CI policy checks passed 26, and production
  client HTTP/Flight transport checks passed four. The data tests include child
  failure collection and a multi-megabyte input consumed in batches below 32KiB;
  this batch-size check is not a measured whole-process RSS ceiling.
- The existing `SqlLatencyBaselineTest` ran ten tests: six passed, one failed and
  three errored. The constant maximum was 969ms against its unchanged 500ms budget;
  Q1/Q5 failed native Flight result retrieval and Q9 failed Stage 2 submission.
  These failures are retained, not disabled, hidden or calibrated away. The SQL,
  iteration counts and maximum-observation budgets were not changed by this work.

The initial Python-embedded JVM prototype crashed at JDK startup on this macOS
host. It was replaced with a separately owned persistent Java process; no JVM
crash workarounds or engine fallback were introduced into the benchmark.

### Q9 join-plan root cause follow-up

The earlier Q9 Stage 2 submission error originated in physical join conversion:
`Physical equality join requires non-empty, paired keys`. The CBO violated the
physical join contract in two connected ways. Hash, broadcast-hash and merge
selection admitted joins without equality keys; Calcite's general join associate
rule could then copy physical joins while replacing their predicates and inputs,
retaining an invalid algorithm or losing the required broadcast exchange.

The three algorithm rules now require equality keys before considering cost or
input size. Join association explicitly matches `LogicalJoin`, allowing physical
algorithms and distributions to be selected for the resulting logical inputs.
The strict physical join converter is unchanged; cross/theta joins use the existing
general join path rather than a serialization fallback.

Validation of this fix:

- The initial focused reproduction failed all four tests, including the same
  empty-paired-key exception. After the fix, the initial focused join/CBO suites
  passed all 41 tests.
- The planner module suite at that stage passed 1,373 tests with no failures, errors or
  skips. Its new regression includes Q9 with CBO enabled, fragmentation for four
  workers, physical exchange checks and actual Substrait conversion/serialization
  of every fragment. This is planner evidence, not a Native execution result;
  the default unit-test capability registry lacks YEAR, so the Q9 conversion test
  does not claim runtime capability qualification. The smaller cross-join test
  also exercises the production validated fragment-serialization entry point.
- A real Iceberg/Native E2E regression checks complete cross/theta/mixed-equality
  results, including duplicates and NULL. An initial retry of E2E and Q9 latency/smoke
  stopped before SQL execution: the shared real-model deployment could not
  resolve `registry.ollama.ai` while pulling tinyllama. No prerequisite was
  disabled, and no timeout or latency budget was increased.
- The full ancestor verify run also found an existing CI workflow-shape assertion
  failure in `RegressionSelectionTest`. Owner-module static verification completed
  its tests but failed on ten SpotBugs findings in untouched classes. These checks
  remain failed; they are not counted as successful verification.

Reproduce the planner suite with `mvn -q -pl pangrova-planner -am test` using the
required JDK 17/Python 3.12 environment. The ancestor CI assertion above must be
resolved for that entire reactor command to pass. The owner-only planner suite
was run against dependencies freshly installed from the same working tree.
The historical ten-test result above remains the last complete latency-suite
run. The focused Q9 acceptance below supersedes its Q9 failure only; it does not
replace a full-suite rerun or establish a scale baseline.

### Real Q9 execution acceptance follow-up (2026-10-18)

The original Java Q9 latency test now passes through the real planner, task
protocol, JNI, Native execution and consumed Flight results. Three additional
causes were traced and repaired after the join-plan fix:

1. Shared E2E startup always pulled models from the public registry, even with a
   prepared image. Real model preparation now happens before Maven. The fixture
   builder verifies every referenced blob size/hash and both manifest hashes,
   records the exact base/model/image identities, and creates an immutable image.
   Tests require that image and verify local inventory without any `/api/pull`.
   Missing models, invalid digests or missing image configuration fail. Production
   regression CI builds the fixture first and passes its immutable image ID.
2. Fragment serialization used `NativeCapabilityRegistry.defaults()`, a static
   name set that omitted YEAR. Production and E2E dispatch now obtain each
   validation snapshot from active Workers' validated actual inventories. A union
   allows a name available on an eligible Worker; exact kind/URN/signature and
   execution-target checks remain mandatory. Empty/malformed inventories fail;
   unknown functions do not become supported through defaults.
3. Native then revealed the exact failing signature: `eq(BIGINT, INTEGER)`.
   Calcite aggregate rewrites can introduce `COUNT(*) = 0` after SQL validation.
   Primitive arithmetic normalization existed, but comparison operands were not
   normalized. The same normalizer now materializes explicit casts to Calcite's
   common primitive numeric type for ordinary and null-safe comparisons, retaining
   each operand's nullability and the declared BOOLEAN result. Native exact binding
   remains strict. Its error now includes argument/output types for diagnosis.

Verified results on JDK 17.0.20 / macOS ARM64, using a freshly built and packaged
matching Native library:

| Check | Result |
|---|---|
| Original Q9 latency test | 10 warmups + all 50 measured executions completed; median **4,999.5ms**, maximum **6,755ms**, unchanged maximum budget **20,000ms**; no failures/errors/skips. |
| Planner module suite | **1,383 passed**, zero failures/errors/skips. The new comparison reproduction failed 7/8 before its fix and passes all eight afterward. |
| Real SQL/Native result regression | **5 passed**: cross/theta/equality joins with duplicates/NULL, plus nonempty SUM and empty-input SUM returning NULL. |
| Real model deployment regression | **4 passed**: real completion/384-dimensional embedding with the registry blocked and zero pulls; missing model, wrong digest and paused-process deadline cleanup. |
| Activation smoke | **4 passed** against the packaged Native runtime. |
| Production planner/session wiring integration | **1 passed**. |
| Actual Native binding/signature regressions | **11 passed**, including exact equality over 12 physical types and incompatible-type rejection. |
| Model fixture configuration / artifact tooling | **7 Java tests**, **5 artifact/CI tests**, and **26 CI policy tests** passed. E2E truthfulness and production stub-pattern checks passed. |

Q9 used the existing small fixture (`100 × scaleFactor` rows/table at multiplier
one), one Worker process and the original SQL. It is not official dbgen SF1,
SF100 or 1TB acceptance. It measures median/maximum observations, not a qualified
P99 or QphH. No workload, iteration count, latency budget, authentication check or
Native signature check was weakened. No new scale performance baseline was
created. The prior full-suite constant/Q1/Q5 outcomes remain historical until a
complete rerun; the broader existing static/CI gate findings above remain open.

Planner Checkstyle passed with zero findings. The coordinator Checkstyle run
reported 496 findings in the existing code; its three findings in the changed
bootstrap file are on unchanged constructor/long-line locations, outside the new
capability wiring. That broader gate remains failed and is not counted as passing
Q9 acceptance.

The final planner `verify -Pstatic-analysis` reran all 1,383 tests successfully,
then failed on the same ten SpotBugs findings in untouched classes. No findings
were reported in the changed planner classes; the verify command remains failed.

## Remaining acceptance blockers

| Stage | Required evidence still outstanding |
|---|---|
| P0 live deployment | A dedicated Linux host configuration, image distribution, routable result endpoints, working shared Iceberg catalog and reference-engine runtime. Local transport fixtures are not deployment acceptance. |
| P1 SF100 | Both full workloads through the real engine, every result matching an agreed Spark/Trino reference, three independent windows and a reviewed real baseline. Current engine-path failures above must be diagnosed before interpreting performance. |
| P2 SF1000 / comparisons | Sufficient dedicated CPU, RAM, spill capacity and storage; 2/4/8 physical-host experiments; isolated Trino/Spark runs; profiles, throughput, failure/cancellation evidence and reproducible costs. |
| P3 certification | Version-aligned official tools and audited workload adaptations; H RF1/RF2, ACID and complete runs; DS complete Load/Power/Throughput/Maintenance sequence and accessibility evidence; official metric formula validation, pricing, availability, full disclosure and independent audit. |

The implementation rejects missing configuration, inadequate capacity,
uncalibrated baselines and incomplete evidence. It does not create replacement
baseline numbers, assert physical resource drain from logical cancellation,
or emit QphH/QphDS from latency/throughput summaries. Cold-cache/open-arrival,
maintenance and destructive failure campaigns need their own controlled drivers.

See [the runbook and acceptance matrix](../testing/tpc-benchmark-methodology.md).

## Certification gap register by specification clause

This register tracks missing implementation and evidence against
[TPC-H 3.0.1](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)
and [TPC-DS 4.0.0](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v4.0.0.pdf).
Every entry remains open; engineering checks do not certify compliance.

| Specification clause | Current evidence and work still required |
|---|---|
| H 1.3–1.6, 4.1–4.3 | Typed engineering data exists; qualify official generation, full load, physical design and access transparency. |
| H 2.1–2.4 | Immutable full queries exist; audit parameters, permitted adaptations and official qualification answers. |
| H 2.5–2.8 | Implement RF1/RF2 and database evolution; retain refresh inputs, results and transaction receipts. |
| H 3.2–3.5 | Execute atomicity, consistency, isolation and durability protocols, including required failure evidence. |
| H 5.1–5.4 | Implement official Power/Throughput ordering, refresh streams, timing and independently checked metric formulas. |
| H 7, 8, 9 | Supply priced/available configuration, full disclosure/supporting files and independent audit. |
| DS 2–3 | Qualify official schema, generation, scale and complete loaded database; SF100 remains engineering-only. |
| DS 4, 7.3, 7.5 | Audit generated parameters, adaptations and official query/result validation; two-engine agreement alone is insufficient. |
| DS 5, 7.4.8–7.4.9 | Implement refresh data, maintenance functions, scheduling, validation and timing. |
| DS 6 | Demonstrate full data access during/after required durable-medium failure in the prescribed maintenance interval. |
| DS 7.4.2–7.4.7 | Implement uninterrupted Load/Power/two Throughput phases, prescribed stream permutations and timing. |
| DS 7.6 | Validate the official metric using load, power, throughput and maintenance evidence together. |
| DS 9, 10, 11 | Supply pricing/availability, full disclosure/supporting files and independent audit. |

Certification work needs a separate execution driver. The ordinary harness's
warmup rounds, fixed engineering order, repeated windows and latency gates must
not be reclassified as the official sequence. Whole-process memory ceilings,
physical resource reclamation and measured native operator telemetry also remain
live-environment acceptance work.

## Additional observability finding

`QueryProfileService.buildOperatorTree()` divides query-wide CPU, rows and memory
among synthetic stages/tasks; `buildTiming()` derives IO wait from wall minus CPU.
Those are not measured Native `OperatorStats`, and parallel CPU cannot in general
be subtracted from wall time to establish IO wait. The harness marks the public
profile as `query-audit-aggregate`, sets `operatorMetricsQualified=false` and labels
the derived fields. It retains the payload for diagnosis without using that tree
as operator-level performance evidence. Wiring actual v2 operator telemetry into
an authenticated public collector remains a P2 prerequisite for operator-level
bottleneck claims; it must follow the Java/native integration rules.
