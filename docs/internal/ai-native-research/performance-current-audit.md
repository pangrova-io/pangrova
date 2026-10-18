# Performance measurement audit

Audited on 2026-10-07. This document distinguishes verified tooling changes from
uncollected performance evidence. It does not certify production performance or
claim industry leadership.

## Official mechanisms and corrections

| Official evidence | Mechanism | Pangrova correction |
|---|---|---|
| [JMH 1.37 JSON writer](https://github.com/openjdk/jmh/blob/1.37/jmh-core/src/main/java/org/openjdk/jmh/results/format/JSONResultFormat.java) | Emits benchmark, mode, parameters, harness settings, primary score, unit, uncertainty and raw data. | Compare the full benchmark/params/mode set; reject duplicates, missing observations and incompatible units/settings. |
| [JMH 1.37 modes](https://github.com/openjdk/jmh/blob/1.37/jmh-core/src/main/java/org/openjdk/jmh/annotations/Mode.java) | Throughput and time modes have different measurement meanings. | Throughput regressions decrease; average, sample and single-shot time regressions increase. Never infer a missing mode. |
| [JMH 1.37 forking sample](https://github.com/openjdk/jmh/blob/1.37/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_12_Forking.java) | JVM profiling can contaminate benchmark results; process isolation matters. | Require positive forks and matching harness/JVM settings. A short CI point estimate is not a significance test. |
| [NIST percentile methods](https://itl.nist.gov/div898/handbook/prc/section2/prc262.htm) | Percentile interpolation is not universally standardized; R6, R7 and R8 are distinct methods. | Explicit R7: zero-based rank `p*(n-1)`, linear interpolation. Maximum and lower median are separate statistics. |
| [Python monotonic performance counter](https://docs.python.org/3/library/time.html#time.perf_counter_ns) | Nanosecond counter supports elapsed-time measurement without wall-clock adjustments. | Replace GNU date wall-clock arithmetic with the monotonic counter; preserve fractional milliseconds. |
| [Python subprocess lifecycle](https://docs.python.org/3/library/subprocess.html) | A process return code does not prove application-level work completed. | Require the CLI's successful execution receipt for the exact SQL, after result consumption and FINISHED. |
| [TPC-H definition](https://www.tpc.org/tpch/) | Official QphH@Size combines database scale, single-stream processing and concurrent throughput. | Label these adapted query runs as latency regression inputs, not certified TPC-H scores. |
| [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram/blob/master/README.md), [wrk2](https://github.com/giltene/wrk2/blob/master/README.md) | Tail distributions and coordinated omission must be considered when designing latency load tests. | Keep serial CLI measurements explicitly labeled; they do not establish open-loop concurrency tails. |

The 1000-request qualification floor is a Pangrova evidence policy, **not** a
universal standard from NIST or TPC. It prevents reporting a three-request maximum
as P99. It does not establish statistical confidence, independent observations,
absence of autocorrelation or representative production load.

## Closed false-positive paths

JMH now rejects missing/empty calibration, empty result arrays, duplicate JSON
keys, duplicate benchmark/params/mode records, missing and extra workload variants,
nonpositive/nonfinite/nonnumeric scores, missing mode/unit and unsupported units.
Both runs must use the same JMH/JDK/VM versions, JVM flags, thread/fork counts,
warmup and measurement settings. Different parameter variants remain separate;
multiple supported modes remain separate. The existing default 10% gate remains a
strict comparison, with equality at the threshold permitted. Exit codes are 0
for within threshold, 1 for a measured regression and 2 for invalid evidence.
The report identifies itself as a point-estimate check, not a significance test.

TPC-H now requires successful run/query status and complete 22-query summaries.
Both calibration and current results declare `percentileMethod=R7`,
`timingScope=cli-end-to-end`, and `minimumSamples>=1000`. Each query contains raw
samples, matching sample counts, qualification, successful warmup count, exact SQL
SHA256 and separate distinct measured/warmup execution IDs. The validator
recomputes percentiles, rejects insufficient samples, repeated executions,
changed SQL, scale or statistical contract, and checks matching warmup/process/query deadlines.
The existing 15% default regression threshold remains.

The runner fails before execution when selected SQL is missing/empty or duplicate
aliases are requested. It writes failed warmup/measurement evidence and exits 1
on a failed invocation. An exit-zero command without an exact-SQL execution
receipt cannot produce an accepted sample. JSON output is encoded once and
atomically replaced; invalid serialization never overwrites a prior result.
Default short runs report sample count, R7 P50, mean, min and max, omitting P99.
Subsets remain useful for diagnosis but cannot pass the complete 22-query gate.

## Verification and evidence limits

Verified using `python3 -m unittest discover -s
pangrova-testing/pangrova-test-benchmark/scripts/tests -v`: 69 tests passed at this
audit point. The tests invoke real comparator shell entry points, compare exact
threshold arithmetic, validate R7 against independent numerical oracles, and
exercise installed OS/Python processes that fail the CLI protocol. These are
tooling tests, **not** database executions or performance calibration. Fixture
latencies, execution IDs and receipts must never enter benchmark baselines.

The batch CLI is a coordinated runtime change outside this script unit. A real
SQL-to-client success, decode-failure, cancellation and timeout chain must be
validated by its owner. The runner kills/reaps its CLI process group after a host
timeout; that is not evidence that remote query resources drained successfully.

The new `CliBatchExecutionE2ETest` retains the original real-HTTP success and SQL
failure assertions and adds the production launcher -> TPC shell/Python runner ->
independent CLI JVM -> Coordinator -> packaged JNI/Iceberg scan -> Flight decode
chain. It executes the repository's unchanged Q1 file on the shared real Iceberg
correctness tables, checks distinct FINISHED warmup/measured query IDs, exact SQL
SHA256, positive raw samples and independently recomputed two-sample summaries.
Its one warmup plus two measured executions deliberately require
`percentileQualified=false`, `minimumSamples=1000` and no P99 field. The fixture's
row-count multiplier is not a calibrated TPC scale or a performance baseline.
An invalid real Bearer credential must fail during warmup with zero samples.
This newly added runtime specification is **not yet run**: the main task owns
Native repair, ASan, current library packaging and the subsequent reactor/E2E run.

Runner authentication now accepts `--token-env NAME`; the owning CLI reads the
same inherited environment variable, so argv contains only its name. Unset or
blank credentials fail preflight. Reports expose only the authentication mode,
never the value. Nonzero CLI status remains a failure with its phase and exit
code; arbitrary client/server stderr and local output paths are not copied into
reports or runner console output. The additional Python credential-preflight
test is source-complete but has not been executed during the frozen build cohort.

The process and query deadlines now have separate owners: `config.timeout`
(`--timeout`, default 600 seconds) bounds the entire CLI subprocess, while
`config.queryTimeout` (`--query-timeout`, default 300 seconds) reaches the CLI's
query/result deadline after submission returns. Producer and validator require
both positive values and process deadline > query deadline, and compare both
against calibration. Missing query deadline metadata is rejected, with no legacy
fallback. This gives the client an opportunity to request cancellation before
the host kills it; it is not a remote cancellation acknowledgement or a physical
resource-drain receipt. The new real-process E2E uses 45/30 seconds respectively.

No source benchmark numbers were changed. Current stored evidence remains:

- `jmh-baseline.json` is an empty array and now fails as uncalibrated.
- `tpch-sf100-baseline.json` has zero measurements and fails as uncalibrated.
- The historical SF10 baseline lacks the new method/raw samples/execution
  evidence. Preserve its history; recalibrate instead of relabeling values.
- Existing nightly workflows explicitly request three measured iterations. Such
  output may describe successful SQL, but cannot pass the P99 gate. Workflow
  changes and genuine dedicated-hardware calibration must be coordinated; do not
  restore an unconditional success path to unblock CI.

## Outstanding work before performance claims

Collect matching-source JMH and full TPC-H baselines on controlled hardware,
retaining build, dependency, dataset/snapshot, SQL, machine and resource-budget
fingerprints. Freeze cache state and measure cold/hot scenarios separately.
The current comparator enforces workload and statistical identity; it does not
verify hardware equivalence or dataset snapshot identity.

Run at least three independent windows for any claimed throughput/P99 improvement,
and report uncertainty, failures and achieved load. For expensive queries, retain
the full small-sample distribution and withhold P99 claims until adequate evidence
exists. Build a separate persistent-client load experiment for concurrency and
open-loop arrivals; include queued and failed requests to avoid biased tails.
The serial runner includes JVM startup and client output decode and cannot
substitute for engine-only CPU/operator timing.

Use fixed model/service capacity for inference comparisons and record service
queue time separately. Compare query correctness and SQL semantics before
publishing any cross-engine throughput result. An industry-best claim requires
reproducible competitor comparisons and a named workload; corrected regression
tooling alone cannot support it.

## Native Flight fixture pause ownership diagnosis

On 2026-10-07 the Release full CTest run stalled in the actual SDK/Flight
reader lifecycle tests. A process sample of
`ActualSdkFailureAfterRouteRemovalWakesPendingReaderWithoutEof` showed its main
thread in `ResultBatchStreamReaderTest::TearDown` ->
`VeloxTaskRunner::releaseTaskAndWaitForDeletion` -> deletion `SemiFuture::wait`,
while the executor thread was idle. This was a fixture ownership defect:
`startPausedRunner()` explicitly requested an SDK Task pause before starting
drivers, but never released that pause after failure or cancellation.

The pinned local SDK `velox/exec/Task.cpp:3261-3273`
(`enterForTerminateLocked`) deliberately defers closing paused off-thread
drivers. Its `Task::resume` terminal branch (`Task.cpp:1231-1253`) closes those
drivers rather than resuming execution. Without the pause owner's resume,
Driver/DriverCtx references retain the Task and its deletion future cannot
complete. The SDK's `TaskPauseTest.resumeFutureAfterTaskTerminated`
(`velox/exec/tests/TaskTest.cpp:1839-1858`) explicitly demonstrates the protocol:
cancel, then owner resume. `raceBetweenTaskPauseAndTerminate` covers abort too.

The fixture now records ownership immediately after requesting pause. Teardown
first cancels, then releases only its owned pause, then performs the original
Task deletion wait. Every test body, assertion and timeout is preserved. No
production cancellation path forces resume: the SDK memory reclaimer owns its
own pause and releases it through a scope guard
(`Task.cpp:3576-3588`), protecting concurrent operator reclamation. Moving that
resume into an unrelated canceller could close operators while reclamation is
still active.

This section records local pinned SDK source and thread evidence, not a model
or performance measurement. The scoped fixture edit passed whitespace checks
and test-body preservation checks; Release rebuild/full CTest and ASan are
delegated to the main task and are not claimed passed here.

## TPC harness follow-up — 2026-10-18

The earlier sections record the October 7 state and remain historical. The
[dedicated TPC audit](../tpc-benchmark-audit.md) and
[methodology](../../testing/tpc-benchmark-methodology.md) now supersede their TPC
workflow/corpus gap inventory. The benchmark owns 22 H templates and 99 DS
templates / 103 DS steps, explicit 8/24-table streaming conversion, controlled
snapshot/reference/environment contracts and a persistent Java-client suite.

PRs validate workload/evidence/HTTP-Flight transport contracts; actual scale
comparisons run on exclusive hardware using three independent five-round windows.
The old strict CLI/R7/P99 comparator is preserved as a separate tail experiment.
The Java small-fixture smoke suite now reports observed maxima and no longer
produces incompatible P99 calibration JSON. Historical baseline numbers remain
unchanged and unqualified.

Real generator SF1 data and client transport were validated; no SF100/1000 or
cross-engine calibration is claimed. The focused Java latency suite produced six
passes, a 969ms constant-query maximum exceeding its unchanged 500ms budget, and
three native query-path errors. Those failures and missing dedicated-host/reference
configuration remain blockers for scale-performance acceptance, not passing
measurements or replaced baselines.

The later focused Q9 root-cause repair and successful real execution acceptance
are recorded in the dedicated TPC audit. They supersede the Q9 error above only;
the other latency cases and SF100/1000 acceptance are not reclassified as passed.
