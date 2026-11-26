---
title: "TPC Benchmark Methodology"
description: "TPC-derived workload generation, execution, and acceptance evidence"
---

# TPC-derived benchmark methodology and runbook

This harness measures engineering workloads. All executable templates and SQL
adaptations are explicit; current official TPC certification is a separate goal.
The workload directory contains 22 TPC-H templates and 99 TPC-DS templates
(103 executable steps) with source and SQL hashes. Unordered results compare as a
multiset digest and ordered results by sequence digest; environment, data files,
snapshots, harness, client runtime, resources, cache, and configuration are compared
before evidence is accepted. Historical zero-valued baselines are preserved but do
not qualify. Live deployment, SF100, SF1000/comparisons, and certification evidence
remain outstanding; see the acceptance matrix in this document.

## Prerequisites and controlled inputs

Use Python 3.12, JDK 17, Maven 3.9+, Docker Compose v2 and dedicated Linux hardware.
Automatic toolkit builds need GCC, make, flex and bison. Pin a compatible Iceberg
runtime for the reference Spark deployment. No runtime, dataset or credential is
silently replaced when a prerequisite is missing.
The legacy source kits compile with explicit `-std=gnu89 -fcommon` compatibility
flags. `build.json` records the command, GCC version and platform; generated-data
provenance includes that record when built by this bootstrap.

From `pangrova-testing/pangrova-testing-benchmark`:

```bash
python3.12 -m venv /absolute/path/tpc-venv
source /absolute/path/tpc-venv/bin/activate
python -m pip install -r requirements-clients.txt
python -m unittest discover -s scripts/tests -v
```

The base requirements cover conversion/workload preparation. Optional pinned
reference clients are Trino Python 0.336.0 and PySpark 4.0.1. Pangrova uses a
persistent **JDK 17 subprocess**, the same-source CLI JAR, production REST client
and production result decoder; it does not embed a JVM in Python.

Keep actual deployment files, credentials and machine-specific state outside the
checkout. Example files under `examples/` contain explicit placeholders, not
runnable hardware claims. Tokens are passed only as environment-variable names.
Use standard credential providers for storage, not secret values in SparkConf or
result manifests.

### Real SQL/Native regression prerequisite

The Java E2E runtime registers real completion and embedding models, including
when running Q9. Prepare their immutable fixture before starting Maven; test
startup only verifies local model inventory and never calls the model registry.
This is separate from SF100/SF1000 data preparation and performance acceptance.

From the repository root:

```bash
docker pull ollama/ollama:0.6.2
python3.12 .pangrova/scripts/prepare-e2e-ai-image.py --receipt /tmp/pangrova-ai-fixture.json
ai_fixture_id=$(python3.12 -c 'import json; print(json.load(open("/tmp/pangrova-ai-fixture.json"))["imageId"])')
mvn -q -pl pangrova-testing/pangrova-testing-integration-tests -am -Pnative-runtime,full-regression \
  '-Dtest=SqlLatencyBaselineTest$TpcHQ9,CboJoinEligibilityE2ETest,ActivationSmokeE2ETest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Dpangrova.e2e.ai.image-id="$ai_fixture_id" test
```

Use a new receipt path for each artifact build. The preparation script records
the exact local Ollama base image, architecture, both manifest hashes and the
resulting immutable image ID. It verifies all referenced blob hashes and sizes,
then configures both models with two inference threads in an offline container.
The image receipt binds the resulting manifests, which retain the original
model layers. This keeps real inference within the test budgets on shared
runners with varying CPU topology. Rebuild any older fixture without the
thread-count receipt.
An existing real Ollama `models` directory can be supplied with `--models-dir`
for an offline artifact build, with the same integrity checks. `--dns` selects a
resolver only for the preparation container; it does not change host or test DNS.
Retain/export the built image with the receipt for reuse on the same architecture.
Missing models or mismatched digests fail without downloading a replacement.
The runtime setup budget remains three minutes. The fixture integration test
checks real completion and embedding inference with the model registry blocked,
zero `/api/pull` requests, failed identity checks and owned-container cleanup.

## Generate and validate data

```bash
python scripts/generate_tpc_data.py --benchmark tpch --scale-factor 100 \
  --output-dir /data/tpc --parallel 8
python scripts/generate_tpc_data.py --benchmark tpcds --scale-factor 100 \
  --output-dir /data/tpc --parallel 8
```

The archive source, commit, checksum and tool version are locked. Generation
checks every child, retains concrete generator failures and publishes a dataset
only after all conversion tasks succeed. Existing destinations are rejected;
inspect an incomplete directory rather than mixing it into a new generation.
The dedicated-host job reserves at least three times logical SF bytes before
generating raw files and Parquet. This does not prove remote-store capacity.

The pinned dsdgen omits later children for small tables. Scales below SF100 use
an unchunked generation; SF100+ chunks the four fact parents while creating
dimensions once. Sales parents create their returns tables. No partial first-child
file is accepted as a complete parallel fact dataset.

A manually supplied prebuilt source directory requires `--tool-source` and
`--generator-provenance`, with version and actual binary SHA256. A qualified scale
run additionally requires the pinned archive/commit, generation parameters,
distribution hash and expected cardinality checks. Arbitrary structural fixtures
cannot become SF100 by changing a metadata field.

Each `parquet/dataset.json` records logical SF separately from physical bytes,
all table schemas/counts, source files, Parquet checksums, compression and layout.
`_SUCCESS` binds the exact manifest digest. Readers verify both before execution.
CSV reading is streaming with an 8MiB input block, explicit trailing field, NULL
and DECIMAL/DATE types; files rotate at one million rows. Memory use is bounded
by block size and process count, not total SF. These raw files are imported once
into the shared Iceberg snapshot, whose final physical layout is common to all
engines.

## Prepare workloads, shared snapshots and references

The checked-in `queries/{tpch,tpcds}/workload.json` targets SF100. For SF1/SF1000,
bootstrap the locked source with `generate_tpc_data.py`, then prepare a new suite:

```bash
python scripts/prepare_tpc_workload.py --benchmark tpch --scale-factor 1000 \
  --tool-source /data/tpc/.build/tpch-32f1c1b92d1664dba542e927d23d86ffa57aa253 \
  --output-dir /data/workloads/tpch-sf1000
```

Use the corresponding pinned TPC-DS source for DS. The manifest fixes template
hashes, qualification parameters/seed, adaptations, projection metadata and each
step's UTF-8 SQL hash. TPC-H has 22 steps; TPC-DS has 103 across 99 templates
(Q14/Q23/Q24/Q39 each have two). Generation parses statement boundaries once.
Execution sends each step intact and never splits strings on semicolons. The
legacy CLI runner rejects multi-step templates before launching a query.

Prepare a **fresh dedicated namespace** through Spark, using a shared catalog
and identical warehouse that all engines can read:

```bash
python scripts/tpc_catalog.py prepare \
  --dataset /data/tpc/tpch/sf100/parquet/dataset.json \
  --binding /private/tpc-config/tpch-sf100-binding.json \
  --spark-config /private/tpc-config/catalog-verifier.json
```

The same raw paths must be available read-only at identical paths to the Spark
import executors. Preparation never overwrites existing tables. It verifies
loaded row counts and records table UUIDs, current main snapshot IDs and metadata
file identities. A failed partial import requires a fresh namespace or deliberate
operator cleanup, not silent reuse.

Before and after each window, a separate Spark observer process verifies the
shared binding using metadata. Each SUT client also queries its own `$refs` (or
Spark `.refs`) and must observe the identical main snapshot. Metadata verification
is outside measurement, and the observer cannot accidentally supply a local
SparkContext to a distributed Spark experiment.

Create controlled environment manifests for Spark and Trino, based on the same
hardware/resource inventory and shared binding. Set `engine.name` to `spark` or
`trino`, actual version/artifact checksum, and the corresponding `client`:

- Trino: `host`, `port`, `tls`, `user`, `catalog`, `schema`, optional `tokenEnv`,
  frozen `sessionProperties`, bounded `requestTimeout`.
- Spark: `catalog`, `schema`, complete pinned `sparkConf`; distributed master,
  executor counts/memory, AQE, statistics and retries must match the recorded
  deployment. Concurrent streams require explicit FAIR scheduling.
- Pangrova: exact CLI `classpath`/SHA256, host/port/TLS, catalog/schema,
  `requestTimeout`, token variable name and result-channel TLS policy. The cluster
  launcher extracts the CLI JAR from the verified engine image.

Capture one full result set per reference engine, against the same SF workload:

```bash
python scripts/run_tpc_suite.py --purpose reference --iterations 1 --streams 1 \
  --workload queries/tpch/workload.json \
  --dataset /data/tpc/tpch/sf100/parquet/dataset.json \
  --binding /private/tpc-config/tpch-sf100-binding.json \
  --environment /private/tpc-config/spark-environment.json \
  --catalog-verifier /private/tpc-config/catalog-verifier.json \
  --output /data/results/tpch-spark-reference-window.json
```

Repeat with the Trino environment and another output. Publish an agreed reference:

```bash
python scripts/tpc_reference.py --workload queries/tpch/workload.json \
  --spark /data/results/tpch-spark-reference-window.json \
  --trino /data/results/tpch-trino-reference-window.json \
  --output /private/tpc-config/tpch-sf100-reference.json
```

Disagreement fails without selecting one engine. Comparison retains NULLs,
duplicates, full strings, complete rows and logical type families. DECIMAL uses
exact decimal arithmetic; floating point uses absolute `1e-8` and relative `1e-6`
tolerance. Native type descriptors remain in evidence. Explicit projection names
are checked; unnamed expressions receive stable positional names while native
names remain recorded, because engines choose different expression labels.
Ordered results compare in order; unordered results use multiset/perfect matching.
Unspecified ORDER BY tie ordering is currently compared conservatively: a tie
mismatch requires reviewed query/reference handling, not a relaxed blanket sort.
A missing reference is an error; an independently agreed empty result is valid.

## Execute and compare independent windows

Ordinary windows use one full-suite warmup per stream, then five full-suite
rounds. Streams execute serial steps and templates, with deterministic template
rotation across streams; this is a derived engineering order, not an official
permutation or an open-arrival load generator.

```bash
python scripts/run_tpc_suite.py --workload queries/tpch/workload.json \
  --dataset /data/tpc/tpch/sf100/parquet/dataset.json \
  --binding /private/tpc-config/tpch-sf100-binding.json \
  --environment /private/tpc-config/environment.json \
  --catalog-verifier /private/tpc-config/catalog-verifier.json \
  --reference /private/tpc-config/tpch-sf100-reference.json \
  --streams 1 --warmup 1 --iterations 5 --output /data/results/window-1.json
```

Use distinct output files/processes for windows 2 and 3. `--preflight-only` writes
an explicit nonmeasurement record that cannot pass calibration. Receipt-digest JSONL journals and an in-progress manifest retain completed
executions if a job is interrupted. A failed warmup
blocks measurements; a failed query remains recorded and prevents acceptance.
Cancellation records its logical outcome and query ID; resource drain remains
`unverified` unless independently established. No failed request is turned into
a successful latency sample.

Pangrova timing starts before sending SQL to the persistent bridge and ends after
the complete typed response reaches the harness. Internal Java adapter time is
recorded separately. Trino/Spark timing similarly includes submission and complete
client materialization. JVM/process construction is outside measurement. Server
planning/queue/execution/operator information is separate: Pangrova JSON profiles,
Trino statistics and Spark executed-plan SQL metrics. Enable
`settings.requireQueryProfile` when profiles are an acceptance requirement;
missing profiles then fail. Metrics absent from an engine are unavailable, not
fabricated zeros.

Driver wall time also includes reference checks, profiling and inter-query
orchestration. Report it as an engineering makespan, alongside per-query client
latencies; do not substitute it into official metric formulae.

Create a baseline only from three successful, complete, observed windows:

```bash
python scripts/tpc_suite_compare.py --calibrate \
  --current /data/results/window-1.json /data/results/window-2.json /data/results/window-3.json \
  --output /private/tpc-config/tpch-sf100-streams1-baseline.json
```

Compare three new windows with that baseline using `--baseline` and `--current`.
SQL/data/snapshots, harness, client runtime, resource/hardware/cache/configuration
must match. The engine build/version/source revision are retained as the tested
treatment; a new engine build is allowed. Client semantic settings, including
SparkConf and Trino session properties, are part of the identity.

A metric regresses when all three current-window values exceed the median of
baseline windows by more than 15%. Threshold equality is allowed. Mixed exceedance
or a baseline spread exceeding the threshold requires review. These are
repeatability checks, not significance tests or narrow tail-confidence claims.

Exit codes: `0` within threshold/calibration, `1` regression, `2` invalid/failed
input, `3` review required, `4` uncalibrated, `5` incompatible experiments. No
nonzero state is a passing CI performance result. Historical SF10 aggregates and
zero SF100 values remain unqualified.

The existing `run-tpc-benchmark.sh` / `tpch-regression-check.sh` remain the distinct
CLI-process end-to-end **tail** experiment. Their R7/P99/full-22-query/at-least-1000
samples contract is unchanged. That sample floor is repository policy, not a TPC
requirement or statistical confidence guarantee. Use the dedicated-host job for a same-source launcher and controlled
TLS/authentication; direct legacy invocations alone check statistical evidence. Multi-step DS suites use the new suite runner.

## Dedicated-host lifecycle and CI

Copy and fill `examples/deployment.json`, `environment-template.json` and
`catalog-verifier.json` outside the checkout. The Compose example uses dynamic
host HTTP ports, one coordinator and four distinct workers on **one Docker host**.
Supply rendered coordinator/worker configuration directories, matching JVM/native
budgets, a shared catalog and client-routable advertised Flight endpoints.

The launcher creates a unique Compose project, checks source image labels,
immutable image identity, actual process IDs, health and enforced CPU/RAM limits.
It rejects actual Docker-host overcommit before launch, derives AUTO HTTP ports,
and extracts the matching CLI JAR. Cleanup removes only owned containers/networks;
it retains data volumes and records incomplete cleanup. A readiness failure still
records created containers for cleanup. Repeated cleanup of a stopped run is safe.

For a TB multi-host experiment, declare separate preconfigured Docker contexts,
images, Compose files and routable addresses. Hosts must have distinct observed
Docker hostnames and workers must have distinct processes/endpoints. Preserve
virtualization and physical-host inventory in the hardware record; multiple
containers are not a physical scale-out result. Remote image distribution and
infrastructure provisioning remain explicit operator prerequisites.

`.github/workflows/nightly-tpc-benchmark.yml` holds an exclusive hardware lease,
builds the canonical same-source image, runs both suites sequentially and retains
90-day artifacts on success and failure. Configure repository variable
`TPC_BENCHMARK_CONFIG_DIR` to the private configuration directory; the scheduled
run is skipped while that variable is unset so an unprovisioned fleet never
occupies the lease indefinitely. Missing
configuration, references, capacity or calibration fails explicitly. The job does
not generate reference truth or replace baselines automatically. For external
Trino/Spark comparisons, stop competing engines and run their manifests sequentially
under the same hardware lease; rotate the engine order between campaigns.

PR check ID `tpch-performance` now verifies full workload, real Parquet, evidence
and persistent HTTP/Flight transport contracts. It does not claim a shared-runner
SQL timing result. Actual scale regressions belong to the exclusive-host workflow.
Existing Java latency smoke budgets and strict tail validation remain active.

## Acceptance matrix and reporting

| Campaign | Scale | Topology / concurrency |
|---|---|---|
| Correctness | Pinned-generator SF1; synthetic fixtures separate | One worker; all templates/steps; agreed references |
| Regular | SF100 | Four worker processes; one stream; 1 warmup + 5 measured rounds × 3 windows |
| Concurrency | SF100 | Four workers; 1/4/8 streams; H additionally 5 |
| TB | SF1000 | 2/4/8 independent worker hosts; 1/4/8 streams; H additionally 7 |
| Strong scaling | Fixed SF1000 | 1/2/4/8 hosts; retain inability to complete at a smaller size |
| Cross-engine | SF100/SF1000 | Pangrova/Trino/Spark sequentially; same snapshots, CPU/RAM and physical layout |

Generate JSON and Markdown reports with `tpc_report.py --windows ... --output ...
--markdown ...`. It retains failed/incomplete windows, recomputes medians from
raw executions and emits cross-engine ratios only for compatible resource,
workload, cache and reference identities. Expansion reports identify their actual
reference worker count and CPU efficiency. Optional `costPerHour` is a declared
infrastructure rate; costs are not official TPC pricing. Cold-cache, open-arrival,
refresh/maintenance and fault campaigns are separate work, not accepted ordinary
warm-cache baselines.

### Tail calibration context and profile interpretation

The dedicated-host tail job first runs a complete typed correctness pass, extracts
the CLI JAR from the same-source running image, uses that exact JAR for all CLI
samples, then checks SUT snapshots again. `tail-context.json` retains the
controlled identity and observed client runtime. Calibrate genuine tail samples
explicitly with `calibrate_tpc_tail.py --results .../tail.json --context
.../tail-context.json --output .../tpch-sf100-tail-baseline.json`. The context is
embedded atomically with the original strict statistical record. The outer job
checks both context and the unchanged tail validator before accepting comparison.

Container IDs/PIDs/AUTO ports are retained as process evidence but normalized out
of cross-run comparability; worker host multiplicities, resource budgets and
hardware stay fixed. Operator-profile output combines distinct evidence classes. `QueryProfileService`
publishes the registered execution DAG, stage transitions, downstream preparation,
dispatch work, selected physical attempts and the runtime-filter wait recorded by
`QueryStageObservations`. For retained Native task reports, `attempts[].operators`
contains actual task counters such as input/output rows and bytes, CPU/wall time,
peak memory, spill, blocked time and producer/consumer/split/connector waits. These
fields are measured telemetry, not a stage/task tree synthesized from query totals.
The execution graph is process-local and currently registered for live execution;
missing telemetry is unavailable rather than a fabricated zero.

Audit totals remain query-owner records and must not be presented as per-attempt
operator measurements. Result-phase timestamps (first row, stream open, EOF,
client close, authority revoke, queue clear and last-buffer release), Exchange
blocked reasons and exact physical-release attribution are still incomplete. Native
object-store export also remains requested-byte accounting: independent GET
attempts, retries, returned bytes and wire bytes are not yet available. The harness
must label these fields unavailable and must not derive operator bottlenecks, IO
wait or physical-read volume from query totals.
