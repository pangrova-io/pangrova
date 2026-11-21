---
title: "Speculative Execution E2E"
description: "Independent-process validation of speculative execution"
---

# Independent speculative execution validation

`IndependentSpeculativeExecutionE2ETest` runs the ten speculative execution cases
through `IndependentLocalProcesses`: one production Coordinator JVM, two production
Worker JVMs with distinct PIDs and endpoints, and separate public CLI JVMs. PostgreSQL
and LDAP use the existing container fixtures. A Pangrova application image is not a
prerequisite for this local validation path.

Provide `pangrova.e2e.local.lib-dir`, `pangrova.e2e.local.jar-manifest` and
`pangrova.e2e.local.native-lib` as for `IndependentLocalProcessesE2ETest`. The runtime
inventory contains `lib/` with ordinary dependencies and the platform Native bundle,
plus `cli/` with exactly one `pangrova-client-cli-*-exec.jar`. `runtime.sha256` is
stored beside those directories. It lists the CLI executable, flat dependency JARs,
then each Coordinator and Worker list followed by the corresponding JARs in name order.
Each child JVM loads the order in its role's verified `classpath.list`. The manifest
must match the frozen runtime files. All embedded Native copies must match the qualified
Release library. Freeze the installed production dependencies before running so concurrent
builds cannot change child JVM inputs. A local run qualifies this frozen runtime on
the host platform.

On macOS, first build the current Release JNI, install its classifier, then install the
matching reactor with `-Pnative-runtime` as described in the build pipeline. From
the checkout root, stage those packaged roles and their attached CLI executable:

```bash
runtime_dir=$(mktemp -d)
release_library="$PWD/pangrova-worker-native/build/libpangrova_native.dylib"
python3 deploy/docker/stage-role-classpaths.py . "$runtime_dir/lib"
mkdir "$runtime_dir/cli"
cp pangrova-client/pangrova-client-cli/target/pangrova-client-cli-*-exec.jar "$runtime_dir/cli/"
(
  cd "$runtime_dir" || exit 1
  export LC_ALL=C
  shasum -a 256 cli/*.jar lib/*.jar \
    lib/coordinator/classpath.list lib/coordinator/*.jar \
    lib/worker/classpath.list lib/worker/*.jar > runtime.sha256
)
```

Linux production CI stages the libraries, Native classifier, manifest and source-revision
receipt from the same immutable deployment image using
`.pangrova/scripts/stage-production-regression-native.sh`.

The executable CLI is never a service or test classpath dependency. Its main Maven
artifact remains thin; the attached `exec` artifact contains dependencies for its
separate JVM. This follows Maven Shade's
[attached-artifact contract](https://maven.apache.org/plugins/maven-shade-plugin/examples/attached-artifact.html).
The POM retains normal transitive dependencies for thin-artifact consumers instead
of publishing a dependency-reduced POM.

The report relay retains the original Worker registration and translates only its
HTTP control port to a transparent local forwarding proxy. Capabilities, worker
identity, task reports, ACKs, Native result endpoints and TLS identities remain real
production values. Flight result bytes travel directly between the real processes.
The fault cases delay actual terminal reports, temporarily hide actual source files,
or disconnect a real task submission before its Worker receives it. They still
require canonical execution-drain receipts and durable admission-owner release.

Run one method to qualify startup, then select only the remaining methods for the
incremental pass. The full class remains the final matrix selector:

```bash
mvn -pl pangrova-testing/pangrova-testing-integration-tests test \
  -Dtest=IndependentSpeculativeExecutionE2ETest \
  -Dpangrova.e2e.local.lib-dir="$runtime_dir/lib" \
  -Dpangrova.e2e.local.jar-manifest="$runtime_dir/runtime.sha256" \
  -Dpangrova.e2e.local.native-lib="$release_library"
```

A missing artifact, rejected runtime hash, child startup failure, or failed scenario
is a test failure; the fixture does not skip or replace it with an in-process worker.

## Independent relational write recovery

`IndependentRelationalWriteRecoveryE2ETest` covers Iceberg, Paimon and Lance.
It kills the original Worker process after staging while the relay holds its
terminal report. A surviving Worker publishes one catalog version with the
expected row count. A separate scenario interrupts journal confirmation and
restarts the Coordinator. The test checks the original query ID, fingerprint and
exact visible rows. The failure follows catalog publication in Iceberg and Paimon.
Lance updates its catalog pointer and journal in a single PostgreSQL transaction,
so the same fault leaves the pointer unchanged until recovery.

## Independent Iceberg clustering recovery

`IndependentClusteringE2ETest` runs both Iceberg clustering modes through the
real CLI and verifies the original rows through the registered Worker. For
the linear rewrite, a PostgreSQL trigger rejects journal completion after
the Iceberg replacement snapshot is already visible. After restarting the
Coordinator and Worker, the test verifies that `SHOW CLUSTERING STATUS`
recovers the same task and snapshot and that the catalog still contains only
one replacement snapshot for that run.

## Independent CDC and Cross-Encoder acceptance

`IndependentCdcProcessesE2ETest` also exercises Iceberg mutations using one
real Kafka partition and a version 2 JDBC catalog. The Worker stages an
UPDATE, PostgreSQL rejects write-journal completion after the Iceberg
snapshot becomes visible, and MAINTENANCE confirms the same intent and
source offset. MAINTENANCE then publishes a DELETE and records COMMITTED
while PostgreSQL blocks acknowledging the new offset. A second restart
advances the offset from the original fingerprint. The query role verifies
that unrelated data remains visible and that each publication produced one
snapshot.

`IndependentCdcProcessesE2ETest` uses the same frozen runtime and process
fixture. The COMBINED case uses public stream DDL, Kafka insert, update and
delete events, and Flight reads of Paimon. The MAINTENANCE case takes over
after the Worker exits and observes the real Paimon snapshot through its SDK.
A PostgreSQL trigger rejects the journal update after the Paimon commit.
After Coordinator restart the same fingerprint reaches COMMITTED, the
snapshot ID stays unchanged, and the exact Kafka offset advances. A further case
forces the Worker process to exit after its real staging report reaches the
relay. The Coordinator receives no successful report for that attempt while the
surviving Worker stages the range and publishes one snapshot.

`IndependentCrossEncoderE2ETest` starts a pinned Text Embeddings Inference image
and model revision. It forwards real model scores through an adapter for the
`/v1/rerank` response contract and stops the model to verify the provider error through both the production CLI
and the public query status endpoint.
The model container needs outbound access to download its pinned weights.

## Measuring public SQL latency across two builds

Use the same macOS arm64 machine, JDK 17, UTF-8 locale, Docker, Iceberg SDK
candidate and configuration for both editions. Install each checkout's Java and
Native artifacts and stage its CLI, exact runtime JARs, SHA-256 manifest and
Release dylib. Build the integration test classes from the corresponding checkout.
Use separate local Maven repositories for the two editions because their SNAPSHOT
coordinates can be identical.

The following shell example takes a checkout, its Maven repository, a staged
runtime, its qualifying Release dylib, and a fresh result CSV. Set
`BENCHMARK_SCRIPTS` to the absolute path to this checkout's
`pangrova-testing/pangrova-testing-benchmark/scripts` directory. For each edition,
set the four variables ending in `_ROOT`, `_REPO`, `_RUNTIME` and `_NATIVE` to
the corresponding absolute paths.

```bash
export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
sql_results=$(mktemp -d)
sql_fork() {
  local project="$1" repo="$2" runtime="$3" native="$4" output="$5"
  local cp="$output.classpath"
  mvn -B -Pnative-runtime -Dmaven.repo.local="$repo" \
    -f "$project/pangrova-testing/pangrova-testing-integration-tests/pom.xml" \
    dependency:build-classpath -DincludeScope=test -Dmdep.outputFile="$cp" &&
  python3 "$BENCHMARK_SCRIPTS/sql_process_comparison.py" \
    --project "$project" --runtime "$runtime" --native "$native" \
    --test-classpath "$cp" --warmup "${SQL_WARMUP:-3}" --iterations "${SQL_ITERATIONS:-12}" --output "$output"
}
set -e
sql_fork "$BASELINE_ROOT" "$BASELINE_REPO" "$BASELINE_RUNTIME" "$BASELINE_NATIVE" "$sql_results/baseline-1.csv"
sql_fork "$CANDIDATE_ROOT" "$CANDIDATE_REPO" "$CANDIDATE_RUNTIME" "$CANDIDATE_NATIVE" "$sql_results/candidate-1.csv"
sql_fork "$CANDIDATE_ROOT" "$CANDIDATE_REPO" "$CANDIDATE_RUNTIME" "$CANDIDATE_NATIVE" "$sql_results/candidate-2.csv"
sql_fork "$BASELINE_ROOT" "$BASELINE_REPO" "$BASELINE_RUNTIME" "$BASELINE_NATIVE" "$sql_results/baseline-2.csv"
```

Each fork creates its own Coordinator, two Worker JVMs, PostgreSQL, LDAP and
Iceberg table. It writes three files, with values 4, 5 and 5 per file. Each
iteration runs a constant and an Iceberg sum in alternating order. A timed
sample includes a fresh CLI JVM, gRPC TLS results and final HTTP status. The
driver checks one row containing 42 and the process harness checks the query
ID, successful consumption, FINISHED status and the agreed protocol. Report
per-workload medians and nearest-rank P95 across the 24 accepted samples per
edition. The 3 warmups per workload per fork are excluded.

The driver also records resource evidence after each timed sample. It waits for
the actually observed task IDs to have a Native owner-released receipt and for
PostgreSQL to report zero tenant admission slots. The delay is measured from the
client check to that observation. The driver records the largest per-operator
Velox memory reservation, aggregate consumer wait in whole milliseconds, and
spill bytes from these receipts. It records the HTTP JSON entity lengths for
the real Worker status reports and the corresponding responses received by the
relay. This covers control traffic at the application layer. A slow or failing
release fails the fork rather than silently omitting it.

For per-role observations on macOS, redirect each fork's combined output to
`<fork>.log` next to its `<fork>.csv`. Start the following sampler before the
forks and create the completion marker after the last fork exits.

```bash
python3 "$BENCHMARK_SCRIPTS/sample_sql_role_resources.py" "$sql_results" &
sampler=$!
# Run the four sql_fork commands above, redirecting to each fork's log.
touch "$sql_results/sampling-complete"
wait "$sampler"
```

For a separate JNI measurement, build the benchmark-only JVMTI agent. Create
a fresh output directory per fork. Setting `JAVA_TOOL_OPTIONS` on the driver
causes its local role JVMs to inherit the agent; each Worker emits a CSV on
graceful VM death. Verify both recorded PIDs against the process harness log.
Each successful returned byte array corresponds to one explicit complete C++
to Java array copy in the three instrumented methods. Totals are for the Worker
lifetime, including initialization, SQL setup, retries and shutdown. Other JNI
calls and transfers, along with JVM-internal GC copies, lie outside this probe.

```bash
clang++ -std=c++17 -O2 -dynamiclib -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/darwin" \
  "$BENCHMARK_SCRIPTS/SqlJniCopyProbe.cpp" -o "$sql_results/libsqljni.dylib"
mkdir "$sql_results/jni-baseline"
SQL_WARMUP=0 SQL_ITERATIONS=2 \
  JAVA_TOOL_OPTIONS="-agentpath:$sql_results/libsqljni.dylib=$sql_results/jni-baseline" \
  sql_fork "$BASELINE_ROOT" "$BASELINE_REPO" "$BASELINE_RUNTIME" "$BASELINE_NATIVE" \
  "$sql_results/jni-baseline/query.csv" >"$sql_results/jni-baseline/run.log" 2>&1
```

Repeat with the candidate runtime and its own output directory. The two
instrumented forks used zero warmups and two timed iterations per SQL workload.
Use one warmup and three timed iterations per workload for the four resource
forks. The longer latency-only experiment below used the original sampling
schedule.

The independent Native retention measurement uses a one-record limit and an
actual spill file. Its historical checkout has the unscoped store overload and
the candidate has the per-handle overload. Both use the baseline production
compile database and its installed SDK libraries. A successful run produces
two CSV files with twelve producer attempts per implementation.

```bash
python3 "$BENCHMARK_SCRIPTS/shuffle_retention_comparison.py" \
  --baseline-native "$BASELINE_ROOT/pangrova-worker-native" \
  --candidate-native "$CANDIDATE_ROOT/pangrova-worker-native" \
  --baseline-build "$BASELINE_ROOT/pangrova-worker-native/build" \
  --output "$sql_results/retention"
```

On 2026-11-14, the four forks recorded the following median / P95 in milliseconds.

| Fork in execution order | Constant | Iceberg sum |
|---|---:|---:|
| Baseline 1 | 3497.21 / 4839.95 | 4583.20 / 7064.39 |
| Candidate 1 | 3232.72 / 4172.97 | 4504.06 / 5248.94 |
| Candidate 2 | 3874.18 / 6179.09 | 5697.49 / 8395.63 |
| Baseline 2 | 5050.56 / 8289.38 | 5628.66 / 15311.37 |

Both codes ran on macOS 13.7.8 arm64 with 8 logical CPUs and 16 GiB RAM. Other
builds and database tests shared the host. The recorded RSS is a one-second
sample of direct child processes, in KiB. Reported maxima begin after the first
accepted measured query. They cover resident process memory including the JVM,
Native code and shared mappings. The dated evidence includes 96 raw query IDs
and latencies, per-role RSS samples, runtime/source identities and CSV digests.
Recorded samples and report
