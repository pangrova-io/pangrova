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
    --test-classpath "$cp" --warmup 3 --iterations 12 --output "$output"
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
[Recorded samples and report](../../pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-process)
