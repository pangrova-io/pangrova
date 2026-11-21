# CI Gates

`.github/workflows/ci.yml` owns execution and `.pangrova/scripts/ci_change_scope.py`
owns PR classification and the Native platform matrix. Do not run every gate locally.

## Selection

| Change or event | Checks |
|---|---|
| Docs, rules, website or ordinary gate tooling PR | Lightweight lint: layout, tooling tests and syntax, environment paths, module manifest, SPI annotations, E2E truthfulness, doc class references, dev artifacts |
| Java runtime/test PR outside JNI, protocol and SPI owners | Above + Linux Native, JDK 17/21 daily tests, static analysis, same-source production full regression |
| Native, Worker/JNI, protocol/SPI, packaging, build/config or unknown input PR | Above + all four Native platforms and ASan/UBSan |
| Production, dependency or benchmark input PR | Above + TPC workload/evidence/transport contracts and JMH comparison |
| Protected-branch push or release | Full correctness chain regardless of diff |

Runtime and test resources take precedence over Markdown naming; deleted and renamed
paths count; an invalid comparison base fails. Website build and deployment belong to
`deploy-website.yml`.

`Validation result` is the single required aggregate check: selected jobs must
succeed, and failures, cancellations, missing scope or unexpected skips fail it. Branch
protection references this check. Releases call the same workflow at the tag and use
artifact names distinct from validation artifacts.

## Ownership

| Check | Owner | Runs |
|---|---|---|
| Module ownership and dependencies | `check-module-manifest.sh`, `ModuleDependencyPropertyTest`, `ModuleBoundaryReferenceTest` | Lint (JDK 17); Maven tests |
| Package cycles | `PackageDependencyAcyclicityPropertyTest`, `PackageDependencyGraphTest` | Maven tests |
| SPI annotations | `check-spi-annotations.sh`; baseline entries only shrink | Lint |
| E2E pattern sanity | `check-e2e-truthfulness.sh` (heuristic) | Lint |
| Documentation class references | `check-doc-class-refs.sh` (heuristic) | Lint |
| Dev artifact hygiene | `check-dev-artifacts.sh`; rejects caches and a recreated `docs/internal/` | Lint |
| Contributor paths | `check-environment-specific-info.sh` | Lint |
| Instruction layout | `.pangrova/setup.sh --check` | Lint |
| Proto descriptors | `proto-descriptor-diff.sh` (exact drift, not compatibility proof) | Runtime CI |
| Stub patterns | `detect-dead-stubs.sh` (heuristic) | Runtime CI |
| Wiring and reachability | `SpiWiringVerificationTest`, `BootstrapReachabilityTest` | Maven tests |
| Golden plans | `TpchPlanStabilityTest`, `TpcdsPlanStabilityTest`; review changed goldens | Maven tests |
| Java structure and bugs | One `verify -Pstatic-analysis` (Checkstyle, SpotBugs/FindSecBugs) | Static analysis job |
| Native build and tests | Warnings as errors, CTest, `check-native-boundaries.sh`, `jni-symbol-symmetry.sh` | Native jobs |
| Native memory and UB | ASan, UBSan | Native/boundary/build PRs, pushes, releases |
| Production execution | Same-source image identity, staged JNI manifest, `native-runtime,full-regression verify`, isolated owner suites | Runtime CI |

Thresholds: Checkstyle limits are in `code-standards.md`; errors fail and size
warnings do not. SpotBugs blocks unsuppressed Medium-or-higher-confidence findings.
Native compiler warnings and sanitizer findings must be zero. `check-spi-stability.sh`
is an optional local diagnostic and does not replace wiring or loader tests.

The production reactor already runs E2E and activation smoke; the pre-test install
stays for independent-process consumers. Daily JDK 17/21 runs exclude the `e2e`
group, which needs the AI model fixture and same-source image, and are not full
regression. No coverage instrumentation runs in CI.

## Retired Checks

Do not recreate removed gates, including fixed dependency-use lists, document-age
alarms, ADR trailer and rename-ledger checks, package file quotas, regex structure
scans, changed-line coverage thresholds (`diff-coverage-check.sh`), Codecov uploads,
standalone ArchUnit jobs, PR checkbox enforcement, reference-only DuckDB fuzzing as a
PR gate, and tracked Git hook samples. Setup installs no hooks and no commit-message
gate.

## Performance

The `tpch-performance` PR check verifies complete derived workloads, real Parquet
conversion, evidence/comparison rejection paths and the production REST client over
HTTP and Flight fixtures; fixture maxima are never P99. `nightly-tpc-benchmark.yml`
owns exclusive-host SF100/SF1000 campaigns with reviewed baselines; missing
configuration, capacity or cleanup is a failed outcome, and its unattended schedule
is skipped until `TPC_BENCHMARK_CONFIG_DIR` provisions the dedicated fleet. See
`docs/testing/tpc-benchmark-methodology.md`.
