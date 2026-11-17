---
title: "Script Inventory"
description: "Retention and consolidation review of repository Shell and Python files"
---

# Script Inventory

This page classifies every tracked `.sh` and `.py` file outside `website/` by its
consumer: CI or build wiring, another script, test discovery, or documentation.
Reference evidence is not runtime execution proof. See the
[repository guide](repository-guide.md) for replacement commands and merge criteria.

**Keep** means a distinct consumer or responsibility exists. **Manual** means an
intentional operator/diagnostic entry point, not an automatically enforced CI gate.
**Merge candidate** retains the current public entry point until its shared
implementation can be verified.

## Repository tooling

Run `.pangrova/scripts/generate-module-baseline.py` to regenerate source facts and
`.pangrova/scripts/check-module-manifest.sh` to check freshness and ownership.
Both require Python and JDK 17 (`JAVA_HOME`, or `java` on `PATH`). The generator
source-launches Testing Kit's `JavaSourceReferences.java`, so no prior Maven build
is needed. Main/test package and reference facts use the same AST scanner as the
Java module policy tests, including qualified code references and wildcard imports.
The unconditional CI lint job provisions Java before tooling tests and the manifest
check. Policy enforcement itself remains in the Maven architecture tests.

Run `.pangrova/scripts/generate-module-baseline.py --graph-dir target/architecture`
to export Graphviz DOT files for module dependencies and production Java package
ownership. The dependency graph labels each declared inter-artifact edge with its
POM scope. Profile-only, plugin and external dependencies require the effective Maven
model for the build being audited. The package graph includes resource JARs as
nodes without Java packages. Graph export leaves the tracked baseline untouched.

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `.pangrova/scripts/build-iceberg-sdk-candidate.sh` | Keep | Build and verify the pinned patched Iceberg SDK in Maven Local. | `.github/actions/setup-iceberg-sdk/action.yml`; `.pangrova/scripts/ci_change_scope.py` |
| `.pangrova/scripts/check-dev-artifacts.sh` | Keep | Reject tracked or staged development caches and a recreated `docs/internal/`; warn on local leftovers unless `--strict`. | `.github/workflows/ci.yml`; `.pangrova/rules/ci-gates.md` |
| `.pangrova/scripts/clean-dev-artifacts.sh` | Keep | Remove ephemeral Java/Python/test caches; `--all` also removes build outputs; never touches runtime state or user plans. | `AGENTS.md`; `.pangrova/rules/development.md` |
| `.pangrova/scripts/check-doc-class-refs.sh` | Keep | Verify class names referenced in docs/architecture/*.md actually exist in the codebase. Catches documentation drift where docs reference renamed or deleted classes. Heuristic: extracts backtick-quoted PascalCase identifiers from architectur | `.github/workflows/ci.yml`; `.pangrova/rules/ci-gates.md` |
| `.pangrova/scripts/check-e2e-truthfulness.sh` | Keep | Reject known misleading E2E patterns; heuristic rather than runtime proof. | `.github/workflows/ci.yml`; `pangrova-testing/pangrova-testing-kit/src/test/java/org/pangrova/testing/testkit/E2eTruthfulnessGateTest.java` |
| `.pangrova/scripts/check-environment-specific-info.sh` | Keep | Reject contributor-machine paths from active repository files. Standard deployment paths, generated artifacts, and version-control metadata are intentionally outside this gate. Usage: check-environment-specific-info.sh              scan the | `.github/workflows/ci.yml` |
| `.pangrova/scripts/check-module-manifest.sh` | Keep | Verify that the committed module baseline matches the reactor and every artifact has an ownership entry. | `.github/workflows/ci.yml`; `.pangrova/rules/ci-gates.md` |
| `.pangrova/scripts/check-native-boundaries.sh` | Keep | Enforce the Native source layout, JNI boundary and recorded capability seams. | `.github/workflows/ci.yml` (Native job); `.pangrova/rules/architecture.md` |
| `.pangrova/scripts/check-native-includes.py` | Keep | Resolve literal source-local C/C++ includes, reject file cycles and report component edges. | `.pangrova/scripts/check-native-boundaries.sh`; `.pangrova/rules/architecture.md` |
| `.pangrova/scripts/check-spi-annotations.sh` | Keep | Enforces that all public top-level interfaces and abstract classes under every manifest-declared -api/-spi contract artifact are annotated with one of: @Stable, @Experimental, or @Internal. This guards the contract surface: any new interface added with | `.github/workflows/ci.yml`; `.pangrova/scripts/.spi-annotation-baseline.txt` |
| `.pangrova/scripts/check-spi-stability.sh` | Manual | Enforces that @Stable-annotated SPI interfaces have at least one implementation in the main source tree (production code). An SPI marked @Stable with zero implementors is aspirational — it promises contract stability but has never been prov | `.pangrova/rules/ci-gates.md` |
| `.pangrova/scripts/ci_change_scope.py` | Keep | Select PR validation; pushes and release calls retain full correctness checks. | `.github/workflows/ci.yml`; `.pangrova/scripts/tests/test_gate_policy.py` |
| `.pangrova/scripts/detect-dead-stubs.sh` | Keep | CI Dead-Stub Detection Script Scans production Java code for stub methods that: 1. Only log "not implemented" warnings 2. Only throw UnsupportedOperationException (bare or with generic message) Distinguishes TRUE STUBS from: - Guard clauses | `.github/workflows/ci.yml`; `.pangrova/scripts/tests/test_stub_detection.py` |
| `.pangrova/scripts/generate-module-baseline.py` | Keep | Regenerate `.pangrova/module-baseline.yaml` and optional Graphviz dependency graphs. | `.pangrova/scripts/check-module-manifest.sh`; `.pangrova/rules/architecture.md` |
| `.pangrova/scripts/jni-symbol-symmetry.sh` | Keep | Verify that the packaged pangrova_native library exports only JNI entry points and that every Java native declaration has exactly one exported short JNI name. | `.github/workflows/ci.yml` (Native job); `.pangrova/rules/ci-gates.md` |
| `.pangrova/scripts/prepare-e2e-ai-image.py` | Keep | Build a real, immutable Ollama fixture before tests; tests never download models. | `.github/workflows/ci.yml`; `.pangrova/scripts/tests/test_ai_fixture.py` |
| `.pangrova/scripts/prepare-lineage-test-image.py` | Keep | Prepare the pinned, unchanged Marquez application on a native Java 17 runtime for reference-backend HTTP tests. | `docs/architecture/query-lineage.md`; `.pangrova/scripts/tests/test_lineage_fixture.py` |
| `.pangrova/scripts/proto-descriptor-diff.sh` | Keep | Proto descriptor diff gate. Generates one FileDescriptorSet from the canonical process/JNI schemas in pangrova-protocol. Maven and CMake consume this same tree; a second "native" descriptor would recreate the ownership split thi | `.github/workflows/ci.yml`; `.pangrova/scripts/tests/proto-descriptor-diff-clean-repository-test.sh` |
| `.pangrova/scripts/stage-production-regression-native.sh` | Keep | Stage the exact runtime JAR inventory and canonical JNI from one immutable production image. | `.github/workflows/ci.yml`; `.pangrova/scripts/ci_change_scope.py` |
| `.pangrova/scripts/tests/checkstyle-boundaries-test.py` | Keep | Exercise real Checkstyle with allowed and rejected structural boundary fixtures. | `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/proto-descriptor-diff-clean-repository-test.sh` | Manual | Manual cold-cache test of descriptor-tool bootstrapping; needs Maven/network. | Manual diagnostic entry point; not wired into default CI. |
| `.pangrova/scripts/tests/spotbugs-immutable-filter-test.py` | Manual | Verify narrow evidence-backed classifications with the pinned SpotBugs Filter API. | Manual diagnostic entry point; not wired into default CI. |
| `.pangrova/scripts/tests/test_ai_fixture.py` | Keep | Actual model artifact integrity and CI prerequisite ordering contracts. | `unittest discover` in CI / owning test directory. |
| `.pangrova/scripts/tests/test_clean_dev_artifacts.py` | Keep | Cleanup preserves sources and removes only build output and retired-module leftovers. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_doc_class_refs.py` | Keep | Regression tests for the documentation class-reference gate. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_gate_policy.py` | Keep | Regression tests for validation selection, preserved checks and release wiring. | `unittest discover` in CI / owning test directory. |
| `.pangrova/scripts/tests/test_lance_ffi_provenance.py` | Keep | Exercise the Lance FFI provenance binding against real archive and header inputs. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_lineage_fixture.py` | Keep | Reject missing or substituted lineage reference-backend artifacts before image creation. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_module_manifest.py` | Keep | POM discovery, shared AST source facts and fail-closed manifest gates. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_native_include_graph.py` | Keep | Include cycles and forbidden Native component edges. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_native_sanitizer_probe.py` | Keep | RE2 sanitizer probe child environment and failure propagation. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_runtime_classpath.py` | Keep | Launchers select the actual role closure without scanning unrelated JARs. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_spi_contract_gates.py` | Keep | Domain API artifacts are covered by both SPI contract gates. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/scripts/tests/test_stub_detection.py` | Keep | Preserve stub findings while avoiding ignored dependency/build tree scans. | `unittest discover` in CI / owning test directory. |
| `.pangrova/scripts/tests/test_velox_atomic_link.py` | Keep | Apply the SDK portability patch and execute its platform selection. | `unittest discover` in `.github/workflows/ci.yml` |
| `.pangrova/setup.sh` | Keep | Verify the canonical repository instructions without creating tool adapters. | `.github/workflows/ci.yml`; `.pangrova/scripts/ci_change_scope.py` |

## Process and operator commands

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `bin/pangrova-catalog.sh` | Keep | Inspect local catalog templates and call catalog management APIs. | `bin/pangrova.sh`; `docs/getting-started/catalogs/catalog-management.md` |
| `bin/pangrova-classpath.sh` | Keep | Shared, sourced launcher classpath resolution; never evaluates build output. | `bin/pangrova-coordinator.sh`; `bin/pangrova-standalone.sh`; `.pangrova/scripts/tests/test_runtime_classpath.py` |
| `bin/pangrova-cli.sh` | Keep | Start the Java SQL client with client-specific arguments and classpath. | `README.md`; `bin/pangrova.sh` |
| `bin/pangrova-cluster-status.sh` | Keep | Report Coordinator cluster status for operators. | `README.md`; `bin/pangrova.sh` |
| `bin/pangrova-container-health.sh` | Keep | Container liveness probe with role and port selection. | `deploy/docker/Dockerfile` |
| `bin/pangrova-coordinator.sh` | Merge candidate | Launch Coordinator with role-specific main class, JVM defaults and PID ownership. | `README.md`; `bin/pangrova.sh` |
| `bin/pangrova-health-check.sh` | Keep | Interactive HTTP/HTTPS health probe, also used by rolling upgrades. | `README.md`; `bin/pangrova-rolling-upgrade.sh` |
| `bin/pangrova-rolling-upgrade.sh` | Keep | Reference SSH-based rolling upgrade orchestration. | `bin/pangrova.sh`; `docs/configuration/operations.md` |
| `bin/pangrova-shuffle.sh` | Keep | Start the durable shuffle service. | `deploy/helm/pangrova/templates/shuffle-statefulset.yaml`; `docs/configuration/reference.md` |
| `bin/pangrova-standalone.sh` | Merge candidate | Launch the combined Coordinator/Worker JVM with standalone configuration. | `README.md`; `bin/pangrova.sh` |
| `bin/pangrova-stop.sh` | Keep | Stop the explicitly selected role using its owned PID file. | `README.md`; `bin/pangrova-rolling-upgrade.sh` |
| `bin/pangrova-worker.sh` | Merge candidate | Launch Worker with role-specific main class, direct-memory defaults and PID ownership. | `README.md`; `bin/pangrova.sh` |
| `bin/pangrova.sh` | Keep | Public command dispatcher for existing role, client and operator scripts. | Dockerfile, Helm templates, README/operator documentation. |

## Deployment and operational support

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `deploy/docker/build-static-curl.sh` | Keep | Build the pinned static curl dependency for the native Docker image. | `deploy/docker/Dockerfile.native-build` |
| `deploy/docker/build-static-libunwind.sh` | Keep | Build the pinned libunwind archives as PIC for the production JNI library. Ubuntu's static libunwind is built with non-PIC LTO and cannot be linked into an AArch64 shared object (R_AARCH64_ADR_PREL_PG_HI21 on __stack_chk_guard). | `deploy/docker/Dockerfile.native-build` |
| `deploy/docker/stage-role-classpaths.py` | Keep | Stage each packaged role's resolved runtime classpath in launch order. | `deploy/docker/Dockerfile`; `.pangrova/scripts/tests/test_runtime_classpath.py` |
| `deploy/docker/verify-native-source.sh` | Keep | Compare source, not a caller-provided tag: Java and JNI must share one checkout. | `deploy/docker/Dockerfile` |
| `deploy/helm/pangrova/tests/test-namespace-isolation.sh` | Manual | Pangrova Helm Chart — Namespace Isolation Verification Test This script deploys the Pangrova Helm chart with multi-tenancy enabled and verifies that: 1. Per-tenant namespaces are created 2. Resource quotas are applied per namespace 3. Netwo | `docs/deployment/helm-kubernetes.md` |
| `docs/operations/game-day/tools/measure-recovery-time.sh` | Manual | measure-recovery-time.sh — Recovery time measurement tool for Pangrova game days. Measures the time between fault injection and service recovery by continuously probing an endpoint and recording success/failure timestamps. Usage: ./measure- | `docs/operations/game-day/index.md`; `docs/operations/game-day/scenarios/gd-001-coordinator-failover.md` |
| `etc/pangrova-env.sh` | Keep | Pangrova — Environment Variable Configuration (Production) Sourced by bin/ startup scripts before launching Java processes. Uncomment and set variables for your environment. DO NOT store plaintext secrets in this file if it is committed to  | `bin/pangrova-cli.sh`; `bin/pangrova-cluster-status.sh` |

## Function-contract generators and tests

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `pangrova-protocol/tools/function-contracts/binding_declarations.py` | Keep | One SQL/structural declaration; actual registration remains a prerequisite. | `pangrova-protocol/tools/function-contracts/generate_structural_contracts.py`; `pangrova-protocol/tools/function-contracts/test_binding_declarations.py` |
| `pangrova-protocol/tools/function-contracts/binding_manifest.py` | Keep | Typed SQL contract projection; registration availability is never generated. | `pangrova-protocol/tools/function-contracts/generate_structural_contracts.py`; `pangrova-protocol/tools/function-contracts/test_binding_manifest.py` |
| `pangrova-protocol/tools/function-contracts/contract_generation_inputs.py` | Keep | Content identity of the actual local generator import closure. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/generate_execution_selection.py` |
| `pangrova-protocol/tools/function-contracts/decimal_formula_probe.py` | Keep | Actual-resolver differential harness for registered integer parameter formulas. | `pangrova-protocol/tools/function-contracts/test_decimal_formula_probe.py` |
| `pangrova-protocol/tools/function-contracts/execution_selection.py` | Keep | One bounded control contract for the actual execution registry entries. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/pom.xml` |
| `pangrova-protocol/tools/function-contracts/generate_execution_selection.py` | Keep | Render both execution-selection evaluators from one control declaration. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/pom.xml` |
| `pangrova-protocol/tools/function-contracts/generate_special_type_programs.py` | Keep | Emit the same structural constraints for Native and canonical Java types. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/generate_execution_selection.py` |
| `pangrova-protocol/tools/function-contracts/generate_structural_contracts.py` | Keep | Generate Native compatibility identities from the canonical declaration. | `pangrova-protocol/pom.xml`; `pangrova-protocol/tools/function-contracts/test_binding_declarations.py` |
| `pangrova-protocol/tools/function-contracts/oracle_responses.py` | Keep | Bounded line transport for the actual Native resolver differential tool. | `pangrova-protocol/tools/function-contracts/decimal_formula_probe.py`; `pangrova-protocol/tools/function-contracts/test_oracle_responses.py` |
| `pangrova-protocol/tools/function-contracts/physical_type_mapping.py` | Keep | One canonical bridge declaration drives factories and binding projections. | `pangrova-protocol/tools/function-contracts/binding_declarations.py`; `pangrova-protocol/tools/function-contracts/generate_structural_contracts.py` |
| `pangrova-protocol/tools/function-contracts/physical_type_manifest.py` | Keep | Typed manifest projection generated from the actual factory bridge program. | `pangrova-protocol/tools/function-contracts/physical_type_mapping.py`; `pangrova-protocol/tools/function-contracts/test_physical_type_mapping.py` |
| `pangrova-protocol/tools/function-contracts/registered_signature.py` | Keep | Normalize reflected registration ASTs; no function names or decimal formulas here. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/binding_declarations.py` |
| `pangrova-protocol/tools/function-contracts/registration_catalog.py` | Keep | Deterministic build intermediate for the actual registered signature export. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/binding_declarations.py` |
| `pangrova-protocol/tools/function-contracts/scalar_implementations.py` | Keep | Validate actual resolver snapshots; physical tags remain a build intermediate. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/registration_catalog.py` |
| `pangrova-protocol/tools/function-contracts/special_type_programs.py` | Keep | Bounded structural type programs, not a SQL-name or runtime registration list. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/execution_selection.py` |
| `pangrova-protocol/tools/function-contracts/test_binding_declarations.py` | Keep | Regression tests for binding declarations. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_binding_manifest.py` | Keep | Actual declaration projection does not invent registration availability. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_contract_generation_inputs.py` | Keep | Actual generator closure identity is distinct from declaration semantics. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_decimal_formula_probe.py` | Keep | Regression tests for decimal formula probe. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_execution_selection.py` | Keep | Control-contract tests do not claim actual Native execution coverage. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_oracle_responses.py` | Keep | Regression tests for oracle responses. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_physical_type_mapping.py` | Keep | Bridge declaration/generator checks do not replace actual Native differential tests. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_registered_signature.py` | Keep | Regression tests for registered signature. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_registration_catalog.py` | Keep | Regression tests for registration catalog. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_scalar_implementations.py` | Keep | Regression tests for scalar implementations. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_special_type_programs.py` | Keep | Structural contract generation checks; Native Tasks supply value oracles. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_type_formula.py` | Keep | Parser/evaluator checks; the registered Native binder is the separate oracle. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/test_type_template.py` | Keep | Regression tests for type template. | Manual `unittest discover -s pangrova-protocol/tools/function-contracts`; no default CI discovery found. |
| `pangrova-protocol/tools/function-contracts/type_formula.py` | Keep | Strict build-time parser for registered integer type constraints, never SQL. | `deploy/docker/Dockerfile.native-build`; `pangrova-protocol/tools/function-contracts/decimal_formula_probe.py` |
| `pangrova-protocol/tools/function-contracts/type_template.py` | Keep | Descriptor-checked canonical Type skeletons and typed substitution paths. | `pangrova-protocol/tools/function-contracts/test_type_template.py` |

## Benchmark tooling and tests

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `pangrova-testing/pangrova-testing-benchmark/scripts/calibrate_tpc_tail.py` | Keep | Explicitly calibrate genuine complete CLI tail evidence and its controlled context. | `pangrova-testing/pangrova-testing-benchmark/baseline/README.md`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_tail_calibration.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/csv-to-parquet.py` | Keep | Convert complete generator output into a validated, bounded-memory dataset. | `pangrova-testing/pangrova-testing-benchmark/scripts/generate_tpc_data.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_data.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/generate_tpc_data.py` | Keep | Generate TPC-derived engineering data with pinned tools and explicit failure ownership. | `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_data.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/integration/test_persistent_transport.py` | Keep | Production client against local HTTP + real Flight transport fixtures; no engine calibration. | `.github/workflows/ci.yml` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/jmh-regression-check.sh` | Keep | Compare complete matching JMH runs. Missing calibration is an error, never a pass. Returns separate pass, regression and invalid-input statuses. | `.github/workflows/ci.yml`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_jmh_regression.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/shuffle_read_comparison.py` | Manual | Measure baseline and current Java partition readers against the same authenticated remote publication. | `docs/testing/shuffle-read-comparison.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/jmh-regression-compare.py` | Keep | Compare complete matching JMH point estimates, without claiming significance. | `pangrova-testing/pangrova-testing-benchmark/scripts/jmh-regression-check.sh` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/prepare_tpc_workload.py` | Keep | Generate explicit query steps from checksum-pinned community TPC templates. | `pangrova-testing/pangrova-testing-benchmark/queries/tpch/README.md`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-benchmark.sh` | Keep | Run complete SQL files through the batch CLI and retain measured latency evidence. See --help for options. Few-sample runs do not emit P99 and cannot pass regression gates. | `pangrova-testing/pangrova-testing-benchmark/queries/tpch/README.md`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_runner.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-suite.sh` | Keep | Preserve the documented complete-suite CLI entry point. | `pangrova-testing/pangrova-testing-benchmark/queries/tpcds/README.md`; `pangrova-testing/pangrova-testing-benchmark/queries/tpch/README.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_benchmark.py` | Keep | Run TPC query workloads via the batch CLI; no claim of certified TPC scores. | `pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-benchmark.sh`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` | Keep | Dedicated-host job: verified datasets, owned cluster, complete windows and comparison. | `.github/workflows/nightly-tpc-benchmark.yml` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_suite.py` | Keep | Execute complete, explicitly stepped TPC-derived suites through persistent clients. | `pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-suite.sh`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/sample_sql_role_resources.py` | Manual | Sample macOS role processes launched by `sql_process_comparison.py`. | `docs/testing/speculative-process-e2e.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/shuffle_retention_comparison.py` | Manual | Compare record-credit exhaustion across two shuffle buffer store implementations. | `docs/testing/speculative-process-e2e.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/sql_process_comparison.py` | Manual | Run one process-isolated SQL benchmark fork against an installed checkout. | `docs/testing/speculative-process-e2e.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_jmh_regression.py` | Keep | Exercise the real shell/Python gate. Numbers are arithmetic oracles, not benchmarks. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_cluster.py` | Keep | Lifecycle ownership tests using a small Docker command collaborator; no engine claims. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_data.py` | Keep | Real Parquet conversion tests; generated fixtures are never performance evidence. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_report.py` | Keep | Report only repeated qualified experiments; retain failures and refuse biased ratios. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_runner.py` | Keep | Statistics/receipt oracles and real CLI rejection tests; not SQL performance evidence. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_suite.py` | Keep | Independent runner and evidence failure tests; fixtures are not engine measurements. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_tail_calibration.py` | Keep | Tail calibration uses complete raw evidence; test numbers never enter baselines. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpch_regression.py` | Keep | Deterministic unit and shell-entry-point tests; fixtures are not calibration data. | `unittest discover` in CI / owning test directory. |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_catalog.py` | Keep | Prepare a shared Iceberg snapshot through Spark, or verify an existing binding. | `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_suite.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_clients.py` | Keep | Persistent engine adapters. Each stream owns one client and consumes all results. | `pangrova-testing/pangrova-testing-benchmark/scripts/integration/test_persistent_transport.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_cluster.py` | Keep | Own benchmark containers on explicit Docker contexts; never infer a multi-host cluster. | `.github/workflows/nightly-tpc-benchmark.yml`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_common.py` | Keep | Strict, shared evidence I/O. Historical results are never implicitly upgraded. | `.github/workflows/nightly-tpc-benchmark.yml`; `pangrova-testing/pangrova-testing-benchmark/scripts/calibrate_tpc_tail.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_contract.py` | Keep | Fail-closed workload, dataset, catalog and controlled-environment contracts. | `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_suite.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_reference.py` | Keep | Publish a reference only after independent Spark and Trino results agree completely. | `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_suite.py`; `docs/testing/tpc-benchmark-methodology.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_report.py` | Keep | Summarize real windows, cross-engine ratios and fixed-scale expansion efficiency. | `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_report.py`; `docs/testing/tpc-benchmark-methodology.md` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_results.py` | Keep | Typed, complete result comparison, including duplicates and legitimate empty results. | `pangrova-testing/pangrova-testing-benchmark/scripts/prepare_tpc_workload.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_suite.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_statistics.py` | Keep | Shared TPC workload latency statistics. R7 is explicit; qualification is policy. | `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_benchmark.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpc_runner.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpc_suite_compare.py` | Keep | Compare three independent full-suite windows; preserve strict tail gates separately. | `pangrova-testing/pangrova-testing-benchmark/baseline/README.md`; `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpch-regression-check.sh` | Keep | TPC-H Performance Regression Check Compares all 22 TPC-H query execution times against the version-controlled baseline. Fails if any query regresses more than 15% from baseline P99. Requires matching explicit scale factors, successful queri | `pangrova-testing/pangrova-testing-benchmark/scripts/run_tpc_job.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpch_regression.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpch-regression-compare.py` | Keep | TPC-H performance regression comparison and report generation. | `pangrova-testing/pangrova-testing-benchmark/scripts/tests/test_tpch_regression.py` |
| `pangrova-testing/pangrova-testing-benchmark/scripts/tpch_regression_validation.py` | Keep | Validate full TPC-H runs before comparison; never infer scale or calibration. | `pangrova-testing/pangrova-testing-benchmark/scripts/calibrate_tpc_tail.py`; `pangrova-testing/pangrova-testing-benchmark/scripts/tpch-regression-compare.py` |

## Other test tools

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `pangrova-testing/pangrova-testing-integration-tests/src/test/resources/tpch/generate_expected.py` | Keep | Generate TPC-H expected results at SF=0.01 using DuckDB as reference engine. | `pangrova-testing/pangrova-testing-integration-tests/src/test/resources/tpch/README.md` |
| `pangrova-testing/pangrova-testing-fuzzer/scripts/run-operator-fuzzer.sh` | Manual | Manual local build/run convenience for the DuckDB reference generator; not Pangrova differential validation. | Manual diagnostic entry point; not wired into default CI. |

## Native build tooling and tests

| File | Decision | Purpose | Consumer or discovery evidence |
|---|---|---|---|
| `pangrova-worker-native/bin/apply-velox-patches.sh` | Keep | Apply/verify the sole pinned SDK ownership, selection and Arrow execution patch. | `deploy/docker/Dockerfile.native-build`; `pangrova-worker-native/bin/record-function-provenance.sh` |
| `pangrova-worker-native/bin/build-darwin-asan-folly.py` | Manual | Build the Darwin ASan Folly dependency with pinned integer-source corrections. | `docs/development/darwin-function-sanitizer.md` |
| `pangrova-worker-native/bin/build-darwin-asan-runtime.sh` | Manual | Builds a private compiler-rt 17.0.6 runtime. Never installs over the compiler. | `docs/development/darwin-arm64-asan-runtime.md` |
| `pangrova-worker-native/bin/build-darwin-llvm-function-prefix.py` | Manual | Build a private LLVM 17 SDK with the upstream Mach-O sanitizer prefix fix. | `docs/development/darwin-function-sanitizer.md` |
| `pangrova-worker-native/bin/build-lance-ffi.sh` | Keep | build-lance-ffi.sh — Build Lance C FFI static library for target platforms Produces liblance_c_ffi.a in lance-ffi target release directory The CMake build (FindLanceCFFI.cmake) auto-detects this location. Usage: ./bin/build-lance-ffi.sh     | `.github/workflows/ci.yml`; `.github/workflows/release.yml` |
| `pangrova-worker-native/bin/build-native.sh` | Keep | build-native.sh — One-command native build pipeline Idempotent: safe to run repeatedly. Skips already-built deps. Usage: ./pangrova-worker-native/bin/build-native.sh              # full pipeline ./pangrova-worker-native/bin/build-native.sh  | `README.md`; `pangrova-worker-native/bin/package-native-lib.sh` |
| `pangrova-worker-native/bin/install-aws-sdk.sh` | Keep | Install AWS C++ SDK for Velox S3 connector support. Version must match Velox's scripts/setup-versions.sh: AWS_SDK_VERSION="1.11.654" Components: s3, identity-management (matches Velox's install_aws_deps) Usage: ./install-aws-sdk.sh [install | `pangrova-worker-native/cmake/FindVelox.cmake` |
| `pangrova-worker-native/bin/lance-ffi-provenance.py` | Keep | Bind the Lance Rust archive and header to the exact protocol, Rust, Cargo and owned SDK patch inputs; reject stale artifacts at configure and build time. | `pangrova-worker-native/bin/build-lance-ffi.sh`; `pangrova-worker-native/cmake/FindLanceCFFI.cmake` |
| `pangrova-worker-native/bin/isolate-arrow-macos-exports.py` | Keep | Keep Arrow's existing public exports and localize static dependency symbols. | `pangrova-worker-native/bin/setup-native-asan-env.sh`; `pangrova-worker-native/bin/setup-native-build-env.sh` |
| `pangrova-worker-native/bin/package-native-lib.sh` | Keep | package-native-lib.sh — Copy built native library into classpath resource dir Usage: ./pangrova-worker-native/bin/package-native-lib.sh [--production] [path-to-dylib-or-so] If no argument is given, auto-detects from pangrova-worker-native/b | `.pangrova/scripts/stage-production-regression-native.sh`; `deploy/docker/Dockerfile` |
| `pangrova-worker-native/bin/prepare-aws-sdk-source.sh` | Keep | Materialize the exact aws-sdk-cpp submodule tree pinned by Velox. GitHub's large-repository clone endpoint is unreliable in the production Docker build; codeload serves the same immutable commits without Git pack negotiation. | `deploy/docker/Dockerfile.native-build` |
| `pangrova-worker-native/bin/prepare-lance-sdk.sh` | Keep | Prepare the exact repository-owned SDK writer drain extension, never mutate Cargo's registry. All unmodified crate files are also provenance-checked. | `deploy/docker/Dockerfile.native-build`; `pangrova-worker-native/bin/build-lance-ffi.sh` |
| `pangrova-worker-native/bin/record-function-provenance.sh` | Keep | Read the generation identity from the actual SDK archive before recording it. | `deploy/docker/Dockerfile.native-build`; `pangrova-worker-native/bin/setup-native-asan-env.sh` |
| `pangrova-worker-native/bin/run-native-tests.py` | Keep | Run CTest with a fresh, process-owned real PostgreSQL/Lance authority. | `.github/workflows/ci.yml`; `pangrova-worker-native/test/RunNativeTestsTest.py` |
| `pangrova-worker-native/bin/setup-native-asan-env.sh` | Keep | Build the ASan-specific dependencies required by FindVelox.cmake. The regular native bootstrap intentionally produces Release artifacts.  ASan must not reuse its Velox, Protobuf, Abseil, RE2, gRPC or Flight archives: mixing instrumented inl | `.github/workflows/ci.yml`; `docs/contributing/developer-guide.md` |
| `pangrova-worker-native/bin/setup-native-build-env.sh` | Keep | setup-native-build-env.sh — Hermetic native build environment for Pangrova Builds ALL C++ dependencies from source into a single prefix tree. No Homebrew/MacPorts/system protobuf/gRPC contamination. Dependency chain (order matters): 1. abse | `.github/workflows/ci.yml`; `.github/workflows/release.yml` |
| `pangrova-worker-native/bin/test_isolate_arrow_macos_exports.py` | Manual | Tests for the Mach-O translation of Arrow's public symbol policy. | `pangrova-worker-native/test/` |
| `pangrova-worker-native/patch_flight_types.py` | Keep | Patch the specific Arrow Flight header ordering issue; used by native builds and tested. | `.github/workflows/ci.yml`; `deploy/docker/Dockerfile.native-build` |
| `pangrova-worker-native/test/CanonicalAbseilPackageTest.py` | Keep | Verify real Abseil package selection and SDK archive-to-target ownership. | `pangrova-worker-native/CMakeLists.txt` |
| `pangrova-worker-native/test/CanonicalFlightPackageTest.py` | Keep | Exercise mandatory Flight package admission without a simulated execution path. | `pangrova-worker-native/CMakeLists.txt` |
| `pangrova-worker-native/test/CanonicalFunctionProvenanceTest.py` | Keep | Verify the actual SDK archive cohort through the production CMake admission gate. | `pangrova-worker-native/CMakeLists.txt` |
| `pangrova-worker-native/test/CanonicalRe2PackageTest.py` | Keep | Exercise the real canonical RE2 gate with CMake-generated SDK caches. | `pangrova-worker-native/CMakeLists.txt` |
| `pangrova-worker-native/test/PatchFlightTypesTest.py` | Keep | Regression tests for PatchFlightTypesTest. | `.github/workflows/ci.yml`; `pangrova-worker-native/CMakeLists.txt` |
| `pangrova-worker-native/test/RunNativeTestsTest.py` | Keep | Regression tests for RunNativeTestsTest. | `pangrova-worker-native/CMakeLists.txt` |


