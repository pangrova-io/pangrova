# CI Gates

`.github/workflows/ci.yml` owns execution; `development.md` owns local validation.
Do not run every gate locally or reread every rule for each task.

## Selection

| Change/event | Checks |
|---|---|
| Documentation, canonical rules, website, ordinary gate tooling PR | Layout, tooling tests/syntax, environment paths, SPI annotations, E2E truthfulness, doc references |
| Java runtime/tests PR outside JNI/protocol/SPI owners | Above + Linux native, JDK 17/21 filtered tests, static analysis, same-source production full regression |
| Native, Worker/JNI, protocol/SPI, packaging, build/config or unknown input PR | Above + all four native platforms and ASan/UBSan |
| Production/dependency/benchmark inputs PR | Above + complete TPC workload/evidence/production-client transport contracts and JMH performance comparison |
| Protected-branch push or release workflow call | Full correctness chain regardless of diff |

`.pangrova/scripts/ci_change_scope.py` defines the PR classifier and native platform
inventory. Runtime and test resources take precedence over Markdown/docs naming. Deleted and renamed
sources are included; an invalid comparison base fails. Website build/deployment
remains owned by `deploy-website.yml`. Native tooling conservatively keeps full
CI unless explicitly classified; local validation still follows actual impact.

`Validation result` is the stable aggregate check. Selected jobs must succeed;
failures, cancellations, missing scope and unexpectedly skipped jobs fail it.
Documentation PRs may skip only unselected runtime/performance jobs. Repository
branch protection must reference this check instead of retired individual jobs;
workflow files cannot update hosted branch protection.

Release calls this same workflow at the tag and waits for validation before
publishing. Release artifacts have distinct names from validation artifacts.

## Check Ownership

| Check | Owner / trigger |
|---|---|
| SPI stability annotation presence | `check-spi-annotations.sh`, once in lightweight lint |
| Proto descriptor/source ownership | `proto-descriptor-diff.sh`, once for runtime CI; exact drift, not semantic compatibility proof |
| E2E pattern sanity | `check-e2e-truthfulness.sh`, once in lightweight lint; does not prove all tests are meaningful |
| Stub patterns | `detect-dead-stubs.sh`, runtime CI; heuristic, not reachability proof |
| Wiring/reachability | `SpiWiringVerificationTest`, `BootstrapReachabilityTest`, daily/full Maven suites |
| Golden query plans | `TpchPlanStabilityTest`, `TpcdsPlanStabilityTest`, daily/full Maven suites; changed goldens reviewed in normal PR review |
| Documentation class references | `check-doc-class-refs.sh`, lightweight docs job; heuristic |
| Java structure and bug patterns | One `verify -Pstatic-analysis`, Checkstyle + SpotBugs/FindSecBugs |
| Native compilation/tests | Warnings-as-errors + CTest; Linux on runtime PRs, all classifiers for native/boundary/build inputs and protected pushes/releases |
| Native memory/undefined behavior | ASan + UBSan for native/boundary/build inputs and protected pushes/releases |
| Production execution | Same-source Bake image, source/image identity, staged JNI/runtime manifest, `native-runtime,full-regression verify` |

The production reactor already runs E2E and `ActivationSmokeE2ETest`; do not run
both again without a changed input or diagnostic reason. The pre-test reactor
install remains necessary for independent-process artifact consumers. Daily JDK
17/21 tests retain their filtered selection and are not full regression. Do not
add coverage instrumentation to the production or daily runs; `-Pcoverage` stays
an optional local diagnostic, not a required check.

## Local Commands and Thresholds

```bash
mvn -q -pl <module> -am verify -Pstatic-analysis
mvn -q -pl <module> -am verify -Pcoverage   # optional local diagnosis
```

Checkstyle owns Java structure: file 800, effective method lines 100, complexity
15, parameters 10, if/for nesting 3 and try nesting 2. SpotBugs blocks unsuppressed
Medium-or-higher-confidence findings; confidence is not severity. Parent POM and
`config/` are authoritative. Native tests and applicable E2E/smoke must pass;
compiler warnings and sanitizer findings must be zero.

Optional diagnostics: `check-spi-stability.sh` checks textual provider evidence;
`jni-symbol-symmetry.sh` checks native exports. Neither replaces actual wiring,
loader or ownership tests. SPI annotation baseline updates only remove resolved
or renamed pre-existing gaps, never hide new unannotated public types.

## Retired Checks and Hooks

Do not recreate retired checks through a blanket local runner:

- Fixed historical dependency-use lists, document age, unused ADR trailers.
- Historical rename ledgers, package file quotas, duplicate regex structure scans.
- Changed-line coverage gates, standalone ArchUnit jobs and Codecov uploads. Keep
  real behavior tests and the optional `-Pcoverage` local report; do not recreate
  the removed `diff-coverage-check.sh` CI threshold.
- PR checkbox/N/A enforcement and self-authored plan-approval phrases. Review
  distributed risks and golden diffs substantively; tests remain authoritative.
- Reference-only DuckDB execution as a PR engine fuzzer. The SQL generator utility
  remains manually runnable in `nightly-operator-fuzzer.yml`; it is not differential
  Pangrova validation and has no automatic nightly schedule.
- Inactive stock Git hook samples tracked under `hooks/`. Setup validates layout
  and installs no hook. No mandatory commit-message/language/prefix hook is added.

Audit evidence and remaining limits: `docs/contributing/quality-gate-audit.md`.

## TPC scale performance ownership

PR check ID `tpch-performance` verifies complete derived workloads, real Parquet
conversion, evidence/comparison rejection paths and the persistent production
REST client over HTTP/Flight transport fixtures. It does not interpret small SQL
fixture maxima as P99 or send three-query summaries into the full tail comparator.

`nightly-tpc-benchmark.yml` owns actual exclusive-host SF100/SF1000 engineering
campaigns, three independent five-round windows and reviewed real baselines.
Missing dedicated configuration, references, capacity, calibration or cleanup is
an explicit failed outcome. The unchanged complete-22-query R7/P99 comparator
remains the separate expensive tail experiment with its 1000-sample floor. No
fixture or zero-valued baseline qualifies. See `docs/testing/tpc-benchmark-methodology.md`.

## C++ Quality Gates

| Gate | Command | Threshold |
|------|---------|-----------|
| Build (warnings-as-errors) | `cmake --build build --parallel` | 0 errors, 0 warnings |
| GTest suite | `python3 pangrova-worker-native/bin/run-native-tests.py` | All discovered tests pass |
| AddressSanitizer | `cmake --preset dev-asan && cmake --build build-asan`; Native runner with `--build-dir pangrova-worker-native/build-asan` | 0 findings |
| UBSan (on demand) | `cmake --preset dev-ubsan && cmake --build build-ubsan`; Native runner with `--build-dir pangrova-worker-native/build-ubsan` | 0 findings |
| Native lib package | `package-native-lib.sh` | Successfully staged to Java resources |
| JNI integration | `mvn -q -pl pangrova-worker -am test` | 100% pass |
| Symbol visibility | Only `Java_*` and `JNI_On*` in `.dylib` exports | `nm -gU` verification |

## Connectivity Gates

| Gate | What it checks | When |
|------|---------------|------|
| Stub patterns | Reject unapproved unimplemented bodies; bytecode wiring tests check reachability | Every PR |
| SPI completeness | Production-used contracts have wired providers; experimental SPIs may precede implementations | SPI/wiring changes |
| JNI bridge symmetry | Every Java `native` decl has C++ counterpart | Native changes |
| Proto symmetry | Proto changes regenerated on both Java + C++ sides | Proto changes |
| Full-chain E2E | SQL → Plan → JNI → Native → Result works end-to-end | Production CI/release; affected local runtime changes |
