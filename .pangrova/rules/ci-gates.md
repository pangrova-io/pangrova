# CI Gates

Gate scripts live in `.pangrova/scripts/`; CI wiring is defined in `.github/workflows/ci.yml`.

The JDK 17/21 daily jobs retain their documented filtered selection; they are not
full regression. The production-regression job builds the canonical same-source
Linux Bake image after Native and sanitizer gates, stages its verified classpath
JNI classifier, and runs `native-runtime,full-regression`, full E2E and
real activation smoke. Workflow
configuration and syntax checks alone are not an executed image or deployment
acceptance result; a missing build runner/image remains a blocker.

## Gate Scripts

| Gate | Script | Purpose | Frequency |
|------|--------|---------|-----------|
| SPI annotation | `check-spi-annotations.sh` | All public SPI types must have `@Stable`/`@Experimental`/`@Internal` | Every PR |
| SPI stability | `check-spi-stability.sh` | `@Stable` types must have ≥1 production impl | Local |
| Proto diff | `proto-descriptor-diff.sh` | Exact descriptor drift and canonical source ownership; not semantic compatibility analysis | Java CI |
| Doc class refs | `check-doc-class-refs.sh` | Class references in docs must resolve | Doc changes |
| Stub patterns | `detect-dead-stubs.sh` | Heuristic check for unimplemented method bodies; not reachability proof | Every PR |
| Plan stability | `plan-stability-check.sh` | Golden query plan diffs flagged | Planner changes |
| Distributed checklist | `distributed-checklist-check.sh` | Applicable distributed risks or justified N/A | Critical-module production/config/build changes |
| E2E truthfulness | `check-e2e-truthfulness.sh` | Reject skipped/constant E2E and simulated workers labeled multi-worker | Every PR |

## Running Locally

```bash
# Run a specific gate
.pangrova/scripts/check-spi-annotations.sh

# Select gates relevant to the diff using the table above.
# Do not run all scripts blindly: some require PR bodies, native binaries,
# descriptors, or an explicit review base.
```

## Validation Scope

Local checks follow `development.md`; CI retains the production-regression chain.
Checkstyle and SpotBugs run once through `static-analysis`. Structural backstops
are file length 800, method length 100, cyclomatic complexity 15, and parameters
10; `if`/`try` nesting limits remain, and `for` nesting is checked by the same parser. Review cohesion before any ceiling is reached.
SpotBugs currently blocks Medium-and-higher-confidence findings; the parent POM
is authoritative (confidence is not bug severity).

TPC-H and JMH run for PRs changing production sources/resources, dependencies,
benchmark inputs, or their workflow/tooling. Docs-only and test-only changes outside
benchmark infrastructure do not trigger these extra performance jobs. Existing
performance regression thresholds and full production correctness checks remain.

## Static Analysis

| Tool | Config Location | Profile |
|------|----------------|---------|
| Checkstyle | `config/checkstyle/` | `-Pstatic-analysis` |
| SpotBugs | `config/spotbugs/` | `-Pstatic-analysis` |
| JaCoCo (optional report) | Parent pom | `-Pcoverage` |
| PIT (mutation) | Parent pom | `-Pmutation` |

```bash
# Run static analysis locally
java -version && mvn -version
mvn -q -pl <module> -am verify -Pstatic-analysis

# Generate an optional coverage report for investigation
mvn -q -pl <module> -am verify -Pcoverage
```

## Retired Checks

Removed checks must not be recreated by a blanket local runner:

- Fixed dependency-use allowlists: declaring every historical library is not a
  correctness invariant. Parent version properties and dependency review remain.
- Document age: commit timestamps do not establish documentation correctness.
- An ADR trailer check with no ADR directory or participating documents.
- Historical rename ledgers: current names and architectural boundaries are checked,
  not whether a valid identifier appeared in an old refactor plan.
- Package file quotas and historical split lists: ownership, cohesion and dependency
  direction govern package boundaries, rather than arbitrary 8/15/25-file limits.
- Duplicate regex structure scanning: Checkstyle is the single executable owner of
  file/method size, parameter count, complexity, and control-flow nesting.
- Changed-line coverage thresholds: behavior, integration, Native, E2E, and smoke
  tests establish acceptance; JaCoCo remains available for optional investigation.

See `docs/contributing/quality-gate-audit.md` for evidence and remaining limitations.

## SPI Annotation Baseline

Pre-existing unannotated types are tracked in `.pangrova/scripts/.spi-annotation-baseline.txt`. Update the baseline only when you annotate or rename a previously-unannotated type.

## Quality Thresholds

| Metric | Threshold | Gate |
|--------|-----------|------|
| SPI annotation | 100% new types | `check-spi-annotations.sh` |
| Proto descriptor drift | Baseline reviewed and synchronized; wire/API compatibility reviewed separately | `proto-descriptor-diff.sh` |
| Checkstyle | 0 violations | `-Pstatic-analysis` |
| SpotBugs | 0 unsuppressed Medium-or-higher-confidence findings | `-Pstatic-analysis` |
| C++ build warnings | 0 (PANGROVA_WERROR=ON) | `cmake --build` |
| C++ gtests | 100% pass | `ctest` |
| ASan findings | 0 leaks, 0 errors | `cmake --preset dev-asan + ctest` |
| Stub patterns | 0 unapproved heuristic stub findings | `detect-dead-stubs.sh` |
| E2E tests | 100% pass | `pangrova-test-e2e` |
| E2E truthfulness | 0 assumptions, disabled tests, constant assertions, or simulated multi-worker claims | `check-e2e-truthfulness.sh` |
| Smoke tests | 100% pass on real SQL/Native connectivity | `ActivationSmokeE2ETest` in `pangrova-test-e2e` |

## C++ Quality Gates

| Gate | Command | Threshold |
|------|---------|-----------|
| Build (warnings-as-errors) | `cmake --build build --parallel` | 0 errors, 0 warnings |
| GTest suite | `ctest --test-dir build --output-on-failure` | 100% pass (92+ tests) |
| AddressSanitizer | `cmake --preset dev-asan && cmake --build build-asan && ctest --test-dir build-asan` | 0 findings |
| UBSan (on demand) | `cmake --preset dev-ubsan && cmake --build build-ubsan && ctest --test-dir build-ubsan` | 0 findings |
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
