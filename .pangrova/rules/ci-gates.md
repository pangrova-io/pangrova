# CI Gates

Gate scripts live in `.pangrova/scripts/`; CI wiring is defined in `.github/workflows/ci.yml`.

## Gate Scripts

| Gate | Script | Purpose | Frequency |
|------|--------|---------|-----------|
| SPI annotation | `check-spi-annotations.sh` | All public SPI types must have `@Stable`/`@Experimental`/`@Internal` | Every PR |
| SPI stability | `check-spi-stability.sh` | `@Stable` types must have ≥1 production impl | Local |
| Proto diff | `proto-descriptor-diff.sh` | Wire-format changes flagged; descriptors in `.pangrova/scripts/proto-descriptors` | Proto changes |
| Diff coverage | `diff-coverage-check.sh` | PRs must hit ≥60% line coverage on changed lines | Every PR |
| BOM usage | `check-bom-usage.sh` | Deps must use parent pom version properties | pom.xml changes |
| Doc class refs | `check-doc-class-refs.sh` | Class references in docs must resolve | Doc changes |
| Doc staleness | `check-doc-staleness.sh` | Docs must not be older than associated source | Every PR (warning) |
| Dead stubs | `detect-dead-stubs.sh` | No unused forwarding stubs | Every PR |
| ADR consistency | `adr-grep-check.sh` | Architecture Decision Records consistency | ADR changes |
| Plan stability | `plan-stability-check.sh` | Golden query plan diffs flagged | Planner changes |
| Distributed checklist | `distributed-checklist-check.sh` | Distributed correctness PR checklist enforced | Every PR |

## Running Locally

```bash
# Run a specific gate
.pangrova/scripts/check-spi-annotations.sh

# Run all shell gates locally
for script in .pangrova/scripts/*.sh; do
  echo "=== Running $script ==="
  bash "$script" || echo "FAILED: $script"
done
```

## Static Analysis

| Tool | Config Location | Profile |
|------|----------------|---------|
| Checkstyle | `config/checkstyle/` | `-Pstatic-analysis` |
| SpotBugs | `config/spotbugs/` | `-Pstatic-analysis` |
| JaCoCo (coverage) | Parent pom | `-Pcoverage` |
| PIT (mutation) | Parent pom | `-Pmutation` |

```bash
# Run static analysis locally
java -version && mvn -version
mvn -q -pl <module> -am verify -Pstatic-analysis

# Generate coverage report
mvn -q -pl <module> -am verify -Pcoverage
```

## SPI Annotation Baseline

Pre-existing unannotated types are tracked in `.pangrova/scripts/.spi-annotation-baseline.txt`. Update the baseline only when you annotate or rename a previously-unannotated type.

## Quality Thresholds

| Metric | Threshold | Gate |
|--------|-----------|------|
| Diff line coverage | ≥60% | `diff-coverage-check.sh` |
| SPI annotation | 100% new types | `check-spi-annotations.sh` |
| Proto wire compat | No breaking changes | `proto-descriptor-diff.sh` |
| Checkstyle | 0 violations | `-Pstatic-analysis` |
| SpotBugs | 0 high/critical | `-Pstatic-analysis` |
| C++ build warnings | 0 (PANGROVA_WERROR=ON) | `cmake --build` |
| C++ gtests | 100% pass | `ctest` |
| ASan findings | 0 leaks, 0 errors | `cmake --preset dev-asan + ctest` |
| Dead stubs | 0 orphans | `detect-dead-stubs.sh` |
| E2E tests | 100% pass | `pangrova-test-e2e` |
| Smoke tests | 100% pass | `pangrova-test-smoke` |

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
| Dead stubs | No unused forwarding stubs or orphan implementations | Every PR |
| SPI completeness | Every SPI interface has ≥1 non-stub implementation | Every PR |
| JNI bridge symmetry | Every Java `native` decl has C++ counterpart | Native changes |
| Proto symmetry | Proto changes regenerated on both Java + C++ sides | Proto changes |
| Full-chain E2E | SQL → Plan → JNI → Native → Result works end-to-end | Every PR |
