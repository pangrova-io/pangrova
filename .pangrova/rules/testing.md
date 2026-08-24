# Testing

## Test Categories

| Category | Location | Tag | Runner |
|----------|----------|-----|--------|
| Unit | `src/test/java/` per module | (default) | JUnit 5 + jqwik |
| Integration | `src/test/java/` per module | `@Tag("integration")` | JUnit 5 + TestContainers |
| Property | `src/test/java/**/property/` | (default) | jqwik |
| E2E | `pangrova-testing/pangrova-test-e2e` | — | JUnit 5 + TestContainers |
| Smoke | `pangrova-testing/pangrova-test-smoke` | — | JUnit 5 |
| Chaos | `pangrova-testing/pangrova-test-chaos` | — | JUnit 5 + Toxiproxy |
| Benchmark | `pangrova-testing/pangrova-test-benchmark` | — | JMH |
| C++ Unit | `pangrova-worker-native/test/` | — | GoogleTest |

## Banned Frameworks

- **Mockito, PowerMock, EasyMock** — all mock frameworks banned. Use real implementations, fakes, or TestContainers.
- **Catch2, rapidcheck** — C++ side uses GoogleTest exclusively.
- **`@Disabled`** annotations — banned in tests. Fix the test or remove it.
- **`java.lang.reflect`** to access private/protected — make the method package-private instead.

## Naming & Location

- `*Test.java` — unit tests
- `*IntegrationTest.java` — integration tests (requires external resources)
- `*PropertyTest.java` — jqwik property-based tests
- PropertyTest location: `property` sub-package (e.g., `org.pangrova.worker.exchange` → `org.pangrova.worker.property.exchange`)
- C++ tests: `<Component>Test.cpp` using GoogleTest, parameterized tests (`TEST_P`, `INSTANTIATE_TEST_SUITE_P`) for data-driven

## Test Writing Principles

- **Given-When-Then**: Each test method has clear setup, action, and assertion
- **One assertion per behavior**: Test a single logical behavior per method
- **Boundary values**: Always test null, empty, max, overflow, negative
- **Equivalence classes**: Partition inputs; test representative from each partition
- **Deterministic**: No `Thread.sleep`, no random seeds without control, no external network
- **Self-contained**: No shared mutable state between tests; use `@BeforeEach` for setup

## Coverage Requirements

- **Diff coverage**: PRs must hit ≥60% line coverage on changed lines (`diff-coverage-check.sh`)
- **Property tests**: Core logic (serialization, parsing, scheduling, resource allocation) MUST have property-based tests
- **C++ coverage**: Not gated in CI, but targeted at critical paths (JNI bridge, memory management)

## Test Completeness Requirements (MANDATORY)

> **A feature without complete tests is NOT done. No exceptions.**

Every new feature, every new code path, every new public method MUST have tests. Test gaps are treated as P1 findings.

### Test Completeness Checklist

For EVERY new feature or code change, verify:

```
□ Every new public class has a *Test.java or *Test.cpp
□ Every new public method has at least one test
□ Boundary values tested: null, empty, max, overflow, negative, edge cases
□ Error/exception paths tested: every catch block, every error branch
□ Concurrency tested: if code is shared across threads
□ Resource cleanup tested: if code acquires resources (connections, buffers, handles)
□ Integration tested: if code crosses component/module boundaries
□ E2E tested: if feature is user-facing (SQL → Plan → Execute → Result)
□ Property tested: if code handles serialization, parsing, or scheduling
□ Full-chain coverage: at least one test exercises the complete path from entry to output
```

### Coverage Gap Protocol

When adding new code, BEFORE declaring done:

```
1. Run diff-coverage-check.sh — must be ≥60%
2. If below 60%:
   a. Identify uncovered lines
   b. Write tests specifically for uncovered code paths
   c. Re-run until ≥60%
3. Manual gap analysis:
   a. Are there code paths that no test exercises end-to-end?
   b. Are there error paths with no test that triggers them?
   c. Are there boundary conditions with no test that hits them?
4. Fill every gap found in step 3
5. Run full test suite to verify all new tests pass
```

### Full-Chain Test Requirement

For every user-facing feature, there MUST exist at least one test that:
- Starts from the user's entry point (SQL statement, API call, CLI command)
- Passes through ALL intermediate layers (parser, planner, optimizer, coordinator, worker)
- Exercises the actual execution path (including JNI boundary if applicable)
- Validates the final output matches expected results
- Lives in `pangrova-testing/pangrova-test-e2e` or `pangrova-testing/pangrova-test-smoke`

If no such test exists → the feature is NOT done. Write it.

## Full-Project Regression Mandate

> **EVERY modification to source code (Java or C++) MUST pass full-project regression before completion.**

Execute the **Full-Chain Build Order** defined in `build-pipeline.md` §Full-Chain Build Order — all 8 steps, in order, all must pass. This is the quality floor. There are no exceptions.

- A change that passes module tests but breaks another module is NOT done
- A Java change that passes Java tests but breaks JNI integration is NOT done
- A C++ change that passes gtests but crashes E2E is NOT done
- The system ships as ONE binary — all modules must work together
- If ANY module fails → fix it in the same session. Do NOT claim "unrelated" without `git stash` evidence.

---

## E2E Infrastructure — TestContainers

> **STRICT**: All E2E/integration tests MUST use TestContainer specifications below.

### Native Library Prerequisite (BLOCKING)

Before executing ANY E2E test, the native library MUST be built and packaged:

```bash
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
./pangrova-worker-native/bin/package-native-lib.sh
java -version && mvn -version && mvn -q -pl pangrova-worker -am test -Dtest="*NativeLoader*" -Dsurefire.failIfNoSpecifiedTests=false
```

If you see `UnsatisfiedLinkError`: STOP, build native lib, re-run ALL tests.

### Native Change → Java E2E Regression Protocol

When ANY file under `pangrova-worker-native/` is modified, execute the **Full-Chain Build Order** in `build-pipeline.md` — all 8 steps, in order.

**Why E2E is mandatory after native changes:**
- Native code handles query execution hot paths (vectorized ops, memory allocation)
- JNI boundary bugs (signature mismatch, null pointer, off-by-one in buffers) often only surface under realistic E2E workloads
- Unit tests exercise isolated functions; only E2E validates the full `SQL → Plan → JNI → Native exec → Arrow result` chain
- `UnsatisfiedLinkError` and `SIGSEGV` crash the entire JVM — cannot be caught/recovered

**Skipping E2E after native change = shipping a time bomb.**

### 1. Database — PostgreSQL TestContainer

| Property | Value |
|----------|-------|
| Image | `postgres:16-alpine` |
| Database | `pangrova_meta` (E2E) / `pangrova_metadata_test` (unit) |
| Username | `pangrova` |
| Password | `pangrova_test` |
| Pattern | JVM-level singleton (`SharedTestContainers.POSTGRES`) |

**Prohibited**: H2, SQLite, shared dev PostgreSQL, embedded-pg.

### 2. LLM / AI Inference — Ollama TestContainer

| Property | Value |
|----------|-------|
| Image | `ollama/ollama:0.6.2` |
| Model | `tinyllama` |
| Port | 11434 |

**Prohibited**: StubInferenceProvider in E2E, external API calls, mock HTTP.

### 3. Network Fault Injection — Toxiproxy TestContainer

For chaos/fault-injection tests. Required for CDC EOS tests, distributed consensus tests.

### 4. Multi-Process Cluster — PangrovaClusterContainer

| Property | Value |
|----------|-------|
| Image | `pangrova/pangrova:latest` |
| Coordinator Port | 8080 |
| Worker Port | 8081 |
| Health Check | HTTP `/v1/health` → 200 |

## Container Lifecycle Rules

- PostgreSQL MUST use JVM-level singleton (not per-class)
- Docker Network required for multi-container scenarios
- ALL container tests MUST use `ContainerTestBase` for lifecycle management.
  If Docker is unavailable, the agent MUST diagnose and fix the Docker setup
  (e.g., set DOCKER_HOST for Colima) — not skip the tests.
- No hardcoded ports — use `.getHost()` + `.getMappedPort()`
- No `Thread.sleep` for readiness — use `Awaitility` or TC wait strategies

## Prohibited Patterns

| Pattern | Why Prohibited | Correct Alternative |
|---------|---------------|---------------------|
| H2 in-memory | SQL dialect mismatch | PostgreSQL TestContainer |
| `StubInferenceProvider` in E2E | Doesn't test real pipeline | Ollama TestContainer |
| External API calls | Non-reproducible | Ollama TestContainer |
| `@Disabled` on container tests | Hides failures | Fix the test |
| New container per test class (PG) | 5-10s overhead | Singleton pattern |
| Running E2E without native lib | UnsatisfiedLinkError | Build + package native first |

## Reference Files

| File | Purpose |
|------|---------|
| `pangrova-testing/pangrova-test-e2e/.../SharedTestContainers.java` | PG singleton |
| `pangrova-testing/pangrova-test-e2e/.../SqlE2ETestBase.java` | Base class |
| `pangrova-testing/pangrova-test-common/.../ContainerTestBase.java` | Docker guard |
| `pangrova-testing/pangrova-test-common/.../PangrovaClusterContainer.java` | Cluster TC |
