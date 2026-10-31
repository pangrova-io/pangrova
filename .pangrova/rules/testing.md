# Testing

## Test Categories

| Category | Location | Tag | Runner |
|----------|----------|-----|--------|
| Unit | `src/test/java/` per module | (default) | JUnit 5 + jqwik |
| Integration | `src/test/java/` per module | `@Tag("integration")` | JUnit 5 + TestContainers |
| Routing integration | `pangrova-testing/pangrova-testing-integration-tests` | `@Tag("routing-integration")` | JUnit 5 + one real worker endpoint |
| Property | `src/test/java/**/property/` | (default) | jqwik |
| E2E | `pangrova-testing/pangrova-testing-integration-tests` (`integration.e2e`) | `@E2ETest` | JUnit 5 + TestContainers |
| Smoke | `pangrova-testing/pangrova-testing-integration-tests` (`integration.smoke`, `@SmokeTest`) | `@Tag("smoke")` | JUnit 5 + packaged JNI |
| Chaos | `pangrova-testing/pangrova-testing-integration-tests` (`integration.chaos`) | `@Tag("chaos")` | JUnit 5 + Toxiproxy |
| Benchmark | `pangrova-testing/pangrova-testing-benchmark` | — | JMH |
| C++ Unit | `pangrova-worker-native/test/` | — | GoogleTest |

## Banned Frameworks

- **Mockito, PowerMock, EasyMock** — all mock frameworks banned. Use real implementations, fakes, or TestContainers.
- **Catch2, rapidcheck** — C++ side uses GoogleTest exclusively.
- **`@Disabled`** annotations — banned in tests. Fix the test or remove it.
- **`java.lang.reflect`** to access private/protected — make the method package-private instead.

## Naming & Location

- `*Test.java` — unit tests
- `*IntegrationTest.java` — component integration tests (external resources when needed)
- `*PropertyTest.java` — jqwik property-based tests
- PropertyTest location: `property` sub-package (e.g., `org.pangrova.worker.exchange` → `org.pangrova.worker.property.exchange`)
- C++ tests: `<Component>Test.cpp` using GoogleTest, parameterized tests (`TEST_P`, `INSTANTIATE_TEST_SUITE_P`) for data-driven

## Test Writing Principles

- **Given-When-Then**: Each test method has clear setup, action, and assertion
- **One assertion per behavior**: Test a single logical behavior per method
- **Boundary values**: Test relevant null, empty, limit, overflow, and negative cases
- **Equivalence classes**: Partition inputs; test representative from each partition
- **Deterministic**: No `Thread.sleep`, no random seeds without control, no external network
- **Self-contained**: No shared mutable state between tests; use `@BeforeEach` for setup

## Coverage Guidance

- **Changed-line coverage**: Reports are diagnostic, not a PR gate; the former
  `diff-coverage-check.sh` threshold is retired. Use `-Pcoverage` locally when a
  report helps locate untested behavior.
- **Property tests**: Use for serialization, parsing, scheduling, and resource allocation when invariants are stronger than enumerated examples
- **C++ coverage**: Not gated in CI, but targeted at critical paths (JNI bridge, memory management)

## Behavior Coverage

Test observable behavior, not a quota of classes, methods, or catch blocks.

- New behavior needs normal cases, relevant boundaries, and important error paths.
- Bug fixes need a focused regression where practical.
- Pure refactors may rely on existing tests that exercise the changed path.
- Resource ownership and concurrent state transitions need cleanup/failure/race
  coverage at their actual boundary.
- Cross-component behavior needs integration coverage; user-facing SQL, protocol,
  JNI, and distributed behavior needs relevant E2E coverage. CLI-only formatting
  does not require a parser-to-native execution test.
- Behavior coverage still needs normal, boundary, and error-path tests at the
  boundaries above; do not manufacture assertions merely to raise a percentage.

## Selecting Regression Scope

Follow `development.md` for validation selection. Full production regression is a
runtime/build/test CI and release requirement, and a local requirement for broad
execution changes. Local documentation, tooling and isolated Java changes do not
automatically require native/ASan/E2E; CI selection follows `ci-gates.md`.
A missing prerequisite is a reported blocker, never a passing or disabled test.

## E2E Infrastructure — TestContainers

Use the TestContainer specifications below when a test needs the corresponding
external service. In-process integration tests do not require unrelated containers.

### Native Library Prerequisite (BLOCKING)

Before E2E tests that load JNI, ensure the native library is built and packaged
from the source revision under test. Reuse a verified matching artifact when native
inputs are unchanged; rebuild/repackage after native runtime or packaging changes:

```bash
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
./pangrova-worker-native/bin/package-native-lib.sh
java -version && mvn -version && mvn -q -pl pangrova-worker/pangrova-worker-core -am test -Dtest="*NativeLoader*" -Dsurefire.failIfNoSpecifiedTests=false
```

Treat `UnsatisfiedLinkError` as a real failure: diagnose the library, symbols, and
classpath, then rerun the affected loader/E2E checks and required broader validation.

### Native Runtime Changes

Native runtime, JNI, packaging, and protocol changes require the applicable
Java/native chain in `build-pipeline.md`: build, native tests, fresh packaging,
Java consumers, and E2E/smoke for affected execution paths. Memory, ownership,
JNI, or native concurrency changes also require ASan validation. Documentation and
build-tool unit-test changes under the native directory require relevant tooling
checks; directory location alone does not require rebuilding the runtime.

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

A test counts as multi-worker or distributed E2E only when every advertised
worker has a distinct process/container and endpoint. Multiple worker IDs that
resolve to one worker endpoint are routing integration; they must use the
`routing-integration` tag and must not claim process isolation, remote data
movement, network partition behavior, or worker-level failure recovery.

## Container Lifecycle Rules

- PostgreSQL MUST use JVM-level singleton (not per-class)
- Docker Network required for multi-container scenarios
- ALL container tests MUST use `ContainerTestBase` for lifecycle management.
  If Docker is unavailable, diagnose the prerequisite and report the concrete
  blocker. Do not disable the test or claim it passed; unrelated host remediation
  is not mandatory to complete independently verifiable work.
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
| Running JNI E2E without a matching native lib | UnsatisfiedLinkError or stale behavior | Verify matching artifact; rebuild/package when inputs changed |

## Reference Files

| File | Purpose |
|------|---------|
| `pangrova-testing/pangrova-testing-integration-tests/.../SharedTestContainers.java` | PG singleton |
| `pangrova-testing/pangrova-testing-integration-tests/.../SqlE2ETestBase.java` | Base class |
| `pangrova-testing/pangrova-testing-kit/.../ContainerTestBase.java` | Docker guard |
| `pangrova-testing/pangrova-testing-kit/.../PangrovaClusterContainer.java` | Cluster TC |
