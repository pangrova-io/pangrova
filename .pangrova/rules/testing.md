# Testing

Validation scope is selected in `development.md`; build commands are in
`build-pipeline.md`.

## Categories

| Category | Location / naming | Marker | Runner |
|---|---|---|---|
| Unit | Module `src/test/java`, `*Test` | — | JUnit 5, AssertJ |
| Property | `property` sub-package, `*PropertyTest` | — | jqwik |
| Integration | `*IntegrationTest` | `@Tag("integration")` | JUnit 5, TestContainers |
| Routing integration | `pangrova-testing-integration-tests` | `@Tag("routing-integration")` | One real Worker endpoint |
| E2E | `pangrova-testing-integration-tests` (`integration.e2e`) | `@E2ETest` (`e2e`, `integration`) | JUnit 5, TestContainers |
| Smoke | `integration.smoke` | `@SmokeTest` (`smoke`) | Packaged JNI |
| Chaos | `integration.chaos` | `@Tag("chaos")` | Toxiproxy |
| Native-dependent | Any Java module | `@Tag("native-required")` | Requires packaged JNI |
| Native process owner | `pangrova-testing-integration-tests` | `@Tag("native-runtime-owner")` | Own JVM per selection |
| Performance E2E | `integration.e2e.performance` | `@Tag("performance")` | JUnit 5 |
| Benchmark | `pangrova-testing-benchmark` | — | JMH |
| C++ | `pangrova-worker-native/test`, `<Component>Test.cpp` | — | GoogleTest, CTest |

Daily runs exclude `native-required`, `routing-integration`, `performance` and
`e2e`; the E2E classes require the immutable AI model fixture and the
same-source production image that full regression provisions.
`SqlE2ETestBase` carries `@E2ETest` so its subclasses inherit the tag, and the
integration module excludes `**/e2e/**/*PropertyTest.java` by path because the
jqwik engine ignores tag filters.
`-Pfull-regression` includes them and excludes only `native-runtime-owner`, whose
selections run in isolated JVMs (see `build-pipeline.md`). A Maven run that selects
no applicable tests is not validation.

Property tests mirror the production package under `property`
(`org.pangrova.worker.exchange` → `org.pangrova.worker.property.exchange`).

## Rules

- No mock frameworks (Mockito, PowerMock, EasyMock): use real collaborators, small
  fakes or TestContainers. C++ uses GoogleTest only.
- No `@Disabled`, `@Ignore` or permanently false assumptions; fix or delete the test.
- No reflection into private members; make the member package-private instead.
- Tests are deterministic and isolated: no uncontrolled random seeds, shared mutable
  state, external public services or network. Wait for readiness and ordering with
  Awaitility, latches or TestContainers wait strategies, never `Thread.sleep`.
- Each test method has clear Given/When/Then and verifies one behavior, covering
  representative equivalence classes and relevant null, empty, limit, overflow and
  negative boundaries.

## Coverage Depth

Test observable behavior, not class or line quotas.

- New behavior: normal, boundary and important error paths.
- Bug fixes: a focused regression test where practical.
- Pure refactors: existing tests that exercise the changed path.
- Resource ownership and concurrent transitions: cleanup, failure and race coverage at
  the real boundary.
- Serialization, parsing, scheduling and allocation: property tests when invariants
  are stronger than examples.
- Cross-component behavior: integration tests. User-visible SQL, protocol, JNI and
  distributed behavior: E2E at the matching boundary. CLI-only formatting needs no
  parser-to-native test.

Coverage reports (`-Pcoverage`) are diagnostic; there is no percentage gate.

## Containers

All container tests extend `ContainerTestBase`, use `getHost()`/`getMappedPort()` and
a Docker network for multi-container scenarios. If Docker is unavailable, report the
blocker; never disable the test or claim it passed.

| Service | Specification | Prohibited substitutes |
|---|---|---|
| PostgreSQL | `postgres:16-alpine`, user `pangrova` / `pangrova_test`, database `pangrova_meta` (E2E) or `pangrova_metadata_test` (unit); JVM-level singleton `SharedTestContainers.POSTGRES` | H2, SQLite, embedded-pg, shared dev databases, a container per class |
| LLM inference | `ollama/ollama:0.6.2`, model `tinyllama`, port 11434 | `StubInferenceProvider` in E2E, external APIs, mock HTTP |
| Fault injection | `ghcr.io/shopify/toxiproxy:2.12.0` for chaos, CDC exactly-once and consensus tests | — |
| Cluster | `PangrovaClusterContainer` on the image from `deploy/docker/Dockerfile`; Coordinator 8080, Worker 8081, health `GET /v1/health` | — |

A test is multi-worker or distributed only when every advertised Worker has its own
process/container and endpoint. Multiple Worker IDs behind one endpoint are routing
integration and must not claim process isolation, remote data movement, partition
behavior or Worker failure recovery.

JNI-loading tests require a Native library built and packaged from the revision under
test; reuse a verified artifact only when Native inputs are unchanged. Treat
`UnsatisfiedLinkError` as a real failure.

## Reference Files

| File | Purpose |
|---|---|
| `pangrova-testing-integration-tests/.../e2e/SharedTestContainers.java` | PostgreSQL singleton |
| `pangrova-testing-integration-tests/.../e2e/SqlE2ETestBase.java` | SQL E2E base class |
| `pangrova-testing-kit/.../testkit/ContainerTestBase.java` | Docker guard |
| `pangrova-testing-kit/.../testkit/PangrovaClusterContainer.java` | Cluster container |
| `pangrova-testing-kit/.../testkit/E2ETest.java` | E2E marker |
