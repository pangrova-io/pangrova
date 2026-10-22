# Development and Verification

This document defines the repository's coding workflow, test strategy, and build
commands. Validation is proportional to the change's blast radius.

## Environment

Use JDK 17 and Maven 3.9+. Verify `JAVA_HOME`, `java -version`, and
`mvn -version` before diagnosing Java build failures. Native development also
requires CMake, Ninja, a C++20 compiler, and the dependencies installed by
`pangrova-worker-native/bin/setup-native-build-env.sh`.

Keep personal paths, credentials, fixed host ports, IDE state, and machine-specific
configuration out of tracked files. Use environment variables, temporary-directory
APIs, or ignored local configuration.

## Before Editing

- Read the target implementation and nearest relevant tests.
- Trace callers before changing a public API, SPI contract, configuration key,
  protocol field, or JNI signature.
- Check dependency direction in `architecture.md` for cross-module changes.
- Identify the cheapest check that can disprove the proposed change.
- Inspect the worktree and preserve unrelated changes.

## Implementation

- Keep changes within the requested ownership boundary.
- Prefer established project abstractions and dependencies.
- Fix causes rather than concealing symptoms with silent fallback, broad exception
  handling, warning suppression, retries, or timeout increases.
- Update both sides of protobuf and JNI contracts in one change.
- Add or update tests when observable behavior changes.
- Do not hand-edit generated protobuf sources.

Detailed language and naming rules are in `code-standards.md`.

## Java Commands

The test/verify commands below already compile. Choose one lifecycle invocation
for the required result; a separate compile is a diagnostic option.

```bash
# Focused module checks
mvn -q -pl <module> -am compile test-compile
mvn -q -pl <module> -am test

# Full reactor checks
mvn -q -DskipTests install
mvn -q test

# Static analysis for an affected module
mvn -q -pl <module> -am verify -Pstatic-analysis
```

Start with the affected module. Run the full reactor when a change modifies a
shared API, parent POM, dependency management, protocol, or cross-module behavior,
or when preparing a release-quality change.

## Native Commands

```bash
./pangrova-worker-native/bin/setup-native-build-env.sh
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
python3 pangrova-worker-native/bin/run-native-tests.py
```

Run AddressSanitizer validation for native memory, ownership, JNI, or native
concurrency changes; Java-only concurrency uses the relevant Java stress/race tests:

```bash
VELOX_BUILD_DIR=/path/to/asan/velox/build \
  cmake --preset dev-asan -S pangrova-worker-native \
  -DPANGROVA_ASAN_PROTOBUF_LIB=/path/to/asan/libprotobuf.a \
  -Dabsl_DIR=/path/to/asan/absl/lib/cmake/absl \
  -DPANGROVA_ASAN_FOLLY_PREFIX=/path/to/asan/folly/install
cmake --build pangrova-worker-native/build-asan --parallel
python3 pangrova-worker-native/bin/run-native-tests.py \
  --build-dir pangrova-worker-native/build-asan
```

## Cross-Language Commands

JNI, protobuf, native packaging, and execution-boundary changes require both Java
and native validation:

```bash
./pangrova-worker-native/bin/package-native-lib.sh
mvn -q -pl pangrova-worker -am test
mvn -q -pl pangrova-testing/pangrova-test-e2e -am test
mvn -q -pl pangrova-testing/pangrova-test-e2e \
  -Dtest=ActivationSmokeE2ETest test
```

Run E2E and smoke suites when the changed path can affect startup, SQL
planning/execution, transport, serialization, native loading, or deployment
connectivity. They are not required for isolated documentation or tooling edits.

## Test Strategy

| Type | Location | Tooling |
|---|---|---|
| Java unit | Module `src/test/java` | JUnit 5, AssertJ |
| Property | `src/test/java/**/property` | jqwik |
| Integration | Usually `*IntegrationTest` | JUnit 5, TestContainers |
| E2E | `pangrova-testing/pangrova-test-e2e` | JUnit 5, TestContainers |
| Smoke | `pangrova-testing/pangrova-test-e2e/.../smoke/ActivationSmokeE2ETest` | JUnit 5, packaged JNI |
| Native | `pangrova-worker-native/test` | GoogleTest, CTest |
| Benchmark | `pangrova-testing/pangrova-test-benchmark` | JMH |

Tests must be deterministic and isolated. Do not use `Thread.sleep`, uncontrolled
random seeds, shared mutable state, or external public services. Use real
collaborators, small fakes, or TestContainers; Mockito, PowerMock, and EasyMock are
not used. Do not add `@Disabled`, `@Ignore`, or permanently false assumptions.

Coverage depth follows risk:

- Pure refactoring may rely on existing tests when the affected path is covered.
- Bug fixes should include a regression test when practical.
- New public behavior needs normal, boundary, and important error-path coverage.
- Serialization, parsing, scheduling, and state-machine logic should use property
  tests when properties are stronger than enumerated examples.
- User-visible SQL, protocol, JNI, or distributed behavior should have integration
  or E2E coverage at the appropriate boundary.

Changed-line coverage is enforced by `.pangrova/scripts/diff-coverage-check.sh`.
Do not write low-value assertions solely to satisfy the threshold.

For TestContainers, use mapped ports and wait strategies. If Docker is unavailable,
report the missing prerequisite accurately; do not disable the test or claim it
passed.

## Selecting Validation

1. After the first edit, run the closest test, compile, syntax, or lint check.
2. Run the owning module suite once the focused check passes. A Maven `test` or
   `verify` already compiles; do not repeat compile/test-compile without a reason.
3. Broaden to dependent modules when shared contracts changed.
4. Run native, packaging, E2E, and smoke checks when the path crosses those
   boundaries.
5. Repeat timing-sensitive tests after concurrency or flakiness fixes.
6. Run repository gates relevant to the changed files. Full production regression
   remains required for runtime/build/test PRs, protected-branch pushes and releases;
   documentation/rule/tooling-only PRs use the lightweight CI checks described in
   `ci-gates.md`. Run full regression locally for broad execution
   changes or when focused checks cannot establish the affected boundaries.
7. Tooling/docs edits under a native directory require their own checks; runtime
   directory location alone does not mandate ASan or rebuilding unchanged JNI.
8. A passing full-regression reactor includes its E2E and activation smoke tests.
   Do not rerun those suites separately unless their inputs changed, they did not
   execute, or a failure needs diagnosis. Report concise results and link long logs.

## Connectivity

For a new or modified integration path, verify the applicable chain:

```text
API or SQL input -> parser/planner -> protocol -> worker/JNI -> native execution
-> result transport -> client
```

Check that interfaces have implementations, implementations are wired, callers use
the wiring, and tests exercise the intended path. For deletions, remove registrations,
callers, tests, configuration, and documentation that exist solely for the deleted
feature.

## Failure Triage

- Capture the concrete error, stack trace, or failing assertion.
- Automatic retries must not turn an intermittent failure into a passing result.
  Preserve unaffected passing evidence and rerun the failed or newly affected
  scope after a relevant source, fixture, or environment correction. Deliberate
  concurrency repetitions and the final unified regression remain required.
- Reproduce it with the smallest relevant command.
- Separate change-caused failures from unrelated baseline or environment failures
  using evidence.
- Do not expand the task to repair an unrelated failure unless it blocks validation
  of the requested change.
- Never claim a check passed if it did not run.

## Completion

- Review the diff for accidental scope, dead code, secrets, machine paths, and
  generated files.
- Update documentation when APIs, behavior, configuration, architecture, or build
  steps change.
- Report the changed files, checks executed, results, and any verified blocker.
