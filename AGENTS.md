# AGENTS.md

Repository guidance for coding agents. This file is the entry point;
`.pangrova/rules/` is the single source of architecture and development policy.
Explicit user instructions take precedence over this file.

## Project

Pangrova is a distributed SQL query engine for lakehouse architecture. A Java
Coordinator parses and plans SQL with Calcite; Workers execute Substrait plans in a
C++20 Velox runtime through JNI and exchange Arrow data over Flight.

| Path | Contents |
|---|---|
| `pangrova-*/` | Maven domain modules; contracts are published under `.api` packages |
| `pangrova-worker-native/` | C++20 native runtime (CMake), JNI bridge and GoogleTest suites |
| `pangrova-protocol/` | Protobuf execution boundary shared by Java and C++ |
| `pangrova-testing/` | Test kit, integration/E2E/smoke suites and JMH benchmarks |
| `docs/`, `website/` | Published documentation and the Docusaurus site |
| `.pangrova/` | Canonical rules, module manifest and executable repository gates |

`.pangrova/modules.yaml` records each artifact's owner and allowed dependencies.

## Commands

Use JDK 17, Maven 3.9+, Docker for container tests, and CMake/Ninja with a C++20
compiler for native work. On a clean machine, run
`.pangrova/scripts/build-iceberg-sdk-candidate.sh` once before Maven.

```bash
mvn -q -pl <module> -am test                      # focused module tests (compiles)
mvn -q -pl <module> -am verify -Pstatic-analysis  # Checkstyle + SpotBugs
mvn -q -DskipTests install && mvn -q test         # full reactor, daily selection
mvn -q -Pfull-regression verify                   # full production regression

./pangrova-worker-native/bin/build-lance-ffi.sh   # before every fresh CMake configure
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
python3 pangrova-worker-native/bin/run-native-tests.py
./pangrova-worker-native/bin/package-native-lib.sh  # stage JNI library for Java tests

python3 -m unittest discover -s .pangrova/scripts/tests -p 'test_*.py'  # gate tooling
bash .pangrova/scripts/check-module-manifest.sh   # module ownership baseline
```

Run the closest check first, then broaden as `development.md` requires. JNI,
protocol, native runtime, and execution-path changes require the affected
Java/native chain in `build-pipeline.md`; full production regression remains a
CI/release gate unless the change is broad.

## Rules

Read each rule relevant to the task before editing; read it once, and reread only
if its content or the task scope changes.

| Rule | Read when |
|---|---|
| `development.md` | Every change: workflow, validation selection, failure triage |
| `architecture.md` | Cross-module, routing, wiring, or Java/native boundary changes |
| `code-standards.md` | Writing Java, C++, JNI, dependencies, logging, or commits |
| `testing.md` | Adding or changing tests and test infrastructure |
| `build-pipeline.md` | Native, packaging, sanitizer, or full-chain builds |
| `ci-gates.md` | Changing CI, gate scripts, or repository tooling |

SPIs may precede implementations. Before changing a contract, verify its callers,
implementation, wiring, and tests.

## Workflow

1. Inspect the target, callers, and nearest tests; preserve unrelated worktree changes.
2. Make the smallest root-cause change consistent with existing abstractions.
3. Add or update tests for changed behavior.
4. Run the validation selected above; report what ran, results, and real blockers.
5. For every new or changed feature, check and update the affected documentation
   under `docs/` in the same change: user workflows, SQL/API contracts,
   configuration, architecture, and operations. Do not leave a feature
   undocumented. Update `.pangrova/rules/` only when architecture constraints or
   repository policy change.
6. Before ending a session, run `.pangrova/scripts/clean-dev-artifacts.sh` and
   confirm `.pangrova/scripts/check-dev-artifacts.sh` passes.

Prefer Conventional Commits with a module scope, for example
`fix(worker): release Arrow buffers on task cancel`. PRs use
`.github/PULL_REQUEST_TEMPLATE.md`.

## Boundaries

- Always: keep both sides of protobuf and JNI contracts in one change; use
  TestContainers with mapped ports for external services.
- Ask first: adding dependencies or upgrading core libraries, changing module
  ownership in `.pangrova/modules.yaml`, or changing CI workflows.
- Never: hand-edit generated protocol sources or renumber existing protobuf
  fields; hide failures with fallback
  behavior, retries, or broad exception handling; disable or weaken tests; add
  mock frameworks; claim a check passed that did not run; revert user changes
  outside the task; commit secrets, personal paths, or machine-specific state;
  recreate the retired non-published `docs/internal/` directory.

Run `.pangrova/setup.sh --check` after changing this file or the rules layout.
Security reports follow `SECURITY.md`.
