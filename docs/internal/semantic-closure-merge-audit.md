# Semantic closure branch integration audit

Date: 2026-10-18.

## Scope and source identity

The final requested acceptance scope is logic review, repository code checks and
compilation, including test sources. Runtime suites, performance experiments and
production deployment are outside this acceptance run.

The integration joins `build-ai-feature` at `11c691d8` with
`codex/ai-native-semantic-closure` at `9e29f38f`; their common ancestor is
`058fb13a`. The retained branch is `codex/ai-native-semantic-closure`. Both source
histories remain reachable through the merge. Existing uncommitted changes in the
original working directories are excluded from this integration.

## Conflict and logic review

The initial merge contained 185 conflicted files. Older overlapping execution
implementations were reconciled with the semantic branch's later runtime changes.
The TPC tooling and Q9 fixes were integrated with those actual owners rather than
replacing the current execution path wholesale.

Reviewed boundaries and resulting behavior:

- Equality-based join rules require paired equality keys; reassociation operates
  on logical joins. Validation captures actual active Worker capabilities while
  preserving Native extension support and exact target admission.
- Primitive comparisons have explicit common argument types. Arithmetic also
  includes the validated SQL result type when selecting a numeric width, retaining
  floating division and operand nullability. There is one normalization path.
- Live exchange input schemas use the actual dispatched producer plans. Older
  prepared-plan bookkeeping is superseded by that stronger source of truth.
- Coordinator control results use a separate cursor for each request. Each cursor
  reaches EOF independently; the bounded acknowledgement buffer remains available
  for independent retries. Relational result transport stays on the Native Worker.
- Execution-anchor renewal returns exact immutable fenced receipts. PostgreSQL
  locks the owned row set before sampling backend time and updates only that set.
  Yield returns the affected row count without constructing a live renewal receipt
  from its terminal deadline. Retiring local fences cannot be stamped during
  durable release or overwritten by a delayed renewal receipt.
- Result expiry scheduling is sealed after publication owners drain; retained
  acknowledgement bytes are released after transport exit. Routing-test overlays
  retain their own owners, never replace the shared SQL fixture, and retain failed
  cleanup owners for retry.
- Shared AI deployment uses a verified immutable real-model artifact prepared
  before Maven. The shared FILE deployment and its cleanup remain wired. Tests do
  not acquire models from the public registry during startup.
- Native resource identity comparison is lexical and does not call blocking
  `URL.equals`. The concrete stream adapter is final, so a rejected constructor
  cannot expose a partially initialized subclass through finalization.
- Duplicate methods and three duplicate Maven test dependency declarations from
  independent branch edits were removed. Quality-store tests follow their actual
  Metadata owner, schema lifecycle, table names and structured error contract.

The protobuf source tree is unchanged relative to the semantic branch. Its
previous descriptor omitted already committed system-table metadata and COPY
fields. The repository generator refreshed the descriptor; decoded review showed
additive messages, fields and enum values, with no removed or renumbered fields.
Generated Java/C++ protocol sources were not edited by hand.

## Build and code-check evidence

| Check | Result |
|---|---|
| Clean full Java reactor build, test-source compilation, Checkstyle and SpotBugs; test execution skipped | Passed: 38 reactor projects after clean; zero Checkstyle/SpotBugs gate findings |
| Native Release library, function-export tooling and complete test executable build; warnings treated as errors | Passed |
| Fresh Native resource packaging and classifier artifact | Passed |
| Java declarations / actual packaged Native JNI exports | Passed: 22 isolated symmetric methods |
| SPI annotations | Passed: 147 public types, zero annotation baseline gaps |
| E2E truthfulness and production stub-pattern checks | Passed |
| Canonical protobuf descriptor | Passed after generated additive baseline refresh |
| Python and shell syntax | Passed: 25 Python files and 7 shell files |
| Maven dependency declaration uniqueness | Passed: 39 POMs |
| CI and dedicated benchmark workflow YAML | Parsed successfully |

The final clean reactor build completed successfully in 4 minutes 35 seconds.
The Native build completed with warnings treated as errors, and the classifier
payload checksum matches the freshly linked library exactly.

The host build uses JDK 17, Maven 3.9 and the configured C++20/LLVM toolchain.
The unchanged Lance FFI archive was reused only after comparing all 33 tracked
FFI input files against its source checkout and recording the archive checksum.
Native dependencies use the pinned cached SDK sources. Reactor artifacts were
installed in a private Maven repository to avoid overwriting artifacts used by
other working directories.

Reproduction, after preparing the pinned dependencies and matching Native bundle:

```bash
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel 2
bash .pangrova/scripts/jni-symbol-symmetry.sh \
  pangrova-worker-native/build/libpangrova_native.dylib
mvn -B clean install -Pnative-runtime,static-analysis -DskipTests \
  -pl '!pangrova-native-bundle'
```

SpotBugs emits non-fatal missing-class diagnostics containing method-like names
such as `toString` and `equals`; its configured finding/error gate is retained.
No Checkstyle/SpotBugs thresholds, exclusions or performance budgets were relaxed.

## Evidence limits

No Java/Native runtime, distributed fault, sanitizer execution or SF100/SF1000
performance acceptance is claimed for the merged source by this review. Earlier
Q9 runtime measurements remain historical evidence for their recorded source.
See [the TPC audit](tpc-benchmark-audit.md) and
[the benchmark methodology](../testing/tpc-benchmark-methodology.md) for scale and
cross-engine qualification prerequisites. Production runtime qualification needs
its own execution evidence.
