---
title: "Quality Gate Audit and Streamlining Rationale"
description: "Item-by-item review of actual gate behavior, duplicate constraints, false-positive risk, and validation cost, with the rationale for keeping, relaxing, or deleting each gate."
---

# Quality Gate Audit and Streamlining Rationale

## 2026-10-23: Structural Numbers Become Visible Diagnostics

Users asked to remove redundant gates according to actual risk. Reviewing the new
materialization kernel showed that centralized validation of immutable
attempt/generation identity triggers constructor-parameter and cyclomatic
complexity thresholds; transaction repository line counts also include a large
amount of declarative SQL. Splitting code into forwarding classes or generic
parameter bags just for the numbers would not prove better responsibilities.

The actual parser checks and XML reports for FileLength=800, MethodLength=100,
CyclomaticComplexity=15, and ParameterNumber=10 are retained, and these four
severities are set to warning. Maven explicitly blocks only on error. Nesting,
illegal imports, dangerous code patterns, module direction, SpotBugs, and
functional and failure tests keep their original blocking semantics; coverage
remains an optional diagnostic.

This is a project risk decision and does not claim a uniform industry standard for
numeric thresholds. The [official Checkstyle cyclomatic complexity
description](https://checkstyle.org/checks/metrics/cyclomaticcomplexity.html) uses
the metric to measure test-path complexity and also permits exceptions when
splitting harms readability or encapsulation;
[FileLength](https://checkstyle.org/checks/sizes/filelength.html) recommends
splitting long classes by specific responsibility. Actual responsibilities and
branch tests still require review. The earlier "hard limit" description on this
page is only a historical decision record; the current section and the canonical
rules govern.

Real Maven/Checkstyle fixtures separately verify that metric overruns still
produce warnings; illegal JDK imports and excessive nesting produce errors and
fail the build; and after removing the error fixture, the build with the overrun
fixture succeeds. Policy tests also check severities and the Maven blocking
threshold. No per-file metric exemptions were added.

### Precise Exclusion of JDBC Parameter Positions

The SQL in `JdbcMaterializationRepository` contains more than ten placeholders,
and MagicNumber treats binding positions 11–26 as business values. Renaming them
to constants such as `INDEX_ELEVEN` cannot explain the SQL. Using the
[official Checkstyle XPath filter
capability](https://checkstyle.org/filters/suppressionxpathsinglefilter.html), only
the first argument of this adapter's `statement.set*` calls and direct integer
indexes in the second argument of three existing JDBC helpers are excluded.
Binding values, other expressions, and other classes remain subject to the
original checks.

Real Maven fixtures confirm that indexes 23–26 pass while the same method's
binding value `777` and business budget `5000` are still blocked; indexes in other
classes are not excluded either. The exclusion does not change SQL or runtime
validation.


## 2026-10-18 Review and Actual Remediation

This round reviewed the actual constraints along "rule entry → script
implementation → Maven/CMake → workflow invocation → artifact consumers". The main
waste came from duplicate execution, overly broad trigger scope, and process gates
that only checked author-typed text without validating behavior. No security,
protocol, ownership, or real-execution requirements were relaxed to save tokens.
The 2026-10-04 data below is retained as a historical snapshot; conclusions that
differ from current behavior are governed by this section and the actual
configuration.

### Findings, Evidence, and Changes

| Area | Finding and impact | Change implemented |
|---|---|---|
| Rule entry | AGENTS listed five validation documents as requirements; workflows duplicated the development lifecycle; setup was described as a per-task prerequisite | Route to the needed rules per task and read each relevant rule once; development unifies validation policy; workflows keep only compatibility guidance; setup is checked only on layout changes or in CI |
| Coding standards | Mandatory branch prefixes, English commits, and bans on referencing designs/task numbers in comments cannot prove code correctness; third-party deprecation warnings were required to be migrated repository-wide immediately | Changed to clear expression and useful provenance; removed language/prefix hard limits; third-party migration is scoped per task, and suppression must be narrow and justified; public-code naming and module-isolation checks unchanged |
| Standard contradictions | The C++ protobuf standard still named the old package while architecture required canonical v2; Java concurrency changes were also broadly required to run ASan | Package name unified to canonical v2; local ASan targets native memory/ownership/JNI/concurrency risk, while Java concurrency uses the corresponding Java tests |
| CI scope | The old scope selected only performance jobs. Documentation PRs still built four-platform Native, two sanitizers, two JDKs, production images, and full regression | scope selects runtime/native/performance and the platform matrix. Documentation, rules, and ordinary tooling PRs keep lightweight checks; runtime/test PRs keep the correctness chain; unknown inputs run conservatively |
| Native build | Ordinary Java leaf-module changes also rebuilt four platforms and two independent C++ sanitizer suites; those suites did not consume the changed Java inputs | JNI/protocol/SPI/Native/packaging/build and unknown inputs keep four platforms plus ASan/UBSan; other Java PRs keep Linux Native and real production-image E2E, omitting three extra platforms and two sanitizer builds whose inputs did not change. Protected-branch pushes and releases still run everything |
| Maven lifecycle | Every JDK ran `clean verify -DskipTests` and then `verify`; a separate ArchUnit job recompiled and repeated tests already in the regular suite | One verify per JDK; ArchUnit assembly/reachability tests continue to be discovered by daily/full suites; the full reactor install needed by standalone process tests is retained |
| Test duplication | The full-regression reactor already included E2E and activation smoke, yet full E2E and a separate smoke run were repeated; two daily JaCoCo runs were not gate inputs | Production full-regression runs E2E/smoke once; subsequent duplicate E2E/smoke, daily coverage instrumentation, the separate ArchUnit job, and the Codecov upload were removed; `-Pcoverage` remains only as an on-demand diagnostic |
| Source-gate duplication | SPI annotation, E2E truthfulness, and proto descriptor ran once per JDK; proto also prepared before an internal prepare | Moved to a single lint job; the descriptor runs once when runtime changes; cheap SPI/E2E pattern checks continue to run |
| Plan approval text | An author could pass with one approval sentence or a label; meanwhile the real golden tests already ran in daily/full Maven | Removed the approval script and duplicate job; retained golden-file existence and actual plan equality assertions, with normal code review explaining golden diffs. The TPC-H 21 / TPC-DS 31 parameterized cases actually passed this round |
| Distributed checklist | Checked only that some PR section had a checkbox or an N/A reason, without verifying fencing, recovery, or idempotency behavior | Removed the body-format script and job; the PR template now covers change/validation/relevant risk; the distributed risk-review reference and the corresponding behavior, assembly, and integration tests remain |
| Fuzzer truthfulness | FuzzerRunner created only a ReferenceEngine, and each fuzzer's `run` merely called DuckDB `reference_.execute(sql)`, with no Pangrova execution or result comparison. CMake also linked only itself and DuckDB, yet depended on the entire Native job | Removed the PR engine gate and daily automatic run; kept a manual self-check explicitly labeled as a reference SQL generator. Did not interpret its pass as evidence of Pangrova query correctness, nor delete the tool source |
| Stub scan cost | Eight greps per production file plus a find traversal of build/dependency trees; the success log also claimed all production methods were implemented | Enumerate tracked plus non-ignored untracked source via Git and match once in a merged pass; kept existing exception categories, added positive/negative cases; fixed a defect where zero matches produced a double-zero count. Success means only that the patterns were not detected; bytecode reachability and execution tests remain separately responsible |
| Hooks | `core.hooksPath`/fsmonitor were not configured, the Git hooks directory contained only `.sample` files; the repository hooks/ directory additionally tracked 13 native Git boilerplate files, and setup installed no hook | Deleted the duplicate boilerplate inside the repository, kept the user `.git/hooks` and Git configuration; added no mandatory pre-commit/pre-push/commit-msg flow. The saving is search noise; it cannot be claimed that a nonexistent interception was removed |
| Environment paths | A repository-wide scan found an existing incident record containing an absolute path to a personal home directory; this finding could be fixed directly without disabling the whole gate | Normalized only the incident-record source path to `~/Library/Logs/DiagnosticReports/`, retaining filename, stack, PID, UUID, and other diagnostic evidence; the repository-wide check then passed |
| Release gap | release.yml depended only on Native packaging, and Java used skipTests; the text "releases require full regression" was not actually wired | Reused the same CI workflow on tags and made the release depend on validation. Release and validation artifacts use different prefixes to prevent conflicts in the same run. Rebuilding artifacts for release must still succeed |
| Release availability | The macos-13 runner was retired, and action-gh-release v1 was flagged by current actionlint as too old at runtime | Intel/ARM runners are explicitly macos-15-intel / macos-15; the release action was updated to v3. See the [GitHub runner announcement](https://github.blog/changelog/2025-09-19-github-actions-macos-13-runner-image-is-closing-down/) and [release action](https://github.com/softprops/action-gh-release/releases/tag/v3.0.3) |

### Execution and Context Boundaries After Streamlining

- Documentation/rule/ordinary-gate-tool PRs start four lightweight jobs: scope,
  lint, doc-consistency, and Validation result; they no longer start Native, the
  full JVM reactor, sanitizers, or production-image builds.
- Java runtime/test changes continue to run the affected correctness chain.
  Worker/JNI, protocol, SPI, build dependencies, or unknown inputs keep the full
  platform set under a conservative policy; this round does not infer that
  arbitrary Java changes are unrelated to the native ABI.
- Protected-branch pushes and release tags do not depend on PR path detection and
  explicitly select the full correctness chain. This is a deliberate retention for
  maintaining the integration baseline and release assurance; after documentation
  merges into a protected branch, full CI still runs.
- test/verify already includes compilation, and E2E/smoke already covered by full
  regression does not get an additional mandatory command. Rerun only when inputs
  change, tests did not execute, or failure localization requires it. Long logs are
  kept in reports; task context contains only commands, results, blockers, and
  necessary samples.
- Ordinary Java PRs save duplicate builds of extra architectures and independent
  sanitizers; production images still use the same source revision and verify the
  image ID, JNI classifier, runtime manifest, and source commit.
- No fixed percentage of token savings is claimed. This round removed locatable
  duplicate reads, output, and execution sources; no before/after token
  measurement of real multi-turn tasks was performed.

### Reverse Protection and Validation Results

A stable `Validation result` aggregate was added so that skipped conditional jobs
are not mistaken for success. A selected job that fails, is canceled, is skipped
unexpectedly, is missing scope, or has inconsistent native/performance
prerequisites fails; only checks explicitly not selected may be skipped. The
production-regression dependency conditions also distinguish "pure Java does not
need an independent sanitizer" from "a sanitizer should run but did not succeed".
The relevant GitHub skip/needs semantics are documented in the [official workflow
documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).

This round actually ran:

- 37 Python gate-policy/exception-fixture/stub-detection checks passed, including
  deletion, cross-directory rename, paths with spaces, missing base, unknown
  input, full event selection, failure/cancellation/skip, and
  tracked/untracked/ignored source boundaries.
- Planner focused reactor under JDK 17: TPC-H 21 and TPC-DS 31, 52 total passed,
  with zero failures/errors/skips.
- Real Checkstyle parser: 9 allowed boundary/API fixtures passed and 14
  out-of-bounds/internal-import fixtures were rejected. Positive and negative
  validation of the SpotBugs filter API also passed; no existing structural
  threshold or security filter boundary was relaxed.
- Four tests for the Flight header patch tool passed; SPI annotation passed for
  142 public types with a historical gap of 0; documentation class-name references
  passed for 179 items; E2E truthfulness, gate shell syntax, and canonical layout
  all passed. The repository-wide environment-path check passed after normalizing
  one existing incident-record path, without adding a whitelist for it.
- actionlint v1.7.12 validated the modified CI/release/manual-generator workflows;
  every workflow YAML parses. The repository-wide stub scan actually passed and
  took about 5 seconds on this machine; the 65-second snapshot from an earlier
  concurrent audit cannot be used to compute a general speedup.
- This round did not start hosted CI, full Native/ASan/UBSan, production images, or
  releases; those results still require real runners, and static checks cannot
  replace deployment acceptance. Existing unrelated documentation/website
  worktree changes were preserved.

### Risks That Still Need to Be Distinguished

Local workflow files cannot reveal the current GitHub branch protection / ruleset
required-check configuration. Repository administrators must align it with
`Validation result` and remove required statuses for retired jobs; this round did
not modify remote repository settings without authorization. Otherwise old
statuses may wait forever, or the new aggregate status may not yet be required.

Checkstyle/SpotBugs/FindSecBugs, the canonical descriptor, JNI lifecycle, real
worker/process boundaries, and full release correctness validation are retained.
Coverage reports are not a gate; `-Pcoverage` is only an on-demand diagnostic.
Textual SPI provider/stub/doc reference scans are diagnostic evidence only and
cannot replace actual assembly and execution.
This round does not keep adjusting naming, complexity, or performance thresholds
merely because "the rules look strict"; such changes need independent
false-positive or repeatable measurement evidence, so that streamlining does not
become a way to bypass real defects.

## 2026-10-04 Historical Audit Snapshot

The conclusion of this audit: validation scope should be selected by impact
boundary, structural metrics should be enforced uniformly by Checkstyle, and
historical process constraints should not become permanent code restrictions.
Protocol, JNI, security, resource lifecycle, real execution evidence, and
production regression remain. Fewer structural warnings do not mean fewer real
defects, nor that full CI has passed.

## Audit Scope and Evidence

Audit date: 2026-10-04. Reviewed `AGENTS.md`, seven `.pangrova/rules/` rules, gate
scripts, actual CI invocations, the parent POM, Checkstyle/SpotBugs configuration,
the corresponding source and bytecode-check tests, and the development and testing
documentation.

The 2026-10-16 incremental audit added the cleanup of coverage, duplicate Java
builds, and the separate ArchUnit job; the original distribution and durations
below remain a 2026-10-04 snapshot.

The 2026-10-19 review found that production regression still explicitly enabled
`-Pcoverage`; although there was no percentage threshold, it forced instrumentation
and report generation for the full reactor. Production CI has removed this
profile; `-Pcoverage` remains only as an on-demand diagnostic tool. Functional,
protocol, Native, real E2E, and static security gates continue to be accepted
against their actual boundaries.
Structural comparison uses the repository-pinned Checkstyle 10.12.7, Maven
Checkstyle Plugin 3.3.1, and JDK 17, not handwritten method-length estimates. The
current worktree and commit baseline `e92532a77fcd813009e659a62e7691f08e972899`
were scanned separately. The commit baseline was exported with `git cat-file
--batch` to a temporary directory, without stashing, switching branches, or
modifying shared source.

The current worktree contains many existing modifications, so the two result sets
are reported separately. The scan includes only each module's `src/main/java` and
excludes generated code under `target`; it keeps the same
`switchBlockAsSingleDecisionPoint=true` and nesting settings as the actual
configuration. The current worktree additionally compared results with and without
the existing suppressions applied. The baseline comparison used that commit's own
suppression file. The newly added `NestedForDepth=3` was validated separately in
boundary tests; the before/after comparison in the table below does not mix the new
loop check into the original metrics.

Raw summaries, example locations, and script observations are stored in the
[audit data](quality-gate-audit-data.json). These are a single audit snapshot and
must not be treated as a general performance baseline for future commits.

## Structural Distribution and Threshold Rationale

| Metric | Current worktree | Commit baseline |
|---|---:|---:|
| Production Java files | 3,087 | 2,977 |
| File lines P50 | 61 | 61 |
| File lines P90 | 237 | 237 |
| File lines P95 | 337 | 338 |
| File lines P99 | 488 | 490 |
| Over 500 lines | 11 | 13 |
| Over 800 lines | 0 | 2 |
| Longest file | 779 | 1,057 |

800 lines is a hard limit for a small number of larger files with complete
responsibilities, not a daily target. The vast majority of files should keep the
existing short structure. Line count alone cannot assert that these 11 files are
false positives, nor does passing after relaxation exempt them from responsibility
review.

### Real Parser Results for the Same Source at Different Thresholds

| Gate | Original threshold → new threshold | Worktree raw warnings, before → after | Worktree with suppressions, before → after | Baseline with suppressions, before → after |
|---|---|---:|---:|---:|
| FileLength | 500 → 800 | 11 → 0 | 6 → 0 | 8 → 2 |
| MethodLength | 50 → 100 | 14 → 0 | 12 → 0 | 15 → 0 |
| CyclomaticComplexity | 8 → 15 | 238 → 22 | 198 → 17 | 137 → 15 |
| ParameterNumber | 7 → 10 | 36 → 13 | 28 → 10 | 28 → 10 |
| Total | — | 299 → 35 | 244 → 27 | 188 → 27 |

The main pressure came from complexity 8, not file length. The old threshold would
block JDBC driver logic with complexity 9, resource ownership and client
configuration code with complexity 10, and a 97-line startup assembly method.
Whether splitting is worthwhile must be judged against responsibility, error
handling, and resource lifecycle.

The new thresholds still block snapshot/model parsing with complexity 17,
write-resource allocation with complexity 19, and assembly methods with 11–13
parameters. These warnings are retained for review of the corresponding changes;
no exemptions were added, and the existing implementations in this worktree were
not included in this refactor.

Checkstyle defines cyclomatic complexity as decision points plus one, including
boolean operations and loops; the old regex scan counted only some
`if/switch/catch`, so the two are not the same metric. The [official
description](https://checkstyle.sourceforge.io/checks/metrics/cyclomaticcomplexity.html)
permits 15 as a practical check limit for real code, but that does not prove every
method with complexity 15 is easy to read. File 800, method 100, and parameters 10
are project policies chosen from this repository's distribution, not official
recommendations. Method-length counting rules are in
[MethodLength](https://checkstyle.sourceforge.io/checks/sizes/methodlength.html).

### Readability Protections

- Maintain single responsibility, clear control flow, and names that express
  business actions.
- The existing depth parameters for `if` and `try` remain 3 and 2. After removing
  regex indentation estimation, loop depth is checked by the same Java parser via
  `NestedForDepth=3`; readability of mixed control flow still requires review.
- Constructors with more than 6 dependencies require responsibility review, but an
  arbitrary `Dependencies` wrapper object is no longer mandatory; the hard limit
  for method and constructor parameters is uniformly 10.
- Interfaces with more than 12 methods prompt a review by consumer responsibility.
  The arbitrary 30-value limit for enums is removed because the size of a domain
  set cannot prove poor design.
- Names such as Adapter, Observer, and Visitor that accurately describe
  responsibilities are allowed; constraints against framework type leakage, wrong
  module direction, and runtime fully-qualified class-name conflicts remain.
- Do not meet the numbers through forwarding classes, generalized context bags,
  abbreviations, compressed statements, or hidden branches.
- Structural metrics cannot prove correctness; behavior, error paths, resource
  cleanup, and concurrency tests remain responsible for their own boundaries.

## Item-by-Item Gate Decisions

| Restriction or check | Actual mechanism and problem | Decision and protection |
|---|---|---|
| Every change requires full regression | Workflow/testing/build-pipeline demanded this in several absolute places, conflicting with development's impact-scoped validation; even documentation under a native directory was required to rebuild | Merged into development's validation policy. Production CI/release regression retained; runtime cross-boundary changes validate the relevant chain, and major execution changes are validated locally end to end |
| Every Java change compiles before test | Maven test/verify itself runs the preceding compile lifecycle | Removed the mandatory duplicate step and kept compile as a diagnostic entry |
| Every native-directory change runs ASan | Equated directory location with memory/ownership risk | Memory, ownership, JNI, and concurrency changes still require ASan; documentation and tool changes run their own checks |
| All historical failures must be fixed on the spot | Turned a local task into repository-wide repair and required operating on a shared dirty worktree to prove a baseline | Failures caused by the change must be fixed; unrelated failures are recorded with logs, baseline export, or isolated reproduction, without claiming unrun/blocked checks passed |
| One test per class/method/error branch | Equated implementation structure with behavioral test coverage and required new tests even for pure refactoring | Changed to behavior and risk coverage; relevant normal, boundary, error, concurrency, and cleanup paths retained; existing coverage can validate pure refactoring |
| Core logic must use property tests | Required jqwik regardless of whether a valuable invariant existed | Choose based on whether invariants are stronger than enumerated examples; do not cancel business regression tests |
| All integration tests must start containers | Local component assembly was also required to introduce PostgreSQL/Ollama/native | Use real containers when the corresponding external service is needed; do not misrepresent in-process integration as distributed E2E |
| Docker unavailable means fixing the host | A local prerequisite failure could block any independently verifiable task | Diagnose and report the specific blocker; do not disable tests or claim success; unrelated host remediation is not forced into the task |
| Checkstyle and SpotBugs run twice | CI first ran each separately and then reran them through static-analysis | Merged into one profile execution; compile prerequisites are satisfied by verify |
| CleanCodeMetricsPropertyTest | A second regex parser of 906 lines; skipped unreadable files, recognized only some methods/branches, had many simple-name exemptions; metrics also disagreed with Checkstyle | Deleted the duplicate implementation and let Checkstyle enforce structural checks uniformly; added allow/reject boundary tests for the real parser |
| Historical rename list | Each test sampled old refactor names and permanently banned class names that might still be reasonable today | Deleted the one-time rename ledger and its scan; retained current type identity, framework isolation, and abbreviation constraints |
| Simple class names must be unique across modules | Java type identity is the fully-qualified name, and duplicate simple names may belong to different domains; the old random sampling could also miss violations | Dynamically discover existing Maven leaf modules, read source package declarations, and exhaustively traverse fully-qualified-name uniqueness, replacing the fixed module list and random pairing; the retained naming constraint became exhaustive, and read failures no longer skip silently. Corrected rule C1's module scope: observability may contain Flight adapters, protocol may contain Arrow; removed simple-name exemptions without broadening framework bans |
| Package file-count thresholds 8/15/25 | Simultaneously maintained 11 historical split packages, 28 exempt packages, and 3 pinned-count packages; 27 of the current 452 declared packages exceeded 15 and 11 exceeded 25. AST statement had 57 types and connector had 47; the count itself cannot prove cohesion | Deleted the three count tests and the historical list instead of admitting them through new exemptions. Review package splits by capability, consumer, and dependency direction; kept package naming checks and made them dynamic and exhaustive over actual package declarations, failing on read errors; module direction and dependency checks remain |
| Pattern-suffix ban | Names became decoupled from responsibilities and could force accurate Observer/Visitor names into generic ones | Allowed real pattern names; still require clear responsibilities and no unnecessary implementation details in public contracts |
| Fixed BOM usage check | The script maintained only an artifactId/import list of eight historical dependencies and did not validate version properties; recursive find rescanned each item and timed out at 120 seconds this time | Deleted the script and CI invocation and corrected the rule description; parent POM version management and dependency review remain, without requiring an unneeded old library to be used forever |
| Documentation 60-day age check | Checked only Git commit time, did not compare associated source, and certainly did not prove documentation facts; all 25 documents were "fresh" this time | Deleted the script and CI invocation; behavior-document synchronization and type-reference checks remain |
| ADR trailer grep | The repository had no docs/architecture/adr directory for it to scan and no participating trailer; it returned 0 immediately | Deleted the empty gate and CI invocation; architecture decisions continue through canonical rules and documentation review |
| Distributed checklist triggered by module directories | Documentation and ordinary tests also triggered it; the old script cleared PR_BODY, used macOS-incompatible truncation, and missed deletions; CI inserting PR body text could be broken by quotes | Changed to trigger on production/config/build changes, while runtime Markdown resources and source packages named docs still trigger; fixed environment input, section extraction, deletion detection, and two CI body-passing paths; allowed justified N/A and rejected bare N/A or checkmarks in other sections posing as evidence |
| P0 evidence must paste all greps | Duplicate logs add context, and line-by-line same-category explanations do not necessarily add review value | Retained same-category defect searches; submit commands, hit counts, representative samples, classification, and full-output links |
| All PRs run extra TPC-H/JMH | Did not distinguish documentation from runtime changes; short-sampled performance variance without stable reruns cannot justify concluding a threshold is too tight | Trigger only for production, dependency, benchmark, and related tool changes; unknown inputs trigger conservatively; existing 10%/15% regression thresholds unchanged and the production correctness chain not deleted |
| Legacy tool rule entry | .claude/.qoder contained only two symlinks to canonical rules, and setup recreated them automatically | Deleted both directories and the recreation logic; only AGENTS.md + .pangrova remain, and setup becomes a read-only layout validation |

## Retained Gates and Their Actual Boundaries

| Check | Observation this time | Why retained and what it cannot prove |
|---|---|---|
| SPI annotations | 139 public types annotated, baseline gap 0; about 5 seconds | Explicit contract stability is valid policy. Annotation itself does not prove a provider is wired |
| SPI stability diagnostic | 22 types passed; about 12 seconds; not invoked as a separate CI job | Retained as an optional local diagnostic. Textual implements/extends, indirect implementation, and lambdas need human understanding and cannot serve as proof of runtime completeness |
| Documentation class-name references | 174 items passed; about 2 seconds | Cheap and catches documentation drift after deletion/rename; PascalCase extraction and the whitelist remain heuristic and do not prove an entire document is correct |
| Stub patterns | Passed this time; about 65 seconds | Prevents undocumented unimplemented methods from entering production paths. It checks empty UnsupportedOperationException/log patterns and context, does not analyze the actual call graph, and cannot claim "no dead code, all methods implemented" |
| ArchUnit wiring/reachability | Checks bytecode reference graphs, entry points, registrations, and existing baselines | Distinct from stub pattern checks; the responsibility separation of both is retained. Static reachability does not equal guaranteed runtime execution and still needs integration/E2E |
| Package naming and module direction | Current naming, source imports/module mapping, and existing exceptions retained | Removing count quotas does not cancel ownership constraints; source rules and baselines are not complete proof of runtime isolation |
| PackageDependencyAcyclicityPropertyTest | Checks bidirectional imports within specified modules, with data-model exceptions | Retained as a gate, but it does not prove cycles of arbitrary length are absent and does not cover all newly added modules; Maven/bytecode boundary checks and review remain complementary |
| E2E truthfulness | Passed; about 0.08 seconds | Rejects known patterns such as disabling, constant assertions, and a single endpoint posing as multiple workers; pattern scanning is not sufficient proof of all test quality |
| Environment path check | 5 forbidden samples and 8 allowed samples passed; about 0.03 seconds | Prevents machine paths from entering active files; this time only the pattern self-check was run and was not claimed as a full repository scan |
| Proto descriptor | Implemented the check this time; did not re-accept the protocol with the modified descriptor | Compares descriptor bytes and canonical source ownership, not semantic compatibility analysis; even safe field additions cause drift, and updating the baseline does not prove wire/API compatibility. The official guidance distinguishes [wire safety from application compatibility](https://protobuf.dev/programming-guides/proto3/#updating) |
| JNI symbol symmetry | Retained the local script and wrapper/loading validation requirements | Symbol existence does not prove ownership, type, and lifecycle correctness; complements JNI/E2E tests |
| Diff coverage ≥60% | Retired on 2026-10-16/19: deleted `diff-coverage-check.sh`, and production regression no longer enables `-Pcoverage` | Must not claim the coverage gate still exists; behavior tests, protocol, Native, real E2E, and static security gates still run against their own boundaries |
| SpotBugs/FindSecBugs | Profile, analysis effort, and filters retained | The configured Medium is detection priority/confidence, not runtime severity. Do not downgrade globally without false-positive samples and security-classification evidence |
| Native/WERROR/sanitizers | Builds and relevant risk scenarios retained | Compiler warnings, memory, and undefined behavior directly affect JVM/query execution and are not merged into a formatting checklist for deletion |
| Production same-source build and real E2E/smoke | Original production-regression and artifact provenance retained | Static analysis/workflow syntax passing cannot replace evidence from actual images, deployment, and SQL→Native→results execution |

The observed durations of retained scripts are wall-clock times from one concurrent
audit on the same machine, not a portable benchmark or a token-consumption
estimate. These durations were not used to infer "some percentage of tokens was
saved".

## Verification Record and Actual Limits

- Checkstyle boundary fixtures used the real pinned parser and validated the
  allowed boundaries 800/100/15/10 and rejection beyond them; they also validated
  constructor, if/for/try depth. The parser must still return failure to reject
  negative cases.
- Gate regression covered documentation/test non-triggering, production and
  benchmark triggering, conservative handling of unknown input, deletion of a
  production file, missing base, PR_BODY environment input, the last Markdown
  section, N/A reasons, and canonical layout not rebuilding removed directories.
- Exhaustive naming reproduced a FlightClientTracingInterceptor false positive;
  after checking the client/server interceptor implementations, server assembly,
  and C1 module scope, the test was corrected and boundary regressions were added
  for allowed adapters and rejected SPI/common/protocol cases. No simple-name
  exemptions were added.
- The three package-structure count gates were removed with justification, and the
  retained naming checks still enumerate all discovered packages; the number of
  test items therefore decreased, without interpreting the reduction as a defect
  or a reduction in behavior tests.
- Structural comparison actually parsed the corresponding file sets across four
  worktree scans and two baseline scans; scans with warnings returned failure as
  expected and were not treated as CI passing.
- The first full module test failed because a subprocess used system Python 3.9;
  the generator requires Python ≥3.10. It was rerun with the existing Python 3.12
  without disabling or modifying the test; after cleaning the owning module and
  rebuilding, 78 tests and the required upstream modules passed with zero
  failures, errors, and skips. The package naming check enumerated 454 packages
  (including package-info-only packages), and the type naming check enumerated
  3,029 production types.
- The broad static-analysis run encountered three EmptyLineSeparator/LeftCurly/
  RightCurly formatting issues in an existing untracked `ExecutorDrain.java`. That
  file is outside this gate change and was not modified, deleted, or exempted. This
  broad validation cannot be claimed as passed.
- 17 gate-policy regressions passed, and 14 real Checkstyle boundary fixtures passed
  allow/reject validation; workflow YAML, job dependencies, shell syntax, and
  canonical layout validation passed.
- Actual hosted CI, production images, and the full Native/ASan/E2E set were not
  executed for this rules/tool change; those gates remain owned by the existing
  production CI. These results do not constitute release or deployment acceptance.

## Review Method

The 2026-10-04 production-remediation recertification added: Checkstyle's
`com.sun.*` whole-package ban incorrectly blocked the public HTTP/HTTPS API
exported by `jdk.httpserver`. According to the
[official JDK 17 API](https://docs.oracle.com/en/java/javase/17/docs/api/jdk.httpserver/com/sun/net/httpserver/package-summary.html),
the existing IllegalImport still rejects `sun.*`, `jdk.internal.*`, and other
`com.sun.*` packages, distinguishing `com.sun.net.httpserver` from its public SPI
only by the exact package boundary. The real pinned Checkstyle red test rejected
three public APIs; after the correction, 9 allowed and 14 structural/internal-API
rejection fixtures actually passed, including static imports and similar-prefix
negative cases. No source-level suppression or fully-qualified-name bypass was
introduced.

In the same round, SpotBugs 4.8.3 misclassified immutable
`ByteString.writeTo(...):void` as a setter. The sole SHA256 getter retains input
ownership of `ByteString.copyFrom` without adding defensive duplicate copies; the
real canonical digest, exported byte-array mutation isolation, and read-only buffer
regressions passed. The false-positive classification matches only the zero-argument
ByteString return signature of `FunctionBindingDeclarations.declarationSha256()`,
same-named ByteString fields, and `MS_EXPOSE_REP`. The real pinned Filter API
validated this new Match with 1 positive case and 8 negatives for
type/method/field/class/rule changes. This does not eliminate the original
filter's package-level `EI_EXPOSE_REP` exclusion limitation for protocol, nor does
it mean the actual mutability vulnerabilities have been fixed; these existing broad
exclusions must be reviewed separately later.

In the same round, keytool warnings were first revalidated against real input
boundaries: `ProcessBuilder(List)` has no shell, but keytool itself still treats an
alias starting with `-` as an operation option. Before the first-character
constraint fix, the real rejection test failed; after the fix, 4 real certificate/
path boundary tests passed, and CA/store/key permission and container-owner tests
also passed. Then only the COMMAND_INJECTION finding for the private
`TestTlsAuthority.run(List):void` was classified, pinning the absolute JDK tool
path and operation flags while passing all parameters as separate argv entries.
The real pinned Filter API's 1 positive and 7 negatives for class/method/signature/
bug changes all passed; the owning module's 89 tests and the full Checkstyle/
SpotBugs run passed. No new package-level command exemption exists. Based on the
[JDK 17 ProcessBuilder parameter contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/ProcessBuilder.html)
and the [actual command sinks of FindSecBugs 1.13.0](https://github.com/find-sec-bugs/find-sec-bugs/blob/version-1.13.0/findsecbugs-plugin/src/main/resources/injection-sinks/command.txt),
it distinguishes real option injection from hints that only propagate parameter
taint; these source references do not replace actual boundary tests.
The 4 mutable-representation warnings for ResultBatch were fixed as real ownership
defects without new exclusions.

Of the 9 subsequent REST findings, 6 metadata null boundaries and 2 broad catches
were corrected against real contracts. The only PREDICTABLE_RANDOM classification
is `RetryPolicy.delayForAttemptWithJitter(int):Duration`: its only caller uses it
for retry delay and does not generate authentication tokens/nonces/identifiers.
After first reproducing and fixing zero-delay, max cap, non-finite multiplier, and
Long.MAX_VALUE upper-bound overflow, bounded jitter was retained; the detector is
not bypassed via SecureRandom performance cost or a changed call form. The real
pinned Filter API validated this new Match's 1 positive and 7 negatives for
owner/method/signature/bug changes, and other random-number uses still receive the
full security check. The actual REST owning/consumer gates must still pass
separately.

The 19 Paimon warnings about borrowed Catalog/Table/FileIO/registry service
handles were classified only by complete constructor/getter signatures, field
names/service types, and a single EI rule; borrowed services were not called
immutable DTOs. The real pinned Filter API validated 1 positive and 9 signature/
field/type/bug-change negatives per item. The actual mutable split lists, schema
mappings, and evolution lists were fixed as ownership defects without DTO
array/collection exemptions; the 45 most recent regressions and Paimon's 369
strict regressions passed. This result does not prove Connector exit or
Coordinator execution routing are closed.

The 11 JDBC pass-through SQL overload delegations were classified only by complete
Class/Method parameter/return signatures and SQL_INJECTION_JDBC, not by treating
"the SQL parser can parse it" as safe. Each Match's 1 positive and 7 change
negatives were revalidated by the same pinned Filter API; the full filter still
does not exclude PreparedStatement.executeQuery concatenation warnings or
Boolean-null warnings. The 11 new real HTTP raw-forwarding and template-binding
items passed; the peer verified the real raw text and rejection response, without
posing as Coordinator or Native SQL execution, so filter tests cannot imply
end-to-end success. SQL injection through arbitrary Number/BigDecimal and temporal
subclasses in Prepared was reproduced as 5 failures with public setObject; the fix
uses closed numeric types and standard typed temporal values, without adding an
exclusion for this real vulnerability. Logs:
`/tmp/pangrova-production-sdk64-jdbc-forward-filter-boundaries-r1.log`,
`/tmp/pangrova-production-sdk64-jdbc-prepared-literal-red-r1.log`.

After PreparedStatement shared the single actual execution owner, two of the
original six identity warnings disappeared because duplicate fields were deleted,
not because excluded dead code started working. The remaining four JDBC getters
are classified by exact Class, complete method signature, field name/type, and a
single EI_EXPOSE_REP rule; they return the real connection, statement, and current
ResultSet that produce the handle and cannot be copied to another resource owner.
The pinned Filter API passed 1 positive/12 negatives for each isolated and full
filter; the full filter still rejects the deleted Prepared getter warnings.
Log: `/tmp/pangrova-production-sdk64-jdbc-identity-filter-boundaries-r1.log`.
The 4 new real HTTP/Flight identity and close items plus the full client r13 gate
passed: 2437 tests with zero failures, errors, and skips, full Checkstyle/SpotBugs
0, 4 minutes 06 seconds, log
`/tmp/pangrova-production-sdk64-client-full-gates-r13.log`. This excludes the
Paimon work in progress and does not prove thread safety, remote-task drain, or
full end-to-end acceptance.

1. Export each module's production Java source from `git ls-tree -r --name-only
   <ref>` and read blobs with `git cat-file --batch`; for the worktree set, use
   `rg --files -g '*.java'` and filter for `/src/main/java/`.
2. Create an isolated Maven project using the Checkstyle plugin and engine pinned
   by the parent POM. Configure each module source root as a sourceDirectory and
   enable only FileLength, MethodLength, ParameterNumber, CyclomaticComplexity,
   NestedIfDepth, and NestedTryDepth.
3. Run the original 500/50/8/7 and proposed 800/100/15/10 thresholds on exactly
   the same source file set; keep the switch counting and nesting parameters. Add
   or remove the same SuppressionFilter in turn, parse checkstyle-result.xml, and
   confirm parsed_files matches the input file count.
4. Do not reduce counts by modifying business source, adding exemptions, or
   filtering failure reports. Record the commit baseline and worktree separately
   and do not attribute uncommitted existing changes to this policy.
5. Use `.pangrova/scripts/tests/checkstyle-boundaries-test.py` to recheck the
   current gate's actual allow/reject behavior, and `test_gate_policy.py` to
   recheck trigger scope and retained constraints.

Official references: [Maven Checkstyle input scope](https://maven.apache.org/plugins/maven-checkstyle-plugin/check-mojo.html),
[SpotBugs check behavior in the verify phase](https://spotbugs.github.io/spotbugs-maven-plugin/check-mojo.html),
[loop nesting check](https://checkstyle.sourceforge.io/checks/coding/nestedfordepth.html).
These references explain tool semantics; they do not replace repository execution
evidence.
