# Development

Validation is proportional to a change's blast radius. Commands are in
`build-pipeline.md`; test policy is in `testing.md`; CI selection is in `ci-gates.md`.

## Before Editing

- Read the target, its nearest tests and the worktree state; preserve unrelated changes.
- Trace callers before changing a public API, SPI, configuration key, protobuf field
  or JNI signature, and check dependency direction in `architecture.md` for
  cross-module changes.
- Identify the cheapest check that could disprove the change.

## Implementation

- Stay within the requested ownership boundary and use established abstractions.
- Fix causes. Do not hide symptoms with silent fallback, broad exception handling,
  warning suppression, retries or longer timeouts.
- Change both sides of protobuf and JNI contracts together; never hand-edit generated
  sources.
- Add or update tests when observable behavior changes.

## Selecting Validation

1. After the first edit, run the closest compile, test, syntax or lint check.
2. Then run the owning module's suite once; `test`/`verify` already compile.
3. Broaden to dependent modules, or the full reactor, when a shared API, parent POM,
   dependency management, protocol or cross-module behavior changes.
4. JNI, protobuf, Native packaging and execution-boundary changes need Native build
   and tests, fresh packaging, Java consumers and E2E/smoke for affected paths. Native
   memory, ownership, JNI or concurrency changes also need ASan; Java-only concurrency
   uses Java stress/race tests.
5. Run E2E and smoke when startup, planning/execution, transport, serialization,
   Native loading or deployment connectivity can change. Tooling and documentation
   edits, even under a Native directory, need only their own checks.
6. Repeat timing-sensitive tests after concurrency or flakiness fixes.
7. Run the repository gates relevant to changed files. Full production regression is
   a CI requirement for runtime/build/test PRs, protected-branch pushes and releases,
   and a local requirement for broad execution changes or when focused checks cannot
   cover the affected boundaries.
8. A passing full regression already includes E2E and activation smoke; rerun them
   only when their inputs changed, they did not run, or a failure needs diagnosis.

## Failure Triage

- Capture the concrete error and reproduce it with the smallest command.
- Separate change-caused failures from baseline or environment failures with
  evidence; fix only what blocks validation of the requested change.
- Retries must not turn an intermittent failure into a pass. Keep unaffected passing
  evidence and rerun the failed or newly affected scope after a relevant correction.
- Report a missing prerequisite such as Docker as a blocker. Never claim a check
  passed if it did not run.

## Completion

- Review the diff for scope creep, dead code, secrets, machine paths and generated
  files.
- Update `docs/` when APIs, behavior, configuration, architecture or build steps change.
- Run `.pangrova/scripts/clean-dev-artifacts.sh` and confirm
  `.pangrova/scripts/check-dev-artifacts.sh` passes.
- Report changed files, checks executed with results, and verified blockers, linking
  long logs.
