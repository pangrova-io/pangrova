# Workflow — Development Lifecycle

`development.md` is the authoritative validation and failure-triage policy.
`testing.md` defines test infrastructure; `build-pipeline.md` provides build
commands. This checklist adds no unconditional full-project regression requirement.

## Before Editing

- Inspect the worktree; preserve unrelated changes.
- Read the affected implementation and nearest tests. Trace callers before
  changing contracts, configuration, or behavior.
- Check `architecture.md` for module, execution, and Java/native boundaries.
- Choose the smallest useful check and the broader validation required by the
  change's impact, using `development.md`.

## During Editing

- Keep one responsibility per class and coherent operations per method.
- Apply the structural backstops and naming guidance in `code-standards.md`.
- Update both sides of JNI/protobuf contracts; regenerate protocol sources.
- Test changed behavior and important failure/resource/concurrency paths. Existing
  tests may cover a pure refactor; no per-class or per-method test-file quota applies.
- Remove registrations, callers, configuration, and docs belonging exclusively to
  a deleted feature. Unimplemented SPIs may precede providers when explicitly
  experimental; production execution paths must be wired and exercised.
- Migrate deprecated APIs when needed for this task; do not expand a small change
  into unrelated cleanup.

## Verification and Completion

- Run the focused check, then the owning module and affected consumers as required
  by `development.md`. A Maven `test`/`verify` invocation already includes compilation;
  a separate compile pass is useful for diagnosis, not mandatory duplication.
- Runtime JNI/protocol/native/execution changes require fresh native packaging,
  affected Java/native tests, and relevant E2E/smoke coverage. Native docs or tooling
  edits require their own syntax/unit checks, not a native runtime rebuild.
- Full production regression remains required in CI and before release. Local
  full-chain validation is required for broad execution changes or when focused
  evidence cannot establish the affected boundaries.
- Update docs for changed behavior/API/configuration. Update rules only for policy
  or architecture changes. Retain ≥60% changed-line coverage for production Java.
- Report checks and results accurately, including verified blockers. A failed or
  unexecuted check must never be reported as passing.

## Failure Resolution

Capture the failure and reproduce it with the smallest relevant command. Fix the
root cause, run the focused regression, then rerun the broader required check.
Do not disable tests, swallow errors, suppress correctness warnings, or introduce
fallback/retry/timeout changes solely to conceal failures.

Separate change-caused failures from baseline/environment failures using concrete
evidence (logs, unchanged failing sources, CI results, or an isolated baseline
checkout). Stashing a shared dirty worktree is not required. Fix change-caused
failures; record unrelated failures and their impact on validation without taking
ownership of unrelated remediation. Never claim full validation while blocked.
