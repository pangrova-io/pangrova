# AGENTS.md

Repository guidance for coding agents. This file is the entry point;
`.pangrova/rules/` is the single source of architecture and development policy.

## Project

Pangrova is a distributed SQL query engine for lakehouse architecture. It combines
Java coordination and Calcite planning with C++20 vectorized execution through JNI.
Respect the module and Java/native boundaries in `architecture.md`.

SPIs may precede implementations. Before changing a contract, verify its callers,
implementation, wiring, and tests.

## Rules

Read only the rules relevant to the task:

- `architecture.md`: module boundaries, execution routing, and wiring.
- `code-standards.md`: Java, C++, dependencies, and JNI conventions.
- `development.md`, `testing.md`, `build-pipeline.md`, `ci-gates.md`, and
	`workflow.md`: implementation, validation, and CI requirements.

Executable gates live in `.pangrova/scripts/`. Verify the canonical rule layout with
`.pangrova/setup.sh --check`.

## Environment

Use JDK 17, Maven 3.9+, Docker for container tests, and CMake with a C++20 compiler
for native work. Do not commit secrets or machine-specific state.

## Workflow

1. Inspect the target, callers, and nearest tests; preserve unrelated worktree changes.
2. Make the smallest root-cause change consistent with existing abstractions.
3. Add or update tests for changed behavior; do not disable or weaken tests.
4. Run the closest check first, then the broader validation required by the rules.
5. Select validation using `development.md`. JNI, protocol, native runtime, and
	execution-path changes require the affected Java/native integration chain in
	`build-pipeline.md`; full production regression remains a CI/release gate.

Never hand-edit generated protocol sources, hide failures with fallback behavior,
or revert user changes outside the task.

## Documentation

Update the relevant `docs/` content when behavior changes. Update `.pangrova/rules/`
only when architecture constraints or repository policy changes.
