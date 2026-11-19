# Contributing to Pangrova

Contributions, bug reports, and documentation fixes are welcome. Participation is
covered by the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities using
[SECURITY.md](SECURITY.md), rather than a public issue.

## Development setup

Use JDK 17, Maven 3.9+, and Python 3.10+. The pinned patched Iceberg SDK also
requires Gradle 8.14.4. Native builds require CMake, Ninja, a C++20 compiler and the
platform prerequisites reported by the native bootstrap. Docker is required for
container tests; a matching native bundle is required for Worker query execution.

```bash
git clone https://github.com/pangrova-io/pangrova.git
cd pangrova
./pangrova-worker-native/bin/setup-native-build-env.sh --check-tools
.pangrova/scripts/build-iceberg-sdk-candidate.sh
./pangrova-worker-native/bin/build-native.sh
mvn clean install -DskipTests
```

The Iceberg script builds checked upstream sources and patches into Maven Local.
Set `GRADLE_BIN` to select Gradle 8.14.4. The native build includes Lance and JNI
packaging. See the [Developer Guide](docs/contributing/developer-guide.md) for
platform setup and the [repository guide](docs/contributing/repository-guide.md)
for directory and script ownership.

## Making a change

1. Search [existing issues](https://github.com/pangrova-io/pangrova/issues) and explain
   the problem or intended behavior.
2. Fork the repository and create a focused branch from `main`.
3. Inspect the implementation, callers and tests. Preserve unrelated changes.
4. Make the smallest root-cause change; update tests and documentation for changed
   behavior.
5. Run the checks appropriate to the change and open a pull request with results
   and any unexecuted checks or known limitations.

Prefer Conventional Commit messages such as `fix(planner): preserve null ordering`.
There is no required local Git hook or mandatory commit-language rule.

## Fix and review workflow

Use this loop for maintenance fixes. Each step should leave evidence that a
maintainer can review without relying on another person's checkout.

1. **Find and track the problem.** Check current `main`, search existing issues
   and PRs, then write the actual and expected behavior, affected versions,
   and the smallest safe validation case in an issue. Link an existing issue
   rather than duplicating it. Use [SECURITY.md](SECURITY.md) for potential
   vulnerabilities; do not put sensitive details or customer data in public issues.
2. **Fix and open a draft PR.** Use a focused branch, fix the root cause, and add
   regression coverage. Link the issue with `Refs #N` for partial work, or
   `Closes #N` only when the fix fully meets the issue's acceptance criteria.
3. **Review and validate the final change.** In a separate review pass, trace
   affected callers and check failure behavior, cleanup, compatibility, and
   the blast radius. The author runs the applicable checks on the final change,
   and records the commands and what passed, failed, or did not run. Distinguish
   tests with fakes, full runtime checks, and CI; one is not evidence for another.
4. **Merge only after the applicable checks pass.** Keep incomplete work as a
   draft. Verify the checks correspond to the final PR head and target branch,
   required checks and reviews are satisfied, and there are no known blockers.
   Do not bypass branch protection. If the PR head or relevant base behavior
   changes, repeat the affected checks.
5. **Verify the outcome.** Record the merge commit, inspect post-merge checks
   and confirm the issue state. Keep unresolved acceptance criteria open.
   Merge does not itself authorize or validate a release or deployment.

For readiness work, keep evidence for the exact release candidate and target
workload. A clean-host Iceberg smoke proves only the documented trial. Track
multi-node correctness and isolation, representative load and tail latency,
failure recovery, and upgrade/rollback separately against the
[production deployment checklist](docs/deployment/production-checklist.md).
Label each capability as passed, failed, or unverified; local success and
green CI alone do not establish production readiness.

## Standards and verification

[AGENTS.md](AGENTS.md) is the policy entry point. The canonical rules live in
[.pangrova/rules/](.pangrova/rules/):

- [Development](.pangrova/rules/development.md): validation selection and failure triage.
- [Architecture](.pangrova/rules/architecture.md): Java/native boundaries and dependencies.
- [Code standards](.pangrova/rules/code-standards.md): naming, structure and JNI conventions.
- [Testing](.pangrova/rules/testing.md): test infrastructure and isolation.
- [Build pipeline](.pangrova/rules/build-pipeline.md): native, packaging and production regression.
- [CI gates](.pangrova/rules/ci-gates.md): check ownership and release requirements.

Use constructor injection, immutable data structures, explicit ownership, and
structured logging. Keep SPI/protocol independent of implementation modules.
Never hand-edit generated protocol sources, disable tests, or hide failures with
fallback behavior. Tests use real collaborators, small fakes, TestContainers and
jqwik; mock frameworks are not used.

Start with a focused check, then broaden according to the development rule:

```bash
# Affected module and its prerequisites
mvn -q -pl pangrova-coordination/pangrova-coordination-control -am test

# Native tests after building and packaging the matching library
python3 pangrova-worker-native/bin/run-native-tests.py

# Website/docs changes
cd website
pnpm install --frozen-lockfile
pnpm run typecheck
pnpm run build
```

Use Node.js 22 and pnpm 10 for the website, matching CI. Full production regression
is required by CI for runtime/build/test changes and releases. Documentation and
ordinary tooling changes use their own checks; see the development rule for the
exact scope. Do not treat a successful compile as runtime validation.

## License

Contributions are licensed under the [Apache License 2.0](LICENSE). Include the
origin and applicable notices when introducing third-party material.
