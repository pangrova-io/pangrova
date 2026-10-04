## Change Type

- [ ] Bug fix
- [ ] New feature
- [ ] Refactoring
- [ ] Documentation
- [ ] Tests
- [ ] **P0 fix** (critical correctness, safety, or security)
- [ ] Other

## Description

<!-- Briefly describe what this PR does -->

## Related Issue

<!-- Related issue number, e.g., Fixes #123 -->

## Changes

<!-- List the main changes -->

-

## Testing

<!-- Describe how to test these changes -->

- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing done

## Execution Evidence (required for non-trivial fixes)

<!--
    For any non-trivial change (bug fix, behavioral change, performance work),
    attach concrete evidence the new code actually ran the intended path.
    Trivial doc/typo/formatting-only PRs may write "N/A".
    Pick AT LEAST ONE; multiple is better. Paste output inline or link to CI run.
-->

- [ ] **Trace / log line** showing the new code path firing (paste excerpt, include event/correlation id)
- [ ] **Metric delta** (counter, histogram, gauge) before vs after the fix (paste numbers or link Grafana/JSON)
- [ ] **Test output** for a new test that fails on master and passes here (paste `mvn -Dtest=... -pl ...` summary)
- [ ] **Benchmark result** for perf work (paste JMH/TPC-H output diff)

**Evidence paste:**

```
<!-- paste log/metric/test output here, or a link -->
```

## P0 Fix — Full-Repo Grep Evidence (required if "P0 fix" checked above)

<!--
    Every P0 fix must demonstrate the same defect is not repeated elsewhere in
    the codebase. Run a grep pattern that catches the *shape* of the bug (not
    just the single line you fixed). Provide the command, hit count, and relevant
    examples below; link or attach complete output when it is long.

    Examples:
      - Bug: `value.toString()` used as Bloom hash input
        Grep: `git grep -n "value\.toString()" -- '*.java' | grep -i bloom`
      - Bug: `Thread.sleep(...)` in scheduler retry loop
        Grep: `git grep -nE "Thread\.sleep\(" -- 'pangrova-*/src/main/java/**/*.java'`
      - Bug: missing fencing token on metadata write
        Grep: `git grep -n "metadataStore\.write" -- '*.java'`

    Group equivalent hits and explain which are already correct, fixed here,
    or tracked in a follow-up issue. Do not paste repeated full logs.
-->

**Grep command:**

```
<!-- e.g.: git grep -nE "Thread\.sleep\(" -- 'pangrova-*/src/main/java/**/*.java' -->
```

**Hit count, relevant examples, and full-output link:**

```
<!-- paste hit count and relevant samples; link complete output if long -->
```

**Triage by defect pattern:**

<!-- Classify each group: FIXED (this PR) / CORRECT (different context) / FOLLOWUP (#issue) -->

-

## Distributed Correctness

<!-- REQUIRED for production/config/build changes in coordinator, worker,
     shuffle, or metadata modules. Documentation and unrelated test changes are exempt.
     Check applicable risks when distributed behavior changes; otherwise write
     "N/A: <reason>". Do not manufacture irrelevant distributed evidence.
     See docs/distributed-correctness-checklist.md for detailed examples. -->

- [ ] **Network partition behavior**: Described how the system behaves if a network partition occurs during this operation (e.g., coordinator cannot reach workers, metadata store is unreachable)
- [ ] **Dual-leader safety**: Confirmed that concurrent execution on multiple coordinators (split-brain) cannot cause data corruption or silent inconsistency
- [ ] **Crash recovery**: Verified that if any node crashes mid-operation, the system recovers to a consistent state (no partial writes, no orphaned resources)
- [ ] **Slow-node tolerance**: Confirmed that a slow or unresponsive node does not block the entire operation indefinitely (timeouts, speculative execution, or fallback paths exist)
- [ ] **Clock drift / monotonicity**: Verified that the operation does not assume synchronized wall-clock time across nodes, or that any wall-clock use tolerates skew up to the configured drift bound (see `docs/architecture/clock-assumptions.md`)
- [ ] **Restart idempotency**: Verified that restarting a coordinator/worker mid-operation does not duplicate side effects (writes, commits, notifications) and any in-flight tasks resume or fail cleanly

<!-- For each checked item, briefly describe the behavior or mitigation: -->

**Partition behavior**: <!-- e.g., "Query fails with COORDINATOR_UNREACHABLE; no partial results returned" -->

**Dual-leader safety**: <!-- e.g., "Fencing token on metadata writes prevents stale coordinator from committing" -->

**Crash recovery**: <!-- e.g., "Task state is persisted before dispatch; coordinator restart replays incomplete tasks" -->

**Slow-node tolerance**: <!-- e.g., "Worker heartbeat timeout triggers task reassignment after 30s" -->

**Clock drift / monotonicity**: <!-- e.g., "Lease renewal uses monotonic clock; wall-clock only used for human-readable logging" -->

**Restart idempotency**: <!-- e.g., "Commit uses (queryId, stageId) dedup key in metadata store; replay is no-op if already committed" -->

## Checklist

- [ ] Code follows project conventions
- [ ] Tests added where necessary
- [ ] Documentation updated (if needed)
- [ ] Commit messages follow conventions
- [ ] No sensitive information exposed
