# Workflow — Development Lifecycle

> Every code change task MUST follow this lifecycle. Checkstyle/SpotBugs and the
> scripts under `.pangrova/scripts/` enforce what can be enforced mechanically; every
> other row below is a manual check and this document is the complete reference.

## Phase 1: Pre-Change (开发前)

Before writing ANY code, you MUST complete:

| # | Check | When | How |
|---|-------|------|-----|
| 1 | **Locate files + callers + callees** | Always | Read source; LSP `findReferences` / `incomingCalls` |
| 2 | **Check module dependency direction** | Cross-module changes | Verify against `architecture.md` Module Dependency Matrix |
| 3 | **Confirm DML execution path** | DML-related changes | Check `architecture.md` DML Routing Table |
| 4 | **Build test map** | Always | Identify unit/integration/property/E2E tests for affected files |
| 5 | **Check SPI stability constraint** | SPI changes | `@Stable` types cannot have breaking changes |
| 6 | **Verify no conflicting conventions** | Always | Check rules/memories for architectural constraints |
| 7 | **Know rollback command** | Always | Record the compile/test entry point before starting |

**Stop signals** — gather more context if any apply:
- Starting edits based only on a filename fragment
- Modifying a method signature without reading its callers
- Adding a dependency without checking parent pom's `dependencyManagement`
- Doing global refactor on an unfamiliar module without SearchAgent

---

## Phase 2: During-Change (开发中)

| # | Check | Scope | Reference |
|---|-------|-------|-----------|
| 1 | Naming compliance (P0/P1 rules) | All new types/methods | `code-standards.md` Naming |
| 2 | Quality gates (lines/branches/params) | All new code | `code-standards.md` Quality gates |
| 3 | Package naming matches directory | New Java files | Enforced by javac; `OuterTypeFilename` checks the file name |
| 4 | Dependency version uses property | `pom.xml` changes | Manual verification against parent `dependencyManagement` |
| 5 | No banned mock frameworks in tests | `*Test.java` | Manual verification — `testing.md` Banned frameworks |
| 6 | JNI signatures match both sides | JNI changes | Manual verification |
| 7 | Protobuf changes regenerated both sides | Proto changes | `.pangrova/scripts/proto-descriptor-diff.sh` |
| 8 | Thread safety annotations | Shared mutable state | `@ThreadSafe`/`@GuardedBy` |
| 9 | Error handling — no swallowing | All catch blocks | ErrorCode + context required |
| 10 | Test completeness | All new code | Every new class/method has tests; every error path tested |
| 11 | Deprecated cleanup | All files in scope | Delete @Deprecated code, migrate callers, no @SuppressWarnings("deprecation") |

> **License headers**: not required per file. The root `LICENSE` and `NOTICE` are
> authoritative — see the rationale block in `config/checkstyle/checkstyle.xml`.
> Files that already carry an Apache header keep it.

---

## Phase 3: Post-Change — Verification (开发后验证)

### Java Changes (MANDATORY)

```bash
java -version
mvn -version

# Compile verification
mvn -q -pl <module> -am compile test-compile

# Test verification
mvn -q -pl <module> -am test

# Cross-module: full install first
mvn -q -DskipTests install
```

- `BUILD FAILURE` or test failures = task NOT complete
- Never use `-DskipTests` / `@Disabled` / `@Ignore` to hide failures

### C++ Changes (MANDATORY — Full Chain)

Native 修改触发**完整验证链** — 执行 `build-pipeline.md` §Full-Chain Build Order 的全部 8 步。

关键要点：
- 禁止跳过 Step 4 repackage（导致 Java 加载旧 native lib）
- 禁止仅跑 pangrova-worker 测试而跳过 E2E（未验证端到端 JNI 路径）
- 把 `UnsatisfiedLinkError` 当 "环境问题" 忽略 = 反模式

### Cross-Language Changes

- [ ] JNI method signatures match on both sides
- [ ] Protobuf schema changes regenerated on both sides
- [ ] Arrow RecordBatch schema aligned on both sides
- [ ] Native library re-packaged to Java resources
- [ ] `NativeLibraryLoader` can load the new version

---

## Phase 4: Post-Change — Integrity (完整性)

| # | Check | When | Standard |
|---|-------|------|----------|
| 1 | Public API Javadoc | New public methods | `@param`, `@return`, `@throws` |
| 2 | SPI annotations | New SPI types | `@Stable` / `@Experimental` / `@Internal` |
| 3 | Configuration docs | New config keys | Document in module README or CONFIG.md |
| 4 | Changelog entry | User-facing changes | `CHANGELOG.md` |
| 5 | Full-chain connectivity | Major changes | `mvn -DskipTests install` + native build + JNI verify |
| 6 | E2E + Smoke tests | Scope warrants it | `pangrova-test-e2e` + `ActivationSmokeE2ETest` |
| 7 | Diff coverage ≥60% | PR submission | `diff-coverage-check.sh` |
| 8 | **Documentation sync** | Behavior/API/arch changes | `.pangrova/rules/`, `docs/` |

---

## Phase 5: Full-Project Regression (全项目回归 — MANDATORY)

> **Every modification that touches source code MUST pass full-project regression before declaring completion.**
> This is NOT optional. This is NOT "if time permits". This is the quality floor.

Execute the **Full-Chain Build Order** defined in `build-pipeline.md` §Full-Chain Build Order — all 8 steps, in order, all must pass.

- If ANY step fails → fix it in the same session. Do NOT skip it. Do NOT claim "unrelated".
- Pre-existing failures are YOUR responsibility when you encounter them.
- `UnsatisfiedLinkError` and `SIGSEGV` are real failures — do NOT treat as "environment issues".

---

## Phase 6: Connectivity Verification (贯通性验证 — MANDATORY)

> **Every add/modify/delete MUST verify the full wiring chain is intact.**
> No dangling interfaces. No orphan implementations. No dead code. No broken circuits.
>
> Wiring invariants: see `architecture.md` §Full-Chain Connectivity Requirements.

### For Additions (新增功能)

Verify the complete wiring chain exists:
```
Interface → Implementation → Registration/Wiring → Caller → Test → E2E coverage
```

| Check | How |
|-------|-----|
| Interface has implementation | LSP findImplementations on new interface |
| Implementation is wired | Constructor injection or ServiceLoader manifest verified |
| Wiring is invoked | At least one production code path calls the new code |
| Code path is tested | Unit test + integration test exercise the path |
| Full pipeline works | E2E test covers the feature end-to-end |
| JNI bridge (if cross-language) | Java native decl + C++ impl + symbol map + both-side tests |

### For Modifications (修改功能)

Verify all consumers are updated:
```
Changed API → All callers updated → All tests updated → E2E still passes
```

| Check | How |
|-------|-----|
| All callers found | LSP findReferences on every changed symbol |
| All callers updated | Every reference compiles with new signature/behavior |
| Tests reflect change | Test assertions match new behavior (not old) |
| Cross-language sync | Proto/JNI changes regenerated on BOTH sides |
| Config docs updated | If defaults or behavior changed |
| No semantic break | E2E tests validate changed behavior works end-to-end |

### For Deletions (删除功能)

Verify ALL traces are removed — partial delete creates dead code:
```
Delete: Interface + Implementation + Wiring + Callers + Tests + Config + Docs + Proto (both sides)
```

| Check | How |
|-------|-----|
| Interface removed | No dangling SPI definition |
| All implementations removed | No orphan classes |
| All wiring removed | No dead ServiceLoader entries, no dead injection |
| All callers removed | No compile errors (proves no references remain) |
| All tests removed | No test for deleted code |
| Config removed | No orphan config keys |
| Docs updated | No references to deleted features |
| Proto cleaned (both sides) | No orphan proto messages, regenerated Java + C++ |
| Dead code scan | Run `detect-dead-stubs.sh`; 0 findings |

---

## Failure Resolution (根因修复 — Root-Cause Only)

When build/test fails:

1. Parse the failing module, class, missing symbol, or full stack trace
2. Read the failing source and its dependencies
3. Trace the code path — understand WHY it fails, not just WHERE
4. Apply 5-WHY analysis to find the root cause
5. Fix at the root cause level — not at the symptom level
6. Re-run with the **exact same command** — never shrink scope

**Root-cause verification**: After every fix, ask:
- Does this fix address WHY the problem occurred, not just make the error disappear?
- If I remove the fix, does the original error return (not a new, different error)?
- Did I add complexity (null guard, cache, retry, sleep) that a simpler root-cause fix would avoid?

**Prohibited workarounds (all are band-aid fixes):**
- Narrowing `-pl` scope to avoid the failing module
- Adding `-DskipTests` or `@Disabled` to skip failing tests
- Catching exceptions and swallowing them with a TODO
- Claiming "unrelated to this change" without `git stash` evidence
- Treating `UnsatisfiedLinkError` as a "known issue"
- Adding `if (x != null)` guard for unexpected null without finding the null source
- Adding `Thread.sleep()` to mask race conditions
- Increasing timeouts to mask performance regressions
- Adding cache layers to mask algorithmic inefficiency
- Adding `@SuppressWarnings` to mask warnings instead of fixing them
- Increasing heap/memory to mask leaks or excessive allocation

**Pre-existing failures policy**: You touch it, you own it. Pre-existing ≠ acceptable. Infrastructure failures are real failures. No partial green.
