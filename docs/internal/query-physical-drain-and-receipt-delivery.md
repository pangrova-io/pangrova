# Query physical closure and terminal receipt delivery

Historical checkpoint: the original implementation below preceded its packaged
Native validation. Current modularization changes and their exact validation
scope are recorded in [the implementation ledger](project-structure-refactoring/domain-modularization-progress.md).
Neither document claims final production or HA closure for the entire project.

## One execution ownership authority

`TaskIdentityRegistry` retains the original canonical request, issued attempt,
actual dispatch target, transport-entry boundary and accepted execution-drain
receipt. Planning/submission and task publication use the same query issuance
gate. Sealing prevents new identities/transport entry; already entered operations
must exit. Registry locks cover only local metadata, never HTTP, JNI, connector
cleanup or journal I/O.

`org.pangrova.query.api.lifecycle.QueryOperation` is the caller-facing ownership
handle. `CoordinatorTaskTracker` creates it only after admission to the original
issued scope; `QueryIssuanceOperation` is private to its owner package. Submission,
state transitions and deferred terminal work use the contract. The Connector
operation contract remains separate and shares the same actual scope owner.
Repeated/concurrent close releases one operation at most once. Logical cancellation
does not authorize early close, and a notification failure does not reopen the
operation or permit a query-ID lookup to substitute a new scope.

`QueryManager` owns a submission operation before leaving its local admission
lock. `QueryDispatcher` additionally owns its real frontend operation through
planning and dispatch. An ambiguous entered HTTP submission requires the existing
exact admission-seal/cancellation path and actual receipt, not a failed Future,
HTTP cancellation ACK, empty task map or later WorkerId lookup.

Actual receipts fan out to all registered observers, preserving speculative
ownership callbacks. Logical task timing cleanup must not delete issuance.
Compacting an exited task drops its plan/Worker inventory while retaining the
exact exit identity needed to acknowledge a lost terminal response.
If a speculative loser was explicitly retired from its exact drain receipt
before a delayed terminal report arrives, the Coordinator may acknowledge that
same Worker/attempt/receipt without reopening its request or publishing output.
An unretired task still needs its completed report publication before a retry
can receive this metadata-only acknowledgement.

## Logical completion versus physical completion

FINISHED/FAILED/CANCELLED remain client-visible logical states. They do not
refund query admission, release throttling permits, decrement the active physical
query count or satisfy `awaitAllQueries`. A rejected admission can publish FAILED
even if HA ownership acquisition failed; acquired reservations remain cleanup
obligations.

`QueryTerminalCleanup` has one actual in-flight cleanup owner per query and bounded
backoff retries. Successful queries wait for real result consumption or expiry;
they are not DELETE-cancelled to truncate a consumer. Failed/cancelled queries use
the existing exact issued-attempt seal path. Physical completion requires:

1. Issuance sealed, submission/frontend operations exited, and every known issued
   attempt backed by validated execution-drain evidence.
2. Actual connector scopes, remote shuffle cleanup and HA/split owners closed.
3. Journal cleanup and exact admission/throttle release succeeded.
4. Issuance retired before the once-only physical flag/count decrement.

Failures preserve the exact cleanup obligation. Retention cannot remove a query
whose physical cleanup is pending. Journal replay contains logical state events,
not full original requests: recovered issuance is explicitly unresolved and may
not infer absence of Native owners from an empty in-memory map.

The actual JDBC purge deletes task/query rows in one transaction. A second-table
permission failure rolls the first deletion back and propagates its SQL cause;
closed journals explicitly reject purge. Recovery does not independently purge
on Worker unreachability. Journal asynchronous queue durability, closed append
and close/flush ownership are not validated by this purge-only correction.

## Worker release and metadata ACK

Result EOF/expiry cleanup first invokes the existing actual Native/resources
release. Only its successful checkpoint supplies execution-drain evidence. A
later FINISHED report preserves the full Native terminal receipt, including its
actual coverage, usage and final statistics. Unsuccessful reports retain their
classified original failure; absence of statistics does not invent work or prove
that no Native owner existed.

`TaskTerminalPublication` retains the first actual post-release envelope and
sequence. Retries reuse that object and canonical bytes; concurrent callers share
the in-flight delivery fact. Preparation/delivery failures remain retryable.
`RemoteTaskStatusReporter` accepts only a real HTTP 200 response with the exact
task ID and `CONTINUE`; empty JSON, stale response or exhausted retry budget is
not success. A cleanup sweep retries a released-but-unacknowledged task without
waiting for its old retention interval, and removes TaskInfo only after the
actual release/publication callback succeeds. Bulk release tries all owners and
preserves failures.

Execution authorization deadlines are unchanged. The metadata-only policy admits
new Worker retry rounds for 360 monotonic seconds from first prepared release
receipt. Each round is bounded by five 30-second requests and 1/2/4/8-second
backoffs (165 seconds). Coordinator compact ACK facts have a fixed 525-second
monotonic retention from first accepted receipt; retries do not slide it. The
issued authority is capped at 50,000 query scopes and 50,000 task identities.
Undrained scopes are never TTL-purged. Capacity exhaustion is explicit, not a
discard of live ownership. Expired receipt delivery raises an error and retains
the owner; it does not extend execution privileges or recover publication rights.

## Validation boundaries and remaining gates

Protocol/issuance tests using generated receipts test strict metadata contracts,
not Native execution. The actual OutputTaskMemoryReleaseTest follows a real Task,
Flight EOF and Native release and additionally checks the later full receipt and
same-envelope retry after an assertion-observer failure. That observation boundary
does not constitute a real HTTP ACK or independent-process E2E.

The 17 query-terminal scenarios now have source migrations without the old
anonymous dispatcher or `FailOnceFinishedJournal`:

- Five logical transition cases remain in `QueryManagerOnAllTasksFinishedTest`.
- Six retention/publication/backoff/concurrency/stale-ticket/stopped-scheduler
  cases use real frontend operation ownership in `QueryTerminalFrontendOwnershipTest`.
- Journal append rejection and timer isolation use real PostgreSQL permission
  errors and a held transaction row lock in `QueryTerminalJournalIntegrationTest`.
- Permit retention during a terminal journal failure uses actual SQL/JNI exit
  evidence in `QueryTerminalJournalPermitIntegrationTest`.
- FINISHED/FAILED/CANCELLED effects and permit retry cases use actual SQL/JNI,
  accepted exit receipts and held frontend ownership in
  `QueryTerminalNativeOwnershipIntegrationTest`.

The pure control-plane cases issue no Worker task and do not claim Native exit.
PostgreSQL committed-write observations replace invented callback counts: an
INSERT denied by the real database is uncommitted, retry commits once, and
physical retry does not append another logical terminal event. CANCELLED also
has the existing asynchronous state-listener event, separate from its synchronous
terminal event. The old assertions refunding permits/active count while cleanup
still failed are corrected to retain those resources until actual physical closure.
The Native fixture preserves actual result values, classified divide-by-zero and
exact task exit receipts, not a synthesized task/report.

Reporter migration uses actual Coordinator HTTP plus transparent network fault
boundaries, not canned successful responses. Source migration and same-source
clean compilation/JNI/E2E remain unverified at this checkpoint; test counts are
not proof of preservation or passage.

Durable Worker receipt outbox, HA recovery of exact issued requests/receipts and
backend-issued Worker process incarnation remain mandatory production gates.
In-memory finite ACK retention does not solve restart/HA. Runtime CPU/scan quota
charging and existing synthetic in-progress TaskStats are separate unresolved
production facts; physical-count correctness does not validate those metrics.

## 超时调度与实际查询 owner

`QueryTimeoutScheduler` 位于 Query Core，只负责周期触发、截止时间计算及其线程的退出屏障。
Server 显式注入 Query API 制品中的 `QueryTimeoutHandler`，由实际 `QueryManager` 实现。
没有未设置 callback 时直接修改状态机的备用路径，也不允许启动后替换 handler。

截止时间包含边界，基于查询创建时间，因此排队时间仍计入超时。QueryManager 对实际观察到的
状态实例尝试 FAILED 转换；并发完成或取消已经获胜时，不发布超时错误。获胜的超时继续走
原 journal、审计、任务取消与终态清理路径，不按 query ID 重新查找另一个生命周期。
重复扫描不会重写已终态查询的错误。

超时不构成物理退出证据。仍在运行的 frontend/metadata/native 操作继续持有其原始 handle，
ReaderPin、准入与执行资源只能在真实 drain 后释放。调度器关闭会等待当前 handler 回调实际
退出；正常进程停机仍会拒绝把未释放的查询资源当作已经关闭。
