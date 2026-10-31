# Query journal commit and shutdown ownership

Status: finite Java source migration prepared. The parent-thread builder has
verified Metadata PostgreSQL behavior and the broader Metadata test/verify
reactor checkpoints described below. Coordinator closest scopes have also run;
the latest eighteen-class scope passed all 100 tests after the failed assertion
and its root-cause correction recorded below.
This document does not assert final clean whole-chain success, durable
issued-attempt recovery, or a durable Worker report outbox. Native/IDL/SDK inputs
are unchanged by this unit.

## Verified production chain and defects

`QueryRuntimeWiring` creates the sole journal through `QueryStateJournalFactory`;
`QueryBootstrap` retains it and passes it to `QueryManager` and
`TaskStatusReceiver`. Query events originate in `QueryJournalLifecycle`; task
events originate in `TaskStatusReceiver` and `LeafStageDispatcher`. PostgreSQL
uses `JdbcQueryStateJournal`. The previous singleton no-op NONE implementation is
replaced by an instance-owned bounded process-local store. NONE explicitly remains
non-durable and cannot validate PostgreSQL durability.

The pre-change JDBC implementation silently accepted writes after close, dropped full-queue
events, removed a flush batch before acquiring a connection, and discarded failed
SQL batches. Its close marked closed before drain, raced two flush paths, and did
not require the flush thread to exit. The original tables contained ordered row IDs but no
event retry identity. A connection failure cannot establish whether COMMIT
succeeded; retries using a new timestamp or UUID are therefore not idempotent.

The pre-change `TaskStatusReceiver` retained drain proof and could invoke terminal listeners before
its task journal append. Cleanup can consequently delete journal rows before the
same report or an accepted asynchronous event inserts them again. Resource proof
retention and publication completion must remain distinct facts.

The pre-change `PangrovaCoordinator.stop` treated an unsuccessful physical drain as a warning,
then yielded anchors. `HighAvailabilityBootstrap.shutdown` subsequently closed
HA. `CoordinatorBootstrap.shutdown` cleared its started flag before cleanup,
preventing retry; its independent try-all list also closed dependent metadata and
authority after query cleanup failure. `QueryBootstrap.shutdown` did not close
the journal, only requested executor shutdown, and did not await physical query
cleanup. Several `CoordinatorShutdownSequence` helpers swallowed close failures.

## Single event and commit contract

`JournalEventContext` groups immutable event identity, original time and issuing
coordinator. The query/task event records retain all existing payload fields but
consume this one header; old constructors are removed, not forwarded. The
original ordered SQL row ID remains the replay order. V50 adds a non-null unique
event UUID to the two existing tables; no parallel execution authority is added.

Query transitions create and retain their event before the first write. Terminal
completion retains the whole event across cleanup retries. A task report uses
the validated canonical complete attempt and report sequence for deterministic
identity, its producer-frozen timestamp, and the issuing owner rather than the
receiving coordinator. Post-release replay retains the same outer envelope.
SCHEDULED events are frozen in the same issued-attempt owner before append.

An insert conflict is accepted only when every stored field equals that exact
event payload. A different payload with the same ID is an explicit error. An
unknown COMMIT outcome retains its event/batch; only a later same-ID exact-payload
PostgreSQL reconciliation can establish success. This does not infer success
from a transport ACK, a closed connection, or a completed Java future.

The migration preserves both original ordered `id` columns, backfills existing
rows with PostgreSQL's built-in `gen_random_uuid()`, and enforces unique, non-null
event IDs. New rows have no random-ID database default: the application must
provide the already frozen identity. PostgreSQL 16 provides this UUID function
without requiring a new extension.
[UUID functions](https://www.postgresql.org/docs/16/functions-uuid.html).

A confirmed same-ID/payload conflict is distinct from an unknown transaction
outcome. It is rejected without overwriting the existing event; an attempt may
retire that rejected write obligation only after actual transaction rollback is
confirmed. If rollback fails, ownership remains unresolved. Pending valid writes
and ambiguous COMMITs are never discarded to make queue capacity or close pass.

PostgreSQL documents unique-index-backed conflict handling and distinguishes
unknown transaction state on a failed connection. These support idempotent
identity reconciliation, not an assumption that a JDBC exception means rollback.
[INSERT](https://www.postgresql.org/docs/16/sql-insert.html),
[connection status](https://www.postgresql.org/docs/16/libpq-status.html).

## Owner and lifecycle barriers

Journal admission and claims use short locks. SQL, flush-thread join and other
I/O occur outside them. Capacity counts queued, claimed and unresolved events;
full or sealed admission throws instead of pretending success. Exactly one
flush owner retains a claimed batch until commit is established. Errors remain
observable and pending events remain owned for retry.

The intended JDBC responsibility split is one admission/pending owner, one SQL
writer, and one SQL reader consumed by the existing journal facade. Both sync
and async appends enter the same ordered pending queue; sync additionally waits
for that event's actual commit attempt. A failed wait does not remove the event.
Same-ID retry joins the retained owner instead of consuming a second queue slot;
changed payload for an already-pending ID is rejected before SQL. Concurrent
waiters share the actual commit-attempt completion, not an unbounded retained
waiter list. This is a single journal execution path, not a synchronous fallback
behind the asynchronous writer.

The SQL writer uses `INSERT ... ON CONFLICT (event_id) DO NOTHING`, followed by
a new same-transaction `SELECT` comparing every payload field. It neither
updates an existing fact nor requires UPDATE permission from an insert-only
writer role. Read Committed's separate statement snapshot is important here:
the subsequent SELECT can observe the committed conflicting row after the
unique-index conflict has resolved. A conflict is not accepted from the INSERT
update count alone. [Transaction isolation](https://www.postgresql.org/docs/16/transaction-iso.html).

The retained batch owns admitted events until a confirmed commit. Confirmed
rollback of an exact-ID payload conflict may reject only that invalid event;
the other valid events from the rolled-back batch remain pending. Connection
failure, uncertain COMMIT, or failed rollback cannot retire the batch. Replay,
latest lookup and purge also count as actual entered SQL operations, so journal
close cannot let the external DataSource close underneath an active reader.
The close barrier requires both the flush owner's actual final exit and every
entered SQL operation's exit, not completion of the append caller's future.

Incoming report processing acquires a lease in the existing exact issued-query
authority, including cleanup reports after dispatch issuance is sealed. It may
retain authenticated drain proof even if publication fails. Publication and DAG
notification cannot overtake required terminal journaling. Only after the report
operation exits can physical cleanup proceed. Already-retired exact replay only
acknowledges retained compact evidence; it never appends or republishes.

The former `acknowledgesDrain` accepted a retained receipt before required
journaling completed. The replacement registers a required report
publication obligation in the same issued scope before receipt
retention, stays owned across journal failure, and completes only after the
same frozen event succeeds (or the validated settled attempt is explicitly
resource-only). A compact replay ACK is permitted only for that completed fact.
Neither retaining Native exit evidence nor closing the current HTTP handler
proves the required journal operation completed.

Query-state listeners have another admitted-writer race: a non-terminal state is
published before its listeners run, allowing a later terminal callback to purge
before the earlier listener appends. Each actual state transition therefore
acquires the same scope operation before state publication and holds it through
listener exit. Physical cleanup seals new scope operations and verifies existing
operations and report obligations are complete atomically in that authority.
The transition's own lease prevents synchronous terminal cleanup from purging;
the existing cleanup retry proceeds after that lease exits. No listener performs
SQL or waits for another operation while holding the scope lock.

Purge freezes new writes for the same active query scope, drains every accepted
queued/claimed/synchronous event, then deletes both tables in one transaction.
The bounded lifetime of that seal must use the existing issuance barrier; an
unbounded tombstone set or removing a marker before writers exit is not valid.
Cross-process stream reopening needs durable scope authority and is not proven
by an in-process operation lease.

The store itself excludes concurrent admission while a per-query purge claim is
active and drains all writes admitted before that claim. It does not retain an
unbounded forever-closed query set. After the claim exits, arbitrary new SPI
callers could write that ID again; the production guarantee comes from the sole
issued/frontend/report scope, which is already sealed and cannot create another
producer. The complete production write inventory is `QueryJournalLifecycle`,
`LeafStageDispatcher` and `TaskStatusReceiver`. Retired receipts are read-only;
none of these paths may reopen a retired query. This is a finite-process
guarantee, not a durable cross-process issued-attempt/outbox authority.

Close seals admission, drains accepted writes, joins the real flush thread and
waits for entered synchronous SQL to exit. Only then is it complete. A timeout,
interruption or database error retains the exact owner and outstanding events;
repeated close retries the same obligation. DataSource lifetime is external and
must extend past all journal I/O.

The process shutdown dependency order is:

```text
stop new issuance
  -> retain HTTP report ingress + HA authority + cleanup execution
  -> actual query/task physical drain and report operations exit
  -> journal pending writes commit and journal close
  -> actual owned executors exit
  -> HTTP / HA / execution authority / connector / DataSource close
```

Independent owners are tried even after an independent failure; dependent owners
are not closed while a prerequisite remains active. Failure retains exact refs
and a retryable shutdown phase. Cancelling a scheduled future is not an exit
barrier, and logical query termination does not return physical reservations.

## Required verification

Preserve original record values, ordering, terminal replay exclusion, owner-scoped
replay, other-query isolation and existing native physical-drain assertions.
Add real PostgreSQL permission-failure/recovery, row-lock/purge ordering,
same-ID conflict, full-queue rejection and close interruption/retry assertions.
Use a real PostgreSQL network fault boundary for COMMIT-response loss, then
reconcile the same event and assert one exact row. Pure lifecycle tests may
observe real entered Runnable/Closeable owners but cannot manufacture SQL commit
or Native drain. Latest packaged JNI is required for report/physical lease
integration. Parent-thread builds and tests remain the only validation writer.

For the commit-response fault, a transparent test transport must forward actual
frontend requests to PostgreSQL and observe its real backend `CommandComplete`
COMMIT response before withholding that response and closing the client side.
It must not synthesize SQL results, acknowledge a transaction, or substitute a
DataSource. An independent real connection then verifies the committed UUID and
full payload; retrying that same frozen event must retain exactly one row and
the original ordered row ID. The real PostgreSQL frame boundary is documented
in [message formats](https://www.postgresql.org/docs/16/protocol-message-formats.html).
The relay's real sockets and pump exits are owned and joined by the test; its
shutdown must not leave a fault-injection thread behind. This finite PostgreSQL
fault boundary passed in the Metadata closest scope recorded below; it is not
distributed failover or Worker outbox evidence.

The existing metadata journal's ten integration assertions remain. The source
now uses the actual Flyway chain rather than hand-created shadow tables and adds
five commit/capacity/ownership fault assertions. Event equality tests use the
same explicit immutable header rather than
weakening equality after event identity is introduced. Owner-scope recovery uses
the actual instance-owned journal and exact tracker; seeded journal replay is a
component test, not proof of process failover or recovered Native ownership.

Additional regression boundaries are:

- Same report after lost ACK keeps event ID, producer time and issuing owner;
  journal failure cannot take the compact-ACK path or release query admission.
- A held real PostgreSQL lock delays an admitted event while purge refuses new
  same-query writes; another query remains independent.
- A state transition paused after publication but before its listener exits
  prevents terminal purge until the actual transition operation exits.
- A held actual SQL/flush owner makes close fail observably without closing its
  DataSource; releasing the lock permits the same close obligation to retry.
- Shutdown timeout or report/journal failure retains HTTP ingress, HA authority
  and DataSource so late exact drain receipt and commit can still complete.
- A closed/full journal rejects admission; an async accepted write is retained
  on connection or transaction failure, not logged and discarded.

The unwired `ConsensusStore` bundle and unused `CoordinatorShutdownHelper`
parallel shutdown implementation are approved dead-code removals. Their sole
nine synthetic bundle tests do not provide PostgreSQL/lease/Native coverage.
Actual distributed lease, JDBC registry/journal and used shutdown sequence tests
remain. Git history preserves the removed artifacts.

## Prepared source and verification ledger

`maxResidentEvents` is an explicit sixth `StateJournalConfig` field consumed by
all factories. Its default is 50,000. JDBC counts still-owned event obligations;
NONE counts retained committed process-local facts. This is an event-count bound,
not a byte/OS/process-memory limit. Existing `retentionHours` remains unconsumed;
there is no unproved TTL garbage collection. Cross-kind IDs are scoped by the two
authoritative tables, not a made-up global UUID namespace.

`JournalSessionPayload` uses the existing JSON serializer for exactly the original
queryId/tenantId recovery fields, including quote, backslash and newline escaping.
Planning's `exactSessionConfig` remains independent. `QueryManagerJournalTest`
verifies this against an actually committed PostgreSQL fact, not lookup absence.

The actual shutdown graph now shares `CoordinatorLifecycleAdmission` dependency
steps, `CoordinatorCallbackLifetime` actual finally-exit, and the common
`ExecutorDrain`. Startup is one-shot; failed shutdown preserves exact owners for
retry. Coordinator execution renewal joins its actual executor and synchronous
backend entrants before yielding the process's anchors. Schedule installation
also owns a callback lease, so close cannot overlook a future published late.

The receiver is split by existing responsibilities (sequence ownership, write
commit, speculation, query completion); it consumes the QueryManager's exact
journal instance. The split adds no alternate task/report executor or wire.
Required report publication exits before accepting a sequence. Deferred query
completion threads hold the same issued metadata operation through actual exit.

Run the closest scopes with JDK 17 and the parent-thread builder only:

```sh
mvn -Pfull-regression -pl pangrova-metadata -am -Dtest=JdbcQueryStateJournalIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -Pfull-regression -pl pangrova-coordination/pangrova-coordination-core -am -Dtest=InMemoryQueryStateJournalTest,StateJournalConfigTest,QueryStateJournalFactoryTest,TaskJournalEventContextTest,TaskIdentityRegistryDrainContractTest,QueryTransitionOwnershipTest,QueryTerminalFrontendOwnershipTest,QueryManagerJournalTest,QueryTerminalJournalIntegrationTest,CoordinatorCallbackLifetimeTest,CoordinatorLifecycleAdmissionTest,CoordinatorExecutionAuthorityIntegrationTest,QueryDurableRecoveryWiringTest,JournalRecoveryServiceTest,JournalRecoveryOwnerScopeTest,QueryBootstrapSchedulerExecutorShutdownTest,QueryBootstrapAllocatorCloseTest,CoordinatorResourceReleaseTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Metadata closest scope is **PASS** on 2026-10-03: the parent-thread builder ran
JDK 17 `mvn -B -Pfull-regression -pl pangrova-metadata -am
-Dtest=JdbcQueryStateJournalIntegrationTest
-Dsurefire.failIfNoSpecifiedTests=false clean test`. All 15 tests passed, with zero
failures, errors and skips; the 12-module reactor completed in 25.945 seconds
(test class 11.64 seconds). Evidence is
`/tmp/pangrova-production-journal-commit-owned-pg-nearest-r1.log`. This preserves
the original ten cases and verifies the five additions: exact immutable retry,
sticky changed-payload rejection, actual blocked-commit resident capacity,
held-writer close/retry, and real PostgreSQL COMMIT-response loss reconciled to
the same event and original ordered row ID. The run also compiled actual V50
resources/callers through the real Flyway-backed integration fixture. The log
contains SLF4J/Testcontainers runtime diagnostics; no zero-warning claim is made.

The parent has now executed Coordinator component assertions, including
first-admission failure distinctions, actual JSON-escaping facts and
report/publication barriers. The precise partial results appear below; they do
not establish a passing complete shutdown assembly or JNI/E2E behavior. The
separately verified common helper's six actual-thread tests and client scopes
also do not prove Coordinator assembly or its JNI/E2E behavior.

The first unfiltered Metadata reactor stopped in TestCommon's structural gates;
downstream modules were skipped, so the 15-test PostgreSQL closest result does
not make that full command pass. The finite source correction delegates durable
embedding-registry construction to the existing `QueryRuntimeWiring`, separates
shutdown's independent drain, semantic query closure and dependent retirement
phases without changing their barriers, and moves the four public write tracking,
fencing and recovery contracts into the existing `query.task.write` owner. The
task-reception package now has eight code files; the write leaf has five.
Catalog reconciliation and task-report completion remain with their existing
receiver owner, so no visibility is widened and the write leaf does not depend
on scheduling/reception. All caller imports and the original recovery test move
with the contracts; no assertion or threshold is removed. These structural
changes are source-ready only until the parent reruns the same unfiltered
Metadata reactor and uniform Coordinator compilation.

The same unfiltered reactor's second run eliminated those three structural
failures but stopped at the registry facade's 504-line class limit; Metadata
again remained skipped. The already-existing report publication owner now
contains that registry's same retained `Fact` representation. The sole issued
registry still serializes its mutable state under the same monitor, while the
frozen retry identity and digest are unchanged. No second publication registry,
public API or task file is added. This source correction also requires the same
full reactor to rerun; it is not a test result. These failures and corrections
describe the then-compiled gate checkpoint, not a mandate to undo subsequent
workspace rule/test changes. Current rules use 800/100/15 limits; the unrelated
rule/test edits are preserved and no further structural refactor is made solely
to pursue the old 500-line threshold.

The parent subsequently completed the original unfiltered Metadata `test`
reactor r3: 12 modules succeeded, with 2,631 tests and zero failures, errors or
skips in 8:17 minutes. Metadata contributed 524 tests. Evidence is
`/tmp/pangrova-production-journal-metadata-reactor-unfiltered-r3.log`. This is a
**compiled-baseline checkpoint**, not the final current-source clean gate:
TestCommon reported 86 tests while its compiler reported nothing to compile,
including class files for tests no longer present in the working tree.

The same Metadata selection under `full-regression,coverage verify` completed
with 2,625 tests, zero failures/errors/skips, and 12 successful modules in 8:48
minutes. TestCommon recompiled its 18 current sources and reported 80 tests;
Metadata again reported 524. Evidence is
`/tmp/pangrova-production-journal-metadata-reactor-unfiltered-coverage-verify-r1.log`.
The six-count difference is accounted for by the workspace removal of the five
old `CleanCodeMetricsPropertyTest` tests and the `NamingConvention` change from
three to two tests; this unit did not delete or weaken those tests or rules.
The verify run produced JaCoCo data but did not start from clean execution data,
so its coverage cannot be attributed exclusively to that invocation. A final
clean current-source full run remains mandatory. Neither Metadata checkpoint
executes Coordinator/Worker report delivery or matched packaged JNI.

After the latest same-source Native package exists, run the original real
`TaskStatusReceiverJournalTest` three and `RemoteStatusReportDeliveryIntegrationTest`
nine cases, plus actual query Native ownership integration. They remain UNRUN in
this source checkpoint and cannot be replaced with constructed successful ACKs.
The full module/whole-chain gates still apply; closest successes do not establish
production readiness or distributed scale.

The closest report-fact fixture now enters the sole actual publication lease,
marks acceptance and exits it before asserting compact lost-ACK retention. It
also rejects ACK before publication and retirement before that lease's exit;
all original execution-deadline, fixed horizon and undrained non-purge assertions
remain. Generated reports in this fixture are protocol component inputs, not
Native execution evidence. The scheduler shutdown fixture runs an actual owned
callback, then asserts actual executor termination and rejection of new work.
The control transport fixture verifies an actual listening socket before
shutdown and connection rejection afterwards; the allocator helper verifies
post-close allocation rejection. The parent executed all fourteen strengthened
assertions successfully in the consumer/Coordinator r3 checkpoint below.
Listener closure is not proof that a retained Flight reader or Native task has
drained.

The parent ran the same 29-module consumer/Coordinator closest selection three
times. r1 stopped at production compilation because `CoordinatorHttpServer.stop`
declares a checked exception; shutdown now preserves that cause in its existing
failure aggregation rather than treating it as success. r2 passed production
compilation but stopped at test compilation on five obsolete `ExecutorDrain.close`
calls. The two fixtures now consume the sole `shutdownAndAwait` API with the
unchanged 3,000 ms bound and original callback/interrupt assertions. r3 passed:
60 selected tests overall, including 26 Coordinator tests and all fourteen
strengthened assertions (registry 7, scheduler 3, allocator 4), zero skips,
29 successful modules, 40.392 seconds. Evidence is
`/tmp/pangrova-production-sdk54-actual-function-catalogue-consumers-coordinator-nearest-r3.log`.
`QueryTransitionOwnershipTest` and `QueryDurableRecoveryWiringTest` compiled in
that run but were not selected for execution.

On 2026-10-04 the parent expanded to the eighteen-class Coordinator
journal/lifecycle and real PostgreSQL closest scope listed above. r1 is **FAIL**:
100 tests, 99 passed, one failed assertion, zero errors/skips. Evidence is
`/tmp/pangrova-production-coordinator-journal-lifecycle-pg-nearest-r1.log`.
Both previous `ExecutorDrain` fixtures actually executed and passed (3 and 5
tests). `QueryTerminalJournalIntegrationTest.failedTerminalJournalCanBeRetried`
failed before restoration of INSERT permission because its assertion required
a direct `SQLException` cause. The actual synchronous wait retains the admitted
commit owner's `JournalWriteException` as its cause, with the real PostgreSQL
permission failure at the root. Neither wrapper retires the accepted event;
terminal cleanup remains pending and does not return query admission.

The test correction requires `ACCEPTED_UNESTABLISHED`, a root `SQLException`
with SQLSTATE `42501`, and the exact query-journal permission diagnostic. All
original logical-FINISHED, retained active query, no terminal row before GRANT,
failed drain wait, same-event retry, one terminal commit, purge and final release
assertions remain. No production exception contract, retry delay, timeout or
polling is changed. A scan of the QueryTerminal tests and QueryManager journal
tests found no other direct-cause SQL assertion requiring this migration. The
r1 teardown failures followed the premature
assertion while INSERT was still revoked; they are not evidence of a successful
cleanup or permission-restored retry.

The focused PostgreSQL rerun first stopped before any tests because the parent
builder omitted its Python 3.12 PATH prerequisite. Function-contract generation
reported that Python 3.10 or later is required. This focused r1 remains **FAIL**,
with zero executed tests; evidence is
`/tmp/pangrova-production-coordinator-terminal-journal-pg-nearest-r1.log`.
After restoring that build prerequisite, the same two-test class completed
**PASS**, with zero failures/errors/skips, in 6.928 seconds (52.360-second
reactor). Evidence is
`/tmp/pangrova-production-coordinator-terminal-journal-pg-nearest-r2.log`.
The permission-failure and permission-restored retry assertions therefore ran
against the actual PostgreSQL journal, rather than a constructed commit or ACK.

The parent then reran the same original eighteen-class scope: r2 is **PASS**,
100/100 tests, zero failures/errors/skips, 73 seconds, finished
`2026-10-04T02:27:32+08:00`. Evidence is
`/tmp/pangrova-production-coordinator-journal-lifecycle-pg-nearest-r2.log`.
This confirms the finite Coordinator component/control and PostgreSQL scope
listed above; it neither replaces the preserved r1 failure nor establishes
matching packaged JNI, independent-process E2E, or production-scale acceptance.
The sixteen matching-JNI report/physical-ownership cases still require a new
same-source Native package before execution.

Fatal flush-thread failure seals new journal admission, preserves pending owners,
and makes waiting synchronous callers fail observably rather than hang on an
orphaned future. Its retained ownership is not automatically claimed committed;
JVM-wide allocation failure recovery is not proved by a control/fault callback.
Ordinary SQL/connection/unknown-COMMIT errors retain the running sole writer for
same-ID retry. A still-live writer, retained connection or entered reader prevents
DataSource retirement. Semantic payload conflict remains sticky even after real
resources drain, so retrying close does not manufacture business success.

Durable issued scope reconstruction, durable Worker report outbox, process restart
incarnation fencing and cross-Coordinator recovery ownership remain separate
production gates. This finite-process unit does not claim those implemented.
