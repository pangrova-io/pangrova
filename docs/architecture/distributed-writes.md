---
title: "Distributed Write"
sidebar_label: "Distributed Write"
---

# Pangrova — Distributed Write Design

## Authenticated planning scope

Catalog/schema discovery, table binding and statistics use an immutable query-local
`ConnectorSession`, captured from the authenticated coordinator session. The production
SQL compiler creates its planner through `forSession`; a process-level compiler cannot
compile table plans before binding. The same scoped compiler remains attached through
SQL/AST compilation, CTAS, Substrait serialization retry and adaptive reoptimization.
Planner depends on the session SPI and an injected factory, not Metadata implementations.

Catalog and schema names are discovered dynamically. Table-name caches belong to that
query's schema instance, so a newly created schema is visible without rebuilding a global
schema tree and one tenant cannot reuse another tenant's table metadata. Backend/authority
failures propagate instead of being represented as a successful empty catalog. A missing
tenant is rejected; connectors receive only the authoritative `tenant_id`, never an alias
or a client property substituted for authenticated tenancy.

`MetadataService` resolution caching separates tenant, user and the SHA-256 digest of
security context plus canonical connector properties. Secrets are not retained in cache
keys. Table/catalog DDL invalidates all affected authenticated scopes; ALTER validation
and readback use the connector rather than the unscoped tiered inventory cache.
The result-cache invalidation resource is process-owned and shared independently of
query planners; coordinator shutdown closes its external store. This does not enable
Coordinator result-row caching or execution.

User metadata operations retain that same authenticated coordinator session through
DDL, CTAS, view lookup, time-travel resolution, connector query lifecycle and split
enumeration. Short table names resolve against the query's catalog/schema; a fully
qualified target rebinds only catalog/schema, preserving authenticated principal and
tenant. Rename targets resolve relative to the source and cannot cross catalogs.
CREATE TABLE/CTAS, CREATE SCHEMA, catalog mutations and rename use their exact
authorization action and resolved target, rather than an ALTER or SELECT substitute.

Index connector accessor resources are process-owned but cannot perform metadata
lookups until bound with `forSession`. Their bound scopes are immutable and reject
catalog/schema drift; they never construct a system session. User index validation,
build submission and query-time pruning bind the authenticated operation scope.
CREATE INDEX and ALTER INDEX REBUILD require current SELECT on the connector-resolved
indexed source columns as well as target ALTER; raw builds reject active source row
filters and indexed-column masks before reading source files or submitting a build.
Background index maintenance is **not yet authorization-complete**: bootstrap pin
and listener acquisition, commit-triggered refresh, consistency checks and garbage
collection still require a trusted service/descriptor authority from the security
assembly. An unbound accessor explicitly rejects these operations. Scheduled Native
generation/rebuild entry points also reject missing current authorization; persisted
DDL-era groups, bearer tokens or synthetic system principals are not a substitute.
The Java index builder now fails the build when snapshot pinning fails, so that
generation cannot publish READY. A durable connector pin implementation and
persisted, currently reauthorized maintenance identity are still required before
an index generation is durable across source snapshot expiration. Iceberg's
query-scoped in-memory read pin is not an index-lifetime snapshot reference.
Commit-triggered file statistics now propagate authorization denial so the durable
commit-log entry remains pending instead of being acknowledged without statistics.
When a table has no READY index, the index processor does not read connector
statistics and lets unrelated durable commit processors advance the event.
The remaining design must persist the maintenance subject and exact table/column
scope, recheck current grants and row/column policy before connector access, then
bind the resulting session to the accessor. Administrative and table operations
need explicit authorization at the relevant scope, consistent with
[Trino's access-control model](https://trino.io/docs/current/security/file-system-access-control.html)
and its [current-policy authorization requests](https://trino.io/docs/current/security/opa-access-control.html).

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

## 1. Current State

Write allocation has one owner: query scheduling after authenticated compilation.
The statement factory performs declaration validation and routes the original AST;
it does not call `beginInsert` or `beginOverwrite`. The dispatched DML plan and its
row-filter/column-mask observations come from the same compilation. Current authority
is checked before allocation and persisted unchanged in the write journal. SQL action
permissions follow the original UPDATE/DELETE/INSERT or MERGE clauses, independently
of the connector's physical append/overwrite mode. Reusing a query write context must
match the admitted receipt, principal and tenant. This path remains under full-chain
verification; the design statement is not evidence of a completed production gate.

Policy-bound query compilation follows bind/validate → authorize and apply policies →
relational optimization. It must not derive expression facts before column masks can
replace them. Mutation plans remain validated `TableModify` input until policies and
copy-on-write conversion establish the complete replacement relation: a predicate that
selects matching rows cannot justify substituting an untouched physical column, or
discarding a nonmatching survivor. Protected read optimization also cannot substitute
an MV source that has not passed the original query's authorization.

Distributed write is partially implemented. All relational DML uses the protected Native task path and reports commit metadata back to the coordinator; DDL remains a Java metadata operation.

Implemented chain:

```
SQL → Parser/Planner → Stage/Task dispatch → Worker NativeWriteExecutor
    → TaskStatusReport write result → TaskStatusReceiver
    → WriteCommitJournal → ConnectorMetadata.finishDistributedWrite()
```

The resultless DML dispatch can acknowledge Worker readiness before execution
finishes. Its scheduling continuation waits for the query's logical terminal
state, which is published after the fenced connector commit or failure. A
speculative winner may replace the original physical task and retire that
task's execution evidence before this continuation resumes; waiting on the
original task ID would incorrectly fail a committed write.

Current implementation anchors:

| Area | Status | Code anchor |
|------|--------|-------------|
| Worker write executor | Implemented | `NativeWriteExecutor` owns per-task sink sessions and commit metadata isolation |
| Coordinator commit aggregation | Implemented | `TaskStatusReceiver` collects write results and calls `finishDistributedWrite()` once per query |
| Recoverable commit journal | Implemented for Iceberg and Paimon, including retryable index-event publication | The redo record includes tenant, original base snapshot, exact Worker-dispatched v2 write binding, immutable fragments, execution fence and canonical commit fingerprint |
| Connector atomic commit | Implemented for Iceberg and Paimon | `IcebergMetadata.finishDistributedWrite()`, `PaimonMetadata.finishDistributedWrite()` |
| Protected DELETE routing | Implemented; full-chain verification pending | `PlanExecutionFactory` passes the original DELETE to the Native compiler. No-WHERE DELETE retains policy-hidden rows, or emits no survivors when unrestricted; it cannot bypass authenticated admission and the journal |
| Copy-on-write mutation policies | Planner and single-worker Iceberg/Paimon E2E passed; distributed verification pending | Target policies restrict UPDATE/DELETE/MERGE matches, while original physical survivor values remain intact. Predicate/assignment masks never become replacement values for untouched target columns. A conditional matched-only MERGE adds the action predicate to target matching; conditional matched+insert, conditional insert, and repeated actions of one kind fail before lowering because their full clause semantics are not implemented. |
| Full relational distributed UPDATE/DELETE/MERGE execution | In progress | Multi-worker row preservation, failure recovery and final-source release gates remain required |
| CDC ingestion | Not distributed/atomic yet | `StreamingBootstrap` wires `ConnectorPageSinkCdcWriter` with no Native dispatcher; its DELETE and INSERT phases commit separately |

Routing rule: relational writes, including DELETE ALL and VALUES, use Native tasks. DDL metadata operations and the final fenced catalog commit remain coordinator-owned through the connector SPI.

---

## 2. Write Transaction Atomicity

### Problem Statement

There is an inherently ambiguous boundary between an external catalog commit and the coordinator
marking its local journal committed. A coordinator crash or timeout in that interval cannot prove
from the RPC result alone whether publication happened. Recovery therefore needs both a durable
redo record and an externally visible idempotency marker.

### Failure Scenarios

| Scenario | Data State | Coordinator State | User Impact |
|----------|-----------|-------------------|-------------|
| Worker crash before commit fragment report | Staged files only | FAILED | Unreferenced files are eligible for delayed orphan cleanup |
| Coordinator crash before catalog commit | Durable PENDING intent | Unknown | New owner replays the exact intent under its execution fence |
| Coordinator crash after catalog commit | Snapshot may be published; journal remains PENDING | Unknown | Recovery finds the fingerprint in Iceberg or Paimon snapshot history, skips physical append and retries index-event publication |
| Catalog commit timeout | Outcome ambiguous | PENDING | Staged files and redo record are retained; recovery proves the result |
| Concurrent table change | Depends on write mode/base snapshot validation | PENDING | Iceberg OCC retry preserves the original base and the same fingerprint |

### Two-Phase Write Commit Protocol

The fix moves catalog commit authority from the worker to the coordinator. Workers write data files and produce commit metadata fragments; the coordinator performs the single atomic catalog commit.

SPI change on `ConnectorPageSink`:

```java
default byte[] finishWithMetadata() {
    return new byte[0];
}
```

The distributed path requires a replay-safe metadata fragment and a connector implementation of
the context-bearing final commit. It must not fall back to a context-free or single-phase commit.

---

## 3. Distributed DML Architecture

Distributed DML follows the same planner→stage→task chain as SELECT when the operation needs worker-side data processing:

```
SQL → Parser → Planner (TableModify) → StageFragmenter → TaskDispatch → Workers
```

### TableModify Relational Operator

Wraps Calcite's `LogicalTableModify`. Captures: target table handle (serializable via `ConnectorOutputTableHandle`), operation type (INSERT/UPDATE/DELETE/MERGE), column assignments, filter predicate, source/target matching expressions. Converts to Substrait `WriteRel` with `ExtensionSingleRel`.

### Stage Fragmentation for Writes

```
Stage 0 (leaf):   TableScan → Filter/Project (read source data)
Stage 1 (middle): Transform (apply UPDATE assignments / MERGE logic)
Stage 2 (root):   Write (via ConnectorPageSink on each worker)
```

### Worker-Side Write Execution

1. Deserialize `WriteRel` from the task request proto
2. Instantiate local `ConnectorPageSink` from serialized output handle
3. Write transformed pages via `appendPage()`
4. Call `sink.finishWithMetadata()` or the connector-specific write executor path to produce commit metadata without final catalog commit
5. Report commit metadata to coordinator via task status

---

## 4. SPI Extensions

```java
// ConnectorPageSink — materialize without commit
default byte[] finishWithMetadata() { return new byte[0]; }

// ConnectorMetadata — single atomic commit from fragments
void finishDistributedWrite(ConnectorSession session,
                            ConnectorOutputTableHandle outputHandle,
                            List<byte[]> fragments,
                            DistributedWriteCommitContext context);

// ConnectorOutputTableHandle — snapshot fixed when begin* creates the handle
OptionalLong writeBaseSnapshotId();

// Connector-owned output-handle codec — same exact bytes for Worker and redo replay
byte[] serialize(ConnectorOutputTableHandle handle);
ConnectorOutputTableHandle deserialize(int codecVersion, byte[] payload);
```

---

## 5. Failure Handling and Recovery

### Orphaned File Cleanup

When writes fail after file materialization but before commit, data files remain unreferenced. Periodic cleanup scans for orphaned files older than retention period (24h default). Iceberg provides `RemoveOrphanFilesAction`; Paimon has `ExpireSnapshotsAction`.

### Write Transaction Journal

Before the first `finishDistributedWrite()` call, the coordinator persists a complete redo record.
The record binds the execution ID, authenticated tenant, catalog/table, write mode, original base
snapshot, exact canonical v2 Worker write binding, and an order-independent digest of all worker fragments. The SHA-256 fingerprint is the
idempotency key used by both the first attempt and every replay; a query ID cannot be reused with
a different fingerprint.

Recovery is scoped to execution IDs claimed through the durable execution-anchor store. The
recovering owner revalidates its fence before catalog reconciliation and before changing journal
state. It reconstructs the connector session with the persisted tenant and decodes the exact
Worker-dispatched output handle only after checking catalog incarnation, revision, provider,
configuration fingerprint, codec identity, payload checksum, table, mode and base snapshot.
The first commit performs the same binding validation. Missing or changed identity fails closed.

For Iceberg, every published snapshot carries the fingerprint, execution ID, tenant and base
snapshot in its summary. Before every initial/retry/recovery publication, Pangrova refreshes the
table and searches all retained snapshots, not only `currentSnapshot()`. A matching fingerprint
with different metadata is corruption and fails closed. A matching marker proves the physical
catalog commit; it does not skip the index-event notification. This also applies to empty
OVERWRITE, which is a real truncate operation rather than an empty-fragment no-op. Paimon uses
the fingerprint as the durable snapshot user marker and resolves the exact committed snapshot
on replay, rather than substituting the current/latest snapshot.

If an Iceberg fingerprint is absent, the connector checks that every retained snapshot on the
current parent chain back to the bound base snapshot is available. A gap caused by snapshot
expiration fails closed instead of re-appending. This check cannot prove an outcome when the
table was rolled back to the original base after publishing and expiring the marker: the visible
state then looks like a never-committed write. Strict exactly-once therefore still requires a
non-expiring external per-write marker or a durable catalog-generation/history proof, in addition
to retention monitoring. The current marker/lineage check must not be treated as that proof.
If the marker is retained but outside the current snapshot ancestry, reconciliation fails closed;
an orphaned snapshot does not prove that its write remains published. The Iceberg output handle
also binds the table UUID, so a stale write or Worker sink cannot target a dropped-and-recreated
table with the same name. The connector handle codec is version 2; pending version-1 intents
must be reconciled before deployment because their payloads do not carry a table UUID.

Catalog commit and journal ACK are not atomically fenced by a shared transaction. In particular,
an old timed-out commit can overlap a recovery attempt after both have checked for the same
fingerprint; Iceberg's generic append API may rebase both commits. Empty overwrites also cannot
use data-file conflict checks to guarantee a single winner. These races remain unresolved P1
until the catalog enforces a durable per-write claim/fence in the same atomic commit boundary,
or recovery quarantines every ambiguous attempt instead of republishing it. A process-local lock
and snapshot-marker lookup alone are insufficient.

Index-event publication is a separate durable step after the external catalog commit, not an
atomic transaction spanning the catalog and Pangrova metadata database. A listener or metadata
store failure must reach the coordinator so that the write journal remains PENDING and recovery
re-drives the same fingerprint and exact snapshot marker. The JDBC `index_commit_log` accepts an
identical journaled event once per tenant/catalog/schema/table/fingerprint and rejects the same
key with different snapshot, type, or file sets. Ordinary non-journaled events have no fingerprint
and remain distinct. An index metadata store that cannot persist the event fails explicitly;
it may not acknowledge a journaled notification through a no-op implementation.
The maintenance poller orders events within each table but selects fairly across tables, so a
failed table head cannot monopolize every bounded poll batch and block unrelated tables.

Every authenticated write now retains one typed authorization receipt: the actual writer and
tenant, the SDK-observed table/schema/generated-definition declaration (including explicit
absence), exact write privileges, compiler-observed read policies, and exact model contracts.
Deferred generated work uses INVOKER authority, not an invented system identity or the writer's
persisted IdP groups. PostgreSQL rechecks current tenant membership, direct roles, grants,
policies, model incarnation/deployment, and backend execution ownership at dispatch and
publication. Deleting a role also clears its model administration/usage policy references in
the same exclusive authority transaction; recreating the name does not restore those policies.

A COMMITTED journal row is collected only after an explicit durable deferred-work ACK and
after its authentic commit-queue event is consumed. Ordinary writes ACK their persisted
no-generated-definition declaration. Generated writes atomically enqueue all durable jobs and
descriptors with the ACK on the same PostgreSQL connection. ACKed replay does not recompile
or turn a completed notification into a failure merely because permissions were later revoked.
The production listener/maintenance-lease/queue path is also used by SQL E2E; tests must not
substitute a direct materialization-service call for a committed SQL write.

These PostgreSQL fences do not atomically fence an external connector SDK schema/definition
change. An actual SDK expected-revision CAS at catalog commit remains necessary; a pre-commit
comparison alone cannot close that race. A confirmed unpublished abort now transitions the exact
fingerprint to durable `ABORTED`, retaining its intent identity. The same identity cannot be
reinserted, published, committed, or removed by ordinary committed-entry GC. The abort and
publication updates are mutually exclusive on the journal row; a published PENDING intent cannot
be aborted. Missing intent evidence fails closed, rather than being treated as a successful abort.
This barrier does not close the separate execution-anchor check/update transaction race or define
safe long-term terminal retention. Ambiguous PENDING writes still require explicit reconciliation.
Native task authorization ends at the earlier of the initial execution lease and task timeout;
database lease renewal does not extend an already issued Native credential. Large builds need
a canonical acknowledged Native lease-renewal contract rather than a widened timeout.

An ambiguous exception never authorizes deletion of staged files. The PENDING record and files are
retained until recovery proves publication or an operator reconciles a quarantined intent.

The coordinator-local CDC sink is not covered by the distributed write journal. Its staged token
allows file deletion only before any catalog publication attempt. Once publication starts, an
exception can mean the snapshot landed and a later notification failed; `discard()` must preserve
the files even if the source offset commit aborts. This prevents deletion of files referenced by a
published snapshot, but does not make CDC exactly-once: the existing connector CDC path still uses
the context-free commit overload, Paimon rejects that overload, and DELETE/INSERT phases are not
one atomic catalog transaction. Native Worker routing and durable CDC commit identity remain
required before claiming production CDC support.

The CDC commit-state store now uses an atomic `PREPARED → COMMITTING` claim so two
coordinators cannot both enter the publication loop for one source batch. A failure
after that claim leaves `COMMITTING` for reconciliation: recovery no longer changes
it to `COMMITTED` without checking the external catalog, and rollback no longer
deletes possibly published files. Migration `V39__cdc_retry_after_rollback.sql`
allows the same source batch to obtain a new prepare token after a proven pre-publish
rollback. These are fail-closed correctness guards, not a durable CDC publication
implementation; Paimon CDC and atomic UPDATE remain unclosed.
Participant rollback now claims `PREPARED → ROLLING_BACK` before aborting staged
files; otherwise a concurrent publisher could claim `COMMITTING` between the
state read and file deletion. PostgreSQL tests exercise this interleaving.
The current JDBC row still records only one representative partition, and
`findLastCommittedOffsets` reconstructs its topic from the stream ID. It
cannot safely restore a multi-topic/multi-partition source. A durable batch
intent must retain every exact `(topic, partition, next_offset)` and expose
those offsets only with a committed publication outcome in one JDBC transaction.

**CDC repair contract (proposed, not implemented).** One source batch must bind tenant,
stream ID, source ownership epoch, exact topic/partition offset ranges, target table UUID,
base snapshot, and canonical output binding before any Worker task is dispatched. Persist
that immutable identity and its attempt state in the write journal. Worker tasks receive
the same canonical v2 task/write descriptor as SQL DML, stage fragments, and return only
validated terminal metadata to the Coordinator; no CDC row materialization belongs in
the Coordinator JVM. Derive the commit fingerprint from this durable source identity and
the exact fragment digest, never from the current offset or a newly selected snapshot on
replay. The source offset acknowledgement follows proof of the exact catalog publication;
on an ambiguous result, preserve files and replay the same intent rather than deleting or
minting a new fingerprint.

An UPDATE batch must publish its delete and insert fragments in **one** target-table
snapshot/commit operation (Iceberg's
[row-delta API](https://iceberg.apache.org/javadoc/1.11.0/org/apache/iceberg/RowDelta.html)
can add rows and delete files to one snapshot; Paimon supports source
[row kinds](https://paimon.apache.org/docs/master/primary-key-table/sequence-rowkind/)
that must be committed together), not the current two
catalog commits. Catalog commit fencing and table UUID rules are the same as for SQL DML;
without the catalog-owned atomic fence specified below, overlapping old and recovered
owners remain an explicit P1. Acceptance requires real Kafka plus two Worker processes,
Iceberg and Paimon target tables, exact row/value and offset assertions, and injected
crashes before staging, after staging, during catalog commit, after publication before
notification, and before offset acknowledgement. Every recovery must show either one
visible catalog commit and one advancing source offset or a quarantined PENDING intent;
`SKIPPED`, component fakes, and a merely correct final row count do not qualify.

Operational invariant: Iceberg snapshot expiration must retain commit-marker snapshots longer than
the maximum write-recovery SLA. Schema migration `V33__recoverable_write_commit_identity.sql`
intentionally has no defaults; deployments with unresolved legacy intents must reconcile them
before upgrading rather than inventing tenant or snapshot identity.

---

## 6. Atomicity by Table Format

**COW Tables (Iceberg)**: Workers serialize `DataFile` descriptors as commit metadata. The
coordinator builds one snapshot update, attaches the durable fingerprint marker and commits one
atomic metadata swap. APPEND, OVERWRITE, DELETE and MERGE share the same recovery identity.

**MOR Tables (Paimon)**: Workers call `tableWrite.prepareCommit()` to get `List<CommitMessage>` (native two-phase support). Coordinator collects all fragments and triggers single compaction commit.

---

## 7. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Partial commit on worker failure | Coordinator tracks per-worker status; abort uncommitted on failure |
| Connector sink not serializable | `serializeOutputHandle()` SPI; fallback to single-phase for non-implementing connectors |
| Memory pressure on write workers | Account write buffers through the existing worker memory-arbitration path |
| Atomicity for COW tables | Single coordinator-side commit after all workers report success |
| Coordinator crash before commit | Durable redo record plus execution-anchor fencing scopes recovery to the new owner |
| Duplicate commit on ambiguous recovery | The same fingerprint is written to and searched across retained Iceberg or Paimon snapshot lineage; index-event replay is idempotent only for the identical journaled event |
| Tenant drift during recovery | Tenant is persisted in the redo record and rebuilt into the connector session |
| Catalog or base snapshot drift during recovery | The exact Worker-dispatched binding and original base are persisted; first commit and replay refuse a changed catalog and never substitute the live snapshot |
| Cleanup after ambiguous publication | Staged files are retained until external publication is proven |

---

## 8. Required Iceberg Catalog Commit Fence (Proposed)

The existing Iceberg Table/SnapshotUpdate API does not expose an atomic condition saying
“publish this snapshot only if this write fingerprint has never published and this owner fence is
still current.” Its catalog metadata compare-and-swap may retry and rebase a valid append. The
snapshot-summary marker, local mutexes, and data-file OCC are therefore insufficient for the
old-timeout-versus-new-owner race, especially for an empty overwrite. This section is a required
future catalog capability, not implemented behavior. Iceberg's [concurrent-write model](https://iceberg.apache.org/docs/1.9.0/reliability/)
and [REST catalog commit requirements](https://iceberg.apache.org/docs/nightly/rest-protocol/)
provide the underlying metadata-swap model, but do not by themselves supply a Pangrova
per-fingerprint owner fence.

The connector needs a catalog-owned fenced snapshot-commit capability backed by a linearizable
transaction over both the table metadata pointer and a durable write-claim record. The claim key
is the table UUID plus the journal's unique query/write ID; its immutable payload binds the
commit fingerprint, tenant, canonical v2 output-binding digest, write mode, original base snapshot
and fragment digest. It also holds
the current owner fence, state (`PENDING` or `PUBLISHED`) and, when published, the exact snapshot
ID and metadata generation. The Coordinator must first persist the existing PENDING write journal,
then present its authenticated execution fence with the connector commit request. A catalog
adapter unable to enforce this contract must reject journaled distributed commits, not fall back
to unfenced `Table.newAppend()` or an in-process lock.

Claim acquisition or takeover atomically advances the fence only for the same immutable payload.
The catalog's snapshot-publication transaction rechecks that fence and the table UUID, verifies
the claim is not already `PUBLISHED`, performs the Iceberg metadata-pointer compare-and-swap,
and records the `PUBLISHED` snapshot proof in that same transaction. A stale owner cannot publish
after a newer fence is installed. A lost commit ACK is reconciled by reading the durable claim:
matching `PUBLISHED` proof returns the exact snapshot; `PENDING` under an expired owner can be
fenced and retried; a different payload, missing table UUID, non-current published snapshot, or
uncertain catalog result fails closed. The claim/tombstone cannot be garbage-collected while any
PENDING journal or old owner request can still reappear. After catalog proof, index-event
publication is retried under the journal and deduplicated separately in `index_commit_log`.

This requires a catalog implementation that owns the atomic metadata-pointer update, such as a
certified REST catalog service or a transactional catalog store. It cannot be implemented safely
by adding a second control database beside an arbitrary Iceberg catalog: a fence check in one
store and a metadata swap in another would recreate the same race. The current
`DistributedWriteCommitContext` also lacks an owner-fence field; a future change must carry the
existing execution-anchor epoch through the canonical v2 write contract, not introduce a
connector-private protocol or a second write path.

Acceptance requires real catalog-backed, two-process fault-injection tests (no mock or skipped
commit path):

1. Hold owner A after it enters catalog publication, expire its lease, give owner B a higher
   fence, then release A. Exactly one snapshot and one durable claim may publish for the same
   fingerprint; the stale owner must be rejected, including nonempty APPEND, DELETE/MERGE and
   empty OVERWRITE.
2. Reverse the race so A publishes before B's takeover. B must observe the exact published
   snapshot and emit no second physical commit. Repeat with the catalog ACK dropped after the
   atomic swap.
3. Replay the same fingerprint with changed binding, fragments, tenant, or base snapshot; reject
   it. Drop and recreate the same table name, then replay an old intent; reject the old table UUID.
4. Expire or roll back the published snapshot while the journal remains PENDING. The durable claim
   must expose the prior publication, and recovery must quarantine the non-current/missing proof
   without writing another snapshot.
5. Repeat with two catalog service instances and concurrent clients against the production
   backend. Verify a single winner and durable recovery after process restart; no test may count
   an unavailable backend or `assumeTrue` branch as PASS.

## Native dataset metadata commands

The canonical runtime exposes `DATASET_SCHEMA_FINGERPRINT` and
`FINALIZE_DISTRIBUTED_WRITE`. Neither command carries data rows. Schema
fingerprinting reuses execution's Substrait `NamedStruct` conversion (including
depth-first nested field names), exports an empty Native vector to Arrow, and
uses the Lance FFI's physical schema fingerprint. Top-level logical column
contracts retain FLOAT32/FLOAT64 VECTOR dimension and element constraints.

Finalization receives the original connector allocation and exact winning
attempt metadata. Native validates the catalog/output-handle checksum, tenant,
generation, operation, original base, schema fence, complete attempt evidence,
and each transaction key under the allocation-derived attempt prefix. Requests
are bounded to 4,096 fragments and 16 MiB total metadata, with an explicit
deadline and execution owner/control fence. CREATE, APPEND and OVERWRITE all
publish only the exact original base plus one; APPEND does not automatically
rebase onto another writer's version.

The current publication authority and the original durable intent owner are
distinct fields. Winners remain bound to the single original intent owner;
their positive control terms may precede the current term but cannot exceed
it. The current authority must have a fence at least as new as the intent;
equal fences require identical owner/incarnation identities. Recovery never
rewrites winner evidence or skips its cohort validation.

The current Lance ABI returns the SHA-256 of actual persisted manifest bytes
alongside the committed version. Only successful publication with consistent
evidence returns COMMITTED. An FFI failure, expired publication, or inconsistent
recovery proof returns UNKNOWN without a version/digest; Native never guesses
NOT_COMMITTED from an error message. A known recovery hint requires the exact
version and manifest SHA-256. History recovery is lazy and deadline-bounded,
but a durable resumable recovery cursor remains additional production work;
this is not an unbounded-scale recovery guarantee.

## 9. Remaining Work

| Area | Remaining work |
|------|----------------|
| Planner coverage | Complete full relational distributed planning for every UPDATE/DELETE/MERGE shape |
| Fault cleanup | Expand orphan-file cleanup and abort handling across all write failure cases |
| Marker retention | Enforce/monitor that Iceberg and Paimon snapshot expiration remain longer than the maximum recovery SLA |
| Iceberg marker loss after rollback | Add a non-expiring per-write marker or durable catalog-generation/history proof; current-to-base lineage checks catch expiration gaps but cannot distinguish rollback to the original base from no commit |
| Upgrade operations | Reconcile pre-V33 PENDING intents before applying the fail-closed recovery-identity migration |
| Table incarnation | Iceberg connector handle v2 carries the immutable table UUID and checks it on Worker sink and final commit; reconcile version-1 pending intents before upgrade and test concurrent drop/recreate against each production catalog |
| Commit concurrency | Add catalog-level durable per-write claim/fence that atomically prevents both an old timed-out commit and recovery from publishing, including empty overwrites; until then, do not claim exactly-once for ambiguous concurrent recovery |
| Event delivery | Add failure-injection coverage across catalog commit, index-event persistence, journal ACK and recovery; the external catalog and metadata database do not share an atomic transaction |
| Performance policy | Add threshold-based routing and benchmarks for coordinator-local vs distributed write choices |
| Connector coverage | Keep Iceberg/Paimon as implemented connectors; add new connectors only with `finishDistributedWrite()` contract tests |
