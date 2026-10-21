# Dynamic Table manual full-refresh slice

This document describes the implemented foreground materialization slice. It is
not the complete target architecture in
[`dynamic-table-materialized-view-delivery-design.md`](../internal/dynamic-table-materialized-view-delivery-design.md).

## Supported SQL and execution

`CREATE DYNAMIC TABLE catalog.schema.name TARGET_LAG = 'PT0S' AS SELECT ...`
creates a durable definition, builds a separate physical table through native
CTAS, and publishes that table only after the worker write has finished.
`ALTER DYNAMIC TABLE catalog.schema.name REFRESH` recomputes into a new physical
table and atomically advances the published head. A query naming the logical
Dynamic Table resolves to the published physical generation from JDBC metadata.
The logical name is tenant scoped and always fully qualified at creation.
The creating session's default catalog and schema are stored with the
definition, so a later manual refresh resolves unqualified source references
under the same defaults even when a different session triggers it.

`PT0S` is a **Pangrova transitional convention meaning manual-only refresh**.
It does not promise zero data lag. A positive `TARGET_LAG` is rejected until a
durable scheduler with leader fencing, source-frontier accounting, and lag
monitoring exists. `REFRESH_MODE = CDC`, `WAREHOUSE`, and `WITH` properties are
also rejected by this slice. `AUTO` and `SNAPSHOT` currently select full
recomputation. `SHOW` and `DESCRIBE` report `SNAPSHOT_FULL_MANUAL`.

The physical generation is committed before its metadata head is published.
V66 reserves an immutable refresh attempt before Native I/O, in the same JDBC
transaction as a new definition or against an exact observed head. One execution
owns one candidate; repeated reservations reconcile that candidate rather than
running Native work again. The definition's SQL and binding defaults are immutable.
The head update compares tenant, fully qualified name, incarnation and revision,
and requires the actual live execution owner and Coordinator control term.

A downstream publication consumer requires an exact catalog commit receipt.
`WriteCommitJournal.requiresPublication` binds that requirement to the original
tenant, principal, execution owner and candidate. `DistributedWritePublicationMetadata`
lets physical-file connectors commit or reconcile their exact intent and return
confirmed version/manifest evidence. Iceberg verifies its persisted commit marker
in current snapshot ancestry and hashes the actual manifest-list bytes. A required
empty INSERT commits a real marker-bearing empty snapshot; ordinary empty writes
keep their existing semantics. Native Dataset connectors retain their existing
transactional source-publication proof. An unsupported connector cannot satisfy a
required publication by reporting generic task completion.

A PostgreSQL trigger copies the exact confirmed receipt into an immutable compact
ledger in the same transaction that marks the write COMMITTED. The ordinary redo
journal can then retire before the logical head advances without losing evidence.
Publishing the head also settles the attempt atomically, retaining its receipt
across later refreshes, journal cleanup and logical DROP. Retrying that settled
attempt returns its original receipt and never rewinds a newer head. A failed build does not advance the old head. Its isolated
candidate is retained because a timed-out Native writer may still commit; only
a fenced, task-drained garbage collector may remove it. A failed initial build
also retains its `CREATING` definition: a timeout does not prove that the external
commit failed. Automatic reconciliation of these rows is still outstanding.
After a publication
error, the candidate is also retained: the JDBC update may have committed even
if its acknowledgement was lost.
Published generations
are retained after replacement or logical DROP so queries planned against an
older generation keep a valid target. Restarted coordinators read the durable
head, rather than rebuilding it from process-local registration.
The query lifecycle now separates Native write completion from the outer DDL
result: the query cannot report FINISHED until publication and its acknowledgement
are buffered. A Native failure or cancellation still wakes the waiting DDL.

The planner retains a component-safe physical-to-logical identity map for each
rewrite and pins a repeated logical reference to one generation within that
rewrite. SELECT privilege, row filters, column masks, and table-level lineage
use the logical Dynamic Table identity. Direct SQL access to `__pg_dt_*`
generations is denied before rewrite, including SQL that joins a logical table
to its physical generation. The prefix is reserved against user table DDL/DML,
including rename targets. A Dynamic Table as the source of a relational write
remains fail-closed until deferred worker authorization can carry the logical
source identity and exact observed policies.

## Scope and remaining acceptance work

This slice provides per-object atomic visibility of a completed full result.
It does not yet provide source snapshot pinning, scheduled target-lag
maintenance, incremental/CDC computation, pipeline-wide snapshot cuts,
automatic recovery of interrupted `CREATING` rows, or safe garbage collection
of retained physical generations. V66 now retains candidate identities before
launch and confirmed commit/publication receipts, including after logical DROP.
The replacement-owner API can reconcile the exact old committed candidate under
a new live owner/control term. Production recovery scheduling and the full
lost-ACK/process-failure matrix still need acceptance. A future recovery/GC owner
must associate candidates with exact task-drain evidence and reader pins before
removing unreachable physical tables. Physical generations currently
share the catalog/schema namespace of the logical object; a dedicated internal
namespace and scoped service write capability are still required for full
ordinary-user CREATE/REFRESH support. A user granted SELECT on the source and
CREATE_TABLE/ALTER/SELECT on the logical Dynamic Table cannot currently publish
or refresh: the Native CTAS write receipt correctly demands INSERT on its
random physical generation, which that user does not own. This is a deliberate
fail-closed production acceptance blocker, not a permission grant to add on
`__pg_dt_*`. The durable query-owned candidate attempt is now available, but a narrowly
scoped internal write receipt and persisted definer/owner authority are still
required before connector allocation. Query-operation ownership and database
owner/control fences are enforced; cancel-versus-publication ordering still
requires a dedicated end-to-end race test and a proven transactional boundary.
This slice does not claim that this cancellation matrix is closed.
The complete Dynamic Table feature must
remain open until these items and a production-bootstrap SQL-to-native E2E test
pass.

## Design checks against primary documentation

The publication boundary follows the same principle as [Apache Iceberg's
optimistic concurrency model](https://iceberg.apache.org/spec/#optimistic-concurrency):
readers use one current metadata pointer, and writers publish a replacement
only if the expected version remains current. Pangrova's JDBC head CAS sits
above the separate physical CTAS commit; it does not claim an atomic
cross-system transaction. Iceberg also requires retaining data files used by
valid snapshots, which motivates retaining old generations until reader pins
and garbage-collection policy are implemented.

[Snowflake documents target lag as a staleness target, not a refresh interval](https://docs.snowflake.com/user-guide/dynamic-tables-target-lag),
and uses a scheduler to pursue it. Its documented minimum positive target lag
also makes `PT0S` clearly Pangrova-specific rather than an industry-standard
zero-lag promise. [Databricks distinguishes manual refresh from scheduled
refresh](https://docs.databricks.com/gcp/en/sql/mv-st/schedule-refreshes),
which is why Pangrova rejects a positive target lag until the scheduler is
implemented and verified.

The compact receipt follows PostgreSQL's [transactional trigger semantics](https://www.postgresql.org/docs/16/trigger-definition.html):
the journal transition and its receipt capture commit or roll back together.
[Row-lock waiting](https://www.postgresql.org/docs/16/explicit-locking.html) can consume
a lease, so publication rechecks the database clock after locks and before commit.
These database guarantees do not create a cross-catalog transaction or replace
owner-role authorization, source pins, cancellation ordering, or reader-aware GC.
