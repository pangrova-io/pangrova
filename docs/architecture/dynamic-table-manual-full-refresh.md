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
The head update compares tenant, fully qualified name, incarnation, definition,
and revision. A failed build does not advance the old head. Its isolated
candidate is retained because a timed-out Native writer may still commit; only
a fenced, task-drained garbage collector may remove it. After a publication
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
of retained physical generations. V60 stores only the current head: candidates
that commit but lose their JDBC publication acknowledgement, failed builds,
and replaced heads lack a durable generation/attempt ledger. A future fenced
recovery/GC owner must persist candidate identities before launch, associate
them with exact task-drain evidence, retain generations pinned by readers, and
only then remove unreachable Iceberg tables. Physical generations currently
share the catalog/schema namespace of the logical object; a dedicated internal
namespace and scoped service write capability are still required for full
ordinary-user CREATE/REFRESH support. A user granted SELECT on the source and
CREATE_TABLE/ALTER/SELECT on the logical Dynamic Table cannot currently publish
or refresh: the Native CTAS write receipt correctly demands INSERT on its
random physical generation, which that user does not own. This is a deliberate
fail-closed production acceptance blocker, not a permission grant to add on
`__pg_dt_*`. A safe resolution needs a durable, query-owned candidate attempt
and a narrowly scoped internal write receipt before connector allocation.
V60's publication CAS does not atomically include a query owner or cancellation
fence. A cancel racing with JDBC publication therefore lacks a proven
transactional boundary; this must be resolved with the durable attempt/owner
protocol before production acceptance.
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
