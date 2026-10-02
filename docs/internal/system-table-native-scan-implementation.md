# System-table SQL: Native scan implementation contract

Status: **IN PROGRESS — not production-ready** (2026-09-28). The current
worktree implements a proposed Iceberg `$snapshots`/`$history` typed v2 split
and Worker Native reader, but Java/full E2E gates have not passed. This is an
implementation specification and failure record, not a production-readiness
assertion.

## Observed break

`table$snapshots` is recognized in `AstBuilder.visitNamedTableRef`, and the AST
retains a `SystemTableReference`. At the audited baseline,
`AstToSqlNodeConverter.convertTableRef` constructed its Calcite identifier
from `ref.name()` instead of the system-table reference; `ref.name()` is
deliberately the *base* table. Thus, at the baseline,
`SELECT * FROM table$snapshots` was silently planned as a base-table scan,
while `SELECT snapshot_id ...$snapshots` failed validation against the base
schema. The original `LazyMetadataSchema` also only resolved ordinary connector
table handles. The existing `ConnectorMetadata.getSystemTable` implementations
are reached by SHOW metadata, not by SELECT planning/split enumeration.

This is present in HEAD, not caused by the current uncommitted AI Native edits.
The 2026-09-27 full Java test log records
`MutationHistoryAuditTest$SnapshotLineage` failures: `snapshot_id` and
`committed_at` were not found; a `$history` star query scanned the base table
and returned zero rows. In addition, `IcebergSnapshotsSystemTable` declares
`timestamp_ms`, not `committed_at`; the E2E expectation is now aligned with the
declared public schema.

## Current worktree progress and remaining boundaries

- SELECT resolution preserves `base$kind`, and Iceberg `$snapshots`/`$history`
  resolve to typed, read-only handles. The connector emits one canonical v2
  `DataSplit` pinned to an immutable metadata JSON URI and object length.
  Worker Native validates the typed variant and streams bounded Velox batches
  from that URI. No metadata rows are materialized in the Coordinator or
  connector JVM for this SELECT path.
- The AST-to-Calcite boundary rejects other `$kind` names before physical
  table lookup; an unimplemented system table cannot silently scan a
  same-named ordinary relation.
- The base table's SELECT privilege is required for either suffix. The E2E
  test no longer accepts an empty snapshot result as a time-travel pass.
- SHOW SNAPSHOTS is rewritten before compilation into the same authorized
  `SELECT * FROM base$snapshots` distributed query, preserving original SQL
  for audit. The former Coordinator `ConnectorSystemTable.rows()` task is
  removed. SHOW FILES/MANIFESTS/PARTITIONS fail closed because their Native
  readers are not yet implemented; direct SELECT for those kinds and Paimon
  system tables also remain open. The whole family is **not** closed.
- Worker Native checks the pinned metadata object's length. A distributed
  deployment requires immutable shared storage reachable from every Worker;
  node-local catalog paths are development-only. A stronger storage version or
  ETag check is needed if same-length overwrites are possible.
- The parser now rejects quoted table names containing `$` explicitly. This
  prevents a quoted literal name from being reinterpreted as a system table,
  but is not full quoted-name support: the current `Identifier`, Calcite
  `SqlIdentifier`/`TableScan`, `LazyMetadataSchema` map lookup, and connector
  `SchemaTableName` all shed quoting identity. Supporting literal quoted `$`
  names requires a typed relation identity propagated through these
  boundaries; this remains an open architectural requirement.

## One execution path

The system-table suffix denotes a read-only table in the existing SQL →
Calcite → Substrait ReadRel → v2 TaskRequest/DataSplit → Worker Native scan →
Arrow result path. It must not become a StatementDispatcher interception or a
Coordinator-side `TypedRow` result path. There is no separate system-table task
protocol, v1 compatibility route, or silent fallback to the base table.

1. **Resolve and authorize.** Make `AstToSqlNodeConverter.convertTableRef`
   preserve the full `base$kind` identifier. In
   `LazyMetadataSchema.resolveTableDirect`, resolve the base table exactly,
   then ask the connector for a typed system-table handle/schema. Unknown
   suffix, missing base table, unsupported connector, denied base-table read,
   or inaccessible metadata must fail closed. Do not infer a system table from
   a regular table with a literal `$` in a quoted name. `MetadataResolvedTable`
   must expose the system-table columns/types, not the base schema.
2. **Pin before scheduling.** The connector returns a read-only system-table
   handle containing the catalog/schema/base table, kind, immutable metadata
   object identity/version and output schema. Snapshot selection and
   authorization are completed once per query. `PlanSplitSourceExtractor` and
   `SplitFetcher` carry the suffixed logical source ID and obtain splits from
   the connector's normal `ConnectorSplitManager`; do not enumerate metadata
   rows in the Coordinator. Split IDs must be stable across retries, and
   disjoint splits must cover the pinned version exactly once. Replace the
   current eager unbounded metadata-row contract with bounded split batches.
3. **Use canonical v2.** Extend `DataSplit.connector_detail` with a typed
   `SystemTableSplitDetail` in the existing v2 connector schema (or an
   equivalent typed variant in the same canonical envelope). It identifies
   the connector-owned metadata object, table kind, format, row range and
   pinned version. `TaskRequestParser.parseSplit` must validate it, reject
   unknown/ambiguous variants, preserve `source_snapshot`, and keep the
   existing task security, deadline, resource-grant, tracing and coverage
   semantics. Generate Java/C++ sources from the one v2 proto; never hand-edit
   generated files.
4. **Read in Native.** Add a bounded system-table reader selected by the
   existing Native scan factory for system-table splits. Iceberg `$snapshots`
   and `$history` read the pinned Iceberg metadata object; Paimon system-table
   kinds need their connector-owned readers. Project/filter/order/limit/join
   remain ordinary Velox operators, so SQL semantics are not reimplemented in
   Java. Empty metadata still produces the declared Arrow schema. Verify
   object identity before/during read; expiry or replacement must fail the
   task, not return a partial result. No Coordinator or Worker JVM row
   materialization may stand in for this scan.
5. **Remove the parallel facade.** SHOW SNAPSHOTS now routes through the
   same system-table query and `ShowMetadataRedirectTask` is removed. Complete
   the Native readers for FILES/MANIFESTS/PARTITIONS before enabling their
  SHOW forms. Evolve/remove the current name-only `ConnectorSystemTable` SPI
  instead of retaining two schemas or execution paths.

`BranchTtlCleanupTask` now lists typed `SnapshotReference` values through the
Catalog metadata SPI for its maintenance decision; it no longer calls the old
`ConnectorSystemTable.rows()`. This is control-plane metadata, not a SQL
`$refs` query. The legacy row SPI still exists for other Iceberg system-table
implementations and is not retired. TTL deletion now compares the observed
snapshot ID and branch type against one refreshed Iceberg metadata base before
an optimistic conditional commit; a changed ref is not deleted. Commit conflict
and unknown commit state defer cleanup to a future fresh evaluation, never an
automatic replay. Iceberg 1.11 REST `RemoveSnapshotRef` does not generate an
`AssertRefSnapshotID` requirement, so `RESTTableOperations.commit(base, updated)`
is not a branch compare-and-swap. The current implementation admits only the
tested Hadoop TableOperations class on an absolute local-file URI; REST,
custom operations, HDFS (not yet exercised), and object-store URIs fail closed.
REST support needs an atomic request carrying both `AssertRefSnapshotID` and
`RemoveSnapshotRef`, plus a real REST catalog concurrency test. Hadoop's
metadata-file rename semantics on object stores are not assumed atomic.
Focused real-catalog tests cover a branch advanced between
listing and conditional deletion; distributed fault injection and a full
production gate are still required to claim large-scale closure.

REST implementation specification: load a fresh table metadata base and
recheck the ref type and expected snapshot ID; build a one-table Iceberg
`TableCommit` with requirements `AssertTableUUID(base.uuid())` and
`AssertRefSnapshotID(branchName, expectedSnapshotId)`, and the sole update
`RemoveSnapshotRef(branchName)`. Submit through the public
`RESTCatalog.commitTransaction(...)` API, which uses the catalog's configured
REST authentication/session. Require the server's `V1_COMMIT_TRANSACTION`
endpoint; if absent, fail closed. On conflict or unknown commit outcome,
return a deferred status without replaying the removal. Before enabling, test
with a real REST catalog service that advances the ref after the client read
and before commit, and verify the newer ref survives; separately inject an
ambiguous response and verify no same-cycle retry or subsequent ref deletion.

### Concrete integration points

| Boundary | Current entry / required change |
|---|---|
| AST → Calcite | `pangrova-planner/.../parse/AstToSqlNodeConverter.java`: preserve `$kind` in `convertTableRef` |
| Calcite catalog | `pangrova-coordinator/.../metadata/LazyMetadataSchema.java`: dynamic `Lookup` resolves supported `$kind` names after canonicalizing the base table without enumerating virtual names; `MetadataResolvedTable.java`: exact column types |
| Connector SPI | `pangrova-spi/.../metadata/ConnectorMetadata.java`, `ConnectorSystemTable.java`, `ConnectorSplitManager.java`: one read-only handle/schema/split contract |
| Connector implementations | Iceberg/Paimon `*Metadata`, `*SplitManager`, `*SystemTables`: pin source metadata and enumerate disjoint system-table splits |
| Split scheduling | `PlanSplitSourceExtractor`, `SplitFetcher`: keep logical `$kind` identity, authorize and schedule without fetching rows |
| Protocol / parser | v2 `connector.proto` and `task.proto` `DataSplit.connector_detail`, `DataSplitProtoCodec`, Native `TaskRequestParser.cpp`: typed variant and exact identity checks |
| Native read | `PangrovaVeloxConnector` reader selection, connector-owned metadata readers, then ordinary Velox operators and Arrow result |
| Control-plane cleanup | `ShowMetadataRedirectTask` and current `ConnectorSystemTable.rows()` callers: migrate to the sole query path |

## Executable acceptance

- Parser/planner: parse → print → Calcite validation preserves `table$kind`;
  SELECT star and explicit metadata columns expose the typed system schema.
  Unknown suffix and quoted literal `$` names cannot scan the base table.
- Connector/protocol: pinned metadata revision and stable/disjoint split IDs;
  Java/C++ v2 round-trip; malformed detail, stale metadata and cross-tenant
  reads reject closed; no generated-source hand edits.
- Native: Iceberg and Paimon nonempty/empty snapshot scans, nulls, projections,
  filters, ordering, limits and joins; bounded memory and split coverage;
  failure/timeout/cancel/retry tests with no duplicate or partial rows.
- SQL E2E: `SystemTableE2ETest`, `IcebergShowMetadataTest`,
  `MutationHistoryAuditTest`, `IncrementalReadTest` and Paimon snapshot tests
  must use actual INSERT/UPDATE/DELETE writes, a packaged JNI library, and
  assert exact metadata columns/values. Fix the `committed_at` vs
  `timestamp_ms` expectation explicitly. Multi-worker E2E must use distinct
  Worker processes/endpoints and verify complete split coverage.
- Run `.pangrova/rules/build-pipeline.md` Step 0–8 in order after protocol or
  Native edits. Existing parser-only and connector-object unit tests are not
  substitutes for this E2E gate.
