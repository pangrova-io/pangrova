# System-table SQL: Native scan implementation contract

Status: **IN PROGRESS — not production-ready** (2026-10-16). Iceberg
`$snapshots`/`$history` and Paimon `$snapshots`/`$tags`/`$options`/`$schemas` have typed
v2 splits, Worker Native readers, and focused SQL E2E coverage. Paimon
`$branches` now has the same chain for local filesystem catalogs only. Iceberg
`$manifests` has a pinned Avro manifest-list Native reader. Iceberg
`$files`/`$entries`/`$partitions` still lack Native readers;
Paimon `$branches` on distributed/object storage, `$files`, and the final full-chain gate remain
open. This is an implementation specification and failure record, not a
production-readiness assertion.

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
  removed. SHOW FILES/MANIFESTS/PARTITIONS still fail closed; the SHOW MANIFESTS
  rewrite has not yet been wired to the new direct `$manifests` SELECT path,
  while the other SHOW readers remain absent. Direct SELECT for Iceberg
  `$files`, `$entries`, and `$partitions`, Paimon `$files`, and nonlocal
  `$branches` also remain open. Paimon `$snapshots`,
  `$tags`, `$options`, and `$schemas` use typed splits over pinned JSON objects
  and a Native reader. Their declared 14, 7, 2, and 7 columns follow the fixed Paimon
  1.4.1 `SnapshotsTable.TABLE_TYPE`, `TagsTable.TABLE_TYPE`, and
  `OptionsTable.TABLE_TYPE`, and `SchemasTable.TABLE_TYPE`, respectively. Paimon
  `$schemas` enumerates pinned historical `schema/schema-ID` objects through
  the selected latest schema ID; its `fields`, `partition_keys`, `primary_keys`,
  and `options` columns are JSON strings, matching the Paimon SDK. Source epoch
  milliseconds become a deterministic UTC SQL timestamp, with millisecond
  source precision. Nullable snapshot and tag fields remain SQL NULL. The whole
  family is **not** closed.
  Paimon `$branches` has a separate local-directory split and Native `lstat`
  reader matching the fixed SDK's `branch_name STRING` and `create_time
  TIMESTAMP(3)` columns. The connector pins branch names, directory mtime,
  device/inode, and the JVM zone offset at that exact mtime. Native checks the
  directory before deriving the local wall-clock timestamp, so Coordinator
  never materializes the row. File URIs are decoded strictly; local paths and
  names with spaces remain valid. A query sees the branch list at bind time;
  branches added later do not appear in its pinned splits. Nonlocal FileIO
  paths fail closed because Native has no equivalent cross-filesystem metadata
  stat API. This is a limited local path, not a production closure.
  The rewrite accepts ordinary ASCII identifier parts, including mixed case,
  and rejects names requiring quoting until relation identity is typed end to
  end. Thus generated mixed-case table names use the same Native reader as
  direct `SELECT`, without interpolating punctuation into SQL.
- Worker Native checks the pinned metadata object's length and, for Paimon
  `$snapshots`, `$tags`, `$options`, and `$schemas`, the SHA-256 of its exact JSON bytes. A distributed
  deployment requires immutable shared storage reachable from every Worker;
  node-local catalog paths are development-only. A stronger storage version or
  ETag/content check remains needed for the older Iceberg JSON system-table paths
  if same-length overwrites are possible. Iceberg `$manifests` pins the complete
  manifest-list SHA-256 and checks it again in Native.
- The [Iceberg specification](https://iceberg.apache.org/spec/) places each
  snapshot's manifest list in a separate Avro object; each manifest is another
  immutable Avro object containing data or delete-file entries. A Native
  `$files`/`$entries` reader must pin and verify that object graph, apply entry
  status and sequence-number inheritance correctly, and preserve partition-spec
  evolution. Parsing only the table metadata JSON or forwarding Java
  `ConnectorSystemTable.rows()` cannot supply the standard rows or the required
  Native execution boundary.
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
| Calcite catalog | `pangrova-coordinator/.../metadata/read/LazyMetadataSchema.java`: dynamic `Lookup` resolves supported `$kind` names after canonicalizing the base table without enumerating virtual names; `MetadataResolvedTable.java`: exact column types |
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
  assert exact metadata columns/values. The incremental-read Iceberg fixture
  now uses the currently declared `timestamp_ms` column; Paimon snapshot SQL
  now has nonempty and empty SQL E2E evidence. Paimon `$schemas` has SQL E2E
  evidence for three schema revisions, projection/filter/order, declared
  column order, and an empty data table with an initial schema. Its focused
  Native test rejects a same-length changed metadata object. For the earlier
  Paimon snapshot paths, mutation, projection, filtering and multi-split
  coverage remain to be validated. Multi-worker E2E must use distinct
  Worker processes/endpoints and verify complete split coverage.
- Run `.pangrova/rules/build-pipeline.md` Step 0–8 in order after protocol or
  Native edits. Existing parser-only and connector-object unit tests are not
  substitutes for this E2E gate.

### `$schemas` incremental evidence (2026-10-16)

This closes only the Paimon `$schemas` query path, not the system-table family.
The fixed Paimon 1.4.1 `SchemasTable.TABLE_TYPE` defines seven columns. Its
`fields`, `partition_keys`, `primary_keys`, and `options` values are JSON
strings. The connector pins each historical schema object's URI, length and
SHA-256; Worker Native checks those bytes and the schema ID before producing
the row. Calcite lookup and SELECT authorization both recognize `$schemas`
while retaining the base table's SELECT grant.

On JDK 17, focused connector binding tests passed 8/8, Coordinator lookup
tests 9/9, authorization tests 46/46, the JNI loader test 1/1, and the
SQL-to-Native `SystemTableE2ETest$PaimonSchemas` tests 2/2. Native focused
parser/reader tests passed 11/11. The latter E2E queried three real schema
versions and the initial schema of a table with no data rows. The built Native
library and the packaged `osx/aarch64` JNI resource both have SHA-256
`a7751636931a633c765b533872d49a0ab2c38f4af29bb28766c9ab86c1ddb12f`.
This was an in-process SQL E2E using the packaged JNI resource; an independent
Pangrova process image has not yet been validated for this `$schemas` path.
Iceberg `$files`/`$entries`/`$partitions`, Paimon
nonlocal `$branches`/all `$files`, and the final full production gate remain open.

### `$branches` local-filesystem increment (2026-10-16)

The fixed Paimon 1.4.1 `BranchesTable.TABLE_TYPE` exposes `branch_name STRING`
and `create_time TIMESTAMP(3)`. Its reader obtains the branch directory's
`FileIO.getFileStatus(...).getModificationTime()`, converts that epoch into a
`LocalDateTime` using the SDK JVM's default zone, and omits the main branch.
Pangrova now pins the directory stat identity and the zone offset at the
directory's exact mtime; Worker Native validates the local directory's mtime,
device and inode before emitting the same wall-clock value. The offset is part
of the source-identity digest, so a changed JVM default zone invalidates a
bound read. Paimon permits nonblank branch names such as `feature beta` and
`a..b`; URI validation checks decoded path segments rather than rejecting
those names. Malformed file URI escapes, an authority, and encoded path
separators fail closed.

With JDK 17 and Paimon 1.4.1, connector binding tests passed 9/9, including
empty and newly added branches, named-directory pinning, URI spaces, exact
millisecond mtime, Shanghai-zone wall-clock equivalence, and mtime/zone drift.
Native parser and reader tests passed 5/5. Packaged classpath JNI SQL E2E
passed 1/1: an empty relation followed by two real `CREATE BRANCH` calls,
projection, filter, sort, and non-null `create_time`. The `osx/aarch64` Native
library and packaged JNI resource both have SHA-256
`eb5cf5d35d2dc9099c24795b226f9d55aa65ed5c5d3312b3176e49587badd4d1`.

This increment supports only local filesystem catalogs. Paimon FileIO also
reports directory modification times for Hadoop/object-storage backends, but
the current Native filesystem abstraction has no portable metadata-status
operation with the same semantics. Such catalogs reject `$branches` rather
than producing Coordinator rows or guessed timestamps. An independent
Pangrova process image and the full system-table production regression have
not been validated for this path. Iceberg `$files`/`$entries`/
`$partitions` and Paimon `$files` remain open.

### Iceberg `$manifests` manifest-list increment (2026-10-16)

The fixed Iceberg 1.11.0 `ManifestsTable` defines 12 typed columns, including
`partition_summaries ARRAY<ROW<contains_null BOOLEAN, contains_nan BOOLEAN,
lower_bound VARCHAR, upper_bound VARCHAR>>`. The connector selects one v2/v3
snapshot manifest-list URI, freezes its length and SHA-256, and sends its
snapshot/sequence/format identity and partition-spec descriptors in a typed
split. Worker Native verifies the exact bytes and Avro OCF header, reads bounded
null/deflate blocks, and emits Velox rows. Empty tables issue no split. V1
snapshots without a manifest list, v4, unsupported codecs and partition-bound
types fail closed; the latter remain a scope limit for full type coverage.

JDK 17 focused Iceberg binding tests passed 3/3, Planner 6/6, Coordinator
55/55, and Native Avro tests 4/4. The packaged JNI SQL E2E passed 2/2:
empty and committed 12-column reads, plus actual nested `east`/`west`
partition bounds. The Native and packaged `osx/aarch64` JNI SHA-256 is
`85db890051b7cb92f5508c64cf2f93bc81e099deb4033059a5e92bdcb0254e40`.
The descriptor change was checked against two complete actual Native fixtures;
Protocol full-module tests passed 441/441. These in-process results do not
close independent-image, cross-filesystem, all partition-bound types, or the
remaining Iceberg `$files`/`$entries`/`$partitions` and Paimon `$files` paths.

### Iceberg `$files` next-path design and misleading legacy facade

The existing `IcebergFilesSystemTable` is **not** an implemented SQL `$files`
scan. `IcebergSystemTables.getSystemTable` exposes it through the old
`ConnectorSystemTable.rows()` facade, while the SQL/Calcite lookup still rejects
`base$files`. Its `table.newScan().planFiles()` loop produces only five Java
`TypedRow` columns (`content`, `file_path`, `file_format`, `record_count`,
`file_size_in_bytes`), with `content` rendered as a string. That scan covers
data-file tasks, not the delete-manifest rows exposed by Iceberg's metadata
table. Its `IOException` handler logs a warning and returns the rows accumulated
so far, which is unacceptable for a distributed metadata query. The nearby
`IcebergSystemTableTest.filesSystemTable_returnsDataFileAfterCommit` tests only
this facade; it is **not** SQL → typed split → Native → Arrow E2E evidence. Do
not route a new SELECT path through this facade or treat its five-column schema
as compatible with the official table.

In fixed Iceberg 1.11.0, `FilesTable.FilesTableScan.manifests()` visits the
selected snapshot's `allManifests(io)`. `BaseFilesTable.ManifestReadTask.rows()`
opens each data or delete manifest through `ManifestFiles.open(...).project(...)`.
`ManifestReader.iterator()` returns `liveEntries()`, excluding `DELETED`
entries; `$entries` has a different status-preserving contract. The `$files`
schema starts with the 21 typed `DataFile.getType(...)` fields, drops the empty
`partition` struct for a fully unpartitioned table, and appends a dynamic
`readable_metrics` struct derived from the base table's primitive columns.
`Partitioning.partitionType(table)` unions active partition fields across the
table's specs by field ID; each manifest's own spec still governs decoding its
partition tuple. Raw metrics include integer-to-long maps and integer-to-binary
bound maps; readable bounds must be decoded using the source column type.
This schema must come from the SDK metadata-table schema at binding time, with
typed nested projection preserved across Calcite, v2 splits and Velox.

The [Iceberg format specification](https://iceberg.apache.org/spec/) defines
v1–v3 leaf manifests as Avro and v4 manifests as incompatible Parquet. V1
entries default absent v2 metadata as specified; v2/v3 entries may inherit
snapshot and sequence numbers from the manifest list, and v3 `first_row_id`
may require row-count-based assignment. A new Native manifest-entry reader may
reuse the bounded Avro OCF decoder from `$manifests`, but it must pin every
selected child manifest's URI, byte length and SHA-256 alongside the selected
snapshot/list identity; verify each object's bytes before emitting rows; apply
the SDK's live-entry and inheritance rules; and handle spec evolution and
the complete nested output types. Unsupported format/version, missing object,
changed bytes, malformed metrics or incompatible projection must fail the
query. V4 remains disabled until its Parquet manifest and changed status/schema
semantics are independently implemented and tested. Acceptance needs exact
schema/value comparison against Iceberg 1.11.0 `FilesTable` for empty,
unpartitioned, multi-spec partitioned, data-plus-delete, metrics, deleted-entry,
and time-travel cases, followed by packaged-JNI SQL E2E; the old facade test
cannot satisfy this gate. See also the [Iceberg 1.11 query documentation](https://iceberg.apache.org/docs/1.11.0/spark-queries/)
for the public `$files` metadata-table contract.
