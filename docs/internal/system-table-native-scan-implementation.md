# System-table SQL: Native scan implementation contract

Status: **IN PROGRESS — not production-ready** (2026-10-21). Iceberg
`$snapshots`/`$history`/`$refs` and Paimon `$snapshots`/`$tags`/`$options`/`$schemas` have typed
v2 splits, Worker Native readers, and focused SQL E2E coverage. Paimon
`$branches` now has the same chain for local filesystem catalogs only. Iceberg
`$manifests` has a pinned Avro manifest-list Native reader. Iceberg `$files`
has fixed scalar/array fields, a typed `column_sizes` map and field-ID-based
partition structs; its complete SDK schema,
large-table throughput, and independent image are not yet validated. Iceberg
`$entries` now has a pinned, status-preserving Native lineage slice with v3
`first_row_id` inheritance; its complete SDK nested schema remains open. Iceberg
`$partitions` now expands frozen entry/snapshot inputs into ordinary Native
joins and aggregates, with SDK schema and focused SQL edge-case coverage;
Paimon `$files` now has its twenty-column Native path; its broader type/filesystem
matrix, nonlocal `$branches`, and the final full-chain gate remain open. This is an implementation specification and failure record, not a
production-readiness assertion.

## Task-owned metadata reads

Every executing system-table scan receives the same `TaskObjectReadBudget` and
cancellation token as ordinary file scans. `MetadataReadContext` carries that
owner into Iceberg JSON, manifest-list and manifest-entry readers, and Paimon
snapshot/schema/tag/options and files readers. A task without an admitted owner
fails; the explicitly unscoped context is reserved for standalone format
inspection. Typed read-unit structures group the immutable parent/child object
references and snapshot declarations instead of extending scalar constructor
argument lists.

Opening a filesystem object and obtaining its initial size hold an active read
attempt. Payload ranges, including checksum passes and subsequent parsing reads,
consume the shared task grant. Splits and parent/child objects do not create new
allowances. The immutable file size is cached while its owner is active, allowing
cleanup to inspect that value after cancellation without starting backend I/O.
Task reports publish the owner's requested-byte ledger in
`usage.object_store_read_bytes`, including local filesystem metadata reads.

Local DataSource cancellation and the task token are combined with Folly's
[cancellation-token merge](https://github.com/facebook/folly/blob/main/folly/CancellationToken.h).
Avro checks that token both before reading a record and while decoding cached
record values. Cancellation remains cooperative: an already-running synchronous
backend call retains its owner until it returns; stopping the consumer does not
pretend that the backend has stopped. The implementation preserves the pinned
Velox [ReadFile contract](https://github.com/facebookincubator/velox/blob/main/velox/common/file/File.h),
including the distinction between a synchronous default future and native
asynchronous I/O. Exact acceptance results are recorded in the rolling inventory.

## Paimon `$files` implementation

The Paimon 1.4.1 `FilesTable.TABLE_TYPE` declares twenty columns. The
[official system-table reference](https://paimon.apache.org/docs/2.1/concepts/system-tables/)
also identifies optional row tracking and written-column metadata; its version
must not replace the exact pinned SDK serializer/display contract. The connector
now resolves `$files` to a read-only selector over the physical base table,
refreshes the SDK catalog entry before a new binding, and freezes the selected
snapshot, schemas and original manifests. It uses SDK `FileEntry.mergeEntries`
with source provenance to select active file identifiers; it does not invent a
second ADD/DELETE merge algorithm. A later catalog schema update does not change
an already-bound query's schema. Explicit historical selectors publish their
snapshot ID in `ConnectorReadBinding`, as ordinary versioned reads do.

The v2 context carries immutable source references, file-identity digests,
physical URIs and schema declarations. Declarations include only schema ID,
fields and key definitions; table options and unrelated configuration are omitted.
It carries no projected system-table rows or preformatted statistics. The complete identifier includes partition,
bucket, level, file name, extra files, embedded file index and external path.
Java and Native use the same domain-separated, big-endian length framing.
Native validates snapshot and manifest SHA-256, decodes original Avro and
BinaryRow fields, and emits vector batches. Unknown, duplicate or missing
selected identities fail rather than returning an incomplete successful result.
The former six-column Java row materializer and its resolver entry are removed.

The output columns are `partition`, `bucket`, `file_path`, `file_format`,
`schema_id`, `level`, `record_count`, `file_size_in_bytes`, `min_key`, `max_key`,
`null_value_counts`, `min_value_stats`, `max_value_stats`, `min_sequence_number`,
`max_sequence_number`, `creation_time`, `deleteRowCount`, `file_source`,
`first_row_id`, and nullable `write_cols ARRAY<VARCHAR>`. The camel-case delete
count name follows the pinned SDK. Statistics use the SDK 1.4.1 TreeMap display
contract, not the JSON representation shown by other Paimon versions. Field-ID
mapping preserves renamed columns and supplies all-null counts for newly added
columns without rewriting old manifest statistics.

Decimal metadata text follows the pinned SDK's `Decimal.toString`, which uses
`BigDecimal.toPlainString`: small values retain their complete declared scale.
Float and double metadata text follows the supported JDK 17 methods, including
legacy integer, subnormal and decimal-boundary behavior. The Native
`PaimonFloatingText` helper uses bounded exact rational arithmetic and defined
unsigned operations for fixed-width compatibility; it does not call Java to
format each value. Shortest-number printers alone do not implement that contract.
The checked-in, independently generated JDK oracle has 40,405 records; the larger
1,034,162-record differential comparison and actual append/primary-key SQL
checks are documented in the rolling inventory. These are finite conformance
corpora, not exhaustive IEEE input enumeration.

The SQL scalar matrix also compares integer limits, fixed/variable text,
decimal widths, date/time boundaries, timestamps, local-time-zone timestamps,
binary declarations and NULL counts with the SDK. The real Parquet writer does
not supply min/max statistics for binary or nanosecond-timestamp fields in these
fixtures. Their NULL statistics match; this does not qualify non-null bounds for
those types, complex-type statistics, or the complete filesystem matrix.

The current local SQL evidence covers append and primary-key files across
commits, empty tables, snapshot selection, rename/add-column evolution, SHOW,
partition overwrite membership, base SELECT revocation, live row policies and
column masks. Controlled SDK serializer fixtures additionally exercise non-null
written columns, external paths, compaction source, first row IDs, extra files
and embedded indexes. These fixtures are explicitly distinguished from actual
batch-write SQL evidence in their README. The full scalar-statistics type and
filesystem matrices, larger metadata objects, scale testing and final same-source
whole-program acceptance remain open; this section is not a production-readiness
claim. Exact test results and sanitizer status belong to the rolling inventory.

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

This describes the original audited baseline, before the fixes recorded below.
The 2026-09-27 full Java test log records
`MutationHistoryAuditTest$SnapshotLineage` failures: `snapshot_id` and
`committed_at` were not found; a `$history` star query scanned the base table
and returned zero rows. The former Java `IcebergSnapshotsSystemTable` also
declared a custom `timestamp_ms` integer and serialized summary string. The
current Native snapshot schema migration replaces these with SDK-compatible
`committed_at`, `manifest_list`, and a typed summary map; see the latest entry
below.

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
  removed. SHOW FILES and SHOW MANIFESTS now use their existing direct Native
  `$files` and `$manifests` SELECT paths; SHOW PARTITIONS uses the same frozen
  relational definition as direct `$partitions` SQL. Direct SELECT for Iceberg
  `$files` has the validated scalar/array slice, a typed `column_sizes` map and
  a field-ID-based `partition` struct where the SDK exposes one; full Iceberg `$files`/`$entries`, full partition-type/filesystem acceptance,
  the Paimon `$files` type/filesystem matrix, and nonlocal `$branches` remain open. Paimon `$snapshots`,
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
  SHOW expansion now uses typed relation components and literal predicates,
  including mixed case, Unicode, dots and quotation marks inside one component.
  It builds an AST directly and uses the same Native reader as direct SELECT.
  The former ASCII-only generated SQL restriction has been removed; the quoted
  literal `$` restriction described below remains a separate open boundary.
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
5. **Remove the parallel facade.** SHOW SNAPSHOTS/FILES/MANIFESTS and
   TAGS/BRANCHES route through the same system-table queries; the former
   informational tasks are removed. Complete the PARTITIONS Native relation
   before enabling its SHOW form. Evolve/remove the current name-only `ConnectorSystemTable` SPI
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
| Calcite catalog | `pangrova-coordination/pangrova-coordination-core/.../metadata/read/LazyMetadataSchema.java`: dynamic `Lookup` resolves supported `$kind` names after canonicalizing the base table without enumerating virtual names; `MetadataResolvedTable.java`: exact column types |
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
  now uses the SDK `committed_at` timestamp column; Paimon snapshot SQL
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

### Iceberg `$files` six-column Native increment (2026-10-16)

Direct SQL `base$files` now resolves through Calcite lookup and the base
table's SELECT grant. The connector selects the exact Iceberg SDK metadata
table, but declares only the six scalar columns implemented by the Native
reader: `content INTEGER`, `file_path VARCHAR`, `file_format VARCHAR`,
`spec_id INTEGER`, `record_count BIGINT`, and `file_size_in_bytes BIGINT`.
This is deliberately a partial SQL schema: `column_sizes`, bounds,
`readable_metrics`, partition fields, and other SDK columns are not exposed
by this slice. A query naming one fails during Calcite validation; `SELECT *`
returns only those six columns. The full 21-plus-column dynamic SDK schema
described above remains the target, not a claim of current compatibility.

The Java binding freezes a v2/v3 snapshot's manifest-list URI, length, SHA-256,
snapshot and sequence numbers, and each child manifest URI, length, SHA-256,
partition-spec ID and content kind. A typed split represents one child. Native
verifies both objects and exact parent membership before reading bounded Avro
OCF batches; it emits live DATA and DELETE manifest entries and skips records
marked DELETED. V1 snapshots without a manifest list, unverified v4 formats,
changed objects, malformed entries, and unsupported SQL columns fail closed.
The former five-column Java facade is not used for this SELECT path.
Iceberg and Paimon metadata readers share one strict local `file:` URI decoder:
escaped spaces resolve to their actual path, while malformed escapes, encoded
path separators, nonlocal authorities, and dot segments are rejected. S3-style
URI normalization retains its existing scheme behavior.

JDK 17 connector binding tests passed 3/3; the adjacent `$manifests` binding
tests passed 3/3. Native focused tests passed 71/71, including URI decoding,
file entries, existing metadata readers, and Shuffle regressions. The
packaged-JNI SQL E2E passed 1/1 for the `$files` slice and 2/2 for the
adjacent `$manifests` regression: SQL values were compared with a separate
Iceberg SDK read after a real data insert and position-delete commit. The
SQL test also checks an empty table, full six-column star output, and a
`PLANNER-0001` rejection for `column_sizes`.

This functional slice does not establish large-table throughput: every child
split currently reopens and hashes the full parent manifest list to prove
membership, so a snapshot with M manifests can incur O(M²) total parent-list
work. A verified precomputed parent index or equivalent bounded identity
proof is needed before claiming production-scale performance. Multi-spec
partition data, all nested metrics, time travel, nonlocal object storage,
independent process image, and the full system-table gate remain open.

### Iceberg `$files` fixed-array increment (2026-10-16)

The binding now takes three more fields from the running Iceberg 1.11 SDK
`MetadataTableType.FILES.schema()` in its official relative order:
`split_offsets ARRAY<BIGINT>`, `equality_ids ARRAY<INTEGER>`, and nullable
`sort_order_id INTEGER`. Native reads those values from the verified child
manifest's `data_file` record and constructs typed Velox arrays. It keeps
SQL NULL distinct from an empty array, bounds each array and batch, rejects
wrong element types or unsorted split offsets, and requires equality IDs on
equality-delete files. The declared SQL schema is now nine columns;
`SELECT *` returns those nine, while `column_sizes`, dynamic `partition`,
`readable_metrics`, and the other unsupported SDK fields still fail at
lookup. This remains a partial public `$files` contract.

Current-source JDK 17 Iceberg binding/adjacent manifest tests passed 6/6,
Planner SQL conversion passed 7/7, and Coordinator lookup/access-control
passed 55/55. The Native manifest-entry suite passed 6/6, including NULL,
empty and populated arrays, DATA and position/equality DELETE manifests,
pin checks and type rejection. A freshly packaged `osx/aarch64` JNI library
(SHA-256 `f7d17bd20a7ab7b030684b675925cd37e71618e9dff3d7a12017df58a1ca603c`)
passed the real SQL E2E 1/1 with independent Iceberg SDK values for all three
file kinds and a nine-column `SELECT *`; all listed tests had zero failures,
errors or skips. This increment did not change protobuf or the capability
descriptor. The complete SDK schema, parent-list O(M²) split cost, cross-file
system-table coverage, and production-scale independent image remain open.

### Iceberg `$files` typed `column_sizes` increment (2026-10-16)

The running Iceberg 1.11 SDK's `FILES.schema()` exposes optional
`column_sizes MAP<INTEGER,BIGINT>` after `file_size_in_bytes`; Apache Iceberg's
[table specification](https://iceberg.apache.org/spec/)
assigns field ID 108, key ID 117, and value ID 118. The SQL declaration now
has ten columns in that official relative order. The Native reader validates
the child Avro writer schema's logical-map encoding and all three field IDs
before decoding, then produces a typed Velox map. It distinguishes SQL NULL
from an empty map, rejects duplicate/nonpositive column IDs and negative sizes,
and bounds per-row and per-batch element counts. `value_counts` and the other
unsupported SDK columns still fail at SQL lookup rather than yielding a
placeholder; v1/v4 and unsupported manifest formats remain fail closed.

Current-source JDK 17 binding tests passed 3/3, full Release Native CTest
passed 4,167/4,167 with the real PostgreSQL Lance broker, and packaged-JNI
`SystemTableE2ETest` passed 24/24 against the Iceberg SDK and real test
containers. The matching ASan reader suite passed 8/8 with
container-overflow checks enabled and no sanitizer finding. ASan required an
isolated Folly archive compiled with address instrumentation: the Velox
`deps-install` Folly archive lacks it, causing a false container-overflow when
`parseJson` grows a schema-field vector that instrumented Pangrova code reads.
The optional `PANGROVA_ASAN_FOLLY_PREFIX` build input selects that matching
archive only in ASan configurations; `cmake/AsanFollyCompat.cmake` disables
macOS libc++'s unavailable UBSan vptr RTTI dependency without disabling ASan.
The affected Iceberg Maven dependency-chain static-analysis gate passed. The
whole-source Mac ASan CTest passed 4,167/4,167 with the real PostgreSQL Lance
broker and no address finding (`detect_leaks=0`); Linux LSan remains an external
gate. The full SDK schema, parent-list O(M²) cost, and the full
production image gate remain open.

### Iceberg `$entries` pinned lineage slice (2026-10-19)

Direct SQL `base$entries` now resolves to a typed, read-only selection over the
same pinned manifest list and child objects as `$files`. The v2 split carries a
distinct `ICEBERG_ENTRIES` kind, and Native checks both object digests and the
child's membership in the selected list before emitting rows. It retains
`status` 0/1/2, resolves null `snapshot_id` and `sequence_number` using the
manifest-list's `added_snapshot_id` and `sequence_number`, and requires explicit
sequence numbers on existing/deleted v2/v3 entries. The current flattened
projection is `status`, `snapshot_id`, `sequence_number`,
`file_sequence_number`, `file_path`, and `data_sequence_number` (the latter
aliases the resolved data sequence), and nullable `first_row_id`. Native keeps
the file sequence independent from the data sequence: an added entry inherits
either null value from the pinned manifest list, while an existing or deleted
entry must carry both original values. For v3 data files, Native preserves explicit row IDs
or assigns null IDs from the manifest's first row ID plus preceding null-ID
files' record counts; v2 returns NULL. This is not the complete Iceberg SDK
`$entries` nested schema. V1 and v4 continue to fail closed.

The nearest Planner test, Coordinator lookup test, Connector binding test,
Native reader suite and packaged-JNI `IcebergV3LineageTest` all pass on the
2026-10-19 source. The SQL suite runs four real Iceberg JDBC cases, including
v2 and v3 writes, sequence-number progression, v3 inherited row IDs, v2 NULL
row IDs, and direct Native reads.

### Iceberg `$files` unified partition projection (2026-10-20)

The Iceberg 1.11.0 SDK's
[unified partition type](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/Partitioning.java)
uses stable partition field IDs across spec changes. The connector now exposes
the SDK's typed `partition` struct when present. Fully unpartitioned tables keep
the SDK schema without that column. Every pinned manifest split carries the
specs' field IDs, canonical output names, result types and transforms. A field
outside the unified type has an empty output name. The same deterministic
encoding and selected metadata location contribute to the read identity, so a
schema/spec change invalidates a cached mapping even when manifest bytes remain
identical.

Native checks the Avro `data_file`/`partition` field identities (2/102), binds
tuple fields by ID, and projects old specs into the unified row type. A rename
retains its value; a new dimension absent from the old spec is NULL. Numeric
promotions preserve values, and logical date/time, microsecond/nanosecond
timestamps, binary/fixed/UUID and signed decimal values retain their typed
physical representation. Invalid field IDs, ambiguous names, scalar type
changes, integer overflow, decimal precision violations and invalid time values
fail the read. The decoded struct follows the ordinary Native result transport.

Release and matching ASan verification each cover 17 focused cases (15
reader/parser/projection cases plus 2 decimal/retired-field boundaries). Iceberg's complete module
passes 437/437; the protocol module passes 447/447 with inventories recaptured
from the new exporter and real JNI startup. Fresh packaged JNI passes 3 new
SQL cases across incremental runs for NULL partitions, field rename, a newly
added bucket dimension, and full struct return through Flight; the existing
lineage suite passes 4/4 and manifest-list SQL cases pass 2/2. All 3 new SQL cases
also assert physical query ownership and active query count drain to zero.
The full-column case found a duplicate Java column whitelist rejecting the
newly bound partition column; column validation now uses the frozen typed
schema, with a regression assertion. Scalar-only reads avoid copying partition
tuples. Java static verification passes for the affected dependency chain and
final changed leaf modules. This incremental evidence does not replace final
full-source regression.

`SHOW PARTITIONS`/`$partitions` still need their aggregation path. The upstream
[partitions implementation](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/PartitionsTable.java)
reads live data/delete entries from the selected snapshot and derives the latest
update from retained snapshot metadata. Cross-spec grouping, NULL/presence
semantics, delete-only partitions and unavailable historical snapshots require
SDK-oracle cases before that path can close. Query expansion must retain bound
relation identity and authorization; the existing textual `ViewExpander` is not
an adequate implementation for the public aggregate metadata relation.

One additional upstream invariant matters for that aggregate: its partition
comparator also compares `StructProjection.projectedFields()`. Two coerced
tuples whose visible values are NULL can still be different keys when their
source specs project different numbers of fields. The frozen spec field IDs can
provide this presence count alongside `spec_id`; grouping by the visible struct
alone would lose it. The next SDK-oracle matrix must cover this case explicitly.

### Iceberg `$entries` nested data-file projection (2026-10-21)

The current Native declaration replaces the earlier flat lineage slice with
the SDK's five top-level fields: `status`, `snapshot_id`, `sequence_number`,
`file_sequence_number`, and `data_file`. The data sequence is the standard
`sequence_number`; the synthetic flat alias has been removed. File path and
row identity are now `data_file.file_path` and `data_file.first_row_id`.
The nested file projection also includes `content`, `file_format`, `spec_id`,
`partition` when partitioned, `record_count`, `file_size_in_bytes`, `column_sizes`,
`split_offsets`, `equality_ids`, and `sort_order_id`, with types and order selected
directly from the pinned SDK's entries schema. It uses the same Native typed
batch renderer as `$files`; entries retain deleted status rows as required by
[the SDK entries contract](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/ManifestEntriesTable.java).
The old Java entries row materializer and its resolver registration are removed.
The shared renderer does not broaden the standalone `$files` declaration:
undeclared top-level `first_row_id` is rejected there, while the entries nested
projection supports it with the actual v3 inheritance semantics.

Each file/entry selection first captures one immutable SDK `TableMetadata` and
uses [StaticTableOperations](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/StaticTableOperations.java)
for the declaration, spec mapping and manifest graph. A deterministic regression
commits a spec rename during manifest-list I/O: the unprotected implementation
mixes the old object selection with the new field name, while the frozen view
retains the selected metadata location and old partition field. Later live
validation still checks table identity and the selected objects.

This is the canonical nested projection needed for partition aggregation;
remaining SDK metrics fields and `readable_metrics` are still unimplemented.
`$partitions` itself is not exposed by this change. The rolling inventory records
Native, SDK-oracle SQL and policy verification results separately.

### Metadata read grants (2026-10-20)

`$entries` now follows the same base-table SELECT authorization as the other
implemented metadata suffixes. A grant on the suffix name cannot authorize
metadata from an ungranted base table. The Coordinator regression covers all
nine supported suffixes, case-insensitive `ENTRIES`, both planned and explicit
SELECT checks, and rejection of an unknown suffix under a base-only grant.
The real authenticated HTTP SQL regression uses JDBC grants and reads the
actual Native Iceberg `$snapshots`, `$history`, `$files`, `$manifests`, and
`$entries` tables. For each relation, suffix-only grants are denied, granting
SELECT on the base permits a nonempty read, and revoking that base grant denies
the next read. Query ownership and active query counts drain to zero. The
nearest real-RBAC suite passes 56/56. Coordinator rolling verification covers all
3,305 invocation records: 3,295 initially pass, then the 10 cases requiring the
explicit current Lance fixture tool pass on their own; static verification
also passes. This is rolling coverage, not one continuous all-green run. This
qualifies SELECT grant identity. The later policy increment below adds base
row-filter and mask eligibility checks for the implemented metadata family.

The relational aggregate approach for `$partitions` also has a current upstream
precedent in [Trino's PartitionsView](https://github.com/trinodb/trino/blob/484/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/system/PartitionsView.java).
Its public schema and grouping differ from the Apache Iceberg SDK contract used
here. Reusing the architectural approach therefore does not replace SDK-oracle
checks for delete counts, spec presence, retained snapshot timestamps and empty
input semantics. Typed connector declarations and query-owned bindings for
the resulting scans remain prerequisites for exposing this derived relation.

### Calcite view expansion context (2026-10-20)

`PangrovaPlanner` now supplies a conversion-local `PlannerViewExpander` to
Calcite. Standard `ViewTable` relations expand through the same parser and
validator as the outer query, sharing its cluster and catalog root. Each
view body resolves unqualified names in its definition schema; expanded scans
retain their base relation identities for later policy application and source
binding. CTEs, aliases and repeated references use normal relational conversion.
A view name appearing inside an output string literal remains data. Mutation
definitions, missing relations, direct/indirect recursion and more than 64
nested views fail compilation. Active expansion state is scoped to the
conversion and unwound even when a definition fails.

Six initial view regressions and the full Planner module pass (1,470 tests).
An added depth-boundary case passes separately, and the string-literal case
was strengthened and rerun to assert the actual output expression. Static
verification passes. Updating the installed Planner also passes the 3 actual
activation SQL/JNI smoke cases, including Native write/read and tableless
multiplicity/NULL handling. The smoke checks ordinary SQL integration; it does
not prove public metadata-view execution. Connector typed derived declarations
and their frozen source bindings are still needed, and the older Coordinator
textual view expansion remains a migration gap. `$partitions`/`SHOW PARTITIONS`
are not complete at this point.

### Frozen connector definitions and subquery authority (2026-10-21)

The experimental `ConnectorDerivedTableHandle` now supplies a
`ConnectorDerivedQuery`: static read-only SQL plus explicitly named immutable
`ConnectorReadBinding` inputs. Each input requires an exact declaration and a
frozen source identity. The definition catalog contains only these aliases;
physical table names are never interpolated into its SQL. Expansion shares the
outer Calcite cluster, validates exact output names and storage types (allowing
nullability differences), and produces ordinary scans carrying the original
physical names and binding objects. Recursion, excessive nesting, mutation,
ambient catalog access, schema drift and definitions retaining no frozen source
are rejected. Connectors provide definitions and bindings, not result rows.

Before policy application, the planner also exposes expression subqueries as
relational children using Calcite's structural
[SubQueryRemoveRule](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/rules/SubQueryRemoveRule.html)
rules. This ensures table authorization and row/column policies see inner
sources. Existing LATERAL decorrelation runs before that structural expansion;
running it again over the newly introduced correlates changes scalar aggregate
and distribution behavior. Subsequent optimization may reduce an inner key to
a literal or expression under a policy. Correlate conversion must preserve the
resulting equality rather than assume both operands are raw columns.

Correlate conversion now materializes deterministic expression/constant keys
before the existing equality-based grouping and join conversion. This preserves
deduplication for existence, nullable absence markers, and COUNT defaults for
unmatched scalar aggregates. A NULL mask may remove the actual correlation
entirely while leaving `requiredColumns` unchanged. Conversion checks
[Calcite's scoped variable collector](https://github.com/apache/calcite/blob/calcite-1.42.0/core/src/main/java/org/apache/calcite/plan/RelOptUtil.java)
against the remaining right relation, preserving normal join cardinality when
no outer binding remains. Any remaining unhandled free variable still fails
conversion. Real SQL/JNI cases cover policy reductions, masked computed keys,
duplicates, NULL comparisons and empty-input COUNT/COUNT+1/SUM semantics.

The PostgreSQL/plugin/Paimon SDK integration validates that a later commit
does not change the selected source or its split snapshot. This mechanism has
no production connector provider yet; canonical Iceberg entry projection,
snapshot metadata and SDK-oracle aggregation remain prerequisites for exposing
`$partitions`. It does not complete that public table or migrate Coordinator
textual views. Current test results and outstanding failures are recorded in
the [rolling inventory](ai-native-research/validation-inventory-2026-10-19.md).

### Base policy eligibility for metadata reads (2026-10-20)

Metadata scans now invoke the mandatory SPI `checkCanReadTableMetadata` against
the base table. The production RBAC implementation requires full SELECT and
rejects an applicable base row filter or column mask before any metadata task
is dispatched. This applies to table, statement and metadata-column checks.
All implemented suffixes use the same eligibility rule. Ordinary data queries
retain their existing Native filter/mask execution.

The policy check resolves roles once for both privilege and policy selection.
A deterministic regression reproduced a role revocation between grant loading
and a second role lookup that erased an applicable mask; one captured role set
now prevents that raw-read path. Table-wide mask enumeration is independent
of projection and current columns, and uses normal enabled/role/priority/exempt
selection. PostgreSQL queries bind every value to fixed statements; storage
errors propagate. In-memory mask replacement also includes catalog and schema,
so equal names in separate scopes cannot remove each other's policies.

The real SQL/JNI matrix passes 6/6 across Iceberg and Paimon. Each connector is
written through SQL, its metadata counts are recorded through Native, and the
matrix covers suffix-only grants, base SELECT, revocation, actual filtered or
masked data, metadata denial while protected, and exact metadata-count recovery
after policy removal. Nine suffix kinds are covered, including valid empty
Paimon tag/branch tables. SHOW SNAPSHOTS/FILES/MANIFESTS for Iceberg and SHOW
SNAPSHOTS for Paimon also reject protected reads. Every case drains query
ownership and active counts to zero. Coordinator nearest policy/grant checks
pass 20/20 and 56/56; SPI full tests pass 721/721. Security's 390-test owner run
passes, followed by the new race case and affected RBAC regressions; the rolling
Security reports contain 391 passing invocations. PostgreSQL mask storage,
policy-store failure, priority/exemption and scope isolation are covered, and
static verification passes for Security/SPI, Coordinator and the changed consumers.
The subsequent full Coordinator run initially passes 3,324 of 3,325 cases; its
single failure is a Worker task-route HTTP 404 in a source-free fixture startup
check, before query execution. Worker startup now eagerly initializes the Jersey
servlet before returning: Jetty's default negative initialization order otherwise
defers servlet initialization until first use. The first-request probe remains a
single request, without retries. The 11 cold-start routing/authentication checks,
the full Worker owner run (1,520 invocations), and 3 SQL/JNI activation smoke cases
pass. With `full-regression` enabled so native-required tests actually execute,
all 12 report-delivery integration cases pass after the repair. The Coordinator
rolling union therefore contains 3,325 passing invocations; this is not a single
continuous full run. Evidence is recorded in
`/tmp/pangrova-metadata-policy-coordinator-rolling-union-20261021.json`.
Four policy scenarios were then strengthened
to warm reads and add/remove policies on the same pipeline; only those changed
scenarios rerun, and all 4 pass without a pipeline rebuild.

[Athena's restriction](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
is a precedent for refusing sensitive unfiltered Iceberg metadata under row/cell
policies. Pangrova applies this rule to its entire implemented metadata family;
it does not claim that every engine uses that same family-wide restriction.
The current tests qualify foreground metadata reads. Deferred/background
metadata sources and policy changes during already running executions still
need their own matrix. Existing SHOW TAGS/BRANCHES return only a pointer to a
system-table query; those placeholders remain to be replaced with actual
Native listing paths. `$partitions` aggregation and final full-chain validation
remain open.

### Iceberg snapshot references (2026-10-21)

`table$refs` now binds a read-only `IcebergSystemTableHandle` to one immutable
metadata JSON location and length, emits an `ICEBERG_REFS` split, and reads the
reference map in Native batches. Its six columns follow the
[Iceberg 1.11 query schema](https://iceberg.apache.org/docs/1.11.0/spark-queries/#references)
and the consumed SDK's `RefsTable`: `name`, `type`, `snapshot_id`,
`max_reference_age_in_ms`, `min_snapshots_to_keep` (INTEGER), and
`max_snapshot_age_in_ms`. The legacy row SPI's misspelled
`max_ref_age_in_ms` column is corrected to the same schema and its two missing
retention columns are added; it does not supply rows to this SQL scan.

The reader preserves nullable retention values rather than inventing defaults.
It follows the SDK's compatibility rule: only metadata that omits the `refs`
map and has a current snapshot synthesizes `main`; an explicitly empty map
remains empty. Reference names decode JSON escapes, including Unicode. Enum
types accept the SDK's case-insensitive spelling and emit uppercase values.
Unknown types, malformed maps, duplicate names, invalid retention and changed
pinned object lengths fail. Projected/count scans still validate reference
objects; filter, aggregate and ordering remain ordinary Velox operators.

The first SQL run exposed a second, independent suffix whitelist in
`LazyMetadataSchema`: the Connector supported the new handle but Calcite could
not discover it. `MetadataTableKind` in SPI now provides the single vocabulary
used by AST conversion, lazy schema lookup and base-table authorization.
Connector-specific supported kinds and immutable source binding remain with
each connector. `$refs` therefore receives the same full base SELECT and
row/mask eligibility checks as other metadata reads. Unknown and not-yet-
implemented suffixes, including `$partitions`, remain rejected.

This closes the standard Native reference scan prerequisite. SHOW TAGS and
SHOW BRANCHES still need their query mapping to these connector-owned metadata
relations; their existing informational rows are not counted as listing
coverage. Deferred/background reads and the remaining system-table work stay
in the open validation inventory.

Validation: the refs SDK binding/schema owner passes 438/438, SPI passes
739/739, Planner passes 1,472/1,472 before the shared-kind refactor and its
changed conversion checks then pass 8/8; Coordinator lookup/grant/policy checks
pass 89/89. The actual refs SQL/JNI matrix passes 3/3 (empty table, exact SDK
retention/null values, and SQL create/drop/moving-main versus pinned-tag reads).
The expanded Iceberg/Paimon policy matrix passes 6/6, covering ten implemented
suffix kinds. All cases assert physical query ownership and active count drain.
The first SQL run's six schema lookup failures are retained separately rather
than hidden; final results are in `/tmp/pangrova-refs-sql-final-20261021.json`.

The affected Native discovery contains 62 cases. Release first passes all 62;
only the reader's 9 cases rerun after SDK-compatible type parsing, followed by
the one strengthened long-name projection case. Final matching ASan passes all
62 continuously, with `detect_leaks=0:halt_on_error=1`. The exact names match
current discovery (`/tmp/pangrova-refs-native-qualified-20261021.json`). Current
full Native discovery has 4,190 cases; this batch does not claim a new full run.
Both the native runtime and the installed Worker JAR contain the same rebuilt
JNI bytes. Protocol passes 447/447 against the first recaptured fixtures; only
the affected 7 actual-inventory contracts rerun after final serving fingerprints
are collected. The full inventory retains 19 capabilities, 99 functions and 2
models. Changed Java owners and the E2E leaf pass static verification, and the
temporary inventory collection class is removed. Test-owned PostgreSQL/model
containers are gone, with approximately 78 GiB disk free at completion.

### Structured SHOW listings (2026-10-21)

`ConnectorMetadata.getMetadataListing` declares a Native source kind and fixed
string equality filters in an immutable `MetadataTableListing`. Iceberg maps
TAGS/BRANCHES to `$refs` with the SDK's `type` values; Paimon maps them to its
separate `$tags`/`$branches` relations. Other implemented SHOW kinds use the same
contract; unsupported listings fail explicitly. This declaration contains no
SQL or rows and does not grant authority or freeze table data itself.

`StatementDispatchPhase` resolves the connector declaration using the current
session and expands it into `MetadataTableQuery`. Compilation builds an AST
directly from its name components and literal values, binds through the ordinary
query-local schema and read bindings, applies base-table authorization and
metadata policy eligibility, and only then optimizes and dispatches to Native.
The context retains the original SHOW text for auditing. The expanded query
does not pass through generated SQL or the data time-travel text extractor.
The old ASCII-only string rewriter, both informational SHOW tasks, their
production/test registrations, and the Branch DDL executor's pointer rows are
removed. Branch/tag mutation handling remains with its DDL executor.

The named-source tests also exposed a separate write admission defect: the
compiled target was joined into a dotted string and later split again.
`DmlWriteContext` now carries `QualifiedTableName` components through admission;
empty components fail before dispatch. The physical output binding remains the
write identity consumed by Native. The protocol's existing `target_table`
string is still a display value for these physical sinks. The JNI write
descriptor no longer carries catalog/schema/table strings: the Java writer
resolves its immutable output handle by the exact task identifier. Native no
longer reparses the display name to select a write target.

Validation: SPI passes 742 tests, Planner 1,475, and Coordinator 3,329 before
the subsequent write-identity correction. The initial SHOW SQL batch passes
17 of 19 cases. The two quoted-name cases expose write-admission,
Iceberg local-path URI parsing, JNI display-name parsing and Worker storage
scope parsing defects. With these causes fixed, a fresh real HTTP/SQL/JNI batch
passes 106/106: both quoted Iceberg/Paimon names, 13 writer lifecycle cases,
75 cleanup/error cases, 2 concurrency cases, 6 metrics cases and 8 catalog/tenant
isolation cases. An unbound task cannot borrow an existing task's output handle;
the existing task remains usable and releases its real files. The raw-path
regression passes 7 Iceberg and 4 Worker cases. Worker scope validation retains
its traversal, authority and warehouse-descendant checks. As described by the
[JDK 17 URI contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/URI.html),
path components require component-aware construction rather than parsing raw
filesystem strings as URI text.

The Release library, packaged resource and installed Worker JAR have identical
JNI bytes; its exported complete capability catalogue is unchanged. The 32
affected Native cases pass in Release and the matching ASan build. The ASan
process reports no AddressSanitizer memory error, but its recoverable Folly
UBSan integer/function-type diagnostics remain open; a successful CTest exit
does not qualify those diagnostics as clean. Raw per-test evidence is retained
at `/tmp/pangrova-task-bound-writer-{native,asan}-raw-20261021.log`, with selected
names and diagnostics in corresponding `*-evidence-20261021.json` files.
The SQL evidence is `/tmp/pangrova-task-bound-writer-real-e2e-20261021.json`.
The final Worker owner run with `full-regression,static-analysis` passes
1,510 tests in 234 fresh report suites, with zero failures, errors or skips;
its evidence is `/tmp/pangrova-task-bound-writer-worker-owner-20261021.json`.
Coordinator and E2E leaf static checks, JNI symmetry (22 methods), SPI annotations
(147 public types), E2E truthfulness and documentation references also pass.
Final full functionality regression remains pending.

### Branch creation source and retention

Branch creation now carries typed `BranchCreationOptions` from both DDL entry
points to the connector. Previously, the parser accepted `FROM SNAPSHOT`,
`FROM TAG` and `RETAIN ... DAYS`, but execution discarded these options.
Snapshot and tag sources are mutually exclusive; identifiers are preserved
exactly, and retention must be positive with checked conversion to milliseconds.

Iceberg publishes the selected snapshot and reference lifetime in one
`ManageSnapshots` commit. A tag source must name an existing tag. Paimon's
pinned SDK supports the current source and a tag source; snapshot sources and
reference lifetimes fail before mutation rather than being silently ignored.

Validation: the SPI owner suite passes 745 tests; complete Iceberg and Paimon
owner runs with static analysis pass 442 and 345 tests, with no skips. The SQL
regression originally reproduced the lost retention value. The first post-fix
E2E run passes 6/8: the E2E bootstrap omitted the four standalone branch/tag
mutation tasks that production already registered. Restoring those tasks makes
the two historical-source cases pass 2/2. Three additional E2E cases pass 3/3:
all four standalone mutation tasks, missing tag rejection, and zero/overflowing
retention rejection with no branch publication. Thus the rolling mutation union
is 11/11; previously passing cases were not rerun. The E2E leaf's static check
also passes. The Coordinator owner run exposed ten missing SDK fixture path inputs:
`pangrova_lance_fixture_tool` was already built, but its property was not passed
to Surefire. The parent POM now supplies the canonical Native build output to
every real-SDK fixture consumer. An alternate build directory requires an
explicit `-Dpangrova.lance.fixture.tool=/absolute/path/to/pangrova_lance_fixture_tool`;
the fixture still rejects a missing/nonexecutable tool. The first affected real
PostgreSQL/SDK lifecycle pair and Coordinator static checks pass 2/2; the other
eight SDK failure cases pass in a later 22-case increment that also checks the
affected HTTP/JNI scenarios. Logs include
`/tmp/pangrova-branch-options-coordinator-sdk-r2-20261021.log` and
`/tmp/pangrova-coordinator-http-sdk-incremental-20261021.log`.
The original owner invocation remains recorded as 3,329 cases with 11 errors:
ten missing fixture inputs and a separate HTTP listener collision. Their
incremental recovery is recorded by exact case in
`/tmp/pangrova-branch-options-coordinator-rolling-20261021.json`; final full
functionality regression remains pending.
Other evidence: `/tmp/pangrova-branch-options-real-e2e-20261021.json`,
`/tmp/pangrova-branch-options-{iceberg,paimon}-owner-20261021.json`, and
`/tmp/pangrova-branch-options-e2e-static-20261021.log`.


### Whole-record selection aggregate prerequisite

`MAX_BY`/`MIN_BY` and their `ARG_MAX`/`ARG_MIN` aliases now use Calcite's
selection aggregate operators and the existing Native implementations. Their
shared bindings use the engine extension URN rather than claiming a standard
Substrait aggregate declaration. Selecting `ROW(spec_id, snapshot_id)` keeps
both fields from one record at the extremal key; independent maxima would mix
records. NULL keys are ignored, selected NULL values remain NULL, and ties may
choose any matching complete record. Partial/final execution retains these
semantics.

Planner rolling validation covers 1,496 cases: the owner run's single old
catalogue-digest error was repaired by recapturing the actual exporter catalogue
and rerunning only that method. Protocol owner/static validation passes 447/447;
real Iceberg scan SQL/JNI passes 6/6. Two new Native cases execute 16 task plans
in Release, and the matching ASan cohort passes 6/6 including four registration
checks. The ASan log contains four previously audited Folly unsigned-integer
hash diagnostics, with no memory or function-type error. Current discovery has
4,194 Native cases; a final full run remains pending. Actual Worker publication
retains 19 capabilities, 101 functions and two models; the exporter publishes
9 capabilities and 85 functions. See the capability fixture README for hashes.
Evidence is `/tmp/pangrova-selection-aggregate-qualified-20261021.json`.

This change does not expose `$partitions`. The provider still needs canonical
snapshot timestamp input, SDK partition-presence grouping semantics, and the
floating-point grouping boundary matrix before it can be accepted.

### Canonical snapshot input for derived aggregation

The snapshot input now follows the pinned Iceberg 1.11
[SnapshotsTable schema](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/SnapshotsTable.java)
and [SnapshotParser semantics](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/SnapshotParser.java).
The six columns are `committed_at` (timestamp with zone, precision 6),
`snapshot_id`, `parent_id`, `operation`, `manifest_list`, and
`summary` (string-to-string map). Native constructs exact timestamps directly
from integer milliseconds. Summary excludes operation; absent/empty JSON
summaries produce NULL, while an operation-only summary produces an empty map.
A nonempty legacy summary without operation uses the SDK's specified overwrite
operation. Non-string values and duplicate summary keys reject the scan.
The reader caps each batch at 4,096 rows even when callers request more.
The obsolete Java snapshot row materializer and its registration are removed;
its two row-list tests are replaced by Native and real SQL tests. Existing
snapshot consumer SQL now orders by `committed_at`.

Validation: the closest binding suite passes 17/17 and the Iceberg owner/static
suite passes 441/441. Release and matching ASan each pass all 12 affected reader,
history and refs cases; this cohort's complete ASan log contains no memory or
UBSan diagnostics. Three new real SQL/JNI cases compare all values with SDK
snapshots, compare SHOW with SELECT, and verify timestamp selection aggregation,
map projection and empty input. Four affected incremental-read/DML-lineage cases
pass separately, for 7/7 real SQL cases. E2E static, documentation references and
truthfulness checks pass. Release, packaged resource, Worker classes, built JAR
and installed JAR contain identical JNI bytes (SHA-256
`963d1c689e63533790a394dfd37d7d8b527889deaecb84735609634db25ff736`).
Evidence: `/tmp/pangrova-snapshot-qualified-20261021.json` and
`/tmp/pangrova-snapshot-native-artifacts-20261021.json`. Discovery now finds
4,197 Native cases; final full functionality regression remains pending.

This completes the snapshot input prerequisite, not `$partitions`. Its derived
provider must still preserve SDK spec-presence grouping and distinguish signed
floating zero while treating all NaNs as one group. Ordinary SQL floating
GROUP BY equality differs from the SDK's Java natural-order comparator here;
the provider must establish that behavior with SDK manifest and Native oracles
before publication.


### Frozen partitions relation: implemented and incrementally verified

Iceberg now publishes a production derived handle whose two inputs are selected
from one immutable SDK metadata object. The Planner expands it before ordinary
base-table privilege and metadata policy checks. Execution uses Native live-entry
scans, a left join to retained snapshots, and ordinary aggregate operators.
The public schema comes from the SDK PARTITIONS schema, with 11 fields or nine
for unpartitioned tables. The obsolete Java partitions row aggregator is removed.

Grouping includes the SDK projected-field presence count and every unified
partition value. Existing Native IEEE functions provide binary keys that retain
signed zero while canonicalizing NaNs; no new private key encoding or Java row
aggregation is introduced. Data and both delete kinds have separate counters;
deleted entries are excluded and delete-only groups remain. A whole-record
MAX_BY pairs spec/snapshot with the maximum retained commit time. Expired entry
snapshots produce NULL last-update fields and default spec 0. Ties may choose
any valid whole record at the maximum timestamp, as the SDK's parallel input
order is not stable. Empty unpartitioned input still produces zero groups.

Real SQL/JNI validation passes 16/16: nine new SDK/boundary cases, six privilege,
revocation and row/column-policy cases, and the original SHOW PARTITIONS case.
The oracle covers v2/v3 deletion metadata, removed entries, delete-only groups,
spec presence with all-NULL values, renamed fields, expired snapshots and both
floating widths' signed-zero/NaN/infinity/subnormal/NULL boundaries. The matching
Native rolling union passes 70 cases in each build, including the 65 affected
Arrow/serialization/shuffle checks. The final constant/lazy/dictionary index
refinement reruns only its two affected cases per build. Strict incoming wire
names/types and actual NULL validation remain in place.

No ASan memory or function-type errors occur. Recoverable optional integer
diagnostics remain qualified: six Hash/Spooky observations in the function
cohort, 20 Hash/F14 observations in the serde cohort, and the F14 control's own
conversion observation. The F14 decrement is defined unsigned arithmetic under
its nonzero guard; the actual function matches explicit subtraction/increment
for 494,722 boundary operations, without changing sanitizer flags. Earlier hash
and rotation controls are retained rather than rerun.

Planner owner validation passes 1,502 cases; Protocol 447; Iceberg 443;
Coordinator focused 41; Parser owner 549 plus one newly covered printer case
in a 48-case affected run. SPI has 750 passed testcase records: a mixed
Jupiter/jqwik suite's summary attribute only counts its last engine, so the
reported sum 747 undercounts three cases. A focused four-case invocation
reproduces all four successful records. Final actual-inventory validation is
7/7 after retaining the complete model-instance publication. The final Worker
inventory contains 19 capabilities, 103 functions and two models; the exporter
contains nine capabilities and 87 functions. Model instance signatures change
with serving-instance identity; they are not global fixture constants.

Evidence: `/tmp/pangrova-partitions-qualified-20261021.json`,
`/tmp/pangrova-partitions-native-qualified-20261021.json`, and
`/tmp/pangrova-partitions-native-artifacts-final-20261021.json`. Native discovery
now finds 4,206 cases. Complete partition logical-type/filesystem acceptance,
other system-table schemas/Paimon files and final full functionality regression
remain open; the overall production goal is not complete.
