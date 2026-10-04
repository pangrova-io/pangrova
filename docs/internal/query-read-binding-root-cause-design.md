# Query read binding: snapshot, declaration and statistics

Status: the complete read/schema/statistics/split and time-travel source cohort is
`SOURCE_READY`, including the typed CBO table-identity migration. The new SDK/PG/
SQL-binding tests have not yet run; source readiness is not their PASS result.
The first real parser run found 7 tests, with 6 PASS and 1 ERROR at a trailing
semicolon. The same grammar's single-statement rule now accepts one optional
terminator and still rejects multiple statements. Root reran all 7 original cases
plus 3 case/terminator cases: 10/10 PASS, 0 skipped, in
`/tmp/pangrova-production-time-travel-parser-nearest-r2.log`. The subsequent
unscreened module regression passed Common 240 + SPI 708 + Parser 528 = 1476/1476,
0 skipped, in `/tmp/pangrova-production-parser-full-module-r1.log`. Those results
do not validate the later SDK, Coordinator, typed-statistics or Native cohorts.

## Root cause and production callers

`PlannerWiring` creates an authenticated planner per compilation. Its schema resolves
`MetadataResolvedTable`, but that table exposes only its row type and declaration.
Calcite 1.42.0 therefore substitutes 100 rows through `RelOptTableImpl.getRowCount`.
The separately constructed `CatalogStatisticsProvider` reads connector statistics;
those statistics never become the scan's Calcite table statistic.

`PlanFragmenter` estimates partition count from relational row count and average
row size, using 256 MiB per partition. The existing missing-statistic substitution
can consequently reduce a large real scan to one partition. Adding live statistics
to the schema would not be sufficient: `SplitFetcher` later resolves the table by
name again and may enumerate a different committed snapshot.

Additional evidence in the original callers, now addressed by the source cohort:

- `SessionContextConverter` does not currently bind the authoritative query ID in
  connector properties. `ConnectorQueryScope.beginQuery` occurs after compilation.
- The Iceberg statistics reader uses `currentSnapshot` even for a selected handle.
  The full split reader consults the old unqualified query pin and silently chooses
  current metadata when a selected pin has expired.
- Paimon statistics read the latest snapshot; its split manager does not consult
  that pin. `PaimonTableStatisticsReader` reads latest rather than selected metadata.
- Lance already has an immutable table ID, version, schema fence and historical
  publication evidence. Its exact handle should be retained, not resolved by name.
- `CompilationPhase` constructs its session planner before extracting version
  overrides. A later scan-only override cannot make planning statistics historical.

## One authority, one owner

Retain the existing immutable connector handle. The new generic read binding is
an owner envelope, not a second snapshot authority or a second data path. It owns:

1. The exact immutable connector handle selected for this scan.
2. Existing `ConnectorTableReadMetadata`, including declaration evidence.
3. Immutable `TableStatistics` obtained from that same selected SDK snapshot.
4. Explicit snapshot state and SDK identity evidence sufficient to detect an
   expired or changed selected source before returning executable splits.

The query-local metadata binding cache supplies both schema lookup and statistics
lookup. `MetadataResolvedTable.unwrap` exposes the same envelope to
`PlanSplitSourceExtractor`. The existing `SplitSourceDescriptor` retains it through
stage fragmentation, constraint removal and source deduplication. `SplitFetcher`
consumes that exact handle. It must not re-resolve current metadata when a binding
already exists. Existing `DataSplit.source_snapshot` remains the wire authority;
no new task protocol or feature-specific source protocol is needed.

The envelope is not an SDK object, a row source, a transaction or an authorization
receipt. The connector session remains authenticated and tenant-bound. Column
access validation remains on the exact bound handle. Pinning never grants access.

## Snapshot states

| State | Statistics | Split behavior |
| --- | --- | --- |
| Selected snapshot | Values from that exact snapshot; unknown fields remain unknown | Enumerate that snapshot; missing/changed evidence fails |
| Empty committed table view | Proven row count 0 | No data splits, even if a later write creates a snapshot |
| Exact metadata-object view | Unknown data cardinality unless genuinely known | Preserve the existing fixed metadata-object handle |
| Source without versioned snapshots | Unknown unless its actual contract proves otherwise | Respect actual native-read capability; unknown statistics alone do not deny a read or authorize a live fallback |

An absent snapshot is never interpreted as “read latest” after an empty view has
been selected. No invalid snapshot ID 0 is sent to a snapshot SDK. Missing optional
row count is -1/unknown, not a guessed value. Backend failures and expired selected
snapshots are errors, not unknown-statistic success.

## Ordinary schema versus explicit time travel

An ordinary read selects data snapshot S and the current immutable declaration D
at binding time. An `ADD COLUMN` may create a newer schema without a new data
snapshot. The legal ordinary query must still see D, with ordinary schema evolution
for older physical files. It must not be transformed into a historical query by
blindly calling `withTimeTravelSnapshot(S)`.

An explicit historical query instead resolves snapshot S and the declaration/schema
belonging to S before validation, optimization and sizing. Missing historical
declaration evidence retains the existing explicit rejection; it does not borrow
current declaration properties.

Iceberg already has FULL/TIME_TRAVEL read modes and immutable schema JSON. Ordinary
binding retains FULL mode with an exact selected snapshot, captured schema and
declaration. Historical binding retains TIME_TRAVEL and exact historical schema.
Split schema/projection must consume the captured schema, not fresh current columns.

Paimon has an immutable selected snapshot and row-type JSON but no read-mode field.
Its connector-private ordinary-binding marker must distinguish current captured
schema from explicit historical schema. `withTimeTravelSnapshot` removes that
marker. The actual SDK `copyWithoutTimeTravel` contract preserves the ordinary
schema while selecting the data snapshot. A later schema mismatch must reject the
scan rather than silently rebind its layout.

Lance's exact historical resolver already validates table ID, schema fingerprint,
version and manifest digest. Binding consumes that resolver once and retains its
result; subsequent data-source validation retains the same authority.

## Identity and expiration

Iceberg validation uses the actual SDK table UUID and selected snapshot manifest,
schema and sequence identity. Location alone is not a table-incarnation fence.

Paimon 1.4.1 exposes `Table.uuid()`, but its default implementation can synthesize
that value from the full table name and earliest schema creation timestamp. It is
not universally a persisted random incarnation identifier. Do not advertise
that timestamp as a strong incarnation identifier. Selected snapshot evidence uses
the real immutable snapshot representation, including commit/manifest/schema
identity and table location. Compare selected evidence before and after actual SDK
split planning; no splits are returned if the selected source changes or expires.
Immutable manifest/file locators remain the actual data-plane inputs. An SDK that
cannot prove the requested consistency must reject that path explicitly.
The current fallback UUID does not prove an adversarial same-location table
recreation in the same timestamp interval, especially for an EMPTY table. A
durable catalog table incarnation remains a production acceptance gap; passing
the real ordinary recreation test does not prove that stronger guarantee.

Existing connector query pins remain lifecycle bookkeeping until their callers
have been audited; they are not a substitute for the bound handle. No unqualified
query pin may select another table's snapshot. Expiration cannot trigger current
snapshot fallback. Query cleanup still belongs to the production query owner, not
the end of a metadata lookup or the end of scheduling.

## Authoritative planning context

The Coordinator overwrites connector `queryId` from its admitted query identity and
removes caller-supplied internal snapshot-selector properties. Version overrides
are produced by the existing SQL/ref resolver. `CompilationPhase` binds these
resolved selectors to the authenticated session before constructing the final
session planner through the existing `SqlCompiler.forSession` API.

Selectors are catalog/schema/table-qualified and canonicalized consistently with
existing name resolution. They are never unqualified query-wide snapshot numbers.
Schema and CBO lookup share a single query-local binding cache; no cross-query live
statistic cache becomes scan authority.

Unknown `Statistic` remains unknown. Partition sizing must recognize unknown scan
evidence rather than turn Calcite's internal 100-row fallback into asserted catalog
cardinality. Existing finite parallelism fallback and actual enumerated split count
bound scheduling; no test-only parallelism override is introduced.

Time-travel SQL removal consumes token source spans from the existing parser
grammar and `AstBuilder`; it does not run a regex or silently return CURRENT on
parse failure. Token indices from the code-point character stream are mapped to
the original Java UTF-16 offsets, preserving supplementary characters, literals,
comments outside removed clauses, quoting and aliases. Physical named references
in scalar/derived subqueries, CTE definitions, set queries and INSERT SELECT are
collected by that same parse tree. CTE names use the current planner's uniformly
case-insensitive matching while retaining source/AST spelling. ANSI quoted
case-sensitive matching is not supported by the current planner configuration.

The current per-table selector/descriptor cannot represent two snapshots of the
same table in one query. Different selected versions, or CURRENT and historical
aliases of one table, are explicitly rejected before binding rather than merged
with last-wins semantics. Repeated aliases of the same exact selected source are
accepted. True per-relation historical joins require a later single-path descriptor
and caller migration. Existing SYSTEM_TIME integer-snapshot behavior is preserved;
this unit does not add timestamp-expression resolution.

The source-span utility reuses the existing ANTLR frontend. Consolidating the
repository's SQL frontend around its designated sole frontend remains separate
architecture debt; this helper does not claim that migration is complete.

## Capability inventory and preservation

Current production `ConnectorMetadata` implementations are Iceberg, Paimon, Lance
and federation metadata. Federation's production `FederationCatalogHandle` already
rejects split and page-source management; this change must not claim it is a native
reader. Pure metadata/type resolution remains available without fabricated snapshot
evidence. File-valued columns and native file inputs do not automatically become
versioned table scans, and must not be denied just because statistics are unknown.
Iceberg system tables already use a fixed metadata-file handle and remain readable.

Preserve ordinary/historical generated declarations, field IDs, typed nullability,
VECTOR dimensions, current schema evolution, static overwrite constraint removal,
incremental scans and durable background exact source allocations. Persisted stage
replay continues to consume its existing frozen durable pages rather than resolve
live catalog metadata. No component test substitutes for public SQL execution.

## Executable acceptance matrix

All new cases use real SDK tables and writes. No synthetic snapshot, row-count
override, business fake, skipped runtime or fixed successful report is acceptable.

- Bind an Iceberg table at S, commit S+1, then enumerate splits: statistic and
  serialized source snapshot must remain S; actual SDK scan reads S's rows.
- Repeat with Paimon actual batch-write/commit and Lance actual publication.
- Two tables with different snapshot IDs in one query retain distinct handles and
  cardinalities. The first table cannot choose the second table's snapshot.
- Bind an empty view, write a first snapshot, enumerate the bound view: zero splits
  and zero-row statistic, not newly committed rows.
- Explicit time travel binds historical row count, schema and declaration before
  Calcite validation. A subsequent current write does not affect it.
- Ordinary `ADD COLUMN` without new data remains legal; its captured current schema
  must not be replaced by the last data snapshot's older schema.
- Expire or replace the selected snapshot: split enumeration fails without a
  current fallback and does not dispatch a different source.
- Unknown summary count yields unknown Calcite statistic, not 100 asserted rows.
  Real partition count remains bounded by actual split inventory and Worker limits.
- Constraint removal and duplicate-source extraction preserve binding identity.
- Real SQL relational metadata receives actual SDK cardinality. The public
  independent-process query selects at least four partitions only when actual
  catalog evidence and real files warrant it.

Minimum data-size threshold for four estimated partitions is greater than
805,306,368 estimated bytes. Actual test data and query projection must justify
that estimate; a guessed row count, contrived nominal-width table or synthetic
metadata override is not evidence.

## Source and verification sequence

Implement the existing-handle binding SPI and real connector implementations first;
then the query-local metadata/statistic owner; then planner descriptor and split
callers; then pre-bind time travel and unknown sizing. Add closest real SDK tests
and public SQL regressions, review all callers, and freeze one complete cohort.

Root owns all build execution. Source-only delivery is `SOURCE_READY`, not PASS.
The closest Java suites precede full compile/regression, same-source native package,
real SQL E2E and independent-process cluster acceptance. Existing Native/ASan test
results do not validate this later Java binding cohort.

New nearest suites are `ConnectorReadBindingTest`,
`PlanningSnapshotSelectorsTest`, `IcebergReadBindingIntegrationTest`,
`PaimonReadBindingIntegrationTest`, `PlanningReadBindingsIntegrationTest`,
`TimeTravelSqlClausesTest`, `TimeTravelExtractorTest`, and
`TimeTravelReadBindingIntegrationTest`. The latter is a real parser/session/PG/
plugin/SDK/schema/plan/split chain, including a historical column renamed away
from the current schema. It does not execute a Native task or prove multiple
processes. Existing session/schema/planner and every public SQL time-travel
regression remain required; no original oracle was removed.

The CBO identity root cause was the String table-name contract and cache in
`StatisticsCollector`, reached through `PangrovaRelMdSelectivity` and
`StatisticsBasedCostEstimator`. Joining and splitting qualified components loses
literal dots. The source cohort now propagates existing `QualifiedTableName`
through the provider, cache, estimator, actual relational scans, index-cost
interfaces and table-specific feedback/invalidation callers, without a String
compatibility overload. A prebuilt statistic cannot be installed under a different
table identity. The query-shape feedback store keeps its String API; the sole
`QualifiedTableName.canonicalKey()` bridge uses `TABLE/` and three UTF-16 length-
prefixed components. It does not grant snapshot authority or rewrite SQL.
`StatementCompletionEvent` derives the typed invalidation target from the original
AST and admitted session; it does not parse the ambiguous result display string.

Additional nearest suites are `QualifiedTableCanonicalKeyTest` (2,000 independently
decoded tuples and boundary cases), `TypedTableStatisticsIdentityTest` and
`TypedStatisticsInvalidationTest`. `PlanningReadBindingsIntegrationTest` preserves
its original actual SDK/PG assertions and adds two real SDK names that collide when
rendered with dots, plus current case-insensitive historical lookup retaining the
selected catalog table's actual spelling. Existing nine pruning predicates now
run against an explicitly qualified `test.db.orders` Calcite fixture and matching
typed injected statistics; they still exercise the statistic rather than silently
missing the newly typed cache.

These changes do not implement ANALYZE file scans, accurate incremental collection,
or a durable optimizer feedback service. Existing standalone placeholder collection
and generic CBO default estimates remain separate gaps, not asserted data-source
facts. `MultiColumnStatisticsProvider` has no actual caller or implementation in
the current repository and is not wired by this change. Neither typed index-cost
interfaces nor their pure arithmetic tests prove a deployed index-statistics
provider. Complete public SQL and independent-process acceptance remain mandatory.
