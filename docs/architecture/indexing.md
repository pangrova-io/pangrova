---
title: "Indexing Architecture"
description: "Index contracts, build and maintenance paths, and pruning boundaries"
---

# Indexing Architecture

> **Accuracy scope.** This is a living architecture document. JVM file-pruning
> indexes and durable Native retrieval artifacts are implemented paths; lifecycle,
> provider, and maintenance items explicitly marked remaining are target design.

## Index storage

PostgreSQL is the control-plane authority for descriptors, mappings, generation
manifests and durable artifact job/page/output metadata. It never stores index
payload bytes.

- Scalar `.idx` objects live under `indexing.base-path` using
  `v3/{catalog}/{schema}/{table}/{indexId}/g{generation}/{attemptId}/{hash}.idx`.
  The Coordinator JVM builds them; the Native Worker reads the exact mapped path
  and validates size, SHA-256, envelope and CRC.
- HNSW, IVF-PQ, DiskANN and BM25 segments are written by Native Workers to the
  generation's staging URI under `indexing.base-path`. `NativeIndexArtifactLayout`
  derives the persisted attempt path, job ID and idempotency key from one immutable
  identity; current paths are
  `v2/tenants/{tenantHash}/artifacts/{indexId}/attempt-{attemptId}/generation-{generation}`.
  Legacy submission paths omit the attempt component and remain unchanged.
  Objects are named `{role}-{sha256}.bin`; only their immutable metadata returns
  to the Coordinator.
- `worker.native.cache.artifact-path` is a per-Worker, content-addressed local
  cache at `{sha256-prefix}/{sha256}.bin`. It is quota-bounded and disposable;
  warm entries are revalidated against the published binding before use.
- Native build scratch is task-local. Index payloads never become JVM heap
  objects, and the Worker cache is never a metadata authority.

The base path must be durable and shared by every process that reads or writes
scalar artifacts. The Coordinator uses one validated root for scalar and Native
artifact storage; conflicting bootstrap and indexing configuration roots are
rejected. Native artifact staging must use a filesystem/object-store URI
supported by the Worker. Production installations must isolate tenant access,
configure retention and backups. Snapshot pin recovery runs only under verified
maintenance leadership; demotion stops new recovery work, while a claimed
operation can finish its exact ledger transition. Keep automatic maintenance disabled until
the lifecycle gates pass. Schema V121 adopts already-published Native jobs that
carry exact persisted dispatch authority as `INTENDED`; maintenance must acquire
and validate their real connector pins before query retention can use them.
Published jobs without that authority must be resubmitted before upgrade rather
than receiving synthetic privileges.

Schema V122 adds the exact indexed-column schema fingerprint and the durable
schema-evolution outbox. New indexes bind the connector column names, canonical
types, parameterized type details and source field IDs at creation. Existing
`READY` or `STALE` descriptors without that binding become `STALE` with
`source_schema_fingerprint_required`; `ALTER INDEX ... REBUILD` establishes the
binding before the replacement generation can be published. An interrupted
initial `BUILDING` descriptor remains recoverable through the existing stuck-build
path and cannot become query-eligible without the binding.

Schema V124 upgrades that binding to `v2:<64 lowercase SHA-256 hex digits>`.
The old display-text digest has no equivalent canonical representation: it is
preserved for inspection, never rehashed or treated as a v2 binding. Quiesce old
coordinators before migration. READY and BUILDING descriptors carrying old or
missing fingerprints become STALE; staging generation claims become FAILED.
Existing STALE descriptor timestamps and reasons are preserved, including V123
operation targets; published generations and retention records are not deleted.
Only an explicit rebuild under current source access can establish a usable
v2 binding. Do not downgrade to a writer that emits the old representation.

The v2 digest uses a binary, big-endian grammar. A frame is a four-byte byte
length followed by that many octets; text is exact UTF-8. The outer digest
contains a frame of `pangrova:index-schema:v2`, a four-byte ordered column count,
and one frame per column. A column declaration contains frames for exact name
and canonical type enum name, one presence byte and optional four-byte field
ID, then one presence byte and optional structured type signature. A signature
contains a frame for its base name and four-byte parameter count; each parameter
contains a named-field presence byte and optional name frame, a numeric-value
selector byte, and either an eight-byte integer or recursively encoded signature.
This contract does not use Java `toString`, display type text, or available-column
ordering. CREATE resolves an exact spelling first, otherwise a unique
case-insensitive user spelling; ambiguity is rejected. An already bound
column must match its exact persisted connector spelling, including on rebuild
and query dispatch. The fingerprint is a schema declaration binding, not proof
of physical table incarnation or field-ID lifetime across a format's history.

Current hard bounds are 50 MiB per scalar Bloom artifact, 100,000 retrieval
candidate rows per plan and 1,000,000 source objects per Native build. These are
fail-closed resource limits, not capacity claims. Oversized inputs retain the
ordinary SQL path where one exists or fail before dispatch with a specific
validation error; they never produce a silently partial result.

## Scope and execution boundary

Pangrova has two deliberately separate index families:

- JVM file-pruning indexes (B-tree, Bloom, prefix, N-gram Bloom, bitmap, hash,
  sparse, R-tree and GIN) implement `IndexStrategyProvider`. They produce small
  per-file metadata used to decide whether a split can be skipped.
- Retrieval indexes (HNSW, IVF-PQ, DiskANN and BM25) are immutable durable
  artifacts. Their build and query data plane runs only in the C++ Native
  worker.

`IndexType` owns the immutable `IndexPhysicalProfile` that selects the scalar or
Native family for DDL, lifecycle, coverage, cleanup, and persistence guards.
This classification does not advertise algorithm support: installed strategy
providers and the Native capability catalog remain responsible for their own
capabilities. Existing index type names, stable IDs, persisted paths, artifact
formats, and publication authority are unchanged.

The Coordinator is a control plane. It resolves metadata, chooses a canonical
Native capability, pins a source snapshot, creates durable work pages and
dispatches ordinary canonical v2 tasks. It never builds, loads or searches an
ANN graph in the JVM.

## Code ownership

Index state and index orchestration have separate owners:

- `pangrova-indexing` separates API/SPI, common Codec, builtin strategies and Core
  orchestration (see `pangrova-indexing/README.md`). Persistence ports and their
  immutable values live in `pangrova-persistence-indexing-api`; existing SQL lives
  in `pangrova-persistence-indexing-postgresql`. Core depends on the ports, not the
  PostgreSQL implementation. Metadata Core does not own index storage.
- Server supplies the JDBC `CurrentTableAuthorization` participant explicitly.
  Publication invokes it on the same transaction connection at the existing lock
  boundary. The participant may neither commit nor close that connection. The
  PostgreSQL production artifact does not depend on Metadata or Indexing Core.
  Narrower persistence ports and removal of permissive optional defaults remain
  separate work; moving the existing broad port does not complete that redesign.
- Planner derives scalar predicates with one physical scan as their provenance.
  Self-joins and unions retain their full split sets when provenance is ambiguous.
  Coordinator looks up READY descriptors for the bound tenant and matching columns.
  Indexing evaluates predicate compatibility and applies pruning.
- `org.pangrova.coordinator.indexing` handles SQL entry, split dispatch and
  lifecycle integration. Vector search passes the compiled query to the indexing
  rewriter, which inspects the Calcite plan inside the indexing module.
- Index generation has exactly one owner: the indexing store allocates and
  fences generations, and the coordinator adapter may only advance them through
  that store.

## Protocol descriptor boundary

`index.proto` defines descriptor-only interchange vocabulary. `IndexManifest`,
`IndexGenerationRef`, and protobuf `GenerationState` do not own runtime index
state, active-generation selection, publication epochs, checksums, analyzer
identity, or corpus statistics. PostgreSQL `IndexDescriptor` and
`IndexGenerationManifest` records remain the durable authority; atomic
publication stores enforce their compare-and-set fences.

Workers receive the narrower `ArtifactBinding` projection of a published
generation. Native BM25 reads analyzer identity from binding properties and
corpus statistics from the `BM25_CORPUS_STATS` artifact. The descriptive proto
fields must not be used as a second manifest implementation. Clarifying this
boundary changes comments only: the wire schema and canonical protocol
descriptor hash are unchanged.

## Canonical retrieval contract

Every retrieval algorithm is identified by the existing generic protocol
objects:

- `CapabilityRef` identifies the exact build or retrieval semantics, version
  and signature.
- `ArtifactFormatRef` identifies the immutable binary format and Native writer
  build.
- `ArtifactWriteBinding` fences a build work page and its staging output.
- `ArtifactBinding` binds a published generation and its source-object-scoped
  objects to a query task.
- `RowLocatorContract` defines how candidate ordinals map back to source rows.
- `ExecutionPolicy` defines coverage, retry and fallback policy.

There is no vector-specific task, heartbeat, result HTTP endpoint or Java
serialization format. Algorithm names in SQL catalog properties are resolved
fail-closed to canonical references from `BuiltInCapabilityCatalog`; unknown
names are rejected before dispatch.

## Durable build flow

1. Index DDL stores an `IndexDescriptor` in `BUILDING` state.
2. `DurableIndexBuildService` validates the indexed column and resolves the
   declared algorithm to a canonical build `CapabilityRef` and
   `ArtifactFormatRef`.
3. The service records a durable snapshot-pin intent, pins the immutable table
   snapshot through the authorized connector, validates the pin, enumerates source
   splits and persists source-map, reduction and finalization pages in
   `GenerationJobStore`. An empty table uses a logical empty-snapshot pin without
   connector I/O. Job creation and the PINNED ledger binding commit in one
   transaction.
4. `DurableGenerationDispatcher` leases pages and dispatches normal v2
   `TaskRequest` objects with an `ArtifactWriteBinding`.
5. A Native worker verifies capability, format, fencing token and resource
   grant before building an immutable artifact object.
6. The Coordinator validates task reports and checksums, then atomically
   publishes the complete generation. Failed or superseded attempts cannot
   publish.

Native reports staged outputs for successful attempts. A cancelled attempt
reports its physical execution drain even if staging had already finished.
The Worker preserves the same cancellation outcome in the seal response and
the terminal status report. During shutdown, the dispatcher stops issuing work
and keeps its task identities until it receives exact drain receipts. A drain
timeout leaves those identities in place and fails shutdown.

Retries reuse stable work-unit identities while attempt IDs and fencing tokens
advance. No in-memory build state is authoritative. Publication revalidates the
exact durable and connector pin; cancellation of an expired or revoked job is
fenced by the current publication owner, epoch and token. Dropping an Iceberg
source table makes its pinned snapshot unavailable, so the dispatcher cancels
the affected job. A maintenance scan is only a candidate list. Cleanup rechecks
the current build binding, active
generation and execution references while claiming the exact ledger row. The
durable transition to `RELEASING` prevents new job binding and retention before
the connector release happens outside the cleanup transaction. An uncertain
connector outcome remains `RELEASING` for exact, idempotent recovery.
With automatic rebuilding disabled, the leader consistency scan still retires a
READY Native generation when the authorized connector reports a different
current snapshot and validated inventory is available. Retirement atomically
marks the descriptor STALE and its exact ACTIVE manifest RETIRED, unless a
replacement build is already reserved. The retired index is no longer selected
for new queries, including new explicit historical reads; those queries use an
ordinary scan where available or fail closed if an exact index is required.
Existing retained executions keep their immutable generation and pin. Separate
pin maintenance can release it after the normal grace interval and only after
all durable execution references have drained. An explicit rebuild can reserve
a replacement from the STALE descriptor; this scan does not submit an automatic build.
Unknown source state and failed validation never trigger retirement.
V130 records the retirement instant and reason on the immutable generation
manifest itself, in the same transaction as the pointer change. Later descriptor
edits do not reset its grace interval. The upgrade conservatively starts grace
at migration time for previously retired manifests whose exact time is unknown.
A maintenance release claim stores a 15-minute retry due time before connector
I/O, and due candidates are ordered by their effective due time, not permanently
by creation time. A failed oldest connector operation therefore cannot
monopolize the bounded batch. Neither grace nor retry expiry overrides live
execution references or releases an ACTIVE generation of a live index.
Periodic READY/STALE consistency and grace-expired DROPPED cleanup walk separate
durable keyset sweeps (V131), ordered by immutable index ID. Each page uses a
fixed upper bound and database LIMIT; DROPPED candidates also use the strict
database grace cutoff. Progress is acknowledged only after processing a page
and a monotonic revision prevents competing or pre-wrap completions. A restart
can replay the last unacknowledged page, while new or newly eligible descriptors
behind the cursor are visited on the next sweep. This cursor is only scheduling
progress and never substitutes for state, generation, lifecycle or pin fences.

Operator pressure observations sample at most 1,001 Native ledger rows per
unreleased state with the existing state/creation-time index. The reported
count is a lower bound when the per-state 1,000-row cap is exceeded, with an
explicit truncation indicator, oldest unreleased intent age, and sample age.
The maintenance leader caches observations; stale, failed, or demoted samples
are not reported as live counts. Metric collection does not query PostgreSQL
or storage. These are Native intent-pressure gauges, not physical retained
bytes, scalar counts, tenant quota enforcement, or an estimate of reclaimable
data.


Scalar index snapshots are construction holds, separate from query-owned snapshot
references. V126 persists an exact attempt-owned pin intent before the connector
operation. A successful synchronous scalar build must exhaust each source cursor
and seal the engine's wrapper against further row reads, then records durable drain evidence
before releasing its build claim; an acquisition failure before reading can
also record drain. The leader releases a drained hold only after the exact
generation is durably published or terminal and the attempt no longer owns a
claim. Release attempts are fenced and retried with a durable retry timestamp.
An expired claim is not proof the reader has stopped; exceptional or legacy
attempts without positive drain evidence are not newly released by this
scheduler and require explicit reconciliation; the existing legacy DROP path
is unchanged. A revoked or changed source scope may defer cleanup.
This evidence covers the engine's synchronous source-reading stack; it is not a
general-purpose proof of connector-internal asynchronous resource closure.
Query-owned and unrelated table snapshot references are never released by this
scalar construction cleanup.

Ordinary query snapshot identity is not itself proof of a connector-owned
physical hold. In particular, Iceberg's process-local query snapshot registry
pins the selected identity for planning continuity but does not create an
Iceberg table tag. External snapshot expiration can therefore race ordinary
historical reads unless an independent connector retention obligation protects
them. A future physical query hold must persist an exact execution/source/
snapshot intent under the admitted execution-anchor fence before worker
dispatch, validate the connector-owned hold, seal read admission before
release, and retain uncertain holds until the actual readers drain. Neither
elapsed time nor coordinator lease expiry alone proves physical read drain.

A persisted unbound submission is recoverable only if its exact generation,
source snapshot and durable authorization still match a live build reservation.
Online replacement resolves the staging reservation rather than the active
descriptor's generation. Recovery does not synthesize a missing submitter or
release a pin when the current source obligation remains uncertain. A stale
reservation with neither a durable pin intent nor a job is fenced as failed
after rechecking the same reservation, allowing an authenticated explicit
rebuild; recovery does not issue connector operations for that reservation.

Native submission identity is distinct from the logical index generation.
The initial descriptor or staging-generation reservation persists a unique
submission ID; restarting a failed staging reservation creates a new ID,
even for the same snapshot and logical generation. Admission retries and
unbound recovery reuse the existing ID, job, page identities and artifact
directory. DDL and refresh capture the attempt before the admission transaction
and carry it through submission and failure; a late callback cannot look up
or fail a newer reservation, even when timestamps coincide.
New submissions use a per-attempt directory; terminal attempts
remain in history and are cleaned up by their exact pin owner. There can be
only one live or published Native job per logical generation; other artifact
types retain generation uniqueness. Recovery never reattaches a superseded
unbound pin to a replacement reservation.

V125 retains exact existing legacy submission owner/job/path identities and
published authority; it does not synthesize an actor for missing historical
evidence. Quiesce older Coordinators before this migration and do not run
mixed submission writers. An older binary cannot safely resume new per-attempt
reservations; a downgrade requires stopping writers and reconciling/draining
all new reservations and retained artifact references before restoring the
previous schema and binary.


Snapshot holds in Iceberg and Paimon use a persistent versioned tag namespace.
New holds use `pangrova_index_<sha256-index-id>_v3_<generation>_<hex-attempt>_<hex-token>`,
with UTF-8 lowercase hexadecimal fields. Existing v2 holds (original padded
Base64, and the interim hex encoding used on the development branch) are read
without renaming; ambiguous or malformed v2 names require explicit reconciliation
and must never be automatically released. Repeated acquisition of an exact
existing hold does not create another tag. Enumeration deduplicates equivalent
encodings, and release removes only matching owner-and-token tags after checking
their stored snapshot. Pre-owner tags have a synthetic generation-zero legacy
owner for cleanup only; validation requires an exact owner-tag. V121 backfilled attempts retain their
own `legacy-<md5-job-id>` identity; adoption acquires a new exact hold before
marking it pinned, including a Paimon snapshot retained solely by a legacy tag.

Paimon 1.4.1 stores tags as `tag-<name>` filenames. With the current decimal
signed-long snapshot ID (at most 19 digits), non-negative integer generation
(at most 10 digits), and the longest current attempt (`legacy-` plus a
32-character digest), the v3 filename occupies at most 215 ASCII bytes,
within the common 255-byte component limit. This does not imply an unlimited
opaque-token storage capacity: a future connector with longer or nonnumeric
tokens must explicitly validate its storage limits and introduce a new codec
version if its persistent representation needs to change.

This is a forward-only write-format upgrade. Quiesce old coordinators and pin
cleanup workers before enabling the new writer; do not run old and new writers
or cleanup workers concurrently. Older binaries cannot read v3 holds. Rolling
back requires stopping new writers, draining all v3 holds and verifying that
the durable ledger has no live or pending v3 pin and the affected tables have
no v3 tags before restarting the old binary. Do not remove live holds merely
to enable rollback; if the system cannot drain them safely, remain on the
new reader/writer. No existing tag is automatically rewritten or dropped on
startup.

Incremental index maintenance observes lineage before opening the snapshot file inventories. For a forward commit, the Connector uses one bound table observation, reads each immutable inventory once and derives the file delta from the same target inventory used for coverage validation. Replayed or superseded snapshots require only the lineage decision.

`IndexGenerationCoverageReader` is the shared coverage authority for periodic consistency checks and commit-driven refresh. Scalar indexes read exact `index_file_mappings` for the active generation. VECTOR and INVERTED indexes instead read the active generation's atomically published `artifact_build_jobs` record and validate its pages, source allocations, output roles, counters and canonical manifest digest before returning source-object paths. Zero-row source partitions remain part of that coverage; scalar mapping counts are never used for Native indexes. Missing, unpublished, corrupt or incomplete Native evidence fails closed and cannot be interpreted as exact coverage. A READY Native generation whose source snapshot and validated source inventory are unchanged does not schedule a replacement on repeated consistency cycles. Forward commits still use connector lineage and file deltas, falling back to a complete replacement only when the predicted target inventory differs.

## Query flow

1. The planner recognizes `ORDER BY <distance>(vector, literal) LIMIT k` and
   selects only a `READY` index whose column and metric match.
2. The plan carries the canonical Native retrieval `CapabilityRef`, a bounded
   search budget and the index generation identity.
3. The Coordinator resolves that generation through
   `PublishedGenerationBindingResolver`, checks source-snapshot coverage and
   materializes source-object-scoped `ArtifactBinding` objects for each task.
4. Normal distributed task routing selects workers advertising the exact
   capability and compatible artifact format.
5. Native operators load the immutable artifact, execute local retrieval and
   emit canonical candidate rows. Native merge operators produce the global
   bounded Top-K.

An unindexed SQL distance query remains an ordinary Native Substrait scan/sort.
It does not enter a Java brute-force implementation. Missing, stale,
incompatible or incomplete artifacts follow the explicit `ExecutionPolicy`;
they never silently return an empty or partial Top-K.

## Schema evolution fencing

Index schema changes use a PostgreSQL write-ahead journal rather than the
in-memory metadata event dispatcher:

1. Before connector DDL starts, the Coordinator claims the canonical source
   table scope (and the rename target), inserts an `INTENDED` operation, and moves
   affected non-dropped descriptors to `STALE` in the same transaction.
2. Only a successful connector mutation records `COMMITTED`. The Coordinator
   applies the exact operation before returning SQL success. Descriptor transitions
   require the original fence, state, generation and update timestamp to match.
3. The maintenance leader recovers bounded batches of unapplied `COMMITTED`
   operations. It cannot infer success from connector schema equality
   or from a missing table. A crash leaving `INTENDED` or an uncertain connector
   result leaves the scope claimed and requires authoritative outcome evidence.
4. An exact post-operation table identity allows table and column renames to
   rebind descriptor coordinates. Confirmed table drops and observed indexed
   column drops drop the affected index. Other compatible changes keep the index
   `STALE` for rebuild; text-sensitive `INVERTED` and `GIN` indexes are dropped on
   confirmed incompatible type changes. Missing or mismatched identity retains
   the old binding conservatively as `STALE`.

The journal states are `INTENDED`, `COMMITTED`, `APPLIED`, `ABORTED` and `UNKNOWN`.
Only failures proved to occur before connector mutation may abort and release
claims; generic connector failures are `UNKNOWN`. Descriptors remain fenced
until explicit rebuild. Periodic reconciliation also inspects external schema
drift that bypassed Pangrova DDL and durably fences missing tables, missing
indexed columns and mismatched types. See the V123 ownership details below.

Query dispatch independently verifies the descriptor fingerprint against the
exact `ConnectorReadSchemaIdentity` captured by the planned scan. Scalar pruning,
vector search and Native BM25 all use this guard. Missing schema evidence, a
missing indexed column or a fingerprint mismatch excludes the index for that
read. Selected read declarations may be historical or superseded since
planning; only independent current-source reconciliation may persist a fenced
`SCHEMA_MISMATCH` journal entry. Multiple scans
of the same table are eligible only when their selected schemas agree, so a
self-join across different snapshots or declarations never borrows the first
scan's index binding.

## Algorithm and format identities

The built-in catalog currently defines these independent contracts:

| SQL algorithm | Build capability | Retrieval capability | Artifact format |
|---|---|---|---|
| HNSW | `pangrova.artifact.build.hnsw` | `pangrova.retrieval.vector.hnsw` | `pangrova.hnsw.segment` |
| IVF-PQ | `pangrova.artifact.build.ivf_pq` | `pangrova.retrieval.vector.ivf_pq` | `pangrova.ivfpq.segment` |
| DiskANN | `pangrova.artifact.build.diskann` | `pangrova.retrieval.vector.diskann` | `pangrova.diskann.segment` |
| BM25 | `pangrova.artifact.build.bm25` | `pangrova.retrieval.text.bm25` | `pangrova.bm25.segment` |

Capability signatures and format schema hashes are compatibility boundaries;
matching a human-readable algorithm name is insufficient.

## Scalar build attempts

Each scalar build submission owns a UUID. The claim owner and the physical file
path both carry that identity. A writer uses
`v3/{catalog}/{schema}/{table}/{indexId}/g{generation}/{attemptId}/{dataFileHash}.idx`
relative to the index root. Reads and mapped-file cleanup use the stored path.
A delayed write by an expired owner can finish in its own directory after takeover
without changing the replacement’s bytes.

Before writing, the owner persists a scalar artifact intent bound to its exact
generation, attempt UUID, source file and canonical artifact path. A successful
physical close transitions the intent to `DRAINED`; publishing a file mapping
consumes the intent within the same mapping transaction. A rolled-back mapping
therefore leaves durable ownership evidence.

The collector considers only drained intents older than the grace interval,
with no exact owner claim or mapped reference. Under the index lifecycle lock
it checks the canonical path and persists `DELETE_PENDING` before removing the
artifact. A failed delete or acknowledgement leaves a retryable intent. Each
bounded poll durably advances a separate monotonic queue priority, not the
drain evidence or deletion authority, so an unavailable oldest batch cannot starve
later eligible work across collector restarts. A dropped
descriptor cannot be purged before its outstanding intents retire.

A crash before recording a successful close, including an uncertain write or
a crash immediately after close, leaves `WRITING` quarantined. Time alone cannot
prove the writer stopped. Reconciliation requires independently confirmed
writer quiescence; this implementation does not automatically reclaim those
files or infer ownership for untracked pre-migration artifacts.

Obsolete mapped scalar artifacts use a separate durable retirement handoff. The
generation-fenced mapping transaction records the exact last unreferenced path,
source file and original descriptor scope before committing. A path-scoped
transaction fence prevents new mappings or scalar pre-write intents from
referencing a pending or deleted retirement. The collector rotates a bounded
durable queue, rechecks references and descriptor authority, and confirms
idempotent physical deletion before recording a permanent completed receipt.
No mapping-removal response alone authorizes deletion. Completed receipts are
retained until a separately proven namespace/nonreuse contract permits removal;
unknown delete outcomes remain retryable. A dropped descriptor cannot be
purged while a pending retirement still requires its authority.

Only the verified v3 attempt-scoped layout is reclaimed automatically, including
a path copied into a later generation. Unknown legacy layouts, changed roots or
scope mismatches retain their durable receipt for explicit reconciliation.
Quiesce old scalar writers before applying V129; path-fenced writes require
READ COMMITTED isolation so absence is checked against fresh metadata.

### Scalar failure and recovery

Initial and online scalar build failures use one reason-preserving generation
fence. An initial failure moves only the exact `BUILDING` descriptor to `STALE`;
an online failure marks only the matching staging row `FAILED` and leaves the
active manifest readable. A DROP, publication, or replacement that wins the race
causes the late failure to lose its fence, with no unconditional state-update
fallback.

Startup and periodic recovery inspect initial `BUILDING` descriptors, failed
initial-generation rows left by a committed failure fence, and `READY`/`STALE`
descriptors with online `BUILDING` rows. The stuck threshold uses
the later of the generation timestamp and claim heartbeat. PostgreSQL rechecks
that threshold while locking the descriptor, staging row and claim, then commits
the failure fence and claim expiration atomically. Mapping writes and publication
serialize on the same descriptor authority, so an old attempt cannot write a
mapping or publish after takeover.

After the failure fence commits, cleanup atomically removes the exact failed
generation's mappings and records the same durable path retirement receipts
used by mapped-artifact GC. A replacement reservation that restarts the same
failed generation performs that handoff in its own transaction. An older
recovery pass cannot retire mappings belonging to a newer reservation or DROP;
observed paths alone never authorize external deletion. Recovery can submit a
snapshot-pinned replacement once the durable handoff commits, without waiting
for unrelated physical deletion. Startup recovery runs before GC, consistency
and commit-driven refresh schedulers start. Native retrieval jobs retain their
own durable dispatcher/publisher recovery protocol.

After DROP and snapshot-pin checks, the collector retires every exact mapped
path through the same transaction; shared references remain intact. The
descriptor is retained until mapped rows and pending receipts have drained.
Physical deletion happens in a later bounded GC pass, not in the descriptor
purge transaction. Direct descriptor purge refuses remaining mapped rows rather
than relying on cascading deletion.

The collector prepares one mutation per index and binds typed arrays for each cohort. Cohort size changes only the bound data.

Obsolete mappings are removed in bounded batches under the observed descriptor
row lock. Removal checks the exact mapping identity and verification timestamp.
The transaction reports only the paths with no remaining mapping, so a current
generation can keep using a shared physical file. File deletion follows commit.

## JVM encoded-data ownership

Indexing API owns encoded index data and pruning hints. Bloom and N-gram arrays,
bitmap payload snapshots, bounding-box coordinate snapshots and row-group BitSet
snapshots cannot mutate their owning object. Bloom evaluation uses independent
read-only ByteBuffer views; Common decodes those views into owned words without
retaining an input alias. Bitmap membership and R-tree coordinate access use
focused methods, avoiding bulk copies in predicate evaluation and tree building.
Serialization may request owned snapshots. The binary layouts are unchanged.

This boundary does not yet establish immutability of arbitrary Comparable keys
or runtime wiring of every statistics DTO. The verified scope and remaining work
are bounded by the current Codec and evaluation tests.

Complete JVM BTree tuple keys now use `CompositeIndexKey` in Indexing API rather
than a Provider-private class. The generic comparable codec owns tag 11: a bounded
arity followed by typed scalar components. Keys require 2–1024 non-null components
of the exact supported immutable scalar classes (String, boxed numeric primitives,
BigDecimal, BigInteger and UUID). Nested tuples, mutable custom Comparable values
and incompatible comparison shapes are rejected. Existing scalar tags and the
common index envelope are unchanged; no historical data migration is introduced.

The builder skips rows with a null key component. Its payload does not prove
null-row coverage or the arity of an empty index. Consequently an empty key set
cannot justify `SKIP`, and a scalar predicate is not a complete tuple predicate.
Native split pruning currently handles single-column B-tree equality, `IN`, and
range predicates for exact String, Integer, Long, Float and Double wire values.
Composite tuple membership remains a JVM evaluation contract; composite B-tree
hints are not sent to the Native consumer.

## JVM indexing SPI

`IndexEngineRegistry` discovers the nine scalar strategies with ServiceLoader. Each
`IndexStrategyProvider` supplies the serializer for the raw data returned by its
builder. Server constructs the file codec from the same provider instances used
for building and evaluation. A duplicate type, a missing payload codec or an empty
production registry fails initialization. The common artifact envelope, checksum
and stable type IDs belong to Indexing Codec. The native VECTOR and INVERTED/BM25
paths use their own durable artifact formats.

## Scalar file-pruning query path

Predicate analysis retains the selected immutable index descriptor. Split evaluation uses it alongside the exact active artifact generation and selected source snapshot. It reads published file mappings in batches and reuses the selected descriptor for availability checks and Worker hints.

The Coordinator evaluates connector file statistics and native connector
capabilities. For string bounds from Iceberg and Paimon, statistics comparisons use
unsigned UTF-8 (Unicode scalar order), not Java UTF-16 order. Floating-point bounds
may omit NaN; without NaN counts, the Coordinator does not use an upper bound to
exclude a greater value or claim every value is equal/below an upper bound.
Invalid or inconsistent bounds do not prove a negative result. The Coordinator then
attaches snapshot-bound `IndexEvalHintDetail` values to
surviving canonical v2 file splits. It does not load scalar index artifacts.
The Native Worker validates the hint's data-file identity, reads the mapped
artifact through its configured filesystem, and verifies its exact byte length and
SHA-256 against the published mapping. It checks the envelope version and CRC
on those same bytes, then evaluates Bloom membership or the sorted B-tree pages
before enqueuing the split for scan. Only a proven negative result skips a split;
missing, corrupt, incompatible or unsupported indexes retain it. Equality and
`IN` on canonical string, integer and boolean keys participate in the Native
Bloom path. Single-column B-tree equality, `IN`, and range predicates support
exact String, Integer, Long, Float and Double values. Java UTF-16 and
signed-zero ordering validates the serialized pages; predicate comparisons use
Velox SQL ordering (UTF-8 byte order, equal signed zeros, NaN above other values).
Bitmap has a JVM strategy and a typed key-to-Roaring payload. Worker scalar
file pruning recognizes Bloom and single-column B-tree artifacts. BSI has no
registered scalar strategy. Paimon deletion vectors use their connector-owned
row-deletion path.
Calcite's [string literal wrapper](https://calcite.apache.org/javadocAggregate/org/apache/calcite/util/NlsString.html)
literals are unwrapped with `getValue()` before creating a hint: its
`toString()` returns quoted SQL with charset notation, not the stored value. Native
membership is checked against a Java-generated positive/negative byte fixture.
DATE, TIMESTAMP, DECIMAL, floating-point Bloom keys and other index types retain
the split until their cross-language value and artifact contracts are verified.
Multi-scan plans do not receive scalar hints until predicate provenance is
bound to a specific scan, because matching columns by name across joined
tables is insufficient to justify pruning either table.

`NativeTaskStats.index_splits_evaluated` and `index_splits_skipped` report
actual Worker decisions. An index-performance claim must use these counters
or scan I/O, not the Coordinator's earlier split-filter counters or Iceberg's
own manifest pruning statistics. The E2E regression reads the counters from
the Coordinator's accepted drain reports for the query's exact attempts.
Overlapping file min/max ranges distinguish native index pruning from
connector pruning.
This placement follows [Velox's split queue contract](https://facebookincubator.github.io/velox/develop/task.html):
the application decides which splits to add before the scan operator reads
them. [Iceberg's planning statistics](https://iceberg.apache.org/docs/nightly/performance/)
can independently remove data files using upper and lower bounds, so those
connector decisions must be measured separately.

## Distributed correctness invariants

- Every query and build uses one immutable source snapshot.
- Index builds that enumerate the current files select the snapshot and read its
  file set through one connector observation. Iceberg opens the catalog table
  once. Explicit historical reads continue to use the requested snapshot.
- Artifact generations are immutable and atomically published.
- Every object checksum, format identity and row-locator schema is validated.
- Workers must advertise the exact capability signature and compatible format.
- Work-page leases use the expiration stored by PostgreSQL. Lease and publication fencing reject stale attempts.
- Durable snapshot-pin intents survive connector/job crash windows. Published
  retrieval pins are retained by exact live execution anchors while candidates
  hydrate source rows; superseded, dropped and terminal builds release them only
  after references drain.
- Candidate and merge budgets are bounded by the task resource grant.
- Global Top-K completeness is explicit; partial results are never silently
  presented as complete.
- Coordinator restart recovery uses durable jobs and manifests, not JVM graph
  state.

## Test gates

Changes to this path require:

- planner tests for algorithm-to-capability resolution and bounded parameters;
- protocol descriptor and capability inventory gates;
- Coordinator tests for snapshot binding, page fencing, publication and worker
  capability matching;
- Native build/query tests for every advertised artifact format;
- packaged JNI end-to-end tests that prove ordinary task routing, distributed
  local retrieval and global Top-K merge;
- static checks that no Java ANN engine, vector ServiceLoader provider,
  vector-result HTTP endpoint or algorithm-specific transport contract exists.

Index state-update contracts must preserve the requested fence and stale reason. The
legacy unconditional overload is not an implementation of optimistic concurrency.
The default timestamp-CAS, state/generation-CAS and reason-preserving overloads reject
unsupported calls before mutation; they never delegate to a weaker update or return a
synthetic success. PostgreSQL compares and updates in one SQL statement. A successful
fenced STALE update persists its reason atomically; non-STALE transitions clear it in
the reason-aware operation. Lifecycle transition validation remains the domain owner's
responsibility. Other persistence ports and remaining optional defaults still require
the planned domain/backend split; this state-update correction does not complete that work.

### Durable table schema operation ownership (V123)

The table-scoped index schema journal is the state owner for index effects of
connector DDL. A write-ahead transaction claims the source name and the rename
target in deterministic order, captures the exact fenced descriptor IDs and
timestamps, and fails in-flight staging generations before connector IO begins.
The physical catalog/schema/table scope is shared across tenants because the
catalog registry is global. Peer-tenant descriptors are conservatively fenced;
their original authority is not borrowed for rebinding. Unrelated table names
remain independent. Index registration, generation reservation and both scalar
and Native publication take the same scope-first transaction lock before
descriptor locks, then release it on commit. No SQL transaction remains open
while a connector operation or observation runs.

The connector DDL caller records COMMITTED only after the actual operation
returns success, capturing source and post-operation identity where supported.
A generic connector error is UNKNOWN, not an abort: it may follow an external
commit. A process loss can leave INTENDED; neither state is runnable work.
The bounded recovery loop applies only confirmed COMMITTED records, compares
the exact observed post-operation identity, and applies changes only to the
original captured descriptor fence. Missing/mismatched identity conservatively
keeps the descriptor STALE without rebinding to a replacement table. A failed
observation is deferred behind other committed operations. A connector without
an exact operation-outcome receipt cannot safely infer completion from a current
schema, so automated UNKNOWN recovery is intentionally unavailable. An operator
must inspect the operation ID and connector-side authoritative operation history;
the persistence inspection APIs distinguish unresolved from runnable operations.
No generic override or fabricated connector authorization is provided. Explicit
rebuild is required after an uncertain descriptor identity.

V123 is a forward migration; V121 and V122 checksums are retained. Older pending
INTENDED and COMMITTED rows both become UNKNOWN because V122 recovery could not
prove which commits were externally completed. Every overlapping legacy claim
is retained, not silently awarded to the oldest. Upgrade requires quiescing old
Coordinators (including schema DDL, builds and publication), applying V123, and
starting only code that participates in the scope lock protocol. Rollback to
V122 writers is not supported while V123 ownership is in use; stop writers and
preserve the metadata backup/claims for a verified forward repair instead.

The ownership boundary deliberately avoids either a long-lived database lock
across a connector RPC (pool consumption and automatic lock release after
process loss) or an optimistic descriptor CAS alone (which cannot serialize
two connector mutations). Durable per-name claims plus short transactional
locks retain ownership across crashes without holding a JDBC connection.
A future connector can extend outcome recovery with an exact operation receipt
adapter, but only a receipt bound to the original operation and physical owner
may discharge an UNKNOWN claim. Observed column names, schema equality or
elapsed time alone are not sufficient. Until generation records carry their
physical owner, completed schema changes leave previously active indexes STALE
and explicitly rebuildable rather than automatically restoring READY.

The SQL coordinator emits `index_schema_operation_outcome_uncertain` with the
operation ID when the caller cannot establish the result. Inspect the durable
record rather than inferring its state from the log (a commit response may have
been lost after persistence). Under elected
maintenance, `index_schema_operations_unresolved` reports a read-only oldest-
first sample of up to 25 operation IDs, operation kinds, states and ages; an
additional row detects a truncated sample. UNKNOWN records are reportable
immediately; INTENDED records are reported after five minutes to avoid alerting
on ordinary in-flight DDL. Repeated reports are limited to every fifteen
minutes. These timings are observability only, never evidence of completion or
permission to release a claim. The diagnostic path does not call the connector,
change journal state, or prevent confirmed recovery on a diagnostic read error.

Iceberg's schema update commit and catalog rename return normally or throw;
its SDK's unknown-commit-state exception expressly leaves the outcome uncertain. Its
catalog drop boolean is not a durable no-mutation receipt across supported
implementations: the Hadoop catalog can purge data before returning a failed
filesystem deletion, and the JDBC client pool retries connection failures
including unknown transaction resolution. A false result therefore fails
closed rather than being logged as a successful drop. SQL `DROP TABLE IF EXISTS`
still returns a no-op for an initially absent table, before any mutation is
attempted; it does not conceal an uncertain result after the mutation call.
Paimon catalog alter, rename and drop provide no persisted operation receipt.
Neither connector's schema/metadata history by itself identifies this journal
UUID. The existing distributed data-write commit context is not a DDL receipt.

An eventual receipt-capable adapter must bind a caller-generated journal UUID
and canonical mutation digest to the exact format table incarnation, catalog
version, expected source identity and requested target scope. It must durably
and atomically record the mutation's result (committed or definitively no
mutation) with the connector operation, and support lookup of that exact
receipt following process loss. A missing receipt without a provider guarantee
of definitive non-execution remains unknown. Table-local metadata markers alone
cannot cover catalog rename/drop or survive deletion of the table; those need
catalog-level outcome authority. Do not implement a generic manual release or
infer success from schema equality, timestamps, or a subsequent absent table.

An authorized metadata operator may inspect unresolved records using the
read-only query:

```sql
SELECT operation_id, tenant_id, catalog_name, schema_name, table_name,
       event_type, state, created_at, updated_at
FROM index_schema_evolution_outbox
WHERE state IN ('INTENDED', 'UNKNOWN')
ORDER BY created_at, operation_id
LIMIT 100;
```

For the operation ID from a diagnostic or the result above, inspect its exact
claimed source and target names using a bound operation ID parameter:

```sql
SELECT catalog_name, schema_name, table_name
FROM index_schema_operation_claims
WHERE operation_id = :operation_id
ORDER BY catalog_name, schema_name, table_name;
```

Do not clear these records or their claims merely because the table appears
unchanged: the original connector operation could still be unresolved. This
journal is wired into the SQL coordinator's DDL path; other catalog mutation
entry points and third-party writers require their own integration with the
same ownership protocol rather than relying on schema observation as a receipt.
