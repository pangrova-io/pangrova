# Canonical relation layout and Arrow execution

## Status and scope

Approved design; implementation and production verification are not complete.
This document records the atomic migration contract, not a claim that the current
workspace implements it. Java layout work can proceed while an earlier Native
checkpoint is building. Native source changes must wait for that checkpoint to
finish. Its results cannot validate this subsequent unit.

Java remains the control/planning plane. Velox is the sole Native task-local
execution representation. Arrow is the interop and data-plane wire representation.
There is one canonical generated Substrait/v2 protocol, one typed execution path,
and no compatibility mode, private row wire format or unsupported-type re-run.

## Verified causes

| Boundary | Actual defect | Required change |
|---|---|---|
| Filter | Native substitutes TRUE for non-BOOLEAN conditions; Read drops them | Preserve nullable BOOLEAN semantics and reject malformed conditions before execution |
| Project | Native emits expressions only; Java SDK appends expressions to input | Lower canonical direct output first, then apply emit |
| Common emit | Native only partially consumes Read.emit | One output remap rule across supported relations |
| Read | Filter ordinals resolve against pruned output; empty emit restores full schema; VirtualTable bypasses filtering/remap | Base-schema filter binding, dependency-aware pruning, shared Read post-processing |
| Artifact overlay | Production uses emit-only Project wrappers ignored by Native | Preserve restoration, authorization/filter ordinals and locator hiding |
| BM25/retrieval | Replacement-only output names; score append fails to shift/extend emit; Sort/Fetch emits are not composed | Use the authoritative layout and ordinal transformation rules |
| Grouping sets | Native strips set index from direct output | Emit keys, measures and canonical i32 set index; let common.emit hide it |
| Exchange | Callback hashes final output ordinals, although keys belong to input | Use Native PartitionedOutput with input keys and independent output layout |
| Flight | Shuffle wraps an opaque serialized page in a binary Arrow column | Send actual typed Arrow RecordBatch, not an Arrow-shaped opaque envelope |

Relevant production code is in the Native plan converter, Planner project/window/
aggregate/correlation emitters, physical-row-locator augmenter, generated-value
overlay binder, BM25 materialized-score binder and retrieval Top-K binder. Existing
component-direct tests do not prove that those complete serialized chains execute.

## Authoritative protocol layout

`SubstraitRelationLayout` describes generated protocol objects, not execution values.
It exposes a fragment-level indexed layout tree and expression type resolution.
Each relation has input schemas, expression context, direct output schema, emitted
output schema and the direct-to-emitted mapping. One post-order traversal indexes
the immutable relation objects; consumers must not recursively re-describe each
expression or create independent schema decoders.

Function admission, retrieval/artifact rewriting and other protocol consumers use
this same authority. Typed extension schemas come exclusively from the canonical
`RelationDetailEnvelope`. Logical VECTOR/model constraints remain in the existing
logical-type and exact operator bindings; they do not create a separate vector
schema protocol.

Rules:

- Read filter context is base_schema, before MaskExpression projection or emit.
- Project direct output is child output followed by expressions; every expression
  is bound against the original child, not previously appended expressions.
- Filter, Sort, Fetch and Exchange direct output preserve input order.
- Join condition context is both inputs; post-join filter context is the
  join-type-specific direct output. Semi/anti/mark shapes must not be guessed.
- Aggregate/grouping-set and Window layouts follow the pinned official contract,
  including intermediate aggregate types and grouping-set index.
- Missing/direct emit is identity. Present empty emit is zero columns, not identity.
  Repeated indices produce repeated values; negative or out-of-range indices fail.
- NamedStruct names are consumed in depth-first order through structs, lists and
  map keys/values. Top-level names are not the first N flattened names.
- Appending a physical helper column preserves all old output ordinals; appended
  Project-expression indices move when input arity changes. Composition applies
  the second mapping to the first mapping, retaining duplicates and empty output.
- Schema inspection validates exact declared/contextual types. It does not guess
  coercions from output type, runtime values or function names.

The pinned SDK permits empty expression lists, and current canonical production
wrappers use Project only to apply emit. Their identity-direct-output lowering must
be retained. Zero-column RowVectors retain row cardinality.

### Logical names and task-local identities

Logical NamedStruct names and SQL RelRoot labels remain unchanged, including
duplicates and all nested DFS names. They cannot be rewritten to compensate for
Velox's name-based top-level field lookup. The same layout authority supplies the
physical top-level identity of ordinal `i` as `$pangrova$column_i`, starting again
at zero at each relation output. Nested struct names remain logical names; decimal,
list/map structure, variations and logical argument-schema constraints are unchanged.

Native Read column assignments map each physical alias to a ColumnHandle containing
the exact declared logical source name. No unknown source column is inferred from
its ordinal. A converter-owned mapping retains the logical output names of each
already lowered relation; it is derived from canonical Read/Project/Join/extension
metadata and common.emit, not another serialized plan or transport row payload.
Function field references resolve the input ordinal to its unique physical name.
Direct Project output and emitted duplicate fields each receive unique physical
aliases, even where their logical names or their shared buffers coincide.

Join input aliases are made disjoint by rebasing the right input's physical aliases
by the left input arity before constructing the single Velox Join. Conditions bind
against this concatenated ordinal context. Semi/anti output then normalizes aliases
to zero-based output ordinals. Remove the independent `_right` and `_1` conventions;
logical side names remain available solely through the authoritative schema context.

Exchange transport schemas use the authority's physical schema (top-level aliases,
unchanged nested names and complete physical types). The actual Arrow writer and
receiver use that same schema. The result writer carries the original SQL label for
each ordinal in Arrow field metadata and clients expose those labels while retaining
ordered descriptors and positional values. It must not materialize SQL result rows
in a name-keyed TypedRow Map, which cannot represent repeated labels.

Actual public result decoding currently uses ArrowBatchDecoder Object[] plus ordered
ColumnDescriptor lists, and JDBC findColumn deliberately returns the first duplicate
label. That positional path can preserve repeated columns. BatchSerializer's separate
Page/TypedRow adapter currently overwrites duplicate names and has no production
deserialize caller; it must not become a result-path substitute. Zero-column result
batches must retain their row count rather than become EMPTY (current decoder and CLI
both drop them). Regression oracles cover duplicate SQL labels with distinct values,
self-join side values, nested names, and nonzero-cardinality zero-column batches.

## Native relation lowering

Split relation lowering from common output mapping. Ordinary operators first emit
their canonical direct shape, then one field-access projection applies common.emit.
Identity mappings allocate no extra projection; repeated values share compatible
Velox buffers with unique task-local names. Preserve nested types, decimal precision/
scale, typed NULL, and root names without changing ordinal semantics.

Read requires an explicit schema, binds filter against the complete base context,
and keeps all filter and output dependencies in the physical scan. Exact residual
filtering remains a Native FilterNode even when conservative storage pushdown also
applies. The scan schema map records the actual scan leaf, not its post-emit output.
VirtualTable follows the same filter/projection/emit order. MaskExpression's recursive
struct/list/map semantics must be implemented; ignoring it or refusing previously
legal functions is not a substitute for implementation.

TopNRowNumber is an optional optimization within the same Velox plan. If the exact
equivalence cannot be proved after projections/remaps, execute the ordinary legal
Velox Window/Filter/Project chain. This is not fallback or a second execution engine.
Do not invent an absent row_number field or remove user expressions to enable it.

## Sole Native partitioned-output sink

The pinned official Velox `PartitionedOutputNode` separates input partition keys
from output layout. Its operator hashes input vectors and serializes the selected
output vectors. It also explicitly supports zero-column output retaining N rows.
Reuse it and official hash partition specifications; delete the callback's duplicate
hash, row partition accumulator and independent shuffle serializer.

Broadcast uses the Native broadcast buffer mechanism: the operator produces one
buffer stream and OutputBufferManager publishes the admitted consumer destinations.
Do not confuse the number of consumers with the operator's production destinations.
Official round-robin partitioning is batch-approximate and covers `exact=false`.
Single-target `exact=true` is exact. Multi-target exact round robin, if admitted,
needs an exact partition function in the same sink, not approximate execution.

Output aliases/repeated fields may need an identity Project before the sink because
Velox output-layout resolution is name-based. That is ordinary task-local typed
lowering, never a private transport row or separate plan path. Hash-key context
continues to use the actual input schema; no output-ordinal patches are permitted.

## Typed Arrow output, direct exchange and durable spool

Use the actual Arrow vector serde for both result and shuffle sinks. All supported
complex physical types must be exportable/importable before execution admission.
Delete the Arrow-to-private-serde result restart; a failed attempt cannot publish
another representation or mix output formats. Do not remove an otherwise legal
SQL type merely to avoid implementing its Arrow mapping.

Task-local output-buffer pages may contain framed Arrow batches. The network must
contain actual typed Flight schema/RecordBatch, not a binary field containing those
frames or NativeRow/Presto pages. Parse frames into retained Arrow batches once,
then publish through the canonical transport. Direct receiver, remote producer,
durable spool and remote receiver migrate together. Existing fences, signed
permissions, leases, checksums, immutable producer cohorts and terminal contracts
remain authoritative. No old/new wire negotiation or downgrade is added.

OutputBufferManager draining starts concurrently with Native execution and covers
all admitted partitions. Bound retained bytes, record counts, pending requests,
batch size, spool capacity and deadlines. Acknowledge only after downstream has
secured the payload within its own grant. Cancellation/failure must terminate blocked
drains, delete owned buffers and stop new writes; clean EOF follows only committed
complete producer output. Consumer abandonment does not cancel unrelated partitions.

Page, Task, memory-pool and transit-lease owners stay alive until all derived Arrow
buffers and asynchronous sends are released. Do not delete owner captures to reduce
memory: that creates use-after-free. Necessary copies/conversions are charged to a
copy ledger. IPC serialization, compression, TLS and network movement are not
physically zero-copy.

## Implementation ownership and dependencies

1. Protocol authority, tests/property tests, retrieval/BM25/overlay consumers;
   Planner explicit remaps and row-locator schema/emit consumers.
2. Function requirement/admission collector consumes the exact authority API;
   no independent relation decoder or function schema registry.
3. After the previous Native build/test checkpoint closes, Native canonical
   direct lowering/emit, Read/Mask, grouping-set output and safe window optimization.
4. PartitionedOutput sink, concurrent bounded output-buffer drain, Arrow serde,
   result re-run removal, direct transport, remote spool and receiver atomic migration.
5. Same-source Release/ASan/package/Java/full regression/E2E/smoke; deployment-scale
   performance and failure acceptance against an explicitly recorded cluster.

No production-complete claim is valid after only steps 1 or 2. New Native/test files
must not enter build-time globs while the earlier checkpoint is running. Generated
proto files are not hand-edited. No fake/stub/mock execution, disabled tests,
constant assertions or runtime-unavailable assumptions count as end-to-end evidence.

## Acceptance matrix

| Chain | Required real oracle |
|---|---|
| Read and VirtualTable | Filter non-emitted base column; TRUE/FALSE/NULL; non-BOOLEAN rejection |
| Emit | Identity/reorder/duplicate/empty/negative/out-of-range; N-row zero-column batch |
| Project | Direct append; offset remap; emit-only Project; nested/decimal/array typed NULL |
| Window | Remapped row_number filter; optimized and ordinary values/types/shape equivalence |
| Aggregate | Sparse compact input; grouping sets; i32 set index; hidden index and exact values |
| Artifact | Real Java serialized binder → Native Task; row locator, both Overlay wrappers, policy filter/mask |
| Retrieval | Added BM25 score through Filter/Project emit; Sort/Fetch mapping composition; stable keys |
| Shuffle | Dropped/reordered key; 1/N partitions; broadcast/RR; typed Arrow values/schema on wire |
| Durable remote | Real producer/spool/consumer with fencing, retries, checksums and complete producer cohort |
| Lifecycle | Backpressure, slow consumer, cancel, deadline, truncated stream, committed EOF, owner cleanup |
| Performance | Scan columns/bytes; partition CPU; copy bytes; peak memory; queue waits; throughput/p95 |

Record commands, source checkpoint, environment and `PASS / FAIL / SKIPPED / BLOCKED`.
The full repository steps 0–8 and sanitizer gates remain required. Distributed
evidence requires distinct real Worker processes, not several IDs for one endpoint.

## Primary evidence

### Additional semantic and resource root causes (implementation in progress)

- All join inputs use the same ordinal physical aliases, with the right input
  rebased by the original left arity. Logical and nested names are unchanged.
  `post_join_filter` is an actual Filter above the join-specific direct output,
  not an ON predicate; the relation's emit applies last. Hash build-side selection
  preserves original output ordinals. SINGLE proof compares the actual preserved
  input-side statistics with the exact join operator output before post-filtering.
- MARK is a nullable reduction, not EXISTS. Pinned Velox NestedLoopJoin's marker
  is two-valued; its unmatched FALSE cannot stand in for UNKNOWN. The actual
  null-aware HashProbe filter tracker implements the required three-valued
  reduction with non-null constant bucket keys and the complete typed condition.
  This legitimate single-Velox lowering can cost O(NM). It requires complete
  partner coverage, task memory limits and future planner costing/distribution
  validation; it is **not** a distributed hash-partition performance claim.
- Canonical set enums distinguish membership against any secondary input,
  membership against every input, deduplication, and counted ALL variants.
  Planner emits those exact SDK enums; Native uses actual null-equal hash joins,
  counting joins and deduplication aggregation rather than a capability-dependent
  private rewrite. Zero-column sets use a real constant key then drop it, so an
  empty input does not acquire a fabricated global-aggregate row. Grouping
  outputs follow expression declaration order, not first reference encounter.
  These changes follow the [official logical relation contracts](https://substrait.io/relations/logical_relations/).
- A write-drain pool created through global `addLeafPool` is not task-scoped.
  The sink must allocate through the actual Task pool's leaf child, whose pinned
  `MemoryPool` retains its ancestor through shared ownership. Payload memory must
  be the actual charged allocation held until every HTTP payload clone is gone.
  Existing spill limits are configuration ceilings, not shared disk leases;
  completed-file checks are not write-before-growth reservations. The physical
  writer needs bounded staging and actual Create finalization, not HEAD/overwrite.
  Rust encoder/cache/multipart allocations are not all allocator-hooked; cache
  configuration or a reservation token alone cannot prove a process hard peak.

### Aggregate/window and wire implementation checkpoint (not validated)

- Aggregate grouping keys, nested/computed value arguments, BOOLEAN masks and
  ordering expressions now use one actual preparation Project over the original
  input context. Per-measure DISTINCT is executed by the pinned AggregationNode
  distinct wrapper, which applies masks before its distinct tuple insertion.
  The old second multi-DISTINCT decoder and synthetic marker-name path are gone.
  Pinned `GroupingSet.cpp` rejects partial distinct and sorted aggregates, so
  legal single-phase distinct or ordered aggregates must not be automatically
  split into partial/final. DISTINCT plus ordering still needs an actual semantic
  lowering and value oracle; the pinned sorted wrapper does not directly support
  that combination. Phase-specific state binding is still an open contract gate.
- The 54 size/distribution combinations in `MarkDistinctCorrectnessTest` formerly
  changed only a test name. All six parameterized coverage directions now create
  actual typed VirtualTable rows, execute a real Task and check NULL-sensitive
  counts against an independent set oracle. These 324 source tests are awaiting
  execution, not 324 reported passes. Additional Task tests cover computed/nested
  keys, original-input FILTER, declaration-order keys and empty grouping sets.
- Consistent window relations and projected window expressions now share one
  typed preparation and window lowerer. Computed partition/order/argument
  expressions are materialized rather than silently dropped by a failed field
  cast. Both shapes retain function phase/options/invocation in the canonical
  generated expression. Actual registry admission, IGNORE NULLS, DISTINCT
  windows and phase contracts still require the unified function-contract unit;
  retaining their proto fields alone does not implement their semantics.
- The pinned Velox tree provides Arrow C Data import/export but no SDK
  `ArrowVectorSerde` implementation. Pangrova's current `ArrowBatchVectorSerde`
  is its named VectorSerde implementation. The source checkpoint has removed
  private four-byte framing, made iterative flush one standard stream containing
  every appended batch, and switched deserialization to the sole bounded stream
  reader with exact schema/EOF checks. The complete shuffle consumers and
  deployment tests are still migration/validation gates. Velox's internal `PrestoSerializedPage` class
  is a generic owner around that payload; its class name is not evidence of a
  second executor and does not justify dropping its buffer-release owner.
- `ShuffleFlightBatchReader` currently advertises one BINARY `data` column and
  places a serialized private page inside it. That is not a typed Arrow wire.
  Its replacement must expose the actual canonical fields and preserve the
  popped batch/transit-credit owner through all shared Arrow buffers. Durable
  chunk storage must carry the same Arrow bytes and schema; signed tickets,
  producer cohorts, checksums, leases and EOF fences remain authoritative.

New canonical join/set/aggregate Task oracles are source changes awaiting the main-thread
Native build. Protocol-only and Calcite-to-proto checks do not validate Task values,
Arrow transport, JNI, distributed partner coverage, JDBC labels, or hard memory.
The entire Arrow/shuffle/lifecycle unit and deployment-scale gates remain open.

Result ownership and command source checkpoint (not yet Native-validated):

- A direct result endpoint is issued only by the actual admitted result producer.
  Its HMAC domain binds the complete security context, task attempt, owner fence,
  RESULT_STREAM scope and finite task deadline. The server requires the exact live
  registration and one claimed reader; raw task-ID tickets are not accepted.
- Producer terminal state/EOF does not release the task grant. A task-local
  ownership state tracks actual RPC readers and the final recursively retained
  Arrow batch/buffer owners, including zero-column nonzero-row batch roots.
  Drain evidence additionally requires stopped production, revoked read authority
  and actual output-queue deletion. The unique generic v2 resource-drain command
  validates the complete admitted task attempt and security before returning only
  metadata, never rows. Terminal cleanup uses this evidence or explicit
  expiry/release, joins Native, then compare-and-removes registry entries.
- Explicit Native release checks real resource drain before deleting the task.
  Failed release keeps the Native handle, Java task tracking and resource grant
  retryable. A completion future or map removal is not resource-release evidence.
  Old tests using controllable executors must be migrated to the real JNI/result
  path; their synthetic FINISHED/resource counters cannot validate this boundary.
- `PangrovaArbitrator::growCapacity` previously ignored actual `growPool(false)`
  on both grant attempts. The source now returns the acquired external grant on
  refusal and throws the actual pool-cap error; success counters advance only on
  committed growth. Standalone SharedArbitrator cap tests do not prove the JVM
  AllocationListener refund; the real JNI callback/accounting oracle is pending.
- UTF8 remains the sole string wire representation. Pinned SDK StringView export
  can share original values and mutate non-inline pointer words while rewriting
  views; API availability is not proof of safe zero-copy export. A source
  vector-before/after UTF8 serde oracle checks both inline and out-of-line strings;
  StringView transport is not enabled or claimed verified.

### Exchange typed-page decision and implementation plan (not implemented)

The following decision is based on the actual pinned Velox source commit
`3da0390118fa4b0addbfbdeb03f438ec1b1237a6`, not on an assumed extension API.
The current Native compilation checkpoint remains frozen; neither SDK source nor
its repository-owned patch has been changed for this decision.

`velox/exec/LocalPlanner.cpp:546` constructs the built-in `Exchange` directly for
`core::ExchangeNode`, before the custom operator translator branch. Registering a
new SourceOperator therefore cannot replace this operator without either a new
plan-node path or an SDK planner change. That alternative would additionally
duplicate the split collection, random source ordering, blocking futures, task
completion and ExchangeClient final-statistics behavior currently implemented in
`velox/exec/Exchange.cpp:93–173,331–358`. Prefer a narrow repository-owned SDK
typed-page extension inside the existing Exchange/ExchangeClient/ExchangeQueue,
with the same canonical ExchangeNode and the same Arrow Flight wire.

Two concrete constraints make an unmodified SDK insufficient:

- `velox/exec/SerializedPage.h:23–40` exposes only serialized bytes and optional
  row counts. Arrow Flight's public `Next()` already yields a RecordBatch. Merely
  adapting that result to this interface requires another encode/decode, even
  though same-process import can reuse compatible buffers. This is an API
  restriction, not a physical requirement of distributed exchange.
- `Exchange.cpp:280–286` chains all independent pages before row deserialization
  when `supportsAppendInDeserialize()` is false. Pangrova's strict canonical
  decoder consumes one complete standard stream and rejects trailing bytes;
  chaining two independently terminated streams violates that contract. Do not
  weaken EOF checks, concatenate private frames, or advertise append support
  without implementing its actual offset/row-count semantics.

The SDK extension must expose an explicit task-local typed payload, with no
Arrow dependency in the SDK execution API: an owned RowVector, exact physical
RowType, actual retained-allocation bytes, independently reported wire bytes,
row count and an ownership/credit lease. The concrete class and method names
remain implementation details to be verified against the pinned SDK before the
patch is committed. It is not a serialized payload, protocol variant, new plan
or fallback. The existing byte-page interface may remain for SDK callers outside
Pangrova; Pangrova's admitted exchange always supplies the single typed payload
and must reject a wrong payload kind instead of selecting another decoder.

Exchange consumes one typed page at a time, validates its exact expected output
type and returns its owned vector. Returning one batch rather than coalescing
multiple pages avoids an otherwise unnecessary copying projection. Larger
batches may be sliced through the SDK's actual vector APIs only after ownership
and retained-byte accounting are verified; returning a whole admitted bounded
batch is the first implementation. A zero-column N-row vector is data. A zero-row
batch is not EOF. Only the existing committed source completion and complete
source cohort may signal EOF. Statistics distinguish wire bytes, retained bytes
and output rows; a compressed wire size cannot stand in for retained memory.

#### Receive, accounting and ownership boundary

1. The Flight producer exposes the canonical physical fields, never BINARY
   `data` containing another format. On the receiver, exact schema/ordinal names,
   nested names, decimal precision/scale and existing signed attempt/partition
   authority are validated before publication. No SQL type is inferred from
   runtime buffers, and no new relation/schema decoder is introduced.
2. `FlightCallOptions.memory_manager` is the actual pinned Arrow receive-allocation
   hook (`arrow/flight/client.h:74`), not FlightClientOptions. Bind it, and the IPC
   read options allocator, to the real shared task leaf through the existing
   ColumnarMemoryPool. Both the allocator and all derived buffers must stay alive
   after the RPC/RecordBatch wrapper is released. Transport-library allocations
   not covered by this hook remain measured external-memory gates, not a claimed
   process hard limit.
3. The existing Arrow C Data bridge imports each received batch once. Compatible
   numeric buffers can remain shared; UTF8, offsets, timestamp representation and
   dictionary conversions use the sole canonical codec and record actual copy
   costs. The import owner retains the RecordBatch, shared allocator and transit
   lease until the final derived vector/buffer is destroyed, including slices,
   dictionary children and zero-column roots.
4. Durable chunks remain the same standard Arrow streams. Their single bounded
   CanonicalBatchStream decode feeds the same typed import/page path. Flight and
   durable storage are two ingress APIs for the same Arrow contract, not two
   wire formats. Delete `enqueueExternalBatch`'s unaccounted `IOBuf::copyBuffer`
   and the live receiver's BINARY extraction when the complete replacement is
   connected; do not leave these as compatibility branches.
5. `ExchangeQueue::totalBytes` and pending-request scheduling must account for
   actual retained bytes and in-flight receive reservations. Wire bytes remain
   separately observable. The SDK's existing request size is a soft target and
   intentionally permits one large page (`ExchangeClient.cpp:293–308`); it is
   not a hard task grant. The actual allocation cap and receiver credit prevent
   overcommit, including compressed expansion and simultaneous pending sources.
   Define and test bounded admission for a single page larger than the queue
   target but within its task grant, rather than deadlocking that legal case.
6. Queue dequeue releases only the queue occupancy, not the task allocation or
   transit ownership. A consumer may still hold the batch. No-more-sources,
   completion futures or a zero queue count prove final owner drain. Cancellation
   must stop actual RPCs, wake blocked requests, revoke new reads and clear queued
   pages, then wait for already delivered owners before releasing the task grant.
   Failed release remains retryable with its handle and grant retained.

The current `PangrovaExchangeSource::performFlightRead` does not set that receive
memory manager and still admits only one BINARY column. `enqueueFlightBatch`
correctly retains a batch owner, but its logged maxBytes excess is not admission.
Those are observed migration gaps, not properties already fixed by this design.
The pinned Exchange operator also does not implement remote dynamic-filter
propagation or spill arbitrary received pages. Preserve its actual behavior and
explicitly validate memory/cancellation; do not claim these capabilities from a
new typed page alone. Any future remote filtering or page spilling must reuse
the existing authority and canonical Arrow codec rather than create a private
source or payload.

#### SDK API ownership and queue readiness

The proposed SDK patch owns `velox/exec/SerializedPage.h`, `Exchange.h/.cpp`,
`ExchangeQueue.h/.cpp` and, where pending retained-byte requests require it,
`ExchangeClient.h/.cpp`. No LocalPlanner interception or new core PlanNode is
needed. The exact implementation must preserve ordinary upstream SDK serialized
callers while making the Pangrova ingress explicit and exclusive:

- Expose a payload kind and a typed-vector accessor on the page abstraction;
  typed pages never implement byte serialization by secretly constructing a
  stream. Accessing the wrong payload kind is an error. The existing public
  byte-page callers remain SDK functionality, not Pangrova runtime negotiation.
- A typed page reports its exact RowVector and retained-allocation charge;
  separate wire-byte evidence prevents compressed sizes from driving queue
  memory admission. Do not use `estimateFlatSize()` as an actual allocation
  counter. The sole import owner deduplicates shared allocation identities,
  counts whole retained allocations rather than just logical slice lengths and
  charges newly allocated conversion buffers separately.
- `Exchange::getOutput` dispatches on the explicit admitted payload kind before
  invoking a serde. Its typed path validates the expected physical type, moves
  one page's vector into the actual output and advances the existing page state.
  It does not merge independently terminated Arrow streams or deserialize the
  returned vector again. Mixed payload kinds within an admitted source fail.
- The delivered RowVector root retains its import owner even with no columns;
  each imported buffer and derived slice/dictionary child must independently
  retain that same owner. Holding a page or root alone is insufficient when a
  downstream operator keeps a child buffer after dropping the wrapper. This
  must be demonstrated through actual SDK import/slice tests, not assumed from
  shared_ptr syntax. Task pool and Arrow allocator destruction follow the final
  allocation owner, not the lifetime of the receive future.
- Queue readiness needs page/cardinality state in addition to retained bytes.
  The current `ExchangeQueue.cpp:127` blocks when totalBytes is below its dynamic
  byte threshold. After earlier nonempty pages, a legitimate zero-column page
  may retain no data buffers but still carry N rows. Applying only that byte
  threshold could withhold the page until source completion. Make such queued
  data eligible to wake/dequeue without fabricating a one-byte allocation or
  treating it as EOF; bounded page-count metadata and in-flight requests still
  prevent an unlimited zero-byte queue. Add a test where the producer requires
  consumption before publishing completion to rule out a hidden deadlock.
- Queue error/close clears pages and resets queue occupancy consistently while
  downstream owners keep their real grant. `ExchangeQueue::closeLocked` and
  `setError` currently clear the deque without resetting totalBytes; preserved
  final diagnostics must not be confused with current live occupancy. Make
  lifetime counters, final peak/statistics and admission counters explicit.

Pangrova owns the actual Arrow receive allocator, canonical import, exact schema
and attempt/sequence validation, queue credit and drain owner implementation in
`src/columnar`, `src/memory` and `src/velox/exchange`. The SDK page carries those
already typed values and owners; it must not import Arrow or decode Pangrova
authority. The source factory's raw SDK pool pointer is not by itself a lifetime
lease: retain the actual shared task leaf when creating the allocator and page.

#### Atomic implementation and acceptance

SDK changes belong to the existing repository-owned patch/application/provenance
mechanism, coordinated with the function registration/selected-signature patch.
The applied source digests, rebuilt archive symbols and combined patch digest
must identify the same pinned source; an old registration-only archive cannot
be accepted as proof of the new exchange API. No installed SDK edits, generated
sources or patch application occur during the current frozen build checkpoint.

| Scope | Required unchanged-path actual oracle |
|---|---|
| SDK typed page | Real built-in ExchangeNode returns owned RowVector values/types; no custom translator or serialized reconstruction |
| Schema and pages | Two independent batches; multiple sources; reordered/duplicate logical labels with canonical aliases; nested decimal/list/map/nulls |
| Cardinality and EOF | Zero columns with N rows; zero-row data before later nonempty data; source complete before cohort complete; queued data before final EOF |
| Credit and allocations | Measured receive/import/queue/consumer retained bytes; compressed expansion; one oversized-within-grant page; simultaneous sources; actual task cap failure |
| Owner retention | Release original Arrow wrapper and page while downstream vector/slice remains; actual allocated bytes remain charged until final owner; ASan UAF/leak check |
| Blocked lifecycle | Actual waiting split and data futures; slow consumer; active RPC cancel; timeout; queue clear with delivered owner still held; retry after owner drain |
| Durable transport | Real signed producer/spool/receiver; sequence/checksum/fence validation; retry immutable chunk; no old BINARY or private-page wire |
| Task semantics | Actual join/aggregate/sort consumer of exchanged rows; unchanged output values and no swallowed source error |
| Performance | Same dataset/cluster comparison of CPU, copied bytes, receive peak, retained peak, blocked time, throughput and p95; no re-encode between Flight Next and SDK Exchange |

The Arrow C Data specification confines shared-memory interchange to one process
and requires explicit release ownership; IPC is the persistent/cross-process
boundary. [Official C Data specification](https://arrow.apache.org/docs/format/CDataInterface.html).
Flight exchanges Arrow data using its standard RPC/IPC model; neither document
promises an end-to-end zero-copy network. [Official Flight specification](https://arrow.apache.org/docs/format/Flight.html).
The tests above must demonstrate the removed avoidable encode/decode separately
from physical network/TLS/compression costs.

### Java lifecycle test migration checkpoint (source compiled, JNI tests pending)

`ActualNativeTaskFixture` now initializes the actual packaged runtime, external
MemoryArbitrator/NativeMemoryCoordinator, canonical typed Values task and real
result/exchange transports. It never substitutes an executor or synthesizes a
terminal report, and teardown asserts actual reservation release rather than
resetting budgets. Runtime-global tests share one resource lock.
`OutputTaskMemoryReleaseTest` preserves release/idempotence/failure/report coverage
through actual tasks, real Flight consumption and exact resource-drain evidence.
Its status reporter only captures production output for assertions; this does
not prove the Coordinator HTTP acknowledgement or a distributed SQL result path.

Pure terminal/drain contract tests call the production generated-proto decoder
and validator without simulating a business task. All seven authority mutations
must actually differ from the admitted request; clearing an already-empty role
list was an invalid negative fixture and has been corrected by using explicit
admitted roles. Compiling these sources does not execute JNI or prove actual RPC
owner drain. Java received batches are not a substitute for server-side reader
and retained-owner evidence. The remaining old controllable-executor lifecycle
fixtures, complete shuffle migration and actual multi-Worker acceptance remain
open gates.

### Creation, exact cancellation and cleanup ownership checkpoint

Status: the following source changes form one atomic lifecycle migration. They
have not yet passed the same-source Native build, packaged JNI, full Java
regression or distributed E2E. Earlier successful builds do not validate this
checkpoint.

An HTTP cancellation acknowledgement is not execution exit. The canonical
`TaskReport.execution_drain_state` distinguishes actual Native owner release
from closed Worker admission with no Native owner. Neither statistics absence,
a zero handle, HTTP NOT_FOUND nor a completed scheduling future establishes
either fact. Native release joins actual execution and write owners before
producing its report; Worker reconciliation retains this genuine evidence while
preserving the unsuccessful state and first error. Result-stream retained
owners remain a separate resource obligation.

The compact `TaskAdmissionSealRequest` binds the originally issued NETWORK
request's deterministic SHA-256, exact attempt and deadline. Cancellation uses
this sole binary control request, not a raw query/task identifier. Compact drain
receipts contain only exact identity, terminal state, origin, error and exit
evidence: they omit result, source coverage, usage, statistics and partition
lists. They are cleanup receipts, never winner/publication authority. A genuine
authenticated receipt remains usable for cleanup even when PostgreSQL rejects
that old attempt's publication lease; no old winner is reinstated.

Native creation reserves the existing HandleRegistry ownership entry before
configuration, callback or SDK construction. Java registers its exact creation
owner before entering JNI and stores the returned primitive handle before
further Java index allocation. Generic runtime command 21 recovers only this
same registry owner or its actually sealed, exited no-owner state. Constructors
and JNI response allocation failures never become no-owner merely because Java
did not receive the handle. Java callback, external shuffle and catalog cleanup
must also succeed before Worker publishes admission-closed evidence. Actual JVM
OOME/lookup-allocation failure injection is still an independent unverified
acceptance boundary; decoder tests are not evidence of such a failure.

All ordinary JNI commands and reads now hold actual operation leases, not locks
over JNI. `NativeOperationLifetime` admits and records exit under short memory
locks; exclusive release/recovery claims wait for entered operations outside
locks and perform JNI and remote cleanup without a held lifecycle lock. Failed
cleanup exits its claim while retaining the exact owner and real release
receipt for retry. Close seals public admission, waits for actual creation and
operation exit, then closes its lower-level transport owner. A creation thread
or entered operation cannot wait for its own exit. This preserves release/close
exclusion without a lock-during-I/O implementation or a compatibility read-lock
path.

External shuffle callback cleanup uses the full TaskRequest seal and complete
producer binding. A conflicting candidate cleans only its own lease; it cannot
abort the installed owner. Native raw abort callbacks are notifications, not
accepted exact-owner cleanup evidence. Metadata durable cancellation locks a
deterministic exact job ID set and updates that same job/page set within one
transaction. Durable dispatch retains issued attempts until authenticated exit;
revoked publication rights do not fabricate FAILED or erase active ownership.

Closest source acceptance inventory:

- Protocol SHA/seal/compact validation and Worker receipt projection tests;
  these are generated-protocol boundary tests, not task execution.
- `NativeOperationLifetimeTest` has five actual thread/exit-barrier tests;
  `TaskNativeCreationTest` retains actual creation-admission and exit coverage.
- `NativeCreationRecoveryContractTest` has six strict decoder tests, including
  rejection of a changed already-published handle. They do not execute JNI.
- `NativeCreationTransactionIntegrationTest` has four packaged-Native creation,
  registry recovery and release cases; `ExecutorCreationRegistryTest` exercises
  actual Native owners. Both remain pending same-source execution.
- The original real JNI release/concurrent-close tests retain their actual
  Native-handle-gone and entered-operation exclusion assertions under the new
  lease API. The real PostgreSQL lock/isolation/requeue and real service exact
  shuffle-owner tests require their complete integration environment.

The build owner must regenerate both protocol sides, run the unchanged full
pipeline and execute these closest suites without replacing original SQL/value
or resource oracles. Outstanding gates include actual JNI allocation failure,
all ambiguous submission/cancel races, independent-process speculative
execution, Worker process-incarnation/restart fencing, failure of a retained
candidate's actual channel join, and large-scale resource/performance acceptance.
Runtime CPU/scan quota recording and current in-progress row/CPU statistics are
not connected/accurate merely because task admission and exit are bounded.
DROP MODEL is not enabled by this generic durable-job cancellation work; its
cross-object durable publication/drop ownership still requires a complete saga.

- [Substrait logical relation semantics](https://substrait.io/relations/logical_relations/)
- [Substrait common emit/output ordering](https://substrait.io/relations/basics/)
- [Substrait depth-first NamedStruct names](https://substrait.io/types/named_structs/)
- Pinned official Java SDK 0.95 source: Project, Rel.Remap, RelProtoConverter,
  Aggregate and MaskExpressionTypeProjector.
- Pinned official Velox source: PartitionedOutputNode, PartitionedOutput,
  OutputBufferManager, FilterProject, Operator::fillOutput and partition functions.

### Native aggregate output names at the Arrow boundary

Velox aggregates can reuse input vectors, including nested ROW values with the
input's field labels. The producer binds those labels by ordinal to the admitted
canonical schema before Arrow export. This binding checks equivalent physical
types and only creates vector wrappers: ROW children, collection offsets/sizes,
dictionary indices, null masks and payload owners are shared without mutating
the source. Constant wrappers retain their immediate value-vector index, which
can differ from the fully decoded index when a lazy input loads a dictionary.

The exact canonical type/name and logical NULL checks still run after binding.
Incoming Arrow schemas still require exact names and types; the producer's
label binding does not relax wire admission. Release and matching ASan cover
ROW/ARRAY/MAP, dictionary/constant/lazy owners, nested values and NULLs, sparse
append, compression and shuffle. The affected serialization cohort passes
65/65 in both builds; the final constant/lazy combination changes only two
cases, which pass 2/2 in both builds. The full final regression remains pending.
