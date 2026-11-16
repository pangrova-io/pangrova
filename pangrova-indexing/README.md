# Indexing Module Boundaries

The target design is defined in `docs/architecture/domain-modularization-adr.md`.
Four real artifacts exist today:

| Artifact | Current ownership |
|---|---|
| `pangrova-indexing-api` | Calling model, strategies, and payload-serializer extension contracts; production dependencies are only Common and Connector API |
| `pangrova-indexing-codec` | Predicate encoding, public index file framing, CRC, type-encoding helpers, and the serializer registry |
| `pangrova-indexing-strategies-builtin` | The nine existing Java strategies plus payload serializers, value normalization, and the Bloom pre-parse representation |
| `pangrova-indexing-lifecycle` | Build, evaluation, caching, and lifecycle; depends on independent Persistence ports; Calcite vector recognition and Native retrieval planning binding live in `core.planner.vector` |

Server discovers the strategies actually loaded through `IndexEngineRegistry` and
populates `TypeSerializerRegistry` with those instances. Each strategy supplies a
payload codec paired with its own original build result. An empty production
registry, duplicate types, or a missing codec fails initialization. The builtin
artifact publishes the service descriptor, and Server consumes it as a runtime
dependency. Strategies hold no threads or connections.

Build orchestration, attempts, and mapping conversion belong to `core.build`;
circuit breaking and evaluation metrics belong to `core.evaluation`; file I/O and
caching belong to `core.storage`; schema-change handling belongs to
`core.evolution`. Business packages borrow only Persistence persistence ports.

`IndexStrategyProvider.prepareForEvaluation` expresses optional read-only
preprocessing and returns the original data by default. Core evaluation services
call it on first read and use the same result for the first evaluation and for
caching; Cache no longer recognizes Bloom. Preprocessing must preserve types,
no-false-negative semantics, concurrency-safe reads, and complete memory
estimation. Bloom estimation now includes the serialized array, the independent
parsed array, and estimated object overhead. Corrupt data must not enter the
cache; evaluation returns UNKNOWN, while cancellation and authorization errors
still propagate. No exact JVM object layout or performance improvement is
promised.

The public file magic, version, type stable ID, schema hash, writer build ID, and
CRC algorithm are unchanged. The preprocessing representation is process-local
and is not passed to payload serializers. Core owns the pure value state
transitions of IndexDescriptor; persistence publication must still pass the
current authority, generation, and transaction fencing.

A real-call audit corrected conclusions from the previous batch:
`NativeIndexHintConsumer` and `IndexEvalSplitFilter` have no production callers
and were retired together with the old `IndexFileEvaluator`/factory,
implementations, and registrations. They cannot prove that Worker Java filtering
is enabled. The random multi-hint aggregation coverage moved to the Core
evaluation service and is not claimed as end-to-end coverage.
`IndexEvaluationService` currently has only construction and no business caller;
it remains an explicitly unwired Java capability and is not connected to a
duplicate execution path merely to improve coverage numbers. The actual Worker
hint chain is: SplitFetcher writes the hint into Proto, the Worker submits the
task through JNI, and the C++ VeloxTaskRunner invokes IndexHintFilter.

Follow-up work includes persistence capability port subdivision and tightening
default semantics, deciding the fate of the Java evaluation capability, full-role
assembly, and an unsupported-capability inventory. Actual validation and
per-class disposition are governed by the current code and tests.

Encoded-data ownership has been tightened: Bloom/N-gram output can be written as a
snapshot or as an independent read-only ByteBuffer; builtin evaluation uses a
read-only view. Bitmap EQ/IN uses fixed-point membership, and R-tree
construction/encoding uses coordinate access; modifying the snapshot does not
change the original payload. BitSet and nested postings likewise isolate input
and output mutations. These component-level loops do not change the unsupported
status of the Java evaluation described above and do not prove immutability of
arbitrary Comparable keys. Buffer ownership is covered by the current Codec and
evaluation tests.

Multi-column BTree keys are now owned by `CompositeIndexKey` in the API, and the
Codec encodes the full tuple with tag 11; the former Provider-private key classes
have been deleted. Tuples accept only the immutable scalar types listed in the
documentation and reject null, nested, oversized components and incompatible
shapes. The builder validates page size and row shape; an empty key set lacks
NULL coverage information and cannot directly conclude SKIP. Real SQL index
creation and published-file readability are verified independently; Native BTree
pruning remains unwired. See the current `CompositeIndexKey` implementation and
tests for details.

Persistence has moved out of Indexing Core into two real artifacts:

- `pangrova-persistence-indexing-api`: existing storage ports, mappings,
  generation manifests, publication requests, and event values.
- `pangrova-persistence-indexing-postgresql`: existing descriptor, state,
  mapping, publication, queue, statistics, and GC SQL.

Server explicitly passes the JDBC API's `CurrentTableAuthorization`, and the
PostgreSQL publication transaction invokes the current authority check in place;
participants cannot commit, roll back, or close the caller's connection. Providers
no longer depend on production classes from Metadata Core or Indexing Core.
Initialization still belongs exclusively to Schema Migrations, and no Memory
backend was added. The existing wide ports and optional default methods remain as
follow-up P5 work; this isolation does not claim that capability port subdivision
is complete. See the current persistence artifact implementation and tests for
details.

PostgreSQL batch selection uses fixed `ANY (?)`/`jsonb_exists_any` SQL and JDBC
text arrays. `JdbcTextSelection` is a backend-private resource owner that releases
resources for the current operation in the order ResultSet, Statement, Array,
Connection; the DataSource still belongs to the assembly layer. The
generation/snapshot, pre-publication invisibility, and 1,000-path batch boundary
remain unchanged. Implementation and failure validation are covered by the current
PostgreSQL Provider tests.

Cancellation intent for a Scalar build belongs to each individual submission, and
CANCELLED cannot authorize a generation publication. A new submission does not
clear the cancellation state of an older request; cancellation is checked before
reads, row iteration, encoding, and new file writes. In-flight calls still exit
through cooperative cancellation, and PostgreSQL isolates late mapping writes
using the current claim and generation. Recovery from a crash after an object
write succeeds but before the mapping commits remains to be verified. The
cancellation boundary is described by the current submission and evaluation
implementations.
