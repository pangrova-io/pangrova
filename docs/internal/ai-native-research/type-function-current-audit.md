# Current type and function contract audit

Audit date: 2026-10-07. Source base: `058fb13a2f260125f2f9c7a2289550c3442a1e05`
plus the existing worktree. This report distinguishes inspected implementation,
the narrow corrected root-cause unit, and boundaries that still require an
atomic implementation. It does not claim full-chain acceptance.

## Primary-source findings and their application

* [Calcite ArraySqlType API](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/type/ArraySqlType.html)
  describes an element type and collection nullability, not a vector dimension.
  Its digest must include all identity-bearing details. The locally pinned
  Calcite 1.42.0 source additionally shows `SqlTypeFactoryImpl.copyArrayType`
  constructing a plain ArraySqlType. A vector subclass alone is insufficient:
  the owning PlannerTypeFactory must preserve its identity during type copying
  and nullability changes.
* [Substrait type classes](https://substrait.io/types/type_classes/)
  define LIST as a variable-length collection. Therefore LIST<FP32> is the
  physical projection of VECTOR, not proof of its fixed dimension. The existing
  canonical LogicalType carries dimension and element precision; those
  constraints must survive binding, fragmentation, replay and output. If they
  are represented within the IR itself, use the declared Substrait extension
  mechanism and the same canonical declaration, not a second private plan.
* [Substrait type system](https://substrait.io/types/type_system/)
  makes nullability part of the type contract. Missing storage information must
  never become a guessed concrete executable type. SQL UNKNOWN is not VARCHAR.
* [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout)
  specifies the fixed-size-list child layout. Matching floating element width,
  fixed length and child nullability are required; changing a field label does
  not change or validate its buffer layout. This applies to Native storage and
  Flight output without creating another execution representation.

These findings directly inform the existing planner-owned metadata adapter,
canonical VectorLogicalType declaration and Native Arrow interoperability.

## Verified current boundaries

| Boundary | Inspected state and root cause |
|---|---|
| Connector declaration to SQL | Production `MetadataResolvedTable.getRowType` calls the single `ConnectorColumnTypeMapper.rowType`; `CompiledWriteDeclaration.metadata` also uses this mapper to compare frozen write identity. Before this correction UNKNOWN became VARCHAR, absent ARRAY/VECTOR parameters became ARRAY<DOUBLE>, and mismatched structural parameter classes could slip through. |
| Dataset declaration to canonical schema | `NativeDatasetSchemaCompiler.compile` uses the same mapper, TypeMapper and Substrait serializer. Its original top-level FLOAT64 VECTOR branch manually constructed LogicalType instead of using VectorLogicalType; this duplicated the declaration and did not assert exact equality with the physical schema. |
| Storage VECTOR to optimized SQL type | Before correction the mapper used unbounded ArraySqlType. The current independent unit now retains a StorageVectorRelDataType carrying the canonical LogicalType, with factory copy/nullability protection. Core Substrait LIST and actual scalar-call/task contracts still lack storage-vector dimension authority; the marker alone does not close these boundaries. |
| Final result binding | Initial inspection found only exact model intents. The current coordinated unit adds static canonical types to final PlanFragment output, StagePlanAccess, SerializedStage and graph identity, and merges them through the same DAGStagePlanBinder. Durable replay reads the existing GenerationStageTemplate result-type bytes. |
| FLOAT64 Native result | Initial inspection found strict FP32-only result admission, index/dimension-only state, FP32-only Flight annotation and client decoding. The coordinated unit now retains the same complete canonical LogicalType in that state; storage validation accepts exact FP32/FP64. Root owns the dirty runner/Flight/reader integration. Client ArrowBatchDecoder returns float[]/double[] according to actual typed Arrow schema, SPI preserves that width, and JDBC/CLI expose actual coordinates. AI validation remains separate and strict FP32. |
| Actual function registration | FunctionCatalogAssembler seals an actual owned registry snapshot; explicit startup finalize/read supplies that inventory to Worker registration. This implementation already exists and must be retained. |
| Actual function planning | `FunctionCatalogView` has no Planner/Coordinator main-source consumer. SqlCompiler still installs NativeCapabilityRegistry.defaults, a separately maintained name list. Actual SQL signature authority is not query-frozen. |
| Function task admission | task.proto field18 is declared, but a main-source search finds no production writer/reader of function_requirements. The single TaskRequestFactory producer and Native parser have not closed the contract. |
| Aggregate states | The catalogue advertises four phases, while Native rejects state-input phases without authoritative original raw inputs. Planner emits INITIAL_TO_RESULT. A registered aggregate does not certify distributed partial/final execution. |

The observed FLOAT64 limitation is **result logical admission and presentation**,
not a justification to remove FLOAT64 storage support or silently narrow it.
Existing canonical storage declarations permit FLOAT32 and FLOAT64; both must
continue to map to their actual element precision. AI embedding model output
has a separate strict FLOAT32 contract and must retain it.

## Implemented independent correction

`ConnectorColumnTypeMapper` now requires an actual matching declaration for
ARRAY, MAP, STRUCT, VECTOR and DECIMAL and rejects UNKNOWN at the common SQL
metadata boundary. Recursive ARRAY/MAP/ROW mapping uses that same adapter.
This corrects invented storage types; it neither changes the canonical VECTOR
dimension interval nor disables legitimate FLOAT64 or nested/decimal behavior.

`NativeDatasetSchemaCompiler` now obtains both FLOAT32 and FLOAT64 VECTOR
LogicalType directly from the same storage-vector SQL marker produced through
`VectorLogicalType.create`, and checks that its physical Substrait type equals
the compiled schema field. No new type table was added. PlannerTypeFactory
preserves storage and exact-model vector markers during copying and top-level
nullability changes; the storage marker's digest/deep equality includes its
dimension and canonical element declaration. Ordinary ARRAY stays distinct.

ConnectorTableMetadata.Column currently carries no recursive storage nullability.
The mapper's existing nullable-column policy must not be reported as an exact
source nullability guarantee. The independent unit preserves that legal policy;
complete source nullability needs the actual canonical declaration at the read
metadata boundary.

New `ConnectorColumnTypeMapperTest` verifies missing/mismatched declarations,
ordered nested names, DECIMAL precision/scale and both VECTOR element widths
against the exact canonical LogicalType. New `MetadataStorageTypeAdmissionTest`
uses production MetadataResolvedTable and SqlCompiler to verify that incomplete
storage declarations fail during SQL planning, before task submission, and that
real SELECT alias and LEFT JOIN planning preserve the canonical vector output
shape. StorageVectorRelDataTypeTest separately exercises factory copy/nullable
operations and identity differences. No mock, fake provider, alternative parser
or skipped test is introduced.

## Atomic VECTOR implementation cut and ownership

1. Introduce a planner storage-vector marker carrying the canonical LogicalType,
   created through VectorLogicalType, rather than another dimension/type table.
   Preserve marker identity through PlannerTypeFactory copy/nullability/least-
   restrictive operations. Same-dimension VECTOR branches use Calcite element
   promotion; NULL branches affect outer nullability. Different dimensions or
   ordinary ARRAY branches deliberately have a common variable-width ARRAY
   type and emit no fixed-dimension VECTOR output declaration. Explicit CAST
   to ARRAY likewise changes output identity. Ordinary ARRAY expressions remain ARRAY. Preserve explicit
   semantic conversions and exact-model vector binding as separate authorities.
2. Extract exact static output contracts from the final optimized row type.
   StagePlanAccess, AuthorizedStageGraph serialized stages, canonical graph
   fingerprint and exchange rewrites must retain those contracts. GenerationStageGraph
   persistence must encode them so restart/replay cannot consult newer catalogue
   information. Existing GenerationStageTemplate already retains canonical result
   type bytes, and GenerationStageGraph owns that template. Reuse these fields and
   their established producers/consumers rather than creating another graph
   format. This work intersects already modified metadata and requires
   coordinated ownership, not an overwrite of existing changes.
3. Bind static storage contracts and exact model contracts in the one
   DAGStagePlanBinder output binding. Reject duplicate/conflicting output indices.
   Existing BoundExecutionPlan/TaskRequest field16 transports the resulting same
   canonical LogicalType; do not introduce another task protocol. Intermediate
   exchange physical LIST layout does not grant or replace dimension authority.
4. Native result admission must retain validated canonical storage-vector
   element precision and both nullability levels, not only dimension. Preserve
   strict FLOAT32 model validation. Executor/runner/Flight and the result reader
   must consume that retained declaration and publish actual FLOAT32/FLOAT64
   Arrow data without narrowing. Validate row lengths, child nulls and declared
   physical shape before publication, while preserving Arrow ownership.
5. Coordinate the already modified FlightResultServer and metadata durable
   graph/codec with their current owners. Only after producer, consumer and
   replay paths are source-ready should one matching Native/package/Java cohort
   be built and the actual value oracles executed.

## Function and temporal semantics still requiring implementation

The current declaration generator hashes SQL binding JSON as semantics and
marks definitions CANONICAL_DECLARED_OUTPUT. That is not an executable unified
decision for integer division, overflow, collation or NULL inference.
RoundSqlFunction and DecimalArithmeticTypeDerivation still contain independent
return formulas. The immutable actual catalogue must drive ordinary SQL return
binding; sole SQL declaration rules must specify implicit casts and semantic
differences. Runtime output-target-based overload selection is not a substitute.

The actual SDK Arrow Bridge inspection shows TIME_MICRO_UTC can enter generic
isTime export, which produces time32/milliseconds and narrows int64 to int32.
Microsecond/nanosecond import divides back to milliseconds. Default timestamp
options are global nano/no-timezone, whereas canonical storage requires each
field's precision and zone. Correct these through the existing SDK patch and
provenance mechanism, including mixed/nested fields and true buffer conversion;
do not relabel unchanged buffers or infer that ValuesNode tests prove Arrow
shuffle/client correctness.

## Validation ledger and required value oracles

| Check | Current evidence |
|---|---|
| `git diff --check` after the independent correction | PASS; whitespace/diff validation only. |
| `.pangrova/setup.sh --check` | PASS; canonical instruction layout only. |
| New metadata/type tests | Source-ready; root owns the single shared build invocation. Not counted as PASS. |
| Native/JNI/provider E2E and full regression | Not executed for this unit. No full-chain acceptance claim. |

The first root-run nearest check in `/tmp/pangrova-ai-native-targeted-20261007.log`
executed 14 ConnectorColumnTypeMapperTest and 6 NativeDatasetSchemaCompilerTest
cases with 0 failures/errors/skips. It preceded the marker/factory changes and
does not verify the current expanded unit or the Coordinator SQL tests.

Nearest selectors: `ConnectorColumnTypeMapperTest`, `StorageVectorRelDataTypeTest`,
`NativeDatasetSchemaCompilerTest`, `MetadataStorageTypeAdmissionTest`; also
retain `TypeMapperTest`, existing MetadataResolvedTable tests and affected write
declaration tests. Use JDK17 and the root-selected Maven/artifact cohort.

Full VECTOR acceptance requires actual SQL storage reads, projection/alias,
join/UNION/CASE type identity, stage fragmentation, shuffle, durable restart and
client Flight values for dimensions 1 and 65535, FLOAT32/FLOAT64, NULL vectors,
disallowed child NULL, wrong row length and incompatible dimensions. FLOAT64
values that would lose precision in FLOAT32 are essential. Existing AI_EMBED
model-purpose and model authorization oracles must remain intact.

Function acceptance requires complete final-IR call coverage, explicit empty
requirements, exact selected entry and aggregate provenance, wrong constants/
options/phase/output rejection, retry/speculation inventory drift and durable
replay against the original query authority. Temporal acceptance requires real
Arrow export/IPC/import/next-stage/client roundtrips for TIME(0/3/6), day-end and
microseconds exceeding int32, timestamp(0/3/6/9), negative epochs, zoned/unzoned
mixed fields, NULL, nested and dictionary/constant encodings. Record copied
bytes, allocations, CPU and retained memory at each boundary. A component PASS
cannot certify this full chain or its performance.

## Coordinated VECTOR source-ready update (2026-10-07)

The storage marker, stage propagation, graph fingerprints, output binder,
existing frozen template reader, native canonical storage/vector-result helpers,
TaskRequestParser retained result state, and client precision handling have now
been implemented as one coordinated cohort. This is source readiness, not an
execution or full-chain PASS. Root owns dirty Native Flight/runner/reader and
template producers, along with matching Native/package/Maven execution.

`PlannerTypeFactory.leastRestrictive` preserves one dimension when all non-NULL
branches carry that dimension, combines outer nullability, and delegates element
promotion to Calcite. Dimension-changing combinations and ordinary ARRAY casts
have ordinary ARRAY result types; no invented dimension is published. Actual
metadata-backed SQL tests exercise alias, outer join, UNION ALL, CASE/NULL and
explicit ARRAY cast. PlanFragment output contracts are asserted from those final
relational types. AuthorizedStageGraphTest verifies that different VECTOR
constraints affect graph and stage fingerprints even when core LIST types are
physically identical. FrozenStageBindingsTest exercises FLOAT64 template replay
without model authority and refusal of an out-of-width ordinal.

The native helpers validate canonical spelling, precision, dimension, variation
and both schema nullability levels through the existing sole storage validator.
Actual Arrow value tests validate list width and nulls, plus double coordinates
`1.0000000000000002` and `-123456789.12345679`. Task parser tests retain the
complete FLOAT64 declaration, refuse divergent canonical spelling, and assert
that the same type is still rejected by the strict AI FLOAT32 validator.

Client extension metadata remains the existing decimal dimension string.
Arrow's typed child determines precision. ArrowBatchDecoder reports canonical
`VECTOR(n)` or `VECTOR(n,DOUBLE)` and produces float[] or double[]; ordinary
unannotated lists remain ordinary arrays. ColumnValue.DoubleVectorValue,
TypedRow's existing raw boundary, JDBC VectorSqlArray/base type/class metadata,
and CLI formatting preserve precision and detached ownership. IEEE storage
coordinates are preserved because the canonical storage declaration has no
finite-coordinate constraint; AI invocation/model algorithm oracles continue
to enforce their own input/output constraints.

Remaining boundaries are explicit: ConnectorTableMetadata cannot expose exact
recursive source nullability; an actual nullable child in an annotated VECTOR
cannot be represented by the client's primitive coordinate array and is
rejected. The existing Java BatchSerializer/CoordinatorResultWriter materialized
Page path is FP32-only and has no canonical precision-carrying declaration; it
is distinct from the native Flight result path and requires a separate canonical
producer correction if used for FLOAT64 vectors. Do not claim those broader
paths or full shuffle/restart/client acceptance from these component tests.

Additional nearest selectors: AuthorizedStageGraphTest,
TaskRequestResultColumnTypeTest, FrozenStageBindingsTest, ColumnValueVectorTest,
ArrowBatchDecoderVectorTest, PangrovaResultSetVectorTest, ResultBatchDecoderTest;
native StorageLogicalTypeContractTest and ParseTaskConfigTest indexed result
cases, plus existing AI admission/value tests. No new tests have been counted
as PASS until root's shared matching build reports actual results.

## Actual function authority audit and next complete repair (read-only, 2026-10-07)

This section is a fresh caller/lifecycle audit while root builds the frozen VECTOR
cohort. No function source, generated source, Maven, Native build or test was
changed/run for this audit. The existing
`docs/internal/ai-native-function-cohort-plan.md` remains NOT IMPLEMENTED.
The minimal complete fix is one query-owned view of the actual publication,
its consumers and final task coverage; creating another function registry,
placeholder inventory, SPI or ordinary-function allowlist is unnecessary.

### What is already real, and what still bypasses it

| Boundary | Current evidence and consequence |
|---|---|
| Native owned registration | `capability/RegisteredFunctionCatalog.cpp:38–64,68–86,116–159` projects actual scalar entries in runtime traversal order and aggregate/window signatures in first-match order. Scalar entry hashes and whole registration hashes already exist. Ordered entries have no individual entry hash/ordinal representation; identical signatures at different ordinals remain distinct actual choices. |
| Publication | `FunctionCatalogAssembler.cpp:147–165` seals actual registration, generated binding declaration, physical projection and execution-selection identity into the immutable publication. `NativeCommandDispatcher.cpp:515–543` finalizes/reads that publication; `JniBridge.cpp:77–82` installs its owned registry into the executor. |
| Worker advertisement | `NativeCapabilityLoader.java:21–78` performs explicit finalize then validates lossless inventory. `WorkerInfo.java:36,53–55` retains it; `WorkerManager.java:383–429` persists/restores it. This path already supplies the input needed by the query compiler. |
| Validated Java catalogue | `FunctionCatalogView.java:38–58,99–143` binds actual scalar entry priority, exact constant obligations, ordered aggregate/window signatures and actual output/intermediate types. `FunctionCatalogContracts.java:35–50,138–161` verifies local declaration/selector/descriptor identities and complete availability. Merely injecting names would discard these implemented decisions. |
| Query entry | `PlannerWiring.java:50–78` constructs a generic compiler/session-planner factory. `CompilationPhase.java:77–95` captures only compiler/session metadata. Its actual wiring at `QueryPipelineExecutor.java:456–460` returns the global compiler. Direct reusable/export compilation at `QueryPipelineExecutor.java:165–191` and direct DML at `215–233` independently call forSession, and must capture the same actual authority before semantic compilation. |
| SQL type authority | `SqlCompiler.java:40–49` still installs NativeCapabilityRegistry.defaults. `PangrovaPlanner.java:47` eagerly creates a static operator table. `PangrovaSqlOperatorTable.java:40–42,65–66,77–85` falls back to Calcite standard inference and hand-built custom operators; RoundSqlFunction and singleton PlannerTypeSystem/DecimalArithmeticTypeDerivation retain separate Native result formula decisions. |
| Scheduling | `DefaultWorkerSelector.java:56,104,115,162,197,235` reads current workers for each operation. `SpeculativeTaskLauncher.java:287–310` independently selects current ACTIVE workers and clones only task attempt identity. A cloned future field18 would survive, but its target is still not restricted to the original actual manifest/cohort. |
| Task production/admission | `TaskRequestFactory.java:238–269` is the one canonical production writer. `CanonicalTaskRequests.java:147–170` validates checksum/security/options/resources but no field18. Native TaskRequestParser has no function_requirements consumption. Existing `capability.proto:494–523` and `task.proto:98` already define the complete envelope. |
| Native anchor authority | `FunctionPlanBindings.cpp:15–37,62–79` resolves plan URN/name against generated startup declarations, not the sealed published manifest; it strips the signature suffix before mapping a name. `VeloxPlanConverter.cpp:106–116` installs those declarations. Actual registry binding still checks types later, but neither the planner-selected entry nor the query manifest is enforced. |
| Window bypass | `SubstraitTypeConversion.cpp:155–172` has a generic expression-window path directly constructing CallTypedExpr. The regular WindowRel path at `SubstraitWindowConversion.cpp:106–143` checks kind/phase/invocation/options and calls bindRegisteredWindow. Both paths must consume one admitted call binding; the unchecked path must not remain an alternate execution lane. |

Paths without a module prefix in the table belong to pangrova-worker-native/src,
except Java paths, which belong to the named owning module's src/main/java.
This table records source state, not execution acceptance.

### Non-obvious final-IR coverage decisions required before field18 admission

1. **Actual call versus Native structural expansion.** Native creates registered
   calls without function anchors: Nested.Struct/List at
   SubstraitNestedExpressionConversion.cpp:24,38; IfThen at
   SubstraitTypeConversion.cpp:149; field references at :77,88; masked
   projection at SubstraitMaskConversion.cpp:94–194; join comparison at
   SubstraitJoinCommon.cpp:136–189; window RANGE bounds at
   SubstraitWindowConversion.cpp:56–69. A collector searching only
   ScalarFunction/WindowFunction messages is incomplete. row_constructor is
   already in the sole sql-bindings.json:132; array_constructor and if do not
   have equivalent declarations there. Add required structural/intrinsic
   exposure to that sole declaration/generator, rather than a private list.
   Specify that descriptor field-number/repeated-index paths identify actual
   serialized owning nodes/clauses. Multiple internal lowering calls cannot
   all impersonate one unique explicit call path. Define each structural
   node's required registrations through the same declaration and verify its
   deterministic expansion with the frozen manifest, or explicitly encode
   unique real owning subpaths. Do not invent unversioned sentinel path numbers
   or silently exempt all non-anchored execution.
2. **Governed AI effective arguments.** Native
   SubstraitFunctionConversion.cpp:165–168 appends governed fields before
   selecting the actual scalar entry. SQL-visible arguments plus invocation ID
   are not the actual registry signature. The collector must bind effective
   types/constants from the same sole governed layout and admitted
   OperatorBinding, and Native must verify the same effective call. Use
   existing GovernedBindingArguments.cpp:73–95 and generated layouts;
   preserve independent model/output/policy/resource/user authorization.
   Hidden values derived from OperatorBinding are validated through that
   binding; they are not literal payloads covered solely by the IR checksum.
3. **Full argument names, not just types.** Existing public
   SubstraitRelationLayout.typeOf(:146–148), describeTree and projectSchema
   supply canonical context. typeOf alone returns Type, which cannot carry
   nested ROW field names. Building argument_schema with guessed field names
   can falsely accept/reject actual named-row signatures. Preserve projected
   source DFS names for references and canonical computed names for constructors
   through the existing Substrait schema/layout utilities. Never treat
   output_type as evidence that a signature was correctly selected.
4. **Aggregate provenance exists locally, not across stages.**
   SubstraitAggregationDistribution.cpp:50–55,74–84 already derives partial
   state with the owned registry and copies original rawInputTypes into local
   final aggregation. The general SubstraitAggregateConversion.cpp:69–76
   rejects state-input phases because their raw signature authority is absent.
   Do not remove that rejection. Final-IR requirements must retain validated
   original_raw_argument_schema and the actual ordered entry; distributed
   partial/shuffle/final planning must carry that provenance through existing
   stages. Native local parallel rewrites must use the same admitted entry and
   intermediate/final types, not independently reselect another overload.
5. **Ordered selected identity is missing.** Scalar requirements can carry the
   selected FunctionImplementationContract.contract_sha256 already returned
   by FunctionImplementationSelector. For aggregate/window, the name-level
   registration digest is insufficient. Define one generated canonical hash
   operation covering registration contract identity, unsigned signature
   ordinal and exact signature contract bytes, with Java/C++ golden vectors
   including duplicate signatures at different ordinals. Reuse existing
   deterministic protobuf FunctionContractHashes/FunctionContractEncoding.
   Do not fabricate a scalar implementation-family record for an aggregate,
   hash the desired return type as a selector, or hand-write two divergent
   encoding formulas. DISTINCT aggregate-window selects the actual companion
   registration declared at RegisteredFunctionCatalog.cpp:99–108; its selected
   entry, process mode and options must be checked too.

### Smallest complete implementation cut using existing owners

* Capture one immutable CapabilityInventory/FunctionCatalogView from
  authenticated eligible WorkerInfo before CompilationPhase's first validator
  and at direct DML/reusable compilation entry. Retain one actual homogeneous
  manifest identity plus the captured eligible worker IDs. Do not merge partial
  inventories. Choose the initial cohort explicitly; loss of compatible workers
  is an error rather than a reason to swap catalogue authority mid-query.
  WorkerInfo currently has no process-incarnation field and registerWorker
  overwrites by workerId (WorkerManager.java:109–113); manifest equality is
  content compatibility, not authenticated registration or attempt fencing.
  Preserve existing security and fencing independently.
* Add explicit actual-view injection to SqlCompiler/its session-planner factory,
  PangrovaPlanner, operator inference and PlannerTypeFactory/type-system context.
  The latter currently uses a process singleton, so installing a mutable global
  catalogue would cross-contaminate concurrent queries. Ordinary scalar/ordered
  return and constant inference must call FunctionCatalogView after explicit
  Planner coercion policy. Replace ROUND/decimal result formulas as consumers
  migrate. Existing generic compiler constructors may support parsing only;
  production semantic compilation cannot fall back to static defaults or a
  fixture manifest.
* Carry the captured concrete authority through existing query/DAG contexts and
  query-scoped WorkerSelector, AQE, retries and speculation. SqlCompiler can
  retain its immutable actual view; Coordinator owns worker membership and
  mutable resource signals. Do not introduce a cross-module worker SPI just to
  pass the manifest. CacheKey currently contains SQL/tenant/principal/session/
  policy only; add function manifest identity to cache identity and retained
  entries so a different execution contract cannot reuse old value results.
* Place the canonical final-IR collector/validator in pangrova-protocol beside
  FunctionCatalogView and SubstraitRelationLayout. It consumes final plan bytes,
  the captured actual view, admitted effective bindings and original aggregate
  provenance. Emit complete FunctionRequirements even for zero calls. Include
  all measure arguments/filter/sort children, window partitions/sorts/bounds,
  nested constructors, casts/conditionals and existing relation extensions.
  Specify canonical path ordering and exact coverage; unknown descriptor kinds
  fail explicitly. Implement Native mirror admission against the same published
  manifest and owned registry, with cross-language golden coverage fixtures.
* TaskRequestFactory.canonicalRequest is the final writer after task-specific
  rewrites and OperatorBinding attempt preparation. BoundExecutionPlan/LeafTask/
  DmlWriteTask/CanonicalTaskSpec carry the retained authority/provenance rather
  than re-read WorkerManager. Collect after overlay/retrieval/generated-column/
  AQE/GenerationPartitionBindings changes and bind field18 checksum to exactly the
  bytes written in field2. Recollect after plan changes; retries that change only
  attempt fields retain the same checksum/call authority. Admission requires
  the explicit envelope, including the zero-call case, in Java and Native.
* Replace Native generated-only anchor resolution with the publication's exact
  definition/reference bindings, then verify requirements before constructing
  execution pools/operators. JniBridge.cpp currently reserves a creation handle
  before task config; it can retain this bounded identity reservation, but
  manifest/call validation must finish before constructTaskExecutor(:82).
  Pass admitted call bindings into VeloxPlanConverter and both scalar/window/
  aggregate lanes. VeloxTaskRunner.cpp:328–334 already requires the owned
  registry snapshot in QueryConfig; preserve it and avoid global registry
  fallback. Validation must ensure actual SDK factory selection equals the
  admitted selected entry, not merely equal resolved result types.
* Durable GenerationStageGraph/GenerationStageTemplate format2 currently stores
  bindings/result types/publication/model authority, not function catalogue
  authority or raw aggregate provenance. Use their existing canonical graph
  codec/store with a single explicit format bump to retain original catalogue
  bytes/checksum plus provenance/requirements. Do not hide them in session
  property strings or add a parallel artifact format. On restart, validate the
  retained view against current binary declaration/descriptor, require compatible
  actual workers, and recompute final partition requirements against that
  original view after physical rewrites. Catalogue digest cannot replace
  current durable claim/model/policy/fencing checks. Old jobs must be explicitly
  rebuilt under the canonical migration rule, never silently rebound to today's
  inventory.

The durable format and intrinsic coverage grammar are real required contract
work, despite field18 already existing; this is not safely completed by adding
only a writer and a parser rejection. Source all producers/consumers together,
then build one matched protocol-generated/SDK/JNI/Java cohort before enabling
required admission. No missing-envelope compatibility mode is necessary.

### Official support and acceptance oracles

The [Substrait extension specification](https://substrait.io/extensions/) defines
URN-plus-signature anchor identity; a function name alone is weaker. Audit the
pinned generated descriptor/version before changing signature spelling merely
to follow latest documentation. The
[Substrait aggregate binding specification](https://substrait.io/expressions/aggregate_functions/)
distinguishes phase, invocation, ordering and decomposable intermediate output.
The [Velox aggregate implementation guide](https://facebookincubator.github.io/velox/develop/aggregate-functions.html)
explains why raw input, state and final output jointly define a signature. The
[Velox expression evaluation guide](https://facebookincubator.github.io/velox/develop/expression-evaluation.html)
provides execution context; Pangrova's pinned SDK owned-snapshot additions,
inspected locally, remain the actual API authority rather than upstream HEAD.

Required positive/negative tests: real registered DECIMAL ROUND/arithmetic;
nested ROW/LIST runtime values and named-row constraints; governed AI effective
arguments/model-purpose authorization; explicit zero-call envelope; exact and
missing/extra/duplicate/tampered paths; changed literal checksum, constant mask,
options, invocation/process/phase, declared output and selected entry; distinct
ordered duplicates and window companion; compatible/incompatible catalogue
workers before compile, retry and speculative copy; final rewrite/AQE checksum;
raw DECIMAL partial/shuffle/final with actual intermediate values; frozen graph
restart with matching/incompatible workers and current claim fencing. Test Java
and Native agreement against the actual published snapshot, not fixture success
invented from output schemas. Existing FunctionCatalogView/RegisteredFunctionBinding
unit tests demonstrate implemented components, not this absent full consumer chain.


## 2026-10-07 fixed Calcite 1.42 frontend: verified atomic migration design

Status: read-only implementation audit; **not implemented and not tested in this
pass**. No source or build changes were made while the root agent verified the
frozen storage-VECTOR unit. This section refines
`docs/internal/ai-native-single-calcite-frontend-implementation.md` against current
callers and the exact installed Calcite 1.42.0 artifact. Parser replacement is
necessary to restore real public SQL acceptance; a parser-only negative test
cannot establish AI purpose validation or data execution.

### Actual parser authority and object lifetime

| Current production site | Verified behavior and necessary replacement |
|---|---|
| `planner/parse/PangrovaParserFactory.java:26–29,42–70` | Owns ANTLR `SqlParser` and `AstToSqlNodeConverter`; `getParser(Reader)` always throws. Main entry parses old `Statement` then directly constructs `SqlNode`; there is no functional Calcite parser behind this factory. Replace with the generated implementation factory and a shared immutable config. |
| `planner/PangrovaPlanner.java:211–214,241–242,271–274,398–405` | String planning and safe planning parse again; pre-parsed DML still converts old AST into another tree. All planning must accept the same planner-owned parsed object, with a separate fresh working tree for each validation/optimization attempt. |
| `parser/SqlParser.java:147–175,374–427` | Global Caffeine cache returns the same old AST under whitespace-normalized SQL keys. The normalizer does not track backticks or comments. Different backtick identifiers can collide, and identical semantic text at different line positions can reuse stale source locations. Do not transfer this cache to mutable Calcite `SqlNode`s. Query-scoped exact-source parse ownership removes the need for that cache. |
| `coordinator/.../phase/QueryExecutionContext.java:17–99` | Context stores original/rewritten SQL strings and an opaque compiled plan; it stores no authoritative parsed object. Add one planner-owned opaque parsed handle and preserve it through every `withXxx` method. |
| `coordinator/.../phase/StatementDispatchPhase.java:23–44` | Parses once to classify/rewrite SHOW, catches syntax failures, then sends SQL to dispatcher for another parse. Make this the mandatory parse owner even with metadata dispatch disabled; syntax error must terminate with its original error code/location. |
| `coordinator/.../dispatch/StatementDispatcher.java:52–63,195–201` | Declares a compiler snapshot, but `parseAstOrNull` accesses the volatile field again and swallows every exception as `parser_fallback`. Pass the parsed handle and captured compiler directly; dispatch only the projected neutral command. Unknown/unsupported commands need explicit diagnostics, not parse failure as read-query classification. |
| `coordinator/.../phase/CompilationPhase.java:77–95` | Calls independent time-travel parsing, strips clauses into another SQL string, then parses that string in `compileForPolicyApplication`. Resolve selectors and strip their extension nodes on a working copy of the original parsed tree; retain original spans for errors/audit. |
| `parser/TimeTravelSqlClauses.java:30–81,111–143` and `coordinator/.../TimeTravelExtractor.java:32–99` | Strictly builds a second ANTLR tree even without time-travel syntax; separately traverses CTE visibility and converts code-point offsets to UTF-16. Replace production use with source occurrences derived from the authoritative Calcite tree, retaining the current positive snapshot-ID, CTE-shadowing, ref-resolution and conflicting-table-selector rejection semantics. |
| `coordinator/.../ViewExpander.java:45–97` | Up to ten passes parse each rewritten string, catch failures, and globally replace fully-qualified table text. Move expansion to planner traversal over actual source nodes. Parse each stored definition through the same grammar in a query-local definition cache, resolve its lexical catalog/schema and security context, then substitute a deep copy of the query node. String replacement can affect literals/comments and CTE shadowing. |
| `planner/rewrite/DynamicTableSqlRewriter.java:53–115` and `planner/DynamicTablePlanningSupport.java:38–78` | Repeatedly parses for rewriting/source collection/CTE analysis; catches parser failures and returns unchanged SQL. Replace with one scope-aware node visitor retaining merge-on-read metadata and depth/cycle policy. `CteReferenceCounter` must operate on the authoritative WITH nodes, not a second reconstructed query AST. |
| `coordinator/.../QueryPipelineExecutor.java:140,169–180,217–227,240–276` | Advisory table extraction, reusable-read classification and write observation reparse. Pre-parsed MERGE is printed and recompiled; other DML is printed for enrichment/retry. Use parsed kind/source/write/rerank projections and `compile(parsed)`. SQL text is audit input, not a second compile authority. The safe-optimization retry in `SqlCompiler.java:240–249` must consume a fresh deep copy of the same parsed tree. |
| `coordinator/.../DispatchContextEnricher.java:70–106` | Rerank reparses `originalSql` and requires old `Query`. Add neutral rerank intent derived from the owned extension node; keep actual operator/model/index binding in existing governed planner/coordinator code. |
| `coordinator/.../dispatch/task/CtasTask.java:51–78,106–126` | Prints and parses the inner query to infer types, then synthesizes an old `InsertSelectStatement` that accepts only Query/SetQuery. Derive source schema and insert from the already parsed inner `SqlNode`; VALUES/WITH/set queries must not become unsupported because of a DTO type gate. |
| `coordinator/.../dispatch/PlanExecutionFactory.java:176` | EXPLAIN prints and recompiles the inner AST. Compile its owned inner node and preserve source positions. |
| `planner/security/expression/FilterExpressionParser.java:98–99` and `MaskExpressionParser.java:87–88` | Policy expressions currently use a second default Calcite config. Use the same generated factory/config and `parseExpression`; construct the virtual-table SELECT from nodes, retaining expression-local coordinates. Do not reject or reinterpret policy syntax based on a separate dialect. |
| `coordinator/.../dispatch/task/CreateViewTask.java:58` and `coordinator/dynamictable/DynamicTableDdlExecutor.java:101–102,216,286` | Views persist source SQL; dynamic views print old query AST and extract sources separately. Store exact inner-source span or a deliberate canonical unparse from the owned query, and derive dependencies by its scoped source visitor. Persisted SQL is parsed with the sole grammar when loaded in a new process. |

`AstToSqlNodeConverter.java:61` and `ExpressionToSqlNodeConverter.java:63` use
`SqlParserPos.ZERO`; `AstToSqlNodeConverter.java:85–90` drops WITH item column
aliases and recursive/search/cycle information. The migration must eliminate
these query/expression conversions rather than leave them as the path for an
already-parsed command. Parse a source once; projection into an immutable neutral
metadata command is not a second parser, provided that command is never used to
reconstruct its relational/expression tree.

Calcite nodes are mutable during validation/rewrite. In the installed 1.42.0
sources, `SqlCall.clone(SqlParserPos)` reuses the operand list; the default
`SqlShuttle` returns unchanged objects when no operand changed. Neither is an
adequate deep-copy operation for concurrent/retry planning. The parsed handle
must keep its original tree private, and a planner-internal explicit deep copier
must copy every call/list/leaf plus custom extension payload and retain positions.
Validate a fresh copy, never expose mutable nodes to coordinator and never reuse
a validated tree for a different tenant/schema/capability snapshot.

### Pinned source and build integration that actually exists

The installed **binary** `calcite-core-1.42.0.jar` already packages
`codegen/templates/Parser.jj`, `codegen/default_config.fmpp` and codegen includes;
the sources jar does not. Read-only byte comparisons against official
`calcite-1.42.0` tag were identical:

| Source inside binary artifact | SHA-256 |
|---|---|
| `codegen/templates/Parser.jj` | `09d58e8397620eabf9420990a7ab6c3b54e8a292ac0d5f128ca2c8fd7c4f9f51` |
| `codegen/default_config.fmpp` | `f953ed4e4cd09dd4ac1a8ec8649b0dd22a34ca2dd9feffef01a2a9b5e685a133` |
| `codegen/includes/compoundIdentifier.ftl` | `97f90c4daed6749371d4538e09657550f05036be24a2e1018e5c30f363004aad` |

Calcite's supported template extension mechanism exposes statement, create,
drop, alter, literal, type and builtin-call productions through FMPP
configuration. The exact pinned server example uses the same core template and
custom includes, then JavaCC; its column grammar parses generated/default
expressions with the core expression production. Reuse that pattern, without
adding calcite-server runtime, JDBC DDL execution, or a second SQL parser.
Sources: [1.42 core config](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/codegen/config.fmpp),
[1.42 server config](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/server/src/main/codegen/config.fmpp),
[1.42 server grammar include](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/server/src/main/codegen/includes/parserImpls.ftl),
[1.42 server generation order](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/server/build.gradle.kts).

The pinned build uses FMPP **0.9.16**, FreeMarker **2.3.29**, JavaCC **4.0**;
FMPP merges local config plus the core default config and runs `fmpp.tools.AntTask`.
JavaCC receives `STATIC=false`, `LOOKAHEAD=1` and a package-specific output
directory. Sources: [FMPP dependencies](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/buildSrc/subprojects/fmpp/src/main/kotlin/org/apache/calcite/buildtools/fmpp/FmppPlugin.kt),
[FMPP task](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/buildSrc/subprojects/fmpp/src/main/kotlin/org/apache/calcite/buildtools/fmpp/FmppTask.kt),
[JavaCC dependencies](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/buildSrc/subprojects/javacc/src/main/kotlin/org/apache/calcite/buildtools/javacc/JavaCCPlugin.kt),
[JavaCC task](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/buildSrc/subprojects/javacc/src/main/kotlin/org/apache/calcite/buildtools/javacc/JavaCCTask.kt).

Minimal Maven integration belongs in `pangrova-planner/pom.xml` because Calcite
is allowed only in planner (`code-standards.md:11`); parser keeps neutral command
DTOs and source diagnostics. Root pom already pins `${calcite.version}=1.42.0`
(`pom.xml:51`). Add tool/plugin versions as parent properties, with build-only
FMPP/FreeMarker/JavaCC dependencies:

1. During initialize/generate-sources, unpack only `codegen/**` from the same
   `${calcite.version}` binary artifact into `target/calcite-codegen`. Do not
   fetch HEAD or runtime templates from the network.
2. Verify the three pinned source hashes before applying a small checked-in
   **source-template** patch. Failure to match must stop generation; do not let
   an upgraded core silently reuse an old template/config.
3. Run FMPP with planner-owned `src/main/codegen/config.fmpp`, includes and
   patched template, providing its exact core default config. Output only to
   `target/generated-sources/fmpp/Parser.jj`.
4. Run pinned JavaCC on that generated grammar with the pinned options; output
   package `org.pangrova.planner.parse.impl` under
   `target/generated-sources/javacc`, add that source root for compilation.
5. Generated Java is never checked in or hand-edited. Source checkstyle must
   exclude only this generated directory, retaining gates on config/helpers and
   source-template patch. Clean generation must succeed without existing target
   classes. Remove ANTLR generation/runtime from parser in the same atomic runtime
   switch, after moving parser grammar tests to planner.

A standard Maven antrun execution can reproduce the official Ant FMPP task plus
JavaCC entrypoint and register the generated source root; exact Maven plugin
versions require implementation-time dependency verification. This audit did
not execute or invent a tested plugin configuration. The pinned JavaCC 4.0
choice is evidence from Calcite, not a suggestion to pick a current 7.x generator
without compatibility tests.

### Which extensions require source-template changes

The unmodified 1.42 grammar has **no general query-tail or table-reference
extension hook**. Read the exact installed Parser.jj productions before claiming
that config/includes alone are sufficient:

- `SqlStmt` (template lines 1163–1220) admits custom statement methods before
  default commands. `TypeName` (around 6140) admits custom `SqlTypeNameSpec`
  methods before builtins. Create/drop/alter and custom literals/calls can use
  existing hooks without a duplicated SELECT grammar.
- `OrderByLimitOpt` (around 704–756) directly parses ORDER BY then LIMIT/OFFSET/
  FETCH. Current Pangrova puts `RERANK BY` between ORDER BY and LIMIT
  (`PangrovaSql.g4:101–113,266–279`). Add one optional query-tail extension
  production in that source template, consuming existing order and fetch nodes
  once and producing an owned rerank query node with strategy/source positions.
  Do not add a second SELECT production or strip RERANK using regex.
- `TableRef3` (around 2375–2445) admits only stock `Snapshot` after a named
  source. Add one optional source-selector extension production there, retaining
  snapshot/branch/tag and incremental selectors as source-owned nodes. Preserve
  sampling/alias ordering and CTE lexical visibility. Current selectors include
  SYSTEM_TIME, SYSTEM_VERSION, VERSION AS OF, BRANCH and TAG
  (`PangrovaSql.g4:248–254`); numeric semantics are still determined by the
  coordinator's existing snapshot contract, not default timestamp snapshot
  validation.
- Calcite lexer has distinct DQID and BTID states (around 9436/9449); default
  Pangrova accepts **both** quoting styles, case preserving
  (`SqlDialect.java:41–44`). Add both quoted token productions to the selected
  Pangrova state by a reviewed source patch; retain the existing parser's
  identifier alternative handling. Do not normalize quotes in the SQL string,
  which loses spans and can change literals. Keep case-insensitive name binding
  and quoted spelling. If legacy multiline/empty quoted names are accepted,
  preserve their declared policy or explicitly document a separately authorized
  syntax change; stock tokens forbid newline and require a character.
- Stock `SqlMerge` (around 1886–1930) only handles one UPDATE and optional INSERT;
  its WHEN clauses omit existing conditional/multiple/DELETE grammar
  (`PangrovaSql.g4:983–1002`). A custom statement method selected before stock
  MERGE can preserve the full clause list using the same core TableRef/expression
  productions. Keep the extension node until the existing native mutation
  compiler checks supported atomic actions; do not erase predicates/clauses into
  the smaller stock `SqlMerge`. Current converter already drops a derived MERGE
  source and overwrites repeated clauses
  (`ExtensionStatementSqlNodeConverter.java:159–197`); migration should expose
  this as an execution semantic issue, not encode that loss as a compatibility
  requirement. Existing DELETE+other-action atomic rejection remains.
- Recursive WITH column aliases are core syntax, but stock support for the
  current SEARCH/CYCLE clauses must be checked and extended in the same template
  production where absent. Preserve all parsed fields; a standalone Java row
  loop (`RecursiveCteExecutionPlan`) does not prove a legal Native recursive
  execution path. Unsupported execution must have a typed planning diagnostic.

The source patch is a small extension of the one pinned Calcite grammar, not a
hand-edit of generated Parser.jj/Java or a runtime alternate grammar. A build-time
patch and hash check gives a reviewable upstream delta while all standard query
productions remain Calcite's. A full vendored rewrite would unnecessarily enlarge
the maintenance surface.

### Complete syntax inventory and canonical type correction

Migration must preserve every accepted command family in
`PangrovaSql.g4:8–86`: ordinary SELECT/set/WITH/EXPLAIN, CHANGES BETWEEN SNAPSHOT,
COMPACT/OPTIMIZE/VACUUM/CALL; catalog/schema/table/view/semantic-view/feature-view/
dynamic-table/stream creation, alteration, show, describe, publish, materialize and
drop; index creation/alteration/drop/show; clustering/recluster; branches/tags;
embedding-model create/alter/drop/show; backfill/cancel/status; storage profiles;
COPY INTO; SET; INSERT VALUES/SELECT and OVERWRITE static partitions; DELETE/
UPDATE/MERGE. A migration that adds only CREATE EMBEDDING MODEL and VECTOR would
break existing routing.

Also preserve expressions and relation forms from grammar lines 127–360:
set quantifiers/precedence, WITH aliases/recursion, named/window/range/filter/
QUALIFY, lateral EXPLODE/POSEXPLODE/GENERATE_SERIES, TABLE calls, sampling/
REPEATABLE, pivot/unpivot, arrays/maps/STRUCT or ROW named fields, parameters,
interval/temporal/hex/decimal literals and typed nested casts. General identifier
function calls parse through the core expression production; AI_EMBED overloads,
purpose, model identity and FLOAT32 policy stay in the existing governed operator
validation/lowering, not another grammar-maintained function list. Keep the
existing AI functions and the independent inference-service boundary.

**Additional root cause independent of Flight:** existing DDL already loses
VECTOR precision. `PangrovaSql.g4:884` accepts `VECTOR(n[,FLOAT|DOUBLE])`, but
`AstDdlDmlBuilder.java:369–380` ignores `vectorElementType` and constructs
`DataTypeSpec.VectorType(n,COSINE)`. The record in
`parser/ast/type/DataTypeSpec.java:283–295` stores a distance metric instead of
coordinate precision and publishes `VECTOR(n, COSINE)`. `DdlTypeMapper.java:104–105`
then builds `new spi.VectorType(n,null)`, default FLOAT32. `VectorTypeSyntaxTest`
currently asserts DOUBLE has the same COSINE DTO as FLOAT; it is an oracle for
precision loss. CTAS `DataTypeSpec.fromRawTypeString` tries to parse DOUBLE as a
`DistanceMetric` (`DataTypeSpec.java:115–128`), so the now-correct final output type
`VECTOR(n,DOUBLE)` cannot be used to create its result table. This is not fixed by
the frozen catalog-to-Flight unit.

Correct the neutral DDL vector declaration to carry dimension + coordinate
precision. Distance metric remains a retrieval/index configuration. Build its
canonical storage `LogicalType` with the existing `VectorLogicalType` helper;
map this same declaration to existing SPI VectorType, including FLOAT32/FLOAT64.
`PangrovaSqlVectorTypeNameSpec.deriveType` must produce
`StorageVectorRelDataType` via that helper; CAST that truly changes to an ordinary
ARRAY must remain ordinary ARRAY. Nested ARRAY/MAP/STRUCT declarations and
precision/scale go through the same existing recursive type bridge, not another
string mapping or defaults for missing fields. Preserve old `ARRAY(type)`,
`MAP(key,value)` and `STRUCT(field type,...)` syntax through data-type extension
methods while admitting Calcite's standard equivalents deliberately.

`GeneratedColumnDefinitionCompiler.java:140–145` currently stores only the old
canonicalSql string in `output_type`; `GeneratedColumnMutationExpressions.java`
re-parses that string using the same metric DTO. Migrate both to the canonical
LogicalType declaration. AI generated output remains strict FLOAT32 with resolved
model dimension; accepting storage FLOAT64 must not admit FLOAT64 AI targets.
Keep ASYNC nullability policy and current SYNC atomic-materialization diagnostic.
Do not activate SYNC by changing grammar acceptance alone.

### Minimum atomic file/module inventory and API shape

This is a frontend-wide atomic change; a small factory patch cannot close its
runtime callers. No new protocol, native parser, codec or SPI is needed.

| Area | Required source inventory |
|---|---|
| Build | root `pom.xml` tool properties; planner `pom.xml`; planner `src/main/codegen/config.fmpp`, extension includes, checked source-template patch and provenance/hash check; parser `pom.xml` remove ANTLR build/runtime. |
| Planner parse owner | Replace `parse/PangrovaParserFactory`; add opaque `ParsedSqlStatement`, planner-private extension command/type nodes, deep copier, shared parser config and neutral command projection; remove `AstToSqlNodeConverter`, `ExpressionToSqlNodeConverter`, `ExtensionStatementSqlNodeConverter` from query runtime after all usages migrate. |
| Planner compile consumers | `SqlCompiler`, `PangrovaPlanner`, `DynamicTablePlanningSupport`, `rewrite/DynamicTableSqlRewriter`, `branch/BranchResolutionRule`, `rerank/MatchFunctionRewriter`, `rerank/RerankerInjectionRule`, `rule/CteReferenceCounter`, stored materialized-view definition/rewrite hooks, generated-column declaration readers, filter/mask expression parsers. Preserve real RelNode security/lineage/Native lowering. |
| Neutral parser module | Existing statement/identifier/source metadata DTOs stay free of Calcite; query/expression-bearing DDL/DML DTO fields must become opaque source/query handles or metadata-only projections that cannot be compiled back into another AST. Correct DataTypeSpec VECTOR semantics and retire its SQL-parser-like canonical metric path. Remove ANTLR grammar, builders/listeners, recovery parser, parse cache and StatementParser registry/runtime extension entrypoints when the last runtime caller is moved. No ServiceLoader parser fallback. |
| Coordinator query lifecycle | `QueryExecutionContext`, `StatementDispatchPhase`, `CompilationPhase`, `StatementDispatcher`, `StatementExecutionContext`, `QueryPipelineExecutor`, `QueryDispatcher`, `DispatchQueryContext`, `DispatchContextEnricher`, `TimeTravelExtractor`, `ViewExpander`, `WritePolicyObservation`, `TableNameExtractor`, `NativeDmlCompileFunction`, `PlanExecutionFactory` and CTAS/EXPLAIN handlers. Registry/security/leader guards continue to use projected command metadata. |
| Metadata command consumers | `DdlExecutor`, `DdlTypeMapper`, `GeneratedColumnDefinitionCompiler`; view/dynamic/semantic/feature/embedding/branch/index/clustering/stream handlers and recovery/registration that hold or print query AST. Simple existing DDL task records and factory registry can remain if the Calcite projection fills them directly; relational bodies never re-enter those DTOs as parser authority. |
| Tests/benchmark/docs | Move grammar/runtime parser cases into planner tests using the actual generated factory. Keep neutral DTO unit/property tests in parser. Update direct-dispatch fixtures to owned parsed handles, the SQL parsing benchmark's dependency/entrypoint, existing single-Calcite implementation doc and this acceptance ledger. |

Recommended interface, matching existing Calcite-free `PlannedQuery` boundary:
`SqlCompiler.parseStatement(String)` returns a final opaque planner-owned
`ParsedSqlStatement`; public methods expose original source, neutral kind/command,
source references, source spans and read/write/rerank intent. Planner-internal
methods supply a fresh deep copied tree. `compileForPolicyApplication(parsed)`,
`compileToPlannedQuery(parsed)`, `compileInnerQuery(parsed, handle)` and
`compileDml(parsed, writeMode)` consume it. Coordinator cannot access `SqlNode`.
Source-selector resolution returns a new parsed handle/work-copy with bound
selectors, not a stripped SQL string that must be reparsed.

Do not add a placeholder interface that stores an arbitrary Object/DTO and still
calls old converters later. Do not preserve `parseToAst` as a compatibility
runtime path. During implementation, a private projection from the same Calcite
node may fill existing metadata-only command records to avoid rewriting every
DDL task; source-expression/query identity stays in the parsed handle. Synthetic
SHOW query or CTAS INSERT is an explicit node transformation in planner, with
source provenance; it is not a textual round-trip to a second grammar.

### Acceptance: original SQL, real grammar, values and error positions

1. Fresh clean `generate-sources` must produce the same parser from the pinned
   artifact/template patch; verify factory `getParser` creates a real
   `SqlAbstractParserImpl`. Runtime/package checks must find no ANTLR classes or
   old AST query conversion. No generated source edits.
2. Run every existing accepted grammar family through the public sole factory
   and project its command. Use the actual old SQL fixtures, not renamed or
   simplified syntax. Standard read cases must include top-level and derived
   `VALUES ('a'),('b')`, `VALUES ROW(...)`, derived column aliases, WITH/CTAS/EXPLAIN
   bodies containing VALUES, mixed quoted identifiers and TABLE/UNNEST/lateral
   expressions. Grammar acceptance is separate from actual Native capability
   acceptance, which must retain its precise planning code.
3. Preserve `AiEmbeddingPurposeSqlProtocolTest`'s actual VALUES input. First
   prove the positive RAW/QUERY/DOCUMENT invocation lowers with exact literal
   purpose and invocation identity; negative AUTO/lowercase/NULL/numeric/
   row-dependent/lossy-cast purpose must then fail as
   `PLANNER_VALIDATION_FAILURE` with `AI_EMBED purpose`, not any parse exception.
   Keep real lossless-cast behavior. Full execution uses the existing independent
   inference service, not a fake Java evaluator.
4. Exercise parser-generated source positions: multiline SQL/comments, quoted
   identifiers, doubled quotes/backticks, tabs/CRLF, Unicode surrogate pairs before
   an error, EOF, trailing garbage and multiple statements at a single-statement
   endpoint. Convert Calcite's one-based column to the public ParseException
   zero-based column exactly once; SourceLocation currently documents one-based
   columns while callers pass zero-based, so resolve that neutral contract and
   assert it. Error token start/end must refer to original source; no wrapper/
   stripped-SQL drift. Invalid custom VECTOR size/type and malformed generated
   clause/model properties need exact clause/token positions too.
5. Real CREATE/ALTER/CTAS tests must inspect catalog SPI VectorType precision and
   dimension, compile a SELECT from the created table, inspect final field16, and
   decode Flight `double[]` values `1.0000000000000002` and
   `-123456789.12345679`. Do not consider matching a neutral COSINE DTO a pass.
   Include typed nested ARRAY/MAP/STRUCT, decimal precision/scale and nullability.
   Generated AI target FLOAT64 must fail at model/type validation with a positive
   FLOAT32 target oracle, rather than fail because grammar cannot parse it.
6. Source rewrite tests must prove exact referenced occurrence only, including
   repeated joins, CTE shadowing, literals/comments containing a view/DT name,
   nested views with schema-relative identifiers and cycles. Keep positive and
   conflicting time-travel selector tests, branch/tag resolution, system-table
   suffixes and per-relation snapshot rejection policy.
7. Parse-owner lifecycle tests must pass the same opaque handle through dispatch,
   source binding, policy application, CTAS/EXPLAIN/DML, rerank and safe retry;
   assert actual parse count via the production factory under a test observer.
   Different whitespace/backtick identifiers must not share source positions or
   identifier content. Concurrent validation/retry for separate sessions must
   prove no shared mutable Calcite tree. A spy that merely stubs compile cannot
   establish this invariant.
8. Conditional/delete/multiple-clause/derived-source MERGE must retain full parsed
   structure; supported cases need real native write+catalog commit value oracles.
   Explicitly unsupported action combinations keep their typed atomic-write
   rejection. Do not pass by silently keeping only the last clause or dropping
   its condition. Parser migration does not authorize a coordinator row executor.
9. Generated depth/input limits must remain enforced; existing 1 MB implementation
   uses Java string length despite its bytes Javadoc, so preserve the present
   unit or make a deliberate documented change. Parse-depth guard in generated
   productions plus StackOverflow conversion must not call a second lexer or
   regex preflight parser. Recovery tooling, if retained, uses the same generated
   grammar and can never enter production planning with an erroneous partial AST.

No tests were run in this audit. The root's separate storage/Flight unit results
must not be used to claim this frontend migration, CREATE/CTAS precision, generated
AI type declaration, or error-position requirements are complete.


## 2026-10-07 scoped CREATE/CTAS VECTOR precision repair (source ready)

The root authorized the independent DDL repair after the parser migration audit;
this does **not** migrate the frontend or add a parser. The existing sole ANTLR
runtime, parsed neutral declaration and existing planner mapping remain in use.
No Maven/native build was run by this agent. `git diff --check` passes; runtime
acceptance belongs to the root's next closest tests.

- Neutral `DataTypeSpec.VectorType` now retains explicit SPI `ElementType` alongside
  the legacy metric. Its old `(dimension, DistanceMetric)` constructor remains
  FLOAT32. Actual SQL `VECTOR(n,DOUBLE)` survives AstDdlDmlBuilder. Default COSINE
  declarations print canonical `VECTOR(n)` / `VECTOR(n,DOUBLE)`, so actual SQL
  printer→same-parser round trips work. Legacy neutral `(n,COSINE/L2/DOT_PRODUCT)`
  parsing and non-default metric round trips remain; metric is not executable
  type authority. `DataTypeSpecParser` delegates VECTOR semantics to the existing
  neutral type parser, rather than a second precision switch. Missing/trailing/
  unknown precision operands cannot default to FLOAT32. SPI VectorType owns the
  dimension bounds in the parser module, which cannot depend on protocol.
- `DdlTypeMapper.parameterizedType` preserves that exact coordinate precision.
  The existing CREATE metadata-building block is extracted as
  `DdlExecutor.declaredTableMetadata` and used by actual CREATE before its catalog
  mutation. `CtasTask.deriveColumnDefinitions` is the actual CTAS output-schema
  helper; it now successfully consumes the existing planner's canonical DOUBLE
  type names because the neutral VECTOR parser understands storage precision.
- Generated declarations use the sole `VectorLogicalType.create` full physical
  type + constraints; the producer rejects FLOAT64 AI target columns with a
  structured planning validation error. Both existing declaration readers
  (`GeneratedColumnMutationExpressions`, `DurableGeneratedValueBuildService`)
  validate the full strict FLOAT32 LogicalType with `requireDimension`, instead
  of extracting only dimension from a raw legacy metric string. Malformed old
  executable declarations require explicit source rebuild; no guessed migration
  or runtime default was introduced. Generated metadata remains the existing
  protobuf/property codec. Clean executable test fixtures were changed to the
  same full canonical declarations, retaining prior negative dimension/capability
  oracles; the root owns two replacements in dirty Iceberg JDBC fixtures.
- The root's focused test revealed the explicit ARRAY-cast oracle used syntax
  absent from the current grammar. Supported syntax is `ARRAY(DOUBLE)`, whereas
  `DOUBLE ARRAY` and `ARRAY<DOUBLE>` are rejected by the real grammar. Inspection
  found even the supported form was broken in the existing CAST converter,
  which treated every parenthesized type as numeric precision. The same converter
  now recursively constructs Calcite `SqlCollectionTypeNameSpec` for ARRAY before
  numeric-precision parsing. The real-SQL oracle still requires a true ordinary
  ARRAY output and no field16 VECTOR declaration; no assertion was removed.
- Actual Iceberg CREATE had another independent precision erasure:
  `IcebergSchemaBuilder.buildVectorType` ignored declared VectorType and invented
  FLOAT32 dimension128/property dimension. It now requires the existing concrete
  VectorType and validates any dimension property. The existing builder's
  `declaredTableProperties` derives/persists dimension from that authoritative SPI
  declaration, preserves other properties and rejects disagreement. Root wired
  the helper into dirty `IcebergMetadata.createTable` for schema/partition/sort/
  creationProperties. Thus a valid SPI declaration without a property also
  retains VECTOR identity on actual SDK readback. No extra precision property
  or storage codec was added: the SDK list element type supplies precision.
- Paimon already maps declared VECTOR precision using PaimonDeclaredTypes and
  derives its dimension property from VectorType; readback detects FLOAT/DOUBLE
  arrays and reconstructs that exact parameterized VectorType. A real connector
  CREATE→SDK schema→metadata readback test now covers both precisions there too.

New real nearest oracles:
`VectorDeclarationPrecisionTest` (canonical/legacy neutral and actual SQL printer
round trips, malformed operands), strengthened `VectorTypeSyntaxTest` (DOUBLE is
no longer equal to default FLOAT), expanded `DataTypeSpecPropertyTest` (every
precision and legacy metric), `DdlVectorPrecisionChainTest` (actual parse→actual
CREATE metadata builder→production MetadataResolvedTable→actual CTAS compile/
output-schema helper→metadata→SELECT/final field16; generated FLOAT32 success and
FLOAT64 typed rejection), `IcebergVectorDeclarationIntegrationTest` (actual
connector CREATE and HadoopCatalog SDK persisted schema/readback plus missing/
conflicting declaration refusal), and PaimonDeclarationMetadataIntegrationTest's
actual CREATE/readback precision cases. These do not simulate data execution;
Flight precision/value acceptance remains the root's separate Native chain.

Remaining boundaries explicitly not claimed complete: frontend migration and its
source positions; generic nested Iceberg ARRAY/MAP recursion still maps children
through enum conversion, so arbitrary nested parameterized types are not fully
stored; typed ALTER TABLE ADD COLUMN is still blocked by the existing connector
SPI's enum-only addColumn signature (`DdlExecutor.applyAddColumn`), and changing
its contract requires inspecting every connector caller/implementation first.
Source/child nullability absence remains as previously recorded. The existing
CREATE/CTAS repair does not authorize broad SPI or parser migration.


## 2026-10-07: actual function semantics, optimizer safety and field18 audit

This is verified read-only design evidence, not an implementation or an E2E
acceptance result. Production sources are frozen while the root runs nearest
validation. The one separately authorized test repair strengthens CTAS alias
preservation; no function source or Native build was changed here.

### Actual authority and current semantic gaps

| Boundary | Actual source evidence | Consequence |
| --- | --- | --- |
| Callback declaration | `pangrova-protocol/src/main/resources/function-contracts/sql-bindings.json:85,177–191` declares every AI and FILE callback `deterministic:false`. `FunctionBindingDeclarations.callbackMetadata:176–187` feeds the actual Java function metadata, `NativeFunctionBridge.buildDescriptors:110–123` preserves it, `JvmScalarFunction.cpp:1194–1203` assigns it to ScalarUdfSpec. | The Native declaration already has the right AI/FILE volatility. Do not create another Java list of volatile names. Temperature zero and embedding repeatability do not make external model/version/policy state pure. |
| Actual registration and publication | `NativeUdfRegistry.cpp:94–105` installs the metadata; `RegisteredFunctionCatalog.cpp:29–58` projects exact scalar/vector metadata and hashes the selected entry; `FunctionCatalogAssembler.cpp:147–171` assembles and seals the same immutable registry. `NativeFunctionBootstrap.java:84–94` registers callbacks before `NativeCapabilityLoader.finalizeStartup:21–30` and `loadInventory:34–67`. | Capture this finalized inventory, not a built-in startup catalogue before callbacks or local declared aliases as execution authority. |
| Calcite SQL operators | `PangrovaSqlOperatorTable.java:40–42,146–240` statically builds manual AI/FILE SqlFunctions; `typedFunction:227–240` has no semantic overrides. `NormalizedAiEmbeddingSqlFunction.java:17–65` likewise has no determinism/dynamic override. `PangrovaPlanner.java:47` constructs the static table before any actual catalogue is injected. | All these calls inherit deterministic=true. Native false does not repair earlier Rex simplification, partition reasoning or constant reduction. |
| Standard/native functions | `PangrovaSqlOperatorTable.java:65–66` falls through to the entire SqlStdOperatorTable. `FunctionMapper.java:61–78` later maps declarations and `FunctionCatalogView` has no production Planner/Coordinator consumer. | Calcite can resolve/fold a function absent from the captured actual worker catalogue. A folded-away call can even evade final IR admission. Every resolved executable operator needs actual admission before optimization. |
| Result/split reasoning | `PlannedQuery.java:233–242` has isDeterministic plus an AI/FILE prefix blacklist; `PartitionLocalPlanInspector.java:47` uses only isDeterministic. | The blacklist protects only cache reuse and is itself a second semantic list. Split independence can still be approved using a falsely deterministic callback. Derive both from admitted compositional semantics. |
| Null invocation | `NativeUdfRegistry.cpp:101–102` sets only deterministic. The actual installed SDK `velox/expression/FunctionMetadata.h:44` defaults defaultNullBehavior=true; `Expr.cpp:512–528` removes all rows having any NULL argument. | Published metadata faithfully advertises an implementation setup that may disagree with the delegate. The correct fix begins at registration, not by making Calcite nullable or inventing a nullability lookup. |
| Concrete legal NULL mismatch | `AiCompletionFunction.java:176–191` treats its third VARCHAR argument NULL as default generation parameters; `invoke:88–111` and batch collection retain the non-NULL model/prompt. Native strict default-null metadata removes that row before callback. | `AI_COMPLETE(model,prompt,CAST(NULL AS VARCHAR))` must preserve the same default-parameter semantics as the two-argument call. It cannot silently become NULL. A NULL FILE overload is a separate typed call; do not guess its branch from an untyped NULL. |
| FILE behavior | `FileFromUrlFunction.java:65–85` and `FileFromBytesFunction.java:71–93` short-circuit required NULL inputs before import/persist and append LocalDateTime.now to FILE. Both actual declarations are false. | Import, storage effects, clock and policy context all preclude implicit expression/result reuse. Content addressing does not authorize suppressing a distinct FILE call. |
| Return nullability | `capability.proto:286–292` carries optional deterministic/default-null metadata. `FunctionCatalogContracts.java:126–129` admits definition nullability only as CANONICAL_DECLARED_OUTPUT. `FunctionNullabilityRegistry.java` is a manual rules table with no production consumer outside itself. | Invocation pruning, result nullability, and error behavior are three distinct contracts. defaultNullBehavior=true never proves that a non-NULL input produces a non-NULL result. Delete the unused table when canonical consumers replace it; do not wire it as another authority. |
| Aggregate/window metadata | `RegisteredFunctionCatalog.cpp:65–75,91–103` publishes ordered signatures and window process/isAggregate but no aggregate/window determinism or NULL/error semantics. Actual SDK Aggregate.h:532–543 contains ignoreDuplicates/orderSensitive/companionFunction, not determinism; WindowFunction.h:49–56 contains process/isAggregate only. | No honest adapter can infer missing traits by treating all aggregates as pure or all functions as volatile. Project actual order-sensitive/duplicate traits and supply missing executable semantic facts in the existing authoritative registration/declaration generation, with required presence and hash coverage. |

AI/FILE NULL and errors must remain governed callback behavior. For example,
AI_EMBED's NULL input returns NULL without embedding (`AiEmbedFunction.java:96–119`),
but purpose NULL/unknown is rejected by existing normalized argument validation;
this is not permission to turn every invalid argument into NULL. All hidden
binding literals must still be validated and remain non-NULL where the canonical
layout requires them.

### Fixed Calcite 1.42 and actual Velox optimizer risks

The installed Calcite 1.42.0 source jar was read directly and cross-checked against
its official tag. `ReduceExpressionsRule.java:1148–1154` checks isDeterministic
and isDynamicFunction before considering a call constant. `ValuesReduceRule.java:177`
uses that same reducer. The current production HEP rule registry includes these
rules (`optimization/HepRuleRegistry.java:117–134`), while both
`PlannerComponentFactory.java:87` and `HepOptimizationPass.java:97` install the
stock RexUtil.EXECUTOR. Wrong flags therefore reach a real optimizer path; an
unsupported Java evaluator exception is not a semantic safety mechanism.
[Calcite reducer source](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/java/org/apache/calcite/rel/rules/ReduceExpressionsRule.java)
and [Values rule](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/java/org/apache/calcite/rel/rules/ValuesReduceRule.java).

A determinism override alone is insufficient. `ProjectMergeRule.java:122–140`
uses `RelOptUtil.pushPastProjectUnlessBloat`; the latter (`3337–3365`) substitutes
bottom expressions solely under complexity/window checks. Thus an outer query
reading an already produced AI alias twice may be rewritten into two callback
occurrences. `FilterProjectTransposeRule.java:155–170` uses the same substitution;
an alias used by a filter and surviving projection can similarly gain another
invocation. Guards must preserve the producer slot and evaluation domain for
volatile or error-sensitive expressions. Keep ordinary deterministic projection
and filter optimizations available through the same HEP registration mechanism.
[Project merge](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/java/org/apache/calcite/rel/rules/ProjectMergeRule.java),
[projection substitution](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/java/org/apache/calcite/plan/RelOptUtil.java),
and [filter transpose](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/java/org/apache/calcite/rel/rules/FilterProjectTransposeRule.java).

`RexProgramBuilder.registerInternal:324–347` unconditionally interns equal Rex
expressions by key; it does not check determinism. Current HEP registers CALC_MERGE
and CALC_REMOVE (`HepRuleRegistry.java:260–265`), not PROJECT_TO_CALC. Consequently
this is a concrete unsafe upstream primitive to protect when a Calc is present,
not proof that every SQL call currently traverses it. Do not introduce Calc
fusion of volatile calls and then claim the false operator flag closes CSE.
[Exact program builder](https://raw.githubusercontent.com/apache/calcite/calcite-1.42.0/core/src/main/java/org/apache/calcite/rex/RexProgramBuilder.java).

There is also an independent execution defect: the installed SDK
`velox/core/QueryConfig.h:230–240` defaults expression.dedup_non_deterministic=true.
`velox/expression/ExprCompiler.cpp:133–147` reuses a compiled non-deterministic
expression when that flag is true. `VeloxTaskRunner.cpp:165–334` never sets it.
Fix the actual query context to false as an invariant, with no session/task
option restoring unsafe deduplication. This preserves deterministic CSE; it does
not disable Velox expression compilation. Distinct SQL occurrences must stay
distinct, while references to a previously materialized producer column share
that column's value. Official current source matches the local observed behavior,
although local SDK source remains the build authority:
[query config](https://raw.githubusercontent.com/facebookincubator/velox/main/velox/core/QueryConfig.h)
and [expression compiler](https://raw.githubusercontent.com/facebookincubator/velox/main/velox/expression/ExprCompiler.cpp).

Native determinism and side-effect freedom are not synonyms. A deterministic
function can throw on a selected row; it cannot be hoisted from an unselected
CASE branch just because its arguments are literals. Existing IfThen lowering
`SubstraitTypeConversion.cpp:130–153` produces checked Velox `if` calls; that
short-circuiting behavior must survive all pre-IR rewrites. Native callback
`JvmScalarFunction.cpp:805–882` receives and forwards only selected rows;
Java exceptions at `1026–1030` become a thrown Native runtime error, and
NativeFunctionBridge.java:150–182 preserves a classified failure rather than
returning NULL. This currently has batch-wide failure semantics, not row-local
recoverable error semantics. Do not promise TRY-per-row recovery without a real
EvalCtx row-error path. Velox's vector API explicitly distinguishes selected
rows, default NULL pruning and per-row errors:
[scalar API](https://facebookincubator.github.io/velox/develop/scalar-functions.html)
and [expression evaluation](https://facebookincubator.github.io/velox/develop/expression-evaluation.html).

### One complete implementation unit, no second function authority

1. Reuse the existing generated sql-bindings declaration and actual published
   FunctionCatalogView. Extend that existing semantic contract only for missing
   required facts: callback null invocation policy, volatility/context dependence,
   error/short-circuit behavior, and aggregate order/duplicate traits. Generate
   callback registration metadata and Native publication from it; include these
   facts in current contract hashes. Do not maintain a second enum/name blacklist.
   For AI_COMPLETE the shared Native vector registration must have
   defaultNullBehavior=false because one legal overload handles a NULL option;
   the existing delegate already short-circuits required model/prompt inputs.
   Carry the explicit canonical registration policy through FunctionMetadata,
   NativeFunctionDescriptor, JNI ScalarUdfSpec and metadata builder; inspect every
   constructor/caller before this existing contract change. Do not infer it from
   the first overload's arity or from its return nullable bit.
2. Capture one finalized compatible worker cohort/view before SQL validation.
   Pass that immutable view through existing PlannerContext/SqlCompiler/session,
   metadata, DML, generated-write and durable/template compile callers. Replace
   the static executable operator inventory with catalogue-driven lookup. Before
   exact operand types are known, an operator's fixed no-argument determinism
   property is the conjunction across its actual eligible overloads; after
   validation, bind exact argument NamedStruct/constants/declared target using
   FunctionCatalogView.bindScalar/bindRawAggregate/bindWindow before any optimizer.
   Effective hidden-argument types/constants derive from the existing governed
   layout, never duplicated ordinal arithmetic or guessed bindings. Unavailable
   entries and unknown required metadata fail optimization admission explicitly.
3. Derive SqlOperator return/null/strong-policy and volatile/dynamic traits from
   that admitted selection. Generate same-kind Calcite adapters for scalar,
   aggregate and window operators, preserving stock syntax/aggregate algorithms
   only when the captured executable definition supports them. Rewrite semantics
   compose the selected targets and argument expressions; special forms use the
   existing selected type-program/vector-companion identity and branch semantics.
   Context-stable functions require a captured query context rather than an
   invented deterministic=true. There is no blanket functions-default-false rule.
4. Protect HEP/CBO rules at their real existing registration points when their
   transformations duplicate, merge, drop or move sensitive expressions across
   producer/selection domains. Exact admitted traits drive predicates; retain
   safe pure rules and structural literal/CASE simplification. The Java evaluator
   must not execute Native-owned functions based on Calcite's independently
   implemented semantics. Constant reduction needs either a canonical proven
   literal-rewrite contract projected into the existing Calcite pipeline or must
   leave that call for the existing Native evaluator; introducing coordinator JNI
   evaluation or a Java interpreter is not allowed. Differential overflow,
   division, cast, collation and NULL tests certify any declared folding rule.
5. Collect all calls from the final emitted Substrait after every physical rewrite,
   including structural lowering intrinsics (`if`, constructors, element access,
   window frame comparisons), window expressions, masks and aggregate sorts.
   `TaskRequest.function_requirements=18` already exists; populate its current
   expression path/schema/constants/selected-entry hash plus fragment checksum
   and captured definition-view hash. Emit an explicit zero-call envelope.
   Standard aliases and declaration identity alone are not execution proof.
6. Fix the second Native window-expression overload:
   `SubstraitTypeConversion.cpp:156–172` directly constructs CallTypedExpr from
   caller output type/name, while `SubstraitWindowConversion.cpp:106–143` performs
   actual window binding for WindowRel. Intrinsic window expressions must lower
   through the existing WindowNode path with exact partition/sort/frame/null
   options and actual binder, or be rejected when the expression cannot represent
   its required context. A scalar CallTypedExpr is not a window implementation.
7. All four aggregate phases use one original raw signature selection. Preserve
   field12 original_raw_argument_schema and selected-entry identity across
   initial→intermediate→intermediate→result and initial→result. The local
   `SubstraitAggregationDistribution.cpp:49–86` retains original raw types but
   rebinds the same name; attach and verify the original admitted identity after
   this rewrite, rather than accepting any same-name state signature. General
   distributed replay must never derive raw types from opaque state bytes.
8. The sole TaskRequestFactory writer and CanonicalTaskRequests/Native parser
   validate field18 against exact final bytes and the process-owned publication
   before constructing executable calls/task resources. No late worker lookup
   changes the authority. Retain the same bytes/hash/requirements/raw provenance
   in current durable/template stage serialization, with one format transition;
   old incomplete records require rebuilding, not replay against today's defaults.
   Worker selection and speculation stay inside the captured compatible cohort.
   Query/plan/template cache keys include that view and relevant context identity.

This is the same field18/cohort implementation already designed above, extended
with verified optimizer and invocation semantics. It needs existing protocol,
SPI/JNI registration, Planner and Coordinator/Native caller changes as one unit;
an isolated Java operator override or Native config line is not full completion.

### Real acceptance oracles (planned, not executed)

- Actual finalized catalogue round trip must assert every exposed scalar's
  required metadata and selected-entry hash; all ordered aggregate/window facts
  and companion traits are present and correspond to the process registry.
  Exercise enabled and absent AI/FILE worker cohorts, unavailable stock SQL names,
  mismatched incarnation/view, missing/forged field18 and durable replay changes.
- With a real governed provider deployment and reuse explicitly disabled in the
  actual OperatorBinding, project the identical AI invocation twice over N rows.
  Record request/row evidence in existing tracing or provider access logs: two
  distinct producer sites and 2N requested inference rows, not token/usage ledger.
  A subquery producing one AI alias then selecting alias twice must produce N
  requested rows and equal alias values. Check the optimized plan/IR producer
  count as well as actual Flight values; output text equality alone proves neither.
- Actual Native owned random implementation repeated over at least 1000 input
  rows must remain distinct expressions, with values in its documented domain
  and varying pairs; random draws may coincide, so do not demand every pair differ.
  Current canonical sql-bindings has no RAND/RANDOM declaration. Until actual
  canonical exposure is implemented, run this oracle against the real owned
  Native registry, and do not pretend SQL RAND E2E is already supported.
- Real SQL AI_COMPLETE(model,prompt,CAST(NULL AS VARCHAR)) yields a non-NULL
  completion using defaults; NULL required prompt yields NULL with zero inference
  rows. Typed NULL FILE overload, AI_EMBED NULL source and invalid/NULL purpose
  each keep their own canonical behavior. FILE required NULL arguments trigger
  no import/persist; actual blob content, checksum/size and timestamp are checked
  for live calls, without using a fake blob service or clock-value inequality.
- Run CASE with dead branch error-cast and dead AI/FILE calls: no branch I/O/error;
  run live bad cast and an actual provider/storage-policy failure: classified
  query failure rather than fabricated NULL/success. Mixed NULL/valid/bad inputs
  preserve documented batch-wide callback failure. TRY row recovery is not
  claimed until implemented through Velox's per-row error API.
- Real VALUES/filter/CASE SQL uses selected input IDs and input-dependent prompts:
  provider-request content must include only visible selected rows, Flight rows
  retain correct IDs/nullability, and a lower materialized AI alias used in both
  filter and projection is not re-invoked. Test outer joins, false/unknown branch
  predicates and reordered filters without assuming textual AND evaluation order.
- Real aggregate initial/result, local partial/final, distributed four-phase
  shuffle and durable replay use decimals and state-erasing signatures, NULL/masks,
  DISTINCT and order-sensitive array aggregation. Real WindowNode tests cover
  ROWS/RANGE, NULL options, aggregate windows and deterministic unique sort keys;
  tie-sensitive row_number is not declared context-independent by inference.
  Keep original type/error oracles and reject state/provenance or function-view
  substitution before execution.

CTAS validation correction: root's nearest run reported 87 tests, 85 pass and two
DDL chain errors. Actual logs show ProjectRemove deletes `Project(result=$0)`;
`PlannedQuery.queryOutputColumns:96` read optimized TableScan names instead of
already captured original `columnNames`. This is an alias-loss production root
cause, not a case-sensitivity test excuse. The strengthened test now quotes the
alias and asserts actual captured/derived names before final field16 precision.
The parent owns the minimal production fix and its rerun. Separately, the reported
8 DurableGeneratedValueBuildServiceTest failures use an actual inventory already
containing generated materializer plus a duplicate fake fixture; no weakening or
new fake was made, and model-chain acceptance is not claimed.


## Isolated Java callback-semantics implementation unit (source ready)

All edits in this unit are confined to the current managed Pangrova worktree.
The original shared repository and its builds remain owned by the other task.
No Maven, CMake or CTest invocation was started by this agent; the root coordinates
all isolated validation. The following is source readiness, not a test pass.

The existing callback declaration is now consumed through
`FunctionBindingDeclarations.callbackMetadataForSqlName`, which resolves the same
canonical scalar SQL identity and returns the already existing runtime metadata.
It neither synthesizes a Native inventory nor introduces a second callback-name
list. DeclaredCallbackSqlFunction projects that metadata into Calcite's
isDeterministic and isDynamicFunction; the latter conservatively prevents plan
reuse for these externally volatile callbacks. PangrovaSqlOperatorTable uses it
for declared runtime callbacks only, retaining non-callback signatures and return
inference. NormalizedAiEmbeddingSqlFunction inherits the same adapter and keeps
all its existing purpose literal/enum/arity/precision validation.

The existing HEP registration mechanism now uses the fixed Calcite 1.42 rule
configuration APIs for PROJECT_MERGE and FILTER_PROJECT_TRANSPOSE. Their operand
predicates require a deterministic producer projection before substitution.
This preserves the evaluation slot and row domain of volatile producers; pure
projections still use the same stock optimization rules. The parent's previous
ValuesReduce additions are preserved, as are existing correlation/window checks.
No new optimization runner, custom relational node or execution path was added.

Regression sources: FunctionBindingDeclarationsTest traverses canonical callback
entries and checks exact metadata/case normalization without duplicating the
function inventory; PangrovaSqlOperatorTableTest checks every declared callback
operator against that source and retains every existing type/arity assertion.
CallbackProducerSqlTest uses actual SqlCompiler parsing, validation and HEP:
two identical AI occurrences retain distinct invocation IDs and output aliases;
AI_COMPLETE, AI_EMBED and FILE_FROM_URL aliases reused twice retain one lower
producer plus two field references; filters of those aliases stay above that
producer in both optimized relation and serialized Substrait. It also checks
literal callbacks with optional NULL remain executable, live CASE retains its
condition/branch in IR, and dead CASE removes external invocation while ordinary
1+2 still becomes the exact value 3. FILE cases use the existing production IR
converter/serializer, because the default Native capability inventory does not
advertise FILE; no fake inventory is injected to hide that pending authority gap.

Closest selectors for root validation:
`FunctionBindingDeclarationsTest,PangrovaSqlOperatorTableTest,CallbackProducerSqlTest,
AiEmbeddingPurposeSqlProtocolTest,AiInvocationExpressionConverterTest,
HepRuleSetCompletenessTest,PangrovaPlannerConstantFoldTest,
PlannedQueryResultCacheSafetyTest,SqlCompilerTest`.
`setup.sh --check` and scoped `git diff --check` pass. Existing installed 1.42
source confirms ProjectMergeRule.Config.withOperandSupplier/as and
FilterProjectTransposeRule.Config.withOperandFor APIs; real compile/tests must
still validate the changes, including exact optimized relation shapes.

This bounded unit does not close actual FunctionCatalogView/cohort injection,
field18 collection/admission/replay, aggregate/window semantic metadata, callback
NULL registration, Velox non-deterministic dedup configuration, or Java constant
folding compatibility with all Native-owned implementations. Their verified root
causes and complete implementation dependencies remain recorded above. It does
not claim real provider/Native/Flight function-semantics E2E acceptance.


Nearest validation feedback and fixture correction (still awaiting rerun):
Protocol/Planner compilation passed in the isolated root-managed Maven run.
Planner's 84 cases had one failure and 17 errors: the actual frontend rejects
FROM (VALUES ...), affecting eight new callback cases and nine pre-existing
embedding-purpose cases. These fixtures now use equivalent supported real
SELECT/UNION ALL sources with the same two distinct rows and purpose values;
all previous NULL/purpose/type/error/alias assertions remain. Standard VALUES
support and the single Calcite parser migration remain unimplemented, and no
production grammar was altered by this test correction.

The new callback suite contains 10 executed cases, not the initially reported 11.
Its dead-CASE failure was an incorrect assumption that final IR was ReadRel:
getRead returned the default empty message, so the assertion failed before any
integer value access. The corrected oracle verifies canonical output STRING/I32
and exact final projected literals (peeling only type-preserving cast structure),
while independently requiring zero scalar calls and no AI invocation intents.
It does not read the unrelated single-row seed's value or assume that removing
a callback also removes the output type-preserving Project. Source is refrozen;
no agent build was started and no corrected pass is claimed yet.


Actual conversion-stage root cause after the second nearest run:
Compilation again passed. Planner's 86 cases contained seven assertion failures,
zero errors; embedding-purpose cases all passed after the supported source fix.
A temporary read-only Main compiled outside the repository from the root's
existing Surefire classpath dumped actual pre-optimization relation, optimized
relation, output types and serialized IR to
`/tmp/pangrova-isolated-semantics-dump.log`. It demonstrated that duplicated alias
callbacks already existed before HEP. Installed Calcite RelBuilder.java:2131–2149
uses its default bloat policy to merge projects during SqlToRel conversion,
without a determinism check. The sole PangrovaPlanner.convert constructor now
adds `withBloat(-1)` through SqlToRelConverter's existing config transform (which
preserves the original pushJoinCondition transform). Automatic construction-time
project merging is disabled; actual project merging remains in the existing
HEP rules, where the producer determinism predicate controls it. This changes
one existing Calcite configuration, not the parser, a new runner or a second
optimization path.

The dump also proved AI_COMPLETE's IS NOT NULL filter disappeared before HEP:
its output was declared required even though the chosen return inference was
named VARCHAR_2000_NULLABLE. Calcite's TO_NULLABLE transformation means nullable
only when an input operand is nullable, unlike FORCE_NULLABLE. The canonical
callback adapter now explicitly preserves the declared nullable callback output
with FORCE_NULLABLE; the same production type factory preserves embedding marker
identity. Non-callback MATCH/BM25 and their inference instances stay unchanged.
This corrects declared output nullability, and does not invent the separate
Native callback invocation/NULL-pruning policy still missing above.

The dead-CASE dump is exact ReadRel with virtual_table.expressions containing
STRING kept and I32 3. The old virtual_table.values field is not the producer's
canonical expression-based representation. The oracle now reads the actual
canonical expression rows, independently checks root schema STRING/I32, retains
zero scalar calls/intents, and checks exact literal types and values. Both prior
root-shape assumptions were corrected using this evidence; no value/type/alias
or producer-count requirement was relaxed. Source refrozen pending root rerun.


Nullable-output evidence clarification: the actual registered scalar/vector
physical signatures do not contain a non-NULL-result guarantee. In the current
Native callback writer, JvmScalarFunction.cpp:1129–1138 explicitly marks a NULL
Java result as SQL NULL; ARRAY/FILE writers similarly retain null bitmaps.
CANONICAL_DECLARED_OUTPUT does not authorize changing this to required merely
because current operands are non-NULL. The adapter preserves the nullable output
already selected by the callback declarations/return inferences, without touching
ordinary MATCH/BM25 and without deriving or changing defaultNullBehavior. This
is a conservative canonical output projection; complete actual inventory semantic
admission remains the separately recorded unfinished unit.


### 第三轮已编译 IR 的精确 oracle 校正（2026-10-07）

隔离构建日志 `/tmp/pangrova-isolated-function-semantics-root-fixed-nearest-20261007.log`
确认 compile PASS、Planner 86 cases / 5 failures / 0 errors；其中 CallbackProducerSqlTest
为 10 cases / 5 failures。该轮 PRE/POST 已验证 producer 保护与 nullable output 根因修复生效。
这五个失败不再证明生产路径重复执行：精确 Surefire report 的 FILE :63 是 DFS 名称断言，
并非 scalarCalls；此前转述的 FILE call count 判断已纠正。

只读临时 Java dump `/tmp/PangrovaSemanticsDump.java` 消费该轮既有编译产物；
`/tmp/pangrova-isolated-semantics-third-complete-dump.log` 记录实际
PangrovaPlanner.planForPolicyApplication → SqlCompiler.compileToResult →
SubstraitConverter.convertWithResult → SubstraitPlanSerializer 的前后关系、canonical layout 与 IR。
FILE 使用同一生产 converter/serializer，不伪造 Native capability；默认 inventory 不广告 FILE
导致 compiler admission 拒绝，故本结构单元不声称 FILE Native admission/E2E 已闭合。
临时 Main 不持久化进仓库、不提供执行器或第二 codec，也没有启动 Maven/native 构建。

- FILE repeated alias：PRE/POST 为 outer Project 两个 `$0` → 单 producer Project → 两行
  Values；最终 IR 仅一项 `file_from_url:` callback。root names 依法包含嵌套 FILE 的
  `uri/mime_type/size/checksum/created_at` DFS 名称，因此使用既有
  SubstraitRelationLayout.rootSchema/topLevelNames 验证两个顶层 alias；两输出完整类型均须
  等于 producer 类型，outer emit `[1,2]`、producer emit `[1]`、两个 selection 必须精确为
  canonical rootFieldReference(0)。保留单 callback 与完整类型，未将重复 callback 接受为正确。
- 三个 filter：最终关系为 Filter → 单 callback Project，单 callback anchor 位于 producer，
  predicate 是 `not(is_null(rootFieldReference(0)))`；三个 scalar refs 分别 callback/not/is_null，
  每项各一次。测试以既有 FunctionBindingDeclarations 的 canonical name/URN 校验 extension
  identity，再逐层证明 predicate 引用 producer 输出，替换错误的总 scalarCalls=2 假设。
  HEP filter 不下推至 callback 参数源；AI invocation hints 仍必须一项，FILE 为零。
- dead CASE：实际 POST 为 Project(nullable VARCHAR cast('kept'), required I32 literal 3) →
  单行 seed Read ZERO=0；emit `[1,2]`。FORCE_NULLABLE 必须保留 output cast，故 final root
  不是 Read。测试按既有 RelationLayout 的 expression context 与 outputMapping 证明两个
  输出类型，校验 seed cardinality=1、nullable STRING/required I32、真实常量 `'kept'`/3；
  独立要求 scalarCalls 与 AI hints 都为空。此处不再猜测 Read/Project 形态或读取 deprecated
  virtual_table.values，消费 actual virtual_table.expressions。

此轮仅修隔离 worktree 的 CallbackProducerSqlTest；canonical declaration consumer、converter
config、HEP guards 与 callback nullability 生产修复不变。全部测试既有核心 oracle 保留并细化。
`git diff --check` PASS。修正测试的 compile/test/static 由 root 统一重跑，当前不预先宣称 PASS。
全 worker function inventory/cohort/field18、Native non-deterministic dedup 与 NULL invocation
policy，以及唯一 Calcite parser 对标准 VALUES 的支持仍属于未闭合单元。


## 2026-10-07 NULL callback policy atomic unit (prepared only; not applied or validated)

All proposed source deltas in this section remain in `/tmp/pangrova-null-policy-unit-draft-20261007`; the isolated checkout and the shared main checkout are frozen for this agent. The baseline records exact file hashes. No generation, Maven, CMake, CTest or JNI execution has been performed for this unit. Earlier Protocol 10 / Planner 86 nearest passes concern the already-applied Java callback flags/guarded optimization unit, and do not validate these NULL/runtime changes. The subsequent Planner owning run reports 1335 cases with 5 errors: ROUND static admission (three), standard protobuf Nested admission (one), and frontend FROM VALUES (one). Those are separate unfinished actual-catalog/frontend gaps.

### Authority and actual execution cause

`sql-bindings.json` has 16 real runtime callback declarations and 19 ordered public/governed signatures. The proposed explicit `defaultNullBehavior=false` belongs to these existing declarations. Java `FunctionBindingDeclarations` and Python `binding_declarations` reject missing/non-boolean policy, and `generate_structural_contracts.py` emits it with the exact original argument and overload order. Existing per-binding and complete-declaration SHA-256 inputs already cover this new semantic field. Do not hand-edit generated headers or package an old actual-native-catalogue artifact with the new declaration.

Worker `NativeFunctionBridge` consumes the same metadata into descriptor field 6 (`optional bool default_null_behavior`). Explicit false retains protobuf presence. Absence is startup-rejected; this is one new cohort, with no old-wire fallback. JNI validates the complete descriptor batch and `NativeUdfRegistry.withScalars` preflights duplicate signatures and name-level determinism/NULL conflicts before callback-owner retention or owned registration publication. Both registration branches consume one prepared VectorFunctionMetadata object. Actual vector/simple/companion inventory already captures NULL policy; Java actual-catalog admission additionally requires the presence of this captured policy. Actual missing-policy negative tests re-seal the entry, registration, registry projection and manifest so a digest rejection cannot substitute for the intended execution-metadata rejection.

Fixed installed Velox `expression/FunctionMetadata.h:44` defaults NULL behavior to true; `expression/Expr.cpp:477-493,1153-1185,1222-1229` prunes NULL input rows before VectorFunction.apply. This currently prevents Java AI_COMPLETE optional NULL parameters from reaching its existing defaults implementation. CALL_ON_NULL makes the production Java implementation authoritative: AI_COMPLETE required model/prompt NULL returns SQL NULL before provider lookup, while third optional VARCHAR NULL uses DEFAULTS. AI_EMBED validates purpose before primary source/model NULL handling, so unsupported/NULL purpose remains an error. FILE_FROM_BYTES requires both bytes and MIME; either NULL returns SQL NULL before persist. FILE_FROM_URL requires URL. FILE has no optional NULL input, so it is not evidence for AI optional-NULL semantics. No new primary-NULL mask, source ordinal policy or name list is proposed.

A typed NULL ROW/VARCHAR overload loses its type when bridged as RawNull and NativeFunctionBridge.resolveSignature takes the first matching overload. AI_COMPLETE currently treats both NULL option overloads as DEFAULTS, but exact typed-overload identity is not fixed by this unit. It needs the existing exact function-requirement/schema binding path and remains open.

Velox `core/QueryConfig.h:231-240` defaults `expression.dedup_non_deterministic` to true; `expression/ExprCompiler.cpp:133-147` can therefore reuse syntactically equal volatile expressions. The sole VeloxTaskRunner QueryConfig is proposed to disable that existing switch. This is distinct from SQL alias references to one producer, which must continue to share that producer.

### Reachability and removed private reuse path

`TaskRequestParser.cpp:1057-1072` refuses reuse policy unless the actual generated AI text descriptor is deterministic; that descriptor is false. `AiInvocationOperatorBinder` does not emit reuse policy. Native governed arguments are produced only from the admitted OperatorBinding, with absent-policy defaults false. SQL arguments cannot supply those hidden arguments. Thus the private JvmScalarFunction semantic lookup/put branch is unreachable for every current canonical callback.

The proposed deletion removes CacheBatchContext, cache construction, approximate key construction/identity normalization, lookup/miss batching/put/result replay, their log/crypto/cache includes and the reuse-enabled offset. It preserves one real JNI batch callback and one real result writer, Arrow/FILE/scalar NULL bitmaps, actual argument order, callback context, selected row cardinality and exact AI_EMBED FLOAT32 dimension checks. Remaining positiveUint64/decodedString helpers serve the real embedding output contract; their misleading cache diagnostic becomes JVM callback. No extra NULL mask is introduced to route around dead reuse.

Full global SemanticResultCache removal remains an explicit subsequent unit. `src/cache/SemanticResultCache.{h,cpp}` has no remaining production lookup/put caller after this bounded deletion. NativeCommandDispatcher's invalidateAll and TaskExecutor releaseTaskUsage/taskUsage are maintenance/metrics of that dead feature, not executable cache use. Remove those together with old feature tests (`test/cache/SemanticResultCacheTest.cpp`, TaskExecutorStartTest's StatsExposeSemanticCacheUsageBeforeRelease), semantic TaskStats fields/Java decode-log surfaces, `ExecutionReusePolicy`, sole declaration governed reuse fields, projection tests and generated/package/build inputs after validating all callers. Generic runtime cache eviction and Velox AsyncDataCache are live storage-cache capabilities and must be distinguished by actual callers; do not remove them merely because their command includes the now-dead semantic invalidateAll. Replace feature-only semantic-reuse tests with real unsafe reuse policy rejection and real non-deterministic provider execution oracles, retaining the rejection throughout migration.

### Draft acceptance and limitations

Positive Native metadata tests use the real canonical FILE_FROM_URL implementation (false/false) across the two registry branches and actual built-in abs/rand snapshots (strict NULL policy with deterministic true/false). They no longer invent positive determinism/NULL combinations or owners. No real deterministic/CALL_ON_NULL implementation has been identified, so that combination is not claimed validated. Same-name AI_COMPLETE overload policy mutation is only a negative preflight input and asserts the actual global signatures/metadata and the unpublished registry remain unchanged.

Existing real-provider OllamaAiSqlE2ETest draft adds: optional NULL defaults with repeated aliases and exactly one provider call; required prompt NULL with zero provider calls; three CASE rows retaining one selected provider row, one constant row and one primary-NULL row. These call the already-configured actual endpoint, with no provider stub. Existing ten-row embedding purpose fixture is changed from frontend-unsupported FROM VALUES to equivalent SELECT/UNION ALL while retaining all purpose, NULL, order, row-count and vector assertions. These cases are prepared and unrun; only an actual service run can close them.

Still to complete before claiming runtime closure: actual JNI descriptor absence/unknown-field preflight regression at the pre-owner lifecycle stage; actual FILE_FROM_BYTES positive plus NULL typed Arrow/JNI selected rows; real-provider execution and affected full registration/package regression. A factory overload consuming the already-parsed canonical NativeFunctionDescriptor is proposed to expose the existing descriptor-to-spec validation, keeping JNI as the sole protobuf parser rather than testing after startup finalization where lifecycle rejection hides presence errors. Root confirmed this API delta; its draft now uses one descriptor-to-spec helper after the sole JNI protobuf parse. No private execution harness or codec is introduced.

Field 18 remains unwritten and unparsed; actual published FunctionCatalogView still is not injected into Coordinator planning/cohort selection/final-IR requirements/durable replay. This NULL unit changes current canonical callback registration semantics only and must not be described as closing actual inventory or field18 authority.


### Confirmed known-callback registration drift guard (draft-only)

Root additionally requires known canonical callback descriptors to exactly match the sole generated RuntimeCallbackSignature: ordered raw arguments (including governed fields), return type, determinism and NULL invocation policy. Previously one entire same-name overload set could consistently drift and still be hashed as an actual registration. The generator now emits an exact lookup returning the existing declaration span (empty for names with no callback declaration), while preserving the strict existing runtimeCallbackSignatures accessor for its callers. The descriptor helper checks every known signature and rejects drift before spec creation. Names without callback declarations continue through the general descriptor/registry path; no copied list or guessed callback metadata is added. Native negative cases mutate actual FILE_FROM_URL and each actual AI_COMPLETE overload's NULL flag, determinism, output, argument count and first raw type; actual registration signatures/metadata remain unchanged. This closes this registration admission gap in the draft, while actual catalog planning/field18/cohort wiring remains separate and incomplete.


## Real FILE/provider E2E deployment overlay (draft-only; no execution)

The NULL overlay now adds an actual FILE test deployment beside the existing SharedAiProvider inside the one SharedNativeFunctionStartup owner. The FILE deployment is configured only when PANGROVA_E2E_FILE_CAPABILITY_KEY is present before that shared startup. Dedicated GovernedFileSqlE2ETest explicitly requires it and the actual published file_from_bytes registration; missing configuration fails rather than skips. A previously initialized Native runtime is never borrowed or re-registered to add FILE. Other existing no-FILE deployments remain valid production configurations and do not pretend to expose FILE.

Existing ActualShuffleBackend uses the pinned MinIO release over HTTP, whereas S3BlobStorage.Settings validates HTTPS and mandatory AES256/known KMS. SharedGovernedFileStorage uses the same Testcontainers/MinIO pattern and the existing localhost certificate/key (certificate valid 2026-09-23 through 2036-09-20, DNS localhost) with real TLS and that pinned MinIO release's supported static KMS for real server-side encryption. There is no test BlobStorage, factory/provider substitute, hostname bypass or trust-all mode. The existing production AiFunctionFactory receives the S3/staging/secret-reference options through NativeFunctionConfig, creates S3BlobStorage through the AWS default credential chain, and registers both real FILE functions before final actual capability publication.

Official implementation sources: [pinned MinIO static KMS configuration](https://raw.githubusercontent.com/minio/minio/RELEASE.2024-12-18T13-15-44Z/internal/kms/config.go) defines MINIO_KMS_SECRET_KEY as a named base64 32-byte key and selects its actual built-in KMS implementation; [MinIO TLS configuration](https://min.io/docs/minio/linux/operations/network-encryption.html) documents --certs-dir plus public.crt/private.key. [AWS default credential chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) gives system properties first. [Pinned AWS SDK 2.25.16 Apache client](https://raw.githubusercontent.com/aws/aws-sdk-java-v2/2.25.16/http-clients/apache-client/src/main/java/software/amazon/awssdk/http/apache/ApacheHttpClient.java), lines 610-640, uses the normal hostname verifier and initializes its own TLS context; therefore merely replacing SSLContext.getDefault is insufficient. The test owner prepares a real JSSE trust store containing the original default issuers and the one existing localhost fixture certificate, and installs its actual trust-store properties before the production S3 client is constructed. No verification is disabled. The normal HTTP/client TLS defaults and AWS credentials/session token are restored after Native shutdown and callback owner cleanup, then the real bucket/client/MinIO container and temporary trust-store/staging files are removed. Preparation failures close both real deployment owners while retaining the original failure.

The E2E entry uses the existing one-fork shared Native/Coordinator/Worker/Flight/Arrow path. FILE and Ollama suites share the credential resource lock, and E2E remains sequential by the existing fork configuration. FILE cases exercise (1) three rows with real positive bytes, bytes NULL and MIME NULL, one exact object publication; (2) four CASE rows with selected positive input, unselected invalid MIME, selected bytes NULL and selected MIME NULL, one object without spurious error; (3) repeated aliases sharing the complete FILE result including creation timestamp; (4) selected invalid MIME failing without object publication. Positive cases read actual object bytes and HEAD metadata from the real bucket: checksum, size, content type application/octet-stream and actual AES256 encryption. FILE struct fields retain the declared MIME image/png and exact checksum/size, while primary NULL rows remain SQL NULL. The assertions inspect the actual Arrow struct decoded by the existing E2E client as a Map; they do not install another FILE codec. Objects are content addressed, so unique payloads per case prevent an earlier object from masking a new write. Object cardinality alone does not prove callback execution cardinality; alias identity/timestamp and the already-existing precise Planner/IR single-producer tests are the additional oracle.

AI cases still use the real existing Ollama endpoint and its real request counters, including required prompt NULL (zero I/O), optional NULL parameters/defaults (one request), CASE selected rows (one request/three output rows), repeated aliases (one producer), repeated execution (one additional request each time), invalid/NULL embedding purpose (error and zero I/O), exact embedding vectors and ten-row batch purpose/NULL behavior. FROM VALUES remains unsupported by the frontend; the ten-row fixture uses equivalent supported SELECT/UNION ALL and explicit quoted aliases for the lower-case names asserted by the real client. No prior assertion is removed.

Execution entry after the changed protocol/declaration/Native cohort has been generated, built and packaged by root:

```sh
(
  task_file_capability_key="$(python3 -c 'import base64,secrets; print(base64.b64encode(secrets.token_bytes(32)).decode())')"
  PANGROVA_E2E_FILE_CAPABILITY_KEY="$task_file_capability_key" \
    mvn -B -ntp -Dmaven.repo.local="${PANGROVA_MAVEN_REPO:?set an isolated Maven repository}" \
      -pl pangrova-testing/pangrova-testing-integration-tests -am \
      -Dtest=GovernedFileSqlE2ETest,OllamaAiSqlE2ETest \
      -Dsurefire.failIfNoSpecifiedTests=false test
  unset task_file_capability_key
)
```

Use the already-discovered JDK 17/Maven environment and isolated root for this command. Generate the task-private key only for this test process, do not print it or change the production env resolver. Run only after the complete new Native/protocol cohort is packaged; the currently running CTest build has not applied this overlay. This agent has not invoked this command, a container, a provider endpoint, or any build. Compilation/static checks, real MinIO SSE responses, actual FILE selected-row execution and actual model endpoint execution remain unverified until root runs them. Field18/Coordinator actual-catalog injection/cohort/durable requirements are still a separate unimplemented unit.


### Nested descriptor payload and real publication request proof (draft-only)

The one descriptor-to-spec helper now uses protobuf reflection for every actual return/argument LogicalType: ListFields must contain exactly the generated kCanonicalSqlFieldNumber and its unknown-field set must be empty. Production Worker NativeFunctionDescriptor currently emits precisely this text-only carrier. Additional populated substrait_type/constraints payload is not silently ignored, and no blacklist of other LogicalType fields or second type mapping is introduced. The actual FILE_FROM_URL and all actual AI_COMPLETE overload inputs have negative mutations for missing canonical text, nested unknown varints, extra physical I64 and extra constraints, through the same helper before callback-owner retention. NULL-policy absence remains separately tested using actual fully re-sealed inventory entries.

Publication and expression evaluation are separate contracts. The current actual production path is FileFromBytesFunction -> GovernedBlobStore.persist (lines 100-110) -> S3BlobStorage.publish (57-72). Every persist writes staging and calls publish, and this S3 implementation unconditionally issues the conditional PUT with If-None-Match:*. Its second same-payload call accepts HTTP 412 and verifies HEAD; it does not use an earlier HEAD to omit PUT. Therefore two identical independent FILE producers currently yield two actual PUT requests while retaining one content-addressed object. The E2E asserts both layers and independent created_at, and additionally uses two different payloads to prove two successful publications/objects. It does not change production idempotence behavior or require an extra write path for counting.

The fixture enables the actual pinned MinIO public metrics endpoint only in this dedicated Testcontainers deployment, scraping it over the same normally verified HTTPS connection. [Pinned MinIO metric registration](https://raw.githubusercontent.com/minio/minio/RELEASE.2024-12-18T13-15-44Z/cmd/metrics-router.go) supports this endpoint; [pinned metrics implementation](https://raw.githubusercontent.com/minio/minio/RELEASE.2024-12-18T13-15-44Z/cmd/metrics-v2.go), lines 2735-2785, exposes completed S3 request totals from the actual server and caches them for ten seconds. [Pinned HTTP counters](https://raw.githubusercontent.com/minio/minio/RELEASE.2024-12-18T13-15-44Z/cmd/http-stats.go), lines 330-344 and 405-423, lowercase API labels and count rejected conditional PUTs too. The test must first receive HTTP 200 with actual completed S3 metric samples; absent metrics or malformed responses cannot become a zero-count fallback. Assertions require exact count stable for eleven seconds, covering one full refresh window. The bounded metrics observation wait does not increase query/production I/O deadlines or hide a blocked execution. Nullable/CASE/alias cases require one PUT, identical or distinct independent calls two PUTs, and selected MIME failure zero PUTs, in addition to exact object bytes, content type, metadata and AES256.

These new six FILE acceptance cases and the existing real-provider suite remain uncompiled/unrun. No result is claimed until the complete generated new cohort is packaged and the parent executes the dedicated entry with a task-private capability key. Both production source inputs and generated protocol inputs remain frozen until parent approval to atomically apply this draft.


### Fixed SDK name identity and batch closure (draft-only)

The installed fixed Velox expression/VectorFunction.cpp:29,234 and exec/Aggregate.cpp:71 both register and resolve through exec::sanitizeName; expression/FunctionSignature.cpp:25-34 implements lowercase identity. Raw NativeUdfRegistry map keys previously allowed FILE_FROM_URL/file_from_url or AI_COMPLETE/ai_complete to bypass duplicate and semantic consistency preflight, then overwrite one actual SDK entry. The canonical descriptor helper now performs declaration lookup using this existing SDK identity and requires the descriptor's exact raw name to equal the generated RuntimeCallbackSignature.name as part of known-callback admission. Generic plugin functions retain the general descriptor/registration path, with no new name list. Both internal maps and both lookup APIs now use the same SDK key. Scalar/aggregate cross-kind collisions and duplicate aggregate identity fail before insertion. Original specs keep their raw callback names so Java callback dispatch is not silently renamed; final SDK registration and the actual captured advertised catalogue use the normalized map identity. Registry finalization and owned-registration retirement remain the same lifecycle.

Verified callers: the sole production withScalars caller is JNI registerFunctionsInternal, with full candidate validation before retainJvmFunctionCallbacks and a second owned candidate commit. registerScalar is called from that candidate builder. registerAggregate has no current production caller; its only repository callers are existing NativeUdfRegistry tests, so this unit protects the existing public API rather than inventing an aggregate JNI implementation. NativeUdfRegistry private maps have no external consumer. registerAllWithVelox prepares all signatures/factories before SDK mutation; appendRegisteredFunctionCatalog projects actual SDK snapshot names, not raw spec names. Fixed SDK FunctionRegistrySnapshot uses the same sanitizeName for captured lookup. Existing registry tests retain all old assertions and are in the closest selectors.

New tests mutate actual FILE/AI_COMPLETE descriptor names into case aliases and require admission rejection, then exercise whole withScalars batches with a valid earlier actual AI overload followed by a duplicate FILE alias or a later case-aliased AI overload with mismatched determinism/NULL policy. The original unpublished registry and actual global signatures/metadata remain unchanged. Aggregate preflight uses the already registered real built-in sum factory copied from the SDK entry (not an invented function implementation), never publishes it, and rejects a sum/SUM duplicate plus both directions of case-aliased scalar/aggregate collision; actual sum/FILE registrations remain unchanged. No fake callback owner or test provider is retained. No additional source paths are added to the 30-existing-file baseline: these changes touch already-owned NativeUdfRegistry h/cpp, JvmScalarFunction.cpp and JvmScalarFunctionMetadataTest.cpp in the tmp overlay only.
