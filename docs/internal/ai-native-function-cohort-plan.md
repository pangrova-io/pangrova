# Actual Function Catalogue → Query Cohort → Canonical Task: Atomic Implementation Specification

## Status and scope

**NOT IMPLEMENTED / NOT ACCEPTED.** This specification was checked against
`058fb13a2f260125f2f9c7a2289550c3442a1e05` on 2026-10-07. It records inspected
production sources, not an inference from the older roadmap. No production code,
protocol generation, SDK build, Maven test, CTest, or benchmark was run for this
document. Historical failures below remain historical until reproduced on the
current frozen artifact cohort.

Scope is database infrastructure only. The Coordinator binds, authorizes,
admits, schedules and fences control metadata; Native Workers execute values.
There is one canonical v2 protocol, one actual Native registry publication,
task-local Velox vectors and Arrow interoperability. Agent orchestration and
Usage Ledger are outside the product scope, not deferred dependencies.

## 1. Verified implementation ledger

Paths below are repository-relative; external SDK paths are relative to the
configured SDK source root. Line numbers identify this audit snapshot.

| Boundary | Inspected evidence | Current state |
|---|---|---|
| Actual Native publication | `pangrova-worker-native/src/capability/FunctionCatalogAssembler.cpp:147–165` assembles the owned snapshot and seals its manifest. `src/jni/command/NativeCommandDispatcher.cpp:515–542` finalizes and returns the same published inventory. | Implemented source path; not a new acceptance result. |
| Worker advertisement | `pangrova-worker/src/main/java/org/pangrova/worker/jni/NativeCapabilityLoader.java:21–78` performs finalize/read/validation. `bootstrap/WorkerBootstrap.java:151` preserves encoded inventory; `bootstrap/phase/WorkerHeartbeatBootstrap.java:100` sends it. | Actual inventory wired, not a static function-name list. |
| Authenticated registration | `pangrova-coordinator/src/main/java/org/pangrova/coordinator/http/endpoint/WorkerResource.java:103–152,288–295` authenticates registration, decodes inventory and creates WorkerInfo. `worker/WorkerInfo.java:36,53–55` and `WorkerManager.java:398,429` retain the complete inventory. | Implemented. A content digest is not authentication or an incarnation fence. |
| Frozen query authority | `query/execution/dispatch/dag/DefaultWorkerSelector.java:56,104,115,162,197,235` reads current active workers for selection/model/retry. | Missing query-local catalogue/cohort. |
| Planner actual consumer | `pangrova-planner/src/main/java/org/pangrova/planner/SqlCompiler.java:43,49` installs static defaults; `forSession:96–106` captures metadata but no function catalogue. `pangrova-protocol/src/main/java/org/pangrova/protocol/function/catalog/FunctionCatalogView.java:38–140` already has immutable actual binding APIs. | No Planner/Coordinator production consumer of FunctionCatalogView. |
| Final fragment requirements | `pangrova-coordinator/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/binding/DAGStagePlanBinder.java:46–76` overlays/retrieval/model-binds after serialization. `dispatch/dag/TaskRequestFactory.java:238–269` writes plan/checksum, not requirements. | No field18 writer. |
| Native requirement admission | `pangrova-worker-native/src/jni/command/TaskRequestParser.cpp:1556–1717` parses canonical requests but never reads function_requirements. | No field18 reader or exact call-coverage comparison. |
| Actual Native call binding | `src/capability/RegisteredFunctionBinding.cpp:54–86` binds scalar/raw aggregate/window through the owned snapshot. | Actual binding exists; it does not supply the missing query/task contract. |

Reproduce the missing-consumer observation with source-only searches:

```sh
rg -n 'FunctionCatalogView' pangrova-planner/src/main pangrova-coordinator/src/main
rg -n 'function_requirements|getFunctionRequirements|setFunctionRequirements' \
  pangrova-protocol/src/main pangrova-planner/src/main pangrova-coordinator/src/main \
  pangrova-worker/src/main pangrova-worker-native/src
rg -n 'TaskRequest\.newBuilder' --glob '*.java' --glob '!**/target/**' .
```

The first search has no consumer; the second finds only `task.proto:98`.
The sole production Java TaskRequest builder is TaskRequestFactory. Test fixture
builders are not alternate production producers, but must migrate with admission.

## 2. Proven defects and their boundaries

### ROUND and nested expressions

`validation/NativeCapabilityRegistry.java:50–110` is a manually populated name
allowlist. `SubstraitPlanValidator.java:61–67,117–170,217–237` does not admit or
descend Nested.Struct/List and does not visit aggregate measure call arguments.
Native already converts those constructors; the producer is not missing.
`validation/RoundSqlFunction.java:31–47` independently computes DECIMAL output
instead of consuming the actual selected signature.

The retained `reviewed-golden-planner-unfiltered-r2` log, lines 23420–23450,
contains three errors from
`CanonicalExpressionSqlProtocolTest#roundUsesNativeDecimalReturnParametersAndDeclaredIntegerPosition`
and one from `#runtimeRowArrayUsesCanonicalNestedConstructorsWithExactDecimalAndNullability`:
unsupported `round` and `NestedList`. These are genuine historical admission
failures, not current-run results. Do not fix them with allowlist additions.

### Aggregate phase advertisement exceeds the execution path

`FunctionCatalogAssembler.cpp:54–63,81` advertises all four aggregate phases.
`SubstraitAggregateConversion.cpp:68–72` rejects state-input phases because it
lacks authoritative raw inputs. `AggregateConverter.java:168,251` emits only
INITIAL_TO_RESULT. A registered aggregate signature therefore does not prove
distributed partial/final execution. Preserve raw DECIMAL parameters, named-row
types and selected signature through partial/shuffle/final; never infer them
from an opaque VARBINARY state. Existing state-phase rejection must not be removed
until this authority is actually consumed and verified.

### Temporal execution values do not establish Arrow correctness

The inspected SDK has base revision
`3da0390118fa4b0addbfbdeb03f438ec1b1237a6` plus the configured repository patch
inputs. The base revision alone is not archive provenance.

* Sole `pangrova-protocol/src/main/resources/function-contracts/sql-bindings.json:18–20`
  maps TIME precision 0–3 to TIME and 4–6 to TIME_MICRO_UTC. Native
  `SubstraitLiteralConversion.cpp:197–201` preserves the corresponding units.
* SDK `velox/type/Type.h:1722–1723` makes isTime() true for both types. SDK
  `velox/vector/arrow/Bridge.cpp:312–317,461–480,818–855` nevertheless exports
  all TIME values as time32/millisecond, narrowing to int32 and asserting TIME
  rather than the microsecond type. Import `Bridge.cpp:1564,2137–2219` restores
  TIME and divides micro/nanoseconds into milliseconds. TIME(6) can therefore
  lose precision or overflow across the actual bridge.
* Planner `LiteralConverter.java:168–194` and Native
  `SubstraitLiteralConversion.cpp:41–58,193–196` preserve timestamp precision
  and negative-epoch normalization. SDK `Bridge.h:39–40` defaults to nano/no
  timezone; `ArrowBatchVectorSerde.cpp:110–128` uses those defaults.
  `CanonicalArrowSchema.cpp:37–72,88–105` changes nested labels/nullability,
  not scalar units/timezones. `StorageLogicalTypeContract.cpp:58–99` requires
  declaration-matching timestamp unit/timezone and TIME unit/width.

These are inspected source defects/gaps, **not newly executed failure or copy-cost
measurements**. Existing `SubstraitTimePrecisionTest` tests conversion to a
ValuesNode, not Arrow export/import. It cannot certify shuffle or client values.
Fix mixed columns and nested fields per canonical declaration; a process-global
timestampUnit or relabelling unchanged buffers is insufficient.

### Independent public frontend blocker

`pangrova-parser/src/main/antlr4/org/pangrova/parser/PangrovaSql.g4:189,198`
accepts derived setQuery, not derived VALUES/VALUES ROW or column aliases.
The sole Calcite frontend migration is tracked separately in
`ai-native-single-calcite-frontend-implementation.md`. Do not rewrite legitimate
purpose SQL into an easier query or count early parser rejection as a successful
model-purpose negative test.

## 3. Complete atomic implementation cut

All parts below must be source-ready together before production admission is
switched. Existing `capability.proto:494–523` and `task.proto:98` already provide
requirements; do not create a vector/private feature protocol or generated-source
edits. Absence of requirements is never a compatibility mode.

1. **Capture once before compilation.** From authenticated actual WorkerInfo,
   capture an immutable same-manifest catalogue and eligible Worker identities.
   Use homogeneous manifest identity for this cut; never union two incomplete
   worker catalogues into synthetic execution authority. No eligible Worker is
   an explicit failure. Content hash does not replace authenticated registration,
   incarnation, current fencing or authorization.
2. **Bind the query compiler explicitly.** PlannerWiring provides a factory;
   CompilationPhase and direct DML compilation supply the actual query view.
   `SqlCompiler.forSession`, PangrovaPlanner and the SQL operator table consume
   the same view for binding, return inference and final plan validation.
   Delete static default admission and independent ordinary-function formulas.
   SQL implicit-cast policy remains explicit Planner policy; registry binding
   must not invent casts or select an overload by its declared return type.
3. **Retain one execution scope.** QueryExecutionContext, DAGQueryContext and
   DAGExecutionState carry the immutable contract. Every WorkerSelector entry,
   retry callback and SpeculativeTaskLauncher filters the frozen set and checks
   current inventory identity. Cache keys/entries and AQE use the same view.
   Losing all compatible Workers fails clearly; a new inventory cannot silently
   replace query authority. Resource/load signals may change without changing it.
4. **Collect the last IR, completely.** Reuse SubstraitRelationLayout,
   SubstraitExpressionTypes and FunctionCatalogView, with descriptor-checked
   field-number/repeated-index paths from the checksum-pinned generated Plan.
   Include nested children, measure arguments/filter/sorts, window children,
   and implicit Native calls such as row/array constructors. Add missing intrinsic
   definitions in the sole binding declaration, not a private function-name list.
   Preserve full DFS argument names, constants, options, invocation, phase,
   window process and actual raw aggregate provenance.
5. **Define selected-entry identity once.** Scalar actual entry hashes already
   exist (`RegisteredFunctionCatalog.cpp:38–64`). Ordered aggregate/window
   selection must retain registration identity, exact signature and ordinal,
   including duplicates. A shared canonical encoding/hash specification with
   cross-language golden tests supplies field13; two independently handwritten
   digest formulas or name-level hashes are not sufficient.
6. **One final task writer.** Collect after overlay, retrieval, governed binding,
   AQE, and ArtifactPartitionBindings rewrites. BoundExecutionPlan and
   LeafTask/DmlWriteTask/CanonicalTaskSpec carry the bound requirements.
   TaskRequestFactory.canonicalRequest writes field18 for ordinary leaf,
   intermediate/root and DML. DurablePartitionRequest.prepare uses that same
   factory. Zero-function fragments still have an explicit complete envelope.
7. **Durable authority survives replay.** ArtifactStageGraph canonical storage,
   its codec/store and all artifact/export/generated-value submitters persist
   the catalogue identity and original provenance with immutable graph templates.
   A partition rewrite recomputes requirements against that retained authority,
   not today's mutable worker inventory. Claim/retry preserves issued fencing
   and current authorization; catalogue content alone cannot authorize replay.
8. **Verify before resources execute.** CanonicalTaskRequests admission, Worker
   admission, Native TaskRequestParser/config and converter verify manifest SHA,
   fragment checksum, path coverage and each exact selected binding against the
   same published owned registry. Reject absent/extra/duplicate calls, detached
   references, stale view, wrong constants/options/output/state. State phases
   consume validated field12 and bind actual raw signatures; factory execution
   still uses the owned registry. Do not add global-registry fallback.
9. **Close the physical type boundary.** Version the SDK bridge change through
   the existing patch/provenance/build mechanism. Support canonical per-field
   temporal units/timezone, nested fields and actual TIME_MICRO_UTC import/export.
   Preserve nulls/encoding/ownership and explicitly account for required copies.
   Do not ship only Java writer, only Native rejection or a stale packaged JNI.

## 4. Builtin, callback and model admission are distinct

`NativeCapabilityInventory.cpp:84–104` publishes AI/rerank capability only when
the actual snapshot contains its callback registration.
`NativeCapabilityAdvertisement.java:59–80` separately obtains configured models
from ConfiguredAiProviderRegistry. `AiInvocationOperatorBinder.java:67–95`
resolves the exact model/output and resource envelope; WorkerCapabilityMatcher
checks operator/artifact/model contracts. Preserve all of these checks.

The frozen cohort must satisfy function selected-entry requirements **and**
operator/artifact requirements **and** exact model/output/constraints and user
authorization. A builtin function manifest neither grants model access nor proves
provider availability, Secret resolution, storage readiness or successful inference.
Governed hidden arguments and rewritten functions require precise contract-aware
collection; they must not become blanket name exemptions.

## 5. Executable acceptance plan

Root owns builds. The following selectors name existing tests; their presence
does not imply execution, discovery counts or PASS. Use JDK17/Maven3.9+,
full-regression/coverage, current packaged JNI and actual source-matching SDK.

| Boundary | Existing original selectors / limitation |
|---|---|
| Historical public admission failures | `CanonicalExpressionSqlProtocolTest#roundUsesNativeDecimalReturnParametersAndDeclaredIntegerPosition` and `#runtimeRowArrayUsesCanonicalNestedConstructorsWithExactDecimalAndNullability` reproduce the retained ROUND/NestedList failures. Preserve all existing type/NULL/CAST assertions. |
| Public purpose/frontend | `AiEmbeddingPurposeSqlProtocolTest`; its legitimate VALUES input must parse before purpose assertions count. |
| Actual Java binder | `FunctionCatalogViewTest,FunctionImplementationSelectorTest,FunctionSignatureBinderTest,FunctionPhysicalTypeProjectorTest`; component tests only. |
| Task/cohort consumers | `TypedCapabilityPlanBindingTest,TaskRequestEpochStampingTest,TaskRequestResultColumnTypeTest,TaskRequestFactoryPropertyTest,WorkerCapabilityMatcherTest,SubmitTaskRequestConverterFieldPreservationTest`; add missing formal-consumer tests, not weaker fixtures. |
| Startup | `NativeCapabilityLoaderTest,WorkerBootstrapCapabilityAdvertisementTest,NativeCommandGetCapabilitiesTest,FunctionRegistryOwnedSnapshotTest,ParseTaskConfigTest`. |
| Native actual values | `RegisteredFunctionBindingTest,CanonicalNestedExecutionTest,CanonicalAggregateExecutionTest,DistinctAggregateWindowExecutionTest`. Add true partial/shuffle/final raw-provenance execution. |
| Temporal baseline, not bridge proof | `LiteralConverterTimestampTest,TypeMapperTest,SubstraitTimestampConversionTest,SubstraitTimePrecisionTest,BatchVectorSerdeEquivalenceTest`. Existing selectors do not contain the missing mixed TIME(6)/timestamp bridge oracle. |
| Public Worker/JNI and result | `SourceFreeNativeWorkerFixtureIntegrationTest`, relevant SQL/provider E2E and `ActivationSmokeE2ETest`; distinguish in-process/routing checks from independently deployed multi-worker evidence. |

Before switching admission add real negative/positive oracles for: explicit empty
envelope; missing/stray/duplicate call path; wrong kind/URN/hash/view/selected
ordinal; detached nested names; constants/options tampering; state/raw mismatch;
last-rewrite checksum; worker half-inventories; retry/speculation inventory drift;
durable restart/replay; authenticated callback/model refusal. No fake successful
provider response, disabled assertion, private reflection or runtime-unavailable
acceptance may substitute for the actual boundary.

Temporal roundtrips must include precision0/3/6 TIME, midnight/day-end and
microseconds exceeding int32; mixed timestamp0/3/6/9 and zoned/unzoned columns;
negative epochs, NULL, nested ROW/LIST/MAP and dictionary/constant encodings.
Exercise canonical Arrow export → IPC → import → next Native stage → final client
values, plus storage/export paths. Record per-boundary copied bytes, allocations,
CPU and peak retained memory. No copy or scalability measurement exists for this
new unit yet; production throughput/latency requires the target deployment.

After nearest checks pass, run affected owning suites and fresh same-source
Native/package/Java/E2E/smoke integration; memory/ownership/JNI/concurrency changes
also require matching ASan. Full production regression and independent-process
scale/SLO checks remain CI/release acceptance gates. Record actual commands,
environment, discovered counts and PASS/FAIL/SKIPPED/BLOCKED. A source plan or a
component PASS cannot certify all features, future correctness or production scale.
