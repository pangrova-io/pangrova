---
title: "Dynamic Filter Lifecycle"
sidebar_label: "Dynamic Filter Lifecycle"
---

# Dynamic filters

Pangrova applies static and runtime Join predicates at different points in query execution.

| Path | Source and scope | Published behavior |
|---|---|---|
| Native hash Join | Velox build operator and probe scan in the same native task | When the complete build hash table is available, HashProbe can push an exact domain or bounded Bloom filter through the Velox driver to the corresponding scan ordinal. `ColumnAssignmentDataSource` evaluates the received filter on projected rows, including aliased columns, across source formats. Velox checks Join type and spill state before publishing. Scan output rows and producer plan node IDs identify the effect. Underlying file reads can still occur. |
| Explicit filter binding | A caller supplies a typed `RuntimeFilterBinding` and an exact target plan node | The native task parser validates initial bindings. Runtime commands can inject a filter into an executing task. These consumers do not create a source of build-side values. |
| Coordinator-generated cross-stage pruning | The planner can identify candidate equijoins. A query still needs a complete build-side summary and the corresponding physical probe scan. | Distributed SQL does not automatically collect and send a build-side predicate. Splits are enumerated with `DynamicFilter.ALL`. |

## Planning and execution

When enabled, `JoinRuntimeFilterPlanner` describes INNER and SEMI equijoins. A LEFT join must keep unmatched probe rows. A predicate derived from the right build side cannot prune its preserved left input. Logical descriptors use the right input of the rewritten Join as build and the left input as probe. They record matching integer or VARCHAR key types. Physical reorderings change the inputs themselves. Statistics alone cannot switch the source of runtime keys. They never authorize filtering by themselves.

For a bounded static case the HEP literal-Join rule reads at most 128 rows from an integer VALUES build input. It traces integer keys through deterministic projections, including a cast that preserves the declared integer width. It adds an exact membership predicate to the probe table scan for INNER and SEMI equality. HEP leaves literal relations to this rule because transitive inference can keep re-inserting filters for nullable probe columns. The original Join retains duplicates. NULL keys do not satisfy ordinary equality, and preserved outer rows are never restricted. This predicate belongs to the compiled plan and does not use a runtime registry.

The protocol carries typed range endpoints and bloom bytes with their value encoding. `TaskRequestParser` validates incoming bindings and `PlanExecutionDriver` installs accepted initial filters. SQL query results can demonstrate Join correctness. Evidence for fewer scanned rows must come from the scan operator.

## Published Shuffle build with a local probe

A broadcast Join can receive its build records from another stage and scan the probe table on the consuming task. `BroadcastJoinRule` wraps its right input in a broadcast exchange. `PlanFragmenter` binds the upstream stage, downstream stage, exchange ID and complete-read requirement. A stage that also scans a local table currently runs at parallelism one. `VeloxTaskRunner` consumes only sealed exchange bindings with the expected source partition count. It feeds the fetched records to the actual hash build. The task then pushes an eligible predicate to its own probe scan. The filter lives with that task and never crosses attempts.

`NativeExchangeDataPlaneE2ETest.PublishedBuildFiltersTheLocalJoinProbe` commits a Direct partition with keys 2, 4, 4, unregisters its producer, and reads the partition through loopback Flight under its epoch, producer attempt and fence. The actual probe scan returns two of its four rows. The Join outputs three rows, preserving the duplicate build key. `HashJoinScanFilterTest` records the producing Join plan node and the scan output for both INNER and LEFT joins over two Parquet sources.

## Pruning a probe scan in another stage

A distributed publisher needs all of the following before it can prune safely.

1. Bind both physical Join inputs and the exact scan node and column. Leaf enumeration order has no Join semantics, particularly for nested joins, reordered inputs and repeated column names.
2. Collect a complete typed summary from every selected build partition and producer attempt. Native task statistics currently report operator timing, row counts and bytes. They do not report the Join-key domain. Partial data, failed attempts and estimates cannot authorize removal of probe rows.
3. Bind publication and reads to the same query, exchange generation and winning attempt set. An empty *complete* domain must be distinguishable from an absent summary. An optional optimization with missing evidence leaves the scan unpruned.
4. Release collection state on success, cancellation and failure. A late result from a retired attempt must not re-create it.

The mutable Connector filter, stage-completion harness and deposited-value collector are test fixtures. Their tests supply build values explicitly and exercise the typed payload and Worker endpoints. They do not represent a distributed publisher or an enabled wait timeout, counter or configuration setting.

For a future repartitioned Join, implement that boundary in the following order.

| Owner | Change and acceptance case |
|---|---|
| Planner | For an INNER or SEMI equijoin, derive build and probe exchange IDs from the rewritten inputs of the same Join. Trace each key ordinal through ordinal-preserving projections to one exact probe scan. A transform, ambiguity, preserved side or unavailable lineage leaves that input unrestricted. Test nested joins with repeated column names. |
| Native producer | Bind an optional summary request to the build exchange output. Accumulate eligible key values from the rows actually emitted by that producer, with explicit type, value and byte caps. Report completion only after the entire output commits. Empty, over-budget and failed inputs have distinct outcomes. |
| Shuffle publication and Query | Bind summary digests to exact producer attempts and the accepted publication. Verify every expected producer and partition. Discard data after failed or obsolete attempts. Aggregate only complete compatible domains. Unknown publication results require read-back before using the predicate. A timeout starts the probe unrestricted. |
| Execution binding and scan | Send the aggregate through the canonical task request with its exact query, exchange generation, scan node, projected ordinal and type. Apply it once per intended attempt. Multiple probe drivers and splits must all observe the accepted predicate before claiming source pruning. |
| Scheduling and resources | Account the collector against a query budget and give the query owner cancellation and close authority. Never introduce a blocking build/probe cycle. Test late arrivals after cancellation, a speculative loser with extra keys, partial partitions, explicit empty partitions and mixed receipts before enabling a new combination. |
