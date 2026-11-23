---
title: "Dynamic Filter Lifecycle"
description: "Static and runtime join filters, published-build behavior, and remaining cross-stage design"
sidebar_label: "Dynamic Filter Lifecycle"
---

# Dynamic filters

Pangrova applies static and runtime Join predicates at different points in query execution.

| Path | Source and scope | Published behavior |
|---|---|---|
| Native hash Join | Velox build operator and probe scan in the same native task | When the complete build hash table is available, HashProbe can push an exact domain or bounded Bloom filter through the Velox driver to the corresponding scan ordinal. `ColumnAssignmentDataSource` evaluates the received filter on projected rows, including aliased columns, across source formats. Velox checks Join type and spill state before publishing. Scan output rows and producer plan node IDs identify the effect. Underlying file reads can still occur. |
| Explicit filter binding | A caller supplies a typed `RuntimeFilterBinding` and an exact target plan node | The native task parser validates initial bindings. Runtime commands can inject a filter into an executing task. These consumers do not create a source of build-side values. |
| Coordinator-generated cross-stage pruning | A bounded build of one INNER BIGINT equality Join and a pinned Iceberg probe scan | The Coordinator uses the elected Native build receipts and committed Exchange before enumerating the probe. The probe uses the original constraints when build evidence is unavailable by the deadline. |

## Planning and execution

For a bounded static case the HEP literal-Join rule reads at most 128 rows from an integer VALUES build input. It traces integer keys through deterministic projections, including a cast that preserves the declared integer width. It adds an exact membership predicate to the probe table scan for INNER and SEMI equality. HEP leaves literal relations to this rule because transitive inference can keep re-inserting filters for nullable probe columns. The original Join retains duplicates. NULL keys do not satisfy ordinary equality, and preserved outer rows are never restricted. This predicate belongs to the compiled plan and does not use a runtime registry.

The protocol carries typed range endpoints and bloom bytes with their value encoding. `TaskRequestParser` validates incoming bindings and `PlanExecutionDriver` installs accepted initial filters. SQL query results can demonstrate Join correctness. Evidence for fewer scanned rows must come from the scan operator.

## Published Shuffle build with a local probe

A broadcast Join can receive its build records from another stage and scan the probe table on the consuming task. `BroadcastJoinRule` wraps its right input in a broadcast exchange. `PlanFragmenter` binds the upstream stage, downstream stage, exchange ID and complete-read requirement. A stage that also scans a local table currently runs at parallelism one. `VeloxTaskRunner` consumes only sealed exchange bindings with the expected source partition count. It feeds the fetched records to the actual hash build. The task then pushes an eligible predicate to its own probe scan. The filter lives with that task and never crosses attempts.

`NativeExchangeDataPlaneE2ETest.PublishedBuildFiltersTheLocalJoinProbe` commits a Direct partition with keys 2, 4, 4, unregisters its producer, and reads the partition through loopback Flight under its epoch, producer attempt and fence. The actual probe scan returns two of its four rows. The Join outputs three rows, preserving the duplicate build key. `HashJoinScanFilterTest` records the producing Join plan node and the scan output for both INNER and LEFT joins over two Parquet sources.

## Pruning a probe scan in another stage

`PlanFragmenter` selects a bounded build on the physical right side of an
eligible INNER equality Join on one BIGINT key. It binds the physical build
stage, its broadcast Exchange, and the exact probe column on the admitted
Iceberg table, snapshot and schema. The stable connector field ID identifies
the probe column.

Each successful elected Native build task reports up to 16 disjoint inclusive
ranges. A successful build that sees only NULL keys publishes an empty domain.
The Coordinator waits up to 200 ms for the complete winning cohort and its
Exchange publication. It merges the ranges, with one enclosing interval when
the range limit is exceeded. A missing, late, or incomplete receipt keeps the
original scan constraints. Retired attempts cannot authorize pruning.

Iceberg combines the retained range disjunction with the original predicates
before enumerating splits. Inclusive file statistics keep files that might
match. The Worker still evaluates the Join on the rows and preserves duplicate
build keys. SQL tests cover the exact results, actual scanned splits and bytes,
and physical release for both single-task and multi-task build cohorts.

Additional key types, multi-key predicates, connector coverage and late
predicates for work that is still unsigned remain in the
[query performance plan](../development/query-performance-parity-plan.md).
