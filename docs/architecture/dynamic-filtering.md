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
| Explicit filter binding | A caller supplies a typed `RuntimeFilterBinding` and an exact target plan node | The native task parser validates initial bindings. The same typed binding works at startup and while a task runs. The sole-scan selector requires precisely one Native table scan. A named selector must match a Native scan ID. |
| Coordinator-generated cross-stage pruning | A bounded INNER Join build with eligible BIGINT equalities and a pinned Iceberg probe scan | The Coordinator uses the elected Native build receipts and committed Exchange when enumerating the probe. The probe uses the original constraints when build evidence is unavailable at enumeration. |

## Planning and execution

For a bounded static case the HEP literal-Join rule reads at most 128 rows from an integer VALUES build input. It traces integer keys through deterministic projections, including a cast that preserves the declared integer width. It adds an exact membership predicate to the probe table scan for INNER and SEMI equality. HEP leaves literal relations to this rule because transitive inference can keep re-inserting filters for nullable probe columns. The original Join retains duplicates. NULL keys do not satisfy ordinary equality, and preserved outer rows are never restricted. This predicate belongs to the compiled plan and does not use a runtime registry.

The protocol carries typed range endpoints and bloom bytes with their value encoding. `TaskRequestParser` validates incoming bindings and `PlanExecutionDriver` installs accepted initial filters. SQL query results can demonstrate Join correctness. Evidence for fewer scanned rows must come from the scan operator.

The Worker HTTP runtime-filter endpoint accepts a single `TaskRuntimeFilterDelivery`
Protobuf message with a 64 KiB body limit and up to four typed row predicates.
It resolves the full `TaskAttemptId`, including the Worker incarnation, against
the running local admission. HTTP 409 identifies a mismatched or unavailable
attempt. HTTP 204 acknowledges that the Native owner accepted the command.
The scan operator provides separate evidence that the predicate took effect.
The Worker passes the validated binding to JNI without decoding a nested payload.
Task startup and online delivery share `RuntimeFilterBinding`.

## Published Shuffle build with a local probe

A broadcast Join can receive its build records from another stage and scan the probe table on the consuming task. `BroadcastJoinRule` wraps its right input in a broadcast exchange. `PlanFragmenter` binds the upstream stage, downstream stage, exchange ID and complete-read requirement. A stage that also scans a local table currently runs at parallelism one. `VeloxTaskRunner` consumes only sealed exchange bindings with the expected source partition count. It feeds the fetched records to the actual hash build. The task then pushes an eligible predicate to its own probe scan. The filter lives with that task and never crosses attempts.

`NativeExchangeDataPlaneE2ETest.PublishedBuildFiltersTheLocalJoinProbe` commits a Direct partition with keys 2, 4, 4, unregisters its producer, and reads the partition through loopback Flight under its epoch, producer attempt and fence. The actual probe scan returns two of its four rows. The Join outputs three rows, preserving the duplicate build key. `HashJoinScanFilterTest` records the producing Join plan node and the scan output for both INNER and LEFT joins over two Parquet sources.

When the Coordinator selects REMOTE Shuffle, the fragmenter uses keyed hash
exchange for eligible equi-joins. This applies to selective builds and to
physical broadcast joins selected by the optimizer. Each input uses its own
join keys, and the hash join retains the complete original condition.

## Pruning a probe scan in another stage

`PlanFragmenter` selects a bounded build on the physical right side of an
eligible INNER Join. It can use up to four required BIGINT equalities from
one conjunction when the probe operands resolve to distinct physical fields
of the same Iceberg scan. The binding identifies the build stage, its Exchange,
the sorted build output ordinals and the selected Iceberg snapshot and schema.
Stable connector field IDs identify the probe fields.

Each successful elected Native build task reports up to 16 disjoint inclusive
ranges per requested output ordinal. A completed ordinal with only NULL keys
has an empty domain. The Coordinator can activate up to four independent stage
pairs per live query, prioritizing the estimated bytes and file count of each
probe. The pairs share one 200 ms window after initial source scheduling. Each
probe waits independently for its complete winning build cohort and committed
Exchange. The Coordinator unions ranges across tasks per ordinal and uses one
enclosing interval when the range limit is exceeded.
Missing summaries and incomplete cohorts keep the original scan constraints.
Complete evidence received after the wait deadline can still prune a probe
that has yet to enumerate splits. Only the winning attempts can authorize
pruning.

Iceberg applies the intersection of the per-field domains and the original
predicates before enumerating splits. Each field domain allows any value in
its range union. Inclusive file statistics keep any file that might match.
The Worker evaluates the complete Join condition on the rows, preserving
build key correlation, duplicates and SQL NULL semantics. SQL tests cover
results, actual scanned splits and bytes, and physical release.

If the original split enumeration had no complete build evidence when it began,
the Coordinator may offer the completed domains to the same probe generation
after its Worker reports RUNNING. Build completion examines the active tasks
in memory. A subsequent RUNNING report can deliver the same binding if the build
finished first. The Coordinator uses the endpoint and Worker incarnation captured
with the exact issued attempt. The path adds no catalog or journal queries. Each
attempt receives at most one offer, with a Coordinator-wide limit of 64 in-flight
RPCs and a bounded build-delivery queue.

A set of singleton intervals is sent as a typed membership predicate. For a
multivalued disjoint domain the enclosing bounds retain every eligible row. The
Native connector observes the binding before subsequent scan batches and can
bind it before opening a later eligible reader. Object reads already issued
remain part of the profile.

Additional key types, connector coverage, filters sharing a build or probe
stage, and production load evidence remain in the
[query performance plan](../development/query-performance-parity-plan.md).
