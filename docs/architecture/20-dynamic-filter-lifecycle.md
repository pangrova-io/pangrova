---
title: Dynamic Filter Lifecycle
sidebar_label: Dynamic Filter Lifecycle
---

# Pangrova — Dynamic Filter Lifecycle

> **Status**: Reference documentation  
> **Related modules**: `pangrova-spi` (contract), `pangrova-planner` (generation), `pangrova-coordinator` (orchestration), `pangrova-connector-*` (consumption)
> **Related docs**: [03-query-execution.md](./03-query-execution.md), [04-query-optimization.md](./04-query-optimization.md)

## 1. Overview

Dynamic filters (also called runtime filters or dynamic partition pruning / DPP) allow the probe side of a hash join to skip reading data that the build side has already proven irrelevant. The build side collects join-key statistics (min/max range or bloom filter), and these statistics are pushed to probe-side scan operators to prune splits, row groups, or individual rows at I/O time.

This document describes the end-to-end lifecycle: when filters are created, how they are pushed to workers, how connectors consume them, and how resources are cleaned up.

### Industry context

| System | Mechanism | Scope |
|--------|-----------|-------|
| Trino | `DynamicFilter` SPI, coordinator-side collection, lazy split enumeration | Split + row-group |
| Spark | Dynamic Partition Pruning (DPP), Adaptive Query Execution (AQE) broadcast | Partition + file |
| Photon | Runtime bloom filters pushed into native scan | Row-group + row |
| StarRocks | Global runtime filter (min/max + bloom + IN-list) | Partition + row |

Pangrova follows the Trino model for the SPI surface and extends it with bloom filter support (inspired by Photon/StarRocks).


## 2. Lifecycle state machine

A dynamic filter follows a strict state progression defined by `DynamicFilter.State`:

```
  PENDING --> COLLECTING --> READY --> APPLIED --> COMPLETED
     |             |           |          |
     +-------------+-----------+----------+
                        |
                        v
                    CANCELLED
```

| State | Meaning | Who sets it |
|-------|---------|-------------|
| **PENDING** | Filter planned but build-side execution has not started. No filter data is available. | Coordinator (query setup) |
| **COLLECTING** | Build-side stage is running and accumulating join key values. Partial filter data may be available for early pruning. | Coordinator (on build-side stage start) |
| **READY** | Build-side stage has completed and the filter is fully collected. The predicate is final. | Coordinator (on build-side stage completion) |
| **APPLIED** | Filter has been delivered to all target probe-side tasks. Coordinator-side bookkeeping state. | Coordinator (after injection) |
| **COMPLETED** | Owning query has finished; filter resources released. | Coordinator (query teardown) |
| **CANCELLED** | Owning query was cancelled or failed; filter discarded. | Coordinator (on error/cancel) |

Terminal states (`COMPLETED`, `CANCELLED`) are absorbing -- once entered, no further transitions occur.


## 3. Phase 1: Creation (query planning)

### 3.1 Planner identification

During Calcite optimization, the planner identifies eligible join patterns:

1. An equi-join where one side (the "build side") is expected to produce a small or moderately sized result (heuristic or cost-based).
2. The probe-side column maps to a partition key, a sort key, or a column with file-level min/max statistics in the underlying storage (Iceberg/Paimon).

The planner annotates the physical plan with **runtime filter targets**: each target identifies the build-side plan node, the probe-side scan node, and the join column.

### 3.2 Filter instantiation

When the coordinator materializes the `StageDAG` for execution, it creates one `SettableDynamicFilter` per runtime filter target:

```java
// pangrova-coordinator: SettableDynamicFilter.java
SettableDynamicFilter filter = SettableDynamicFilter.create(
    "order_date",   // target column
    true            // awaitable = probe side should block
);
```

The filter starts in `PENDING` state. An optional timeout is configured via `filter.setTimeout(duration, scheduler)` so probe-side scans are not blocked indefinitely if the build side is slow.

### 3.3 Registration

Each filter is registered in the `RuntimeFilterRegistry` (keyed by target stage ID) so that `DynamicFilterBinder` can look it up at dispatch time.


## 4. Phase 2: Collection (build-side execution)

### 4.1 Build-side stage execution

When the build-side stage starts, the coordinator transitions the filter:

```
PENDING --> COLLECTING
```

As the build-side tasks execute on workers, they produce the join-key column data. When the build-side stage completes (all tasks `FINISHED`), the `RuntimeFilterStageHandler` processes the results.

### 4.2 Filter generation

`RuntimeFilterStageHandler` uses `RuntimeFilterGenerator` to create the filter from build-side results:

1. **MIN_MAX filter**: Extracts the minimum and maximum values of the build-side join column. Effective for range-partitioned or sorted probe-side data.
2. **BLOOM filter**: Builds a `SplitBlockBloomFilter` from the distinct join-key values. Effective for high-cardinality keys where range pruning is insufficient.

The generator applies selectivity thresholds -- if the build side produces too many distinct values (low selectivity), no filter is generated (the cost of evaluation would exceed the I/O savings).

### 4.3 State transition

On successful generation:

```java
filter.setMinMax(minValue, maxValue);  // transitions to READY
// or
filter.setBloom(bloomPredicate);       // transitions to READY
```

```
COLLECTING --> READY
```

If filter generation is skipped (low selectivity or error):

```java
filter.cancel();  // transitions to CANCELLED
```

```
COLLECTING --> CANCELLED
```

Transitioning to `READY` completes the `CompletableFuture` returned by `isBlocked()`, unblocking any waiting probe-side scans.


## 5. Phase 3: Delivery (coordinator to workers)

### 5.1 Binding to probe-side tasks

When the coordinator dispatches probe-side leaf-stage tasks, `DynamicFilterBinder.resolveFilters()` checks the `RuntimeFilterRegistry` for filters targeting the probe stage:

```java
// pangrova-coordinator: DynamicFilterBinder.java
List<DynamicFilterPayload> filters = DynamicFilterBinder.resolveFilters(
    stageId, queryId, runtimeFilterRegistry);
```

If a filter is available, it is serialized into a `DynamicFilterPayload` protocol message:

- **MIN_MAX**: Serialized as `column|min|max` (UTF-8 encoded).
- **BLOOM**: Serialized via `SplitBlockBloomFilter.toByteArray()` (custom binary format).

### 5.2 Task request injection

The serialized filter payloads are included in the `SubmitTaskRequest` sent to workers via `TaskRequestFactory`:

```java
TaskRequestFactory.createLeafTask(session, planBytes, splits,
    shuffleConfig, writeConfig, dynamicFilters, ...);
```

### 5.3 Late injection

For tasks already running when the filter becomes ready, the coordinator uses the `injectDynamicFilter` RPC:

```java
// pangrova-coordinator: RemoteTaskDispatcher.java
dispatcher.injectDynamicFilter(taskId, planNodeId, filterProto);
```

This sends an `InjectDynamicFilterRequest` to the worker's REST API.

### 5.4 Post-delivery state

After all probe-side tasks have received the filter:

```
READY --> APPLIED
```


## 6. Phase 4: Consumption (connector side)

### 6.1 Split manager

The `ConnectorSplitManager.getSplits()` method receives the `DynamicFilter` as a parameter:

```java
// pangrova-spi: ConnectorSplitManager.java
ConnectorSplitSource getSplits(
    ConnectorTransactionHandle transaction,
    ConnectorSession session,
    ConnectorTableHandle table,
    DynamicFilter dynamicFilter,
    Constraint constraint);
```

**Blocking behavior**: If `dynamicFilter.isAwaitable()` returns `true`, the split manager can call `dynamicFilter.isBlocked()` to obtain a `CompletableFuture` that completes when filter data is available. This allows the split enumeration to delay until the build side produces a filter, avoiding unnecessary I/O on partitions that would be pruned.

**Split-level pruning**: For partitioned tables (Iceberg/Paimon), the split manager uses `dynamicFilter.min()`, `dynamicFilter.max()`, and `dynamicFilter.mightContain(partitionValue)` to skip entire partitions or data files whose partition values fall outside the filter range.

### 6.2 Page source

The `ConnectorPageSourceProvider.createPageSource()` method also receives the filter:

```java
// pangrova-spi: ConnectorPageSourceProvider.java
ConnectorPageSource createPageSource(
    ConnectorTransactionHandle transaction,
    ConnectorSession session,
    ConnectorSplit split,
    ConnectorTableHandle table,
    List<ColumnHandle> columns,
    DynamicFilter dynamicFilter);
```

**Row-group pruning**: The page source uses file-level and row-group-level column statistics to skip row groups whose min/max values do not overlap the filter range.

**Row-level filtering**: For bloom filters, individual rows can be tested against `mightContain()` before being included in output batches.

### 6.3 No-op sentinel

When dynamic filtering is not applicable (no eligible joins, DPP disabled), the sentinel `DynamicFilter.ALL` is passed. It accepts all values, is always in `READY` state, and never blocks.


## 7. Phase 5: Cleanup

### 7.1 Normal completion

When the query completes successfully, the coordinator transitions all filters for the query:

```
APPLIED --> COMPLETED
```

The `SettableDynamicFilter` clears its internal state (predicate, min/max values, listener list). The `RuntimeFilterRegistry` entry is removed.

### 7.2 Query cancellation or failure

On query cancellation or failure:

```
(any state) --> CANCELLED
```

`cancel()` completes the blocked future (if still pending) to unblock any waiting scan operators. After cancellation, `mightContain()` returns `true` for all values (effectively disabling the filter).

### 7.3 Timeout

If the build side does not produce a filter within the configured timeout (`pangrova.optimizer.dynamic_filter_timeout`), the blocked future completes automatically, allowing probe-side scans to proceed without the filter. The filter may still transition to `READY` later and be applied opportunistically to subsequent split fetches.

### 7.4 Resource ownership

| Resource | Owner | Cleanup trigger |
|----------|-------|-----------------|
| `SettableDynamicFilter` instance | Coordinator | Query completion/cancellation |
| `RuntimeFilterRegistry` entry | Coordinator | Query completion/cancellation |
| Serialized `DynamicFilterPayload` | Worker task | Task completion |
| `CompletableFuture` (blocked) | `SettableDynamicFilter` | State transition to READY/CANCELLED |
| Bloom filter byte array | `RuntimeFilter` | GC after registry removal |


## 8. Thread safety

All `DynamicFilter` implementations must be thread-safe. The SPI contract documents this requirement explicitly:

- The filter may be read by multiple scan threads concurrently.
- The coordinator updates state from a separate thread.
- `SettableDynamicFilter` uses `AtomicReference` for state transitions and `CopyOnWriteArrayList` for listeners.
- The blocked future is replaced atomically.


## 9. Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `pangrova.optimizer.dynamic_filter.enabled` | `true` | Enable/disable dynamic filter generation |
| `pangrova.optimizer.dynamic_filter.timeout` | `10s` | Maximum time probe side waits for build side |
| `pangrova.optimizer.dynamic_filter.max_distinct_values` | `10000` | Selectivity threshold for bloom filter generation |
| `pangrova.optimizer.dynamic_filter.bloom_fpp` | `0.01` | False positive probability for bloom filters |


## 10. Observability

| Metric | Type | Source | Description |
|--------|------|--------|-------------|
| `pangrova.optimizer.dynamic_filter.generated` | counter | RuntimeFilterStageHandler | Filters successfully generated |
| `pangrova.optimizer.dynamic_filter.timeout` | counter | SettableDynamicFilter | Filters that hit the wait timeout |
| `pangrova.dynamic_filter.rows_filtered` | counter | ConnectorPageSource | Rows eliminated by dynamic filters |
| `pangrova.dynamic_filter.splits_pruned` | counter | ConnectorSplitManager | Splits skipped via filter pruning |


## 11. Key source files

| File | Role |
|------|------|
| `pangrova-spi/.../connector/metadata/DynamicFilter.java` | SPI contract with lifecycle state machine |
| `pangrova-coordinator/.../adaptive/SettableDynamicFilter.java` | Mutable coordinator-side implementation |
| `pangrova-coordinator/.../adaptive/DynamicFilterBinder.java` | Serializes filters into protocol messages |
| `pangrova-coordinator/.../handler/RuntimeFilterStageHandler.java` | Generates filters from build-side results |
| `pangrova-planner/.../runtimefilter/RuntimeFilterGenerator.java` | Min/max and bloom filter construction |
| `pangrova-planner/.../runtimefilter/RuntimeFilterRegistry.java` | Per-query filter storage |
| `pangrova-connector-iceberg/.../IcebergSplitManager.java` | Iceberg split-level pruning |
| `pangrova-connector-iceberg/.../IcebergPageSourceProvider.java` | Iceberg row-group pruning |
| `pangrova-connector-paimon/.../PaimonSplitManager.java` | Paimon split-level pruning |
| `pangrova-connector-paimon/.../PaimonPageSourceProvider.java` | Paimon row-group pruning |
