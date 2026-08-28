---
title: "JNI Bridge"
sidebar_label: "JNI Bridge"
---

# Pangrova — JNI Bridge

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

## 1. Protocol Boundary Contract

### 1.1 Dual Proto Design

Pangrova maintains two separate Protobuf definition files for the Java and Native (C++) layers:

| Proto File | Location | Package | Owner |
|------------|----------|---------|-------|
| Java-side | `pangrova-protocol/src/main/proto/pangrova/protocol/v1/` | `pangrova.protocol.v1` | Java team |
| Native-side | `pangrova-worker-native/proto/pangrova_native_protocol.proto` | `pangrova.native.protocol.v1` | Native team |

The dual proto design exists because:

1. **Different build systems**: Java uses Maven + protobuf-maven-plugin; C++ uses CMake + protoc.
2. **Different consumers**: Java proto generates Java classes used by Coordinator and Worker Java layers; Native proto generates C++ classes used by the Velox execution engine.
3. **Different evolution cadence**: Java-side messages (REST DTOs, Flight tickets) evolve with the control plane; Native-side messages evolve with the execution engine.

### 1.2 Boundary Contract

The single serialization boundary between Java and Native is the JNI bridge in `pangrova-worker`:

```
Java Layer                          JNI Bridge                    C++ Layer
─────────────────────────────────────────────────────────────────────────────
TaskRequestBuilder.build()    →   nativeCreateTaskExecutor()  →  TaskRequestParser
  (Java proto serialization)        (byte[] transfer)             (Native proto deserialization)

StatusSerializer              ←   nativeGetTaskStatus()       ←  TaskExecutor.state()
  (Native proto serialization)      (byte[] transfer)             (C++ state)
```

**Critical rule**: The byte stream produced by `TaskRequestBuilder` (using Java proto) must be correctly parseable by `TaskRequestParser` (using Native proto). This requires **field-number-level alignment** between the two proto definitions for all messages that cross the JNI boundary.

### 1.3 Messages That Cross the JNI Boundary

| Message | Direction | Alignment Required |
|---------|-----------|-------------------|
| `TaskRequest` | Java → Native | YES — field numbers must match |
| `SplitInfo` | Java → Native | YES — field numbers must match |
| `WriteConfig` | Java → Native | YES — field numbers must match |
| `ResourceLimit` | Java → Native | YES — field numbers must match |
| `NativeInitConfig` | Java → Native | YES — field numbers must match |
| `TaskStatus` / `TaskResponse` | Native → Java | YES — field numbers must match |

Messages that do NOT cross: `FlightTicket` (Java-only), `NativeCommand` (Native-only), `PrefetchSplitsRequest` (Native-only), `CacheStats` (Native-only).

### 1.4 TaskRequest Field Alignment

The Native proto is the **authority** for field numbers. Java proto must align exactly.

| Field # | Name | Type | Notes |
|---------|------|------|-------|
| 1 | `task_id` | string | |
| 2 | `query_id` | string | |
| 3 | `substrait_plan` / `plan_fragment` | bytes | Same wire type, different name |
| 4 | `splits` | repeated SplitInfo | |
| 5 | `resource_limit` | ResourceLimit | |
| 6 | `session_properties` | map\<string, string\> | |
| 7 | `task_type` | TaskType enum | Numerically aligned |
| 8 | `stage_id` | int32 | Java-only, reserved in Native |
| 9 | _(reserved)_ | — | Permanently reserved |
| 10 | `partition_id` | int32 | Java-only, reserved in Native |
| 11 | `write_config` | WriteConfig | |

### 1.5 SplitInfo Field Alignment

| Field # | Name | Type | Notes |
|---------|------|------|-------|
| 1 | `split_id` | string | |
| 2 | `connector_id` | string | |
| 3 | `file_path` | string | |
| 4 | `file_format` | FileFormat enum | |
| 5 | `start_offset` | int64 | |
| 6 | `length` | int64 | |
| 7 | `column_mapping` | repeated ColumnMapping | Schema evolution support |
| 8 | `partition_values` | map\<string, string\> | Partition pruning metadata |
| 9 | `iceberg_info` | IcebergSplitInfo (oneof) | Iceberg-specific |
| 10 | `paimon_info` | PaimonSplitInfo (oneof) | Paimon-specific |

### 1.6 Enum Alignment

**TaskType**: `UNSPECIFIED=0, READ=1, WRITE=2, SHUFFLE_READ=3, SHUFFLE_WRITE=4`

**TaskState**: `UNSPECIFIED=0, PENDING=1, RUNNING=2, FLUSHING=3, FINISHED=4, FAILED=5, CANCELLED=6`

---

## 2. Memory Ownership Model

### 2.1 Data Flow (Pull Model)

```
Java ResultPuller thread
  → NativeTaskExecutor.getNextBatch(handle)
    → [JNI] NativeRuntime.nativeGetNextBatch(handle, arrowArrayPtr, arrowSchemaPtr)
      → C++ TaskExecutor::getNextBatch(row_vector)  // pop from OutputQueue
      → ArrowBridge::exportRowVectorToJava(row_vector, arrowArray, arrowSchema, pool)
      → return kDataAvailable
    → [JNI return to Java]
    → Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema)
    → serializeToIpc(root)
    → close allocator → close VectorSchemaRoot
```

### 2.2 Memory Ownership Hierarchy

```
C++ side:
  VeloxMemoryGovernor (singleton) → MemoryManager (singleton)
    → QueryCtx (per-task) → task MemoryPool (per-task)
      → RowVector buffers (per-batch, allocated from task pool)

Java side:
  RootAllocator (per-call in getNextBatch)
    → VectorSchemaRoot (imported from ArrowArray)
    → ArrowStreamWriter serializes to IPC bytes
    → allocator.close() releases Java-side buffers
```

### 2.3 Arrow C Data Interface Ownership Rules

- `exportToArrow(row_vector, *arrowArray, pool)` is zero-copy: buffer pointers directly reference RowVector memory
- ArrowArray.release callback captures `shared_ptr<RowVector>` to prevent premature destruction
- When Java calls ArrowArray release, the callback destructs the RowVector
- RowVector lifecycle is tied to the ArrowArray.release callback

### 2.4 Protobuf Usage Assessment

Protobuf for control-plane (task creation, commands, status) is appropriate. IPC bytes for data-plane (batch transfer) avoids serialization overhead. The hot path (`getNextBatch`) does NOT use Protobuf.

---

## 3. Known Issues and Fixes

### 3.1 Plan-pool Lifetime Corruption

**Root cause:** Plan-conversion vectors retained buffers owned by memory pools that
were destroyed before the plan tree. Destruction later reached Velox
`NonPODAlignedBuffer::freeToPool()` with a dead pool.

**Fix:** `VeloxTaskRunner` retains `plan_pool_holders_`, declared before the plan and
task members so reverse member destruction releases the plan before its pools.

### 3.2 ExecutorHandleMap Race Condition

**Root cause**: `nativeReleaseExecutor` calls `release()` then `remove(handle)`. Concurrent `nativeGetNextBatch` may access released executor.

**Fix**: Remove handle from map BEFORE calling `release()`.

### 3.3 Pool Access After Task Completion

**Root cause**: After `PlanExecutionDriver::executeVeloxPlan()` completes, `veloxTaskRunner()` returns nullptr. Unconsumed RowVectors in OutputQueue still reference the valid task pool.

**Fix**: Use `row_vector->pool()` instead of `runner->veloxTask()->pool()`.

### 3.4 Library Load Crash — jemalloc Symbol Collision (RC1, fixed)

**Root cause**: Homebrew jemalloc without prefix replaces global malloc. Protobuf static initializers call `free()` before jemalloc initialization.

**Fix**: Skip jemalloc linkage in CMakeLists.txt; use `-undefined dynamic_lookup` on macOS.

### 3.5 Library Load Crash — Protobuf Version Conflict (RC2, fixed)

**Root cause**: System protoc v33+ generates code incompatible with Velox-bundled protobuf 3.21.x.

**Fix**: Prefer bundled protoc from `_deps/protobuf-build/protoc`.

### 3.6 Library Load Crash — gflags Dual Registration (RC3, fixed)

**Root cause**: Static + dynamic libgflags simultaneously causes flag re-registration crash.

**Fix**: Filter out `libgflags*.a`; link only `libgflags.dylib`.

### 3.7 NativeLibraryLoader Safety

- Dependency pre-loading disabled by default (dylib's `@rpath` sufficient for dyld)
- Two-strategy loading: `java.library.path` first (dev/test), classpath extraction second (production)

---

## 4. Evolution Rules

1. **Adding fields**: Add to both proto files with the same field number.
2. **Removing fields**: Mark as `deprecated`. Never reuse field numbers.
3. **Renaming fields**: Allowed (names are not part of wire format).
4. **Changing types**: Forbidden for cross-boundary messages. Create a new field.
5. **Adding enum values**: Add to both protos with the same numeric value.

### Validation Tests

- `ProtocolConsistencyPropertyTest`: validates field number alignment across both protos
- `TaskRequestRoundTripPropertyTest`: validates Java-serialized bytes parse correctly with Native proto
