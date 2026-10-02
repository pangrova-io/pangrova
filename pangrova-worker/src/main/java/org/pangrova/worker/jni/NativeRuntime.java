package org.pangrova.worker.jni;

import org.pangrova.worker.memory.AllocationListener;

/**
 * JNI bridge to the native vectorized execution engine.
 * Bidirectional: Java calls into Native for control-plane operations;
 * Native calls back to Java for memory arbitration ({@link org.pangrova.worker.memory.AllocationListener})
 * and write operations ({@link org.pangrova.worker.jni.write.NativeWriter}).
 *
 * <p>Lifecycle: {@link #initNativeRuntime} and {@link #shutdownNativeRuntime} are
 * synchronized and track an {@code initialized} flag. The C++ side
 * (NativeEngineLifecycle) handles idempotent re-init, so calling
 * {@code nativeInit} multiple times is safe. The Java flag exists to
 * skip redundant JNI round-trips and to guard {@code nativeShutdown}
 * against double-shutdown.</p>
 *
 * <p>Library loading ({@link NativeLibraryLoader#load()}) remains
 * one-shot in the static initializer — only the init/shutdown guard
 * was removed.</p>
 */
public final class NativeRuntime {

    static {
        NativeLibraryLoader.load();
    }

    private static volatile boolean initialized = false;

    private NativeRuntime() {}

    // --- Lifecycle (synchronized wrappers) ---

    /**
     * Initializes the native runtime and tracks initialized state.
     * Idempotent — skips if already initialized to prevent global
     * singleton re-registration errors in shared-JVM test environments.
     */
    public static synchronized void initNativeRuntime(byte[] configProto, AllocationListener allocationListener) {
        if (initialized) {
            return;
        }
        nativeInitInternal(configProto, allocationListener);
        initialized = true;
    }

    /**
     * Shuts down the native runtime. Idempotent — calling after
     * already shut down (or before any init) is a no-op.
     *
     * <p>Note: In single-fork test suites ({@code reuseForks=true}), the
     * native runtime persists for the entire JVM lifetime. Shutdown resets
     * the Java {@code initialized} flag but C++ global singletons (connector
     * registry, DWIO factories, filesystem factories) survive. The C++
     * {@code NativeEngineLifecycle} supports clean re-init after shutdown.</p>
     */
    public static synchronized void shutdownNativeRuntime() {
        if (!initialized) {
            return;
        }
        nativeShutdownInternal();
        initialized = false;
    }

    /**
     * Returns whether the native runtime is currently initialized.
     */
    public static boolean isInitialized() {
        return initialized;
    }

    /**
     * Throws {@link IllegalStateException} if the native runtime is not initialized.
     * Must be called before any native method invocation to prevent SIGSEGV / null
     * dereference on the C++ side when the native runtime manager is not yet set up.
     */
    static void requireInitialized() {
        if (!initialized) {
            throw new IllegalStateException(
                    "Native runtime not initialized. Call NativeRuntime.initNativeRuntime() first.");
        }
    }

    // --- Native lifecycle methods ---
    // Method names MUST match JNI symbols in libpangrova_native.dylib:
    //   Java_org_pangrova_worker_jni_NativeRuntime_nativeInitInternal
    //   Java_org_pangrova_worker_jni_NativeRuntime_nativeShutdownInternal
    public static native void nativeInitInternal(byte[] configProto, AllocationListener allocationListener);
    public static native void nativeShutdownInternal();

    /**
     * P1-7: Triggers native memory pool reclaim (operator MemoryReclaimer spill).
     * Called by {@link org.pangrova.worker.memory.NativeSpillBridge#requestSpill}
     * when the Java-side arbitrator denies a native allocation, closing the gap
     * where requestSpill was a no-op signal that never drove actual reclaim.
     *
     * @param targetBytes the amount of memory to attempt to reclaim
     * @return actual bytes freed by operator spill (0 if no reclaim occurred)
     */
    public static native long nativeRequestReclaim(long targetBytes);

    // --- Task management ---
    public static long nativeCreateTaskExecutor(byte[] taskRequestProto) {
        requireInitialized();
        return nativeCreateTaskExecutor0(taskRequestProto);
    }

    public static long nativeCreateTaskExecutorWithTrace(
            byte[] taskRequestProto, String traceId) {
        requireInitialized();
        return nativeCreateTaskExecutorWithTrace0(taskRequestProto, traceId);
    }

    public static long nativeCreateTaskExecutorWithTraceContext(
            byte[] taskRequestProto, String traceId, String spanId) {
        requireInitialized();
        return nativeCreateTaskExecutorWithTraceContext0(taskRequestProto, traceId, spanId);
    }

    public static void nativeStartTask(long executorHandle) {
        requireInitialized();
        nativeStartTask0(executorHandle);
    }

    public static void nativeStartTaskWithTrace(
            long executorHandle, String traceId) {
        requireInitialized();
        nativeStartTaskWithTrace0(executorHandle, traceId);
    }

    public static void nativeCancelTask(long executorHandle) {
        requireInitialized();
        nativeCancelTask0(executorHandle);
    }

    /** Returns the final canonical task report after joined cleanup, before Native ownership is erased. */
    public static byte[] nativeReleaseExecutor(long executorHandle) {
        requireInitialized();
        return nativeReleaseExecutor0(executorHandle);
    }

    // Status
    public static byte[] nativeGetTaskStatus(long executorHandle) {
        requireInitialized();
        return nativeGetTaskStatus0(executorHandle);
    }

    public static byte[] nativeGetTaskStats(long executorHandle) {
        requireInitialized();
        return nativeGetTaskStats0(executorHandle);
    }

    // Unified command channel — all control-plane operations route through here.
    // Returns serialized NativeCommandResponse proto bytes.
    // NOTE: executorHandle is the authoritative routing key. The C++ side
    // (dispatchCommandWithHandle) overrides NativeCommand.executor_handle in the
    // proto payload with this JNI parameter to prevent routing mismatches.
    public static byte[] nativeCommand(long executorHandle, byte[] commandProto) {
        requireInitialized();
        return nativeCommand0(executorHandle, commandProto);
    }

    // Memory snapshot for heartbeat observability.
    // Returns serialized MemorySnapshot proto bytes, or null if bridge unavailable.
    public static byte[] nativeGetMemorySnapshot() {
        requireInitialized();
        return nativeGetMemorySnapshot0();
    }

    // --- Write bridge batch operations ---
    // Serialize columnar data interface pointers into IPC record batch bytes.
    // Used by the write bridge to convert native structs for ConnectorPageSink.
    public static byte[] nativeSerializeBatchFromPointers(long arrowArrayPtr, long arrowSchemaPtr) {
        requireInitialized();
        return nativeSerializeBatchFromPointers0(arrowArrayPtr, arrowSchemaPtr);
    }

    // Extract row count from a native array struct.
    public static int nativeGetBatchRowCount(long arrowArrayPtr) {
        requireInitialized();
        return nativeGetBatchRowCount0(arrowArrayPtr);
    }

    // Extract column count from a native schema struct.
    public static int nativeGetBatchColumnCount(long arrowSchemaPtr) {
        requireInitialized();
        return nativeGetBatchColumnCount0(arrowSchemaPtr);
    }

    // --- Write executor registration ---
    // Registers a Java NativeWriter instance for C++ write tasks to call back into.
    // Must be called after nativeInit and before any WRITE task is dispatched.
    public static void nativeRegisterWriter(Object writer) {
        requireInitialized();
        nativeRegisterWriter0(writer);
    }

    // --- Shuffle ticket HMAC key injection ---
    // Sets the HMAC-SHA256 key(s) for shuffle ticket verification on the native side.
    // Called once during WorkerTaskBootstrap startup after worker registration.
    // Supports dual-key rotation: currentKey is the active signing key,
    // previousKey (may be empty) is accepted during the rotation window.
    // Keys are raw bytes (not base64-encoded).
    /** Installs current and optional previous HMAC keys for native shuffle tickets. */
    public static void nativeSetHmacKey(byte[] currentKey, byte[] previousKey) {
        requireInitialized();
        if (currentKey == null || currentKey.length < 32) {
            throw new IllegalArgumentException(
                    "currentKey must contain at least 32 bytes");
        }
        if (previousKey != null && previousKey.length > 0 && previousKey.length < 32) {
            throw new IllegalArgumentException(
                    "previousKey must be empty or contain at least 32 bytes");
        }
        if (previousKey == null) {
            previousKey = new byte[0];
        }
        nativeSetHmacKey0(currentKey, previousKey);
    }

    // --- Shuffle egress registration ---
    // Registers a Java ShuffleByteEgress instance for C++ shuffle tasks to
    // call back into when committed partition bytes need to cross to the JVM.
    // Must be called after nativeInit and before any JVM-resident shuffle provider
    // is activated (local-spill, object-store, celeborn). The "native" provider
    // does not use this path (bytes stay in C++).
    public static void nativeRegisterEgress(Object egress) {
        requireInitialized();
        nativeRegisterEgress0(egress);
    }

    /**
     * Registers Java scalar functions in the native expression engine.
     * Registration is fail-closed: all descriptors are installed atomically
     * before task execution is allowed to start.
     *
     * @param functionBridge callback target retained by the native runtime
     * @param descriptors function signatures to expose to native execution
     */
    public static void nativeRegisterFunctions(
            NativeFunctionBridge functionBridge,
            java.util.List<NativeFunctionDescriptor> descriptors) {
        requireInitialized();
        if (functionBridge == null) {
            throw new IllegalArgumentException("functionBridge must not be null");
        }
        if (descriptors == null) {
            throw new IllegalArgumentException("descriptors must not be null");
        }
        byte[][] serializedDescriptors = descriptors.stream()
                .map(NativeFunctionDescriptor::toBytes)
                .toArray(byte[][]::new);
        nativeRegisterFunctions0(functionBridge, serializedDescriptors);
    }

    // --- Internal native methods ---
    // JNI symbol names: Java_org_pangrova_worker_jni_NativeRuntime_nativeCreateTaskExecutor etc.
    private static native long nativeCreateTaskExecutor0(byte[] taskRequestProto);
    private static native long nativeCreateTaskExecutorWithTrace0(byte[] taskRequestProto, String traceId);
    private static native long nativeCreateTaskExecutorWithTraceContext0(byte[] taskRequestProto, String traceId,
            String spanId);
    private static native void nativeStartTask0(long executorHandle);
    private static native void nativeStartTaskWithTrace0(long executorHandle, String traceId);
    private static native void nativeCancelTask0(long executorHandle);
    private static native byte[] nativeReleaseExecutor0(long executorHandle);
    private static native byte[] nativeGetTaskStatus0(long executorHandle);
    private static native byte[] nativeGetTaskStats0(long executorHandle);
    private static native byte[] nativeCommand0(long executorHandle, byte[] commandProto);
    private static native byte[] nativeGetMemorySnapshot0();
    private static native byte[] nativeSerializeBatchFromPointers0(long arrowArrayPtr, long arrowSchemaPtr);
    private static native int nativeGetBatchRowCount0(long arrowArrayPtr);
    private static native int nativeGetBatchColumnCount0(long arrowSchemaPtr);
    private static native void nativeRegisterWriter0(Object writer);
    private static native void nativeSetHmacKey0(byte[] currentKey, byte[] previousKey);
    private static native void nativeRegisterEgress0(Object egress);
    private static native void nativeRegisterFunctions0(
            NativeFunctionBridge functionBridge, byte[][] descriptors);
}
