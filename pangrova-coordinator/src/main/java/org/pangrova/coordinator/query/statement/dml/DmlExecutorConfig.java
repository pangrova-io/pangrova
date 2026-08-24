package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.coordinator.query.execution.CoordinatorMemoryGuard;
import org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction;
import org.pangrova.coordinator.query.task.NativeWriteCommitTracker;

import java.util.function.Consumer;

/**
 * Configuration bundle for {@link DmlExecutor}, reducing constructor
 * parameter count to comply with the ≤4 dependency governance limit.
 *
 * @param writeCallback      optional callback invoked after each write with the table name
 * @param compileAndDispatch optional function for dispatching DML to native workers
 * @param memoryGuard        optional coordinator memory guard for tracking allocations
 * @param maxConcurrentDml   maximum concurrent DML operations (positive)
 * @param dmlTimeoutMs       timeout in milliseconds for acquiring a DML permit (positive)
 * @param writeCommitTracker optional tracker for native write commit metadata (null when disabled)
 */
public record DmlExecutorConfig(
        Consumer<String> writeCallback,
        NativeDmlCompileFunction compileAndDispatch,
        CoordinatorMemoryGuard memoryGuard,
        int maxConcurrentDml,
        long dmlTimeoutMs,
        NativeWriteCommitTracker writeCommitTracker) {

    /** Default max concurrent DML operations. */
    public static final int DEFAULT_MAX_CONCURRENT_DML = 16;
    /** Default DML acquire timeout in milliseconds. */
    public static final long DEFAULT_DML_TIMEOUT_MS = 30_000;

    /** Backward-compatible constructor without writeCommitTracker. */
    public DmlExecutorConfig(
            Consumer<String> writeCallback,
            NativeDmlCompileFunction compileAndDispatch,
            CoordinatorMemoryGuard memoryGuard,
            int maxConcurrentDml,
            long dmlTimeoutMs) {
        this(writeCallback, compileAndDispatch, memoryGuard,
                maxConcurrentDml, dmlTimeoutMs, null);
    }

    /**
     * Creates a config with defaults for concurrency and timeout.
     */
    public static DmlExecutorConfig of(Consumer<String> writeCallback,
                                       NativeDmlCompileFunction compileAndDispatch) {
        return new DmlExecutorConfig(writeCallback, compileAndDispatch, null,
                DEFAULT_MAX_CONCURRENT_DML, DEFAULT_DML_TIMEOUT_MS, null);
    }

    /**
     * Creates a minimal config for test use.
     */
    public static DmlExecutorConfig defaults() {
        return new DmlExecutorConfig(null, null, null,
                DEFAULT_MAX_CONCURRENT_DML, DEFAULT_DML_TIMEOUT_MS, null);
    }
}
