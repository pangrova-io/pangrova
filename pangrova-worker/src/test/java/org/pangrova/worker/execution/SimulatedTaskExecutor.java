package org.pangrova.worker.execution;

import org.pangrova.spi.execution.BatchResult;
import org.pangrova.spi.execution.TaskExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit-test-only TaskExecutor SPI implementation for testing without native library.
 *
 * <p>Simulates the execution loop: createTask allocates a handle,
 * startTask marks it active, getNextBatchResult returns configurable batches
 * then EOF, releaseTask frees the handle. NOT a mock — this is a real
 * implementation of the SPI contract for in-JVM testing.</p>
 *
 * <p><b>Restriction:</b> This class is restricted to {@code pangrova-worker} unit tests only.
 * E2E tests (in {@code pangrova-testing/pangrova-test-e2e}) MUST use {@code NativeTaskExecutor}
 * with the real native engine. See Requirement 38.6.</p>
 */
public final class SimulatedTaskExecutor implements TaskExecutor {

    private static final int EOF = 1;

    private final AtomicLong handleCounter = new AtomicLong(1);
    private final Map<Long, TaskState> tasks = new ConcurrentHashMap<>();
    private final int batchesBeforeEof;
    private final int errorOnBatch;
    private final int notReadyBeforeData;

    /**
     * Creates a simulated executor that produces N batches then EOF.
     *
     * @param batchesBeforeEof number of DATA_AVAILABLE batches before EOF
     */
    public SimulatedTaskExecutor(int batchesBeforeEof) {
        this(batchesBeforeEof, -1, 0);
    }

    /**
     * Creates a simulated executor that returns error on a specific batch.
     *
     * @param batchesBeforeEof number of batches before EOF (if no error)
     * @param errorOnBatch     batch index to return error (-1 for no error)
     */
    public SimulatedTaskExecutor(int batchesBeforeEof, int errorOnBatch) {
        this(batchesBeforeEof, errorOnBatch, 0);
    }

    /**
     * Creates a simulated executor with configurable NOT_READY cycles.
     *
     * @param batchesBeforeEof   number of DATA_AVAILABLE batches before EOF
     * @param errorOnBatch       batch index to return error (-1 for no error)
     * @param notReadyBeforeData number of NOT_READY (null) returns before first data batch
     */
    public SimulatedTaskExecutor(int batchesBeforeEof, int errorOnBatch, int notReadyBeforeData) {
        this.batchesBeforeEof = batchesBeforeEof;
        this.errorOnBatch = errorOnBatch;
        this.notReadyBeforeData = notReadyBeforeData;
    }

    @Override
    public long createTask(byte[] taskRequestBytes) {
        long handle = handleCounter.getAndIncrement();
        tasks.put(handle, new TaskState());
        return handle;
    }

    @Override
    public void startTask(long executorHandle) {
        requireTask(executorHandle).started = true;
    }

    @Override
    public BatchResult getNextBatchResult(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        long poll = state.batchCount.getAndIncrement();
        // NOT_READY phase
        if (poll < notReadyBeforeData) {
            return BatchResult.notReady();
        }
        long dataPoll = poll - notReadyBeforeData;
        if (errorOnBatch >= 0 && dataPoll >= errorOnBatch) {
            return BatchResult.error("Simulated batch error at batch " + dataPoll);
        }
        // DATA phase
        if (dataPoll < batchesBeforeEof) {
            return BatchResult.data(new byte[]{0x01});
        }
        return BatchResult.eof();
    }

    @Override
    public BatchResult getShuffleBatchResult(long executorHandle) {
        TaskState state = requireTask(executorHandle);
        long poll = state.shuffleBatchCount.getAndIncrement();
        // NOT_READY phase for shuffle
        if (poll < notReadyBeforeData) {
            return BatchResult.notReady();
        }
        return BatchResult.eof();
    }

    @Override
    public void releaseTask(long executorHandle) {
        TaskState state = tasks.remove(executorHandle);
        if (state != null) {
            state.released = true;
        }
    }

    @Override
    public void cancelTask(long executorHandle) {
        TaskState state = tasks.get(executorHandle);
        if (state != null) {
            state.cancelled = true;
        }
    }

    @Override
    public boolean isFinished(long executorHandle) {
        TaskState state = tasks.get(executorHandle);
        if (state == null || state.cancelled) {
            return true;
        }
        if (state.started && !state.finishSignaled) {
            state.finishSignaled = true;
            if (errorOnBatch >= 0) {
                state.errorMessage = "Simulated batch error at batch " + errorOnBatch;
            }
        }
        return state.finishSignaled;
    }

    @Override
    public byte[] executeCommand(long executorHandle, byte[] commandProto) {
        // Simulate command execution — return a minimal success response.
        // NativeCommandResponse: field 1 (success) = true → tag 0x08, value 0x01
        return new byte[]{0x08, 0x01};
    }

    /** Returns the error message for a finished task, or null if no error. */
    public String getErrorMessage(long handle) {
        TaskState state = tasks.get(handle);
        return state != null ? state.errorMessage : null;
    }


    /** Returns true if the handle was released. */
    public boolean isReleased(long handle) {
        return !tasks.containsKey(handle);
    }

    /** Returns the number of active (unreleased) tasks. */
    public int activeTaskCount() {
        return tasks.size();
    }

    private TaskState requireTask(long handle) {
        TaskState state = tasks.get(handle);
        if (state == null) {
            throw new IllegalStateException("Unknown handle: " + handle);
        }
        return state;
    }

    private static final class TaskState {
        volatile boolean started;
        volatile boolean released;
        volatile boolean cancelled;
        volatile boolean finishSignaled;
        volatile String errorMessage;
        final AtomicLong batchCount = new AtomicLong(0);
        final AtomicLong shuffleBatchCount = new AtomicLong(0);
    }
}
