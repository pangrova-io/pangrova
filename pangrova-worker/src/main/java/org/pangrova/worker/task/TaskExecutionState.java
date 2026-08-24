package org.pangrova.worker.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.resource.memory.TaskMemoryPool;
import org.pangrova.worker.buffer.OutputBufferManager;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Mutable execution state for a running task, tracked via atomics.
 * Separated from immutable task metadata ({@link TaskInfo}) to enforce
 * the immutability contract for metadata fields (§6) while providing
 * thread-safe mutable state for execution tracking.
 *
 * <p>Concurrency strategy: all fields are atomic — no external
 * synchronization required. CAS on executorHandle prevents
 * double-release at the JNI boundary.</p>
 */
@ThreadSafe
final class TaskExecutionState {

    // Signaled exactly once when the task enters a terminal state.
    private final CountDownLatch terminalLatch = new CountDownLatch(1);

    private final AtomicReference<TaskManager.TaskState> state =
            new AtomicReference<>(TaskManager.TaskState.RECEIVED);
    private final AtomicLong executorHandle = new AtomicLong(0);
    private final AtomicLong reportSequence = new AtomicLong(0);

    /** Wall-clock instant when the task first transitioned to a terminal
     * state (FINISHED/FAILED/CANCELLED). 0 means not-yet-terminal. Used by
     * the cleanup scheduler to apply retention against terminal age, not
     * against task start time. */
    private final AtomicLong terminalEpochMillis = new AtomicLong(0);

    // Resource accounting
    private final AtomicLong allocatedMemory = new AtomicLong(0);
    private final AtomicLong allocatedBufferMemory = new AtomicLong(0);
    private final AtomicReference<TaskMemoryPool> taskMemoryPool = new AtomicReference<>();

    // Output stats
    private final AtomicLong outputBatches = new AtomicLong(0);
    private final AtomicLong outputBytes = new AtomicLong(0);

    // Buffer reference (legacy path)
    private final AtomicReference<OutputBufferManager> taskBuffer = new AtomicReference<>();

    // Write commit metadata (set by NativeWriteExecutor.commitWrite)
    private final AtomicReference<byte[]> commitMetadata = new AtomicReference<>();

    // Per-task shuffle server port (set by TaskLifecycleManager.startShuffleViaSpi)
    private final AtomicLong shufflePort = new AtomicLong(0);

    // --- State machine ---

    TaskManager.TaskState getState() { return state.get(); }

    boolean compareAndSetState(TaskManager.TaskState expected, TaskManager.TaskState update) {
        boolean ok = state.compareAndSet(expected, update);
        if (ok && isTerminal(update)) {
            // Record terminal time on first successful transition to a terminal state.
            terminalEpochMillis.compareAndSet(0L, System.currentTimeMillis());
            terminalLatch.countDown();
        }
        return ok;
    }

    /**
     * Blocks until the task enters a terminal state or timeout expires.
     * @return true if terminal state reached, false if timed out.
     */
    boolean awaitTerminal(long timeout, TimeUnit unit) throws InterruptedException {
        return terminalLatch.await(timeout, unit);
    }

    /** Returns terminal-time as Instant, or null if not yet terminal. */
    java.time.Instant terminalTime() {
        long t = terminalEpochMillis.get();
        return t == 0L ? null : java.time.Instant.ofEpochMilli(t);
    }

    private static boolean isTerminal(TaskManager.TaskState s) {
        return s == TaskManager.TaskState.FINISHED
                || s == TaskManager.TaskState.FAILED
                || s == TaskManager.TaskState.CANCELLED;
    }

    // --- Executor handle (CAS for double-release prevention) ---

    long executorHandle() { return executorHandle.get(); }

    void setExecutorHandle(long handle) { executorHandle.set(handle); }

    boolean compareAndSetExecutorHandle(long expected, long update) {
        return executorHandle.compareAndSet(expected, update);
    }

    // --- Report sequencing ---

    long nextReportSequence() { return reportSequence.incrementAndGet(); }

    // --- Resource accounting ---

    long allocatedMemory() { return allocatedMemory.get(); }

    void setAllocatedMemory(long bytes) { allocatedMemory.set(bytes); }

    long allocatedBufferMemory() { return allocatedBufferMemory.get(); }

    void setAllocatedBufferMemory(long bytes) { allocatedBufferMemory.set(bytes); }

    TaskMemoryPool taskMemoryPool() { return taskMemoryPool.get(); }

    void setTaskMemoryPool(TaskMemoryPool pool) { taskMemoryPool.set(pool); }

    // --- Output stats ---

    long outputBatches() { return outputBatches.get(); }

    void addOutputBatches(long count) { outputBatches.addAndGet(count); }

    long outputBytes() { return outputBytes.get(); }

    void addOutputBytes(long bytes) { outputBytes.addAndGet(bytes); }

    // --- Buffer ---

    OutputBufferManager taskBuffer() { return taskBuffer.get(); }

    void setTaskBuffer(OutputBufferManager buffer) { taskBuffer.set(buffer); }

    // --- Write commit metadata ---

    byte[] commitMetadata() { return commitMetadata.get(); }

    void setCommitMetadata(byte[] metadata) { commitMetadata.set(metadata); }

    // --- Shuffle port ---

    int shufflePort() { return (int) shufflePort.get(); }

    void setShufflePort(int port) { shufflePort.set(port); }
}
