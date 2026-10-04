package org.pangrova.worker.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.resource.memory.TaskMemoryPool;
import org.pangrova.shuffle.ShuffleServerHandle;
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
    private final AtomicReference<CompletableFuture<byte[]>> executorRelease =
            new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> resourcesRelease =
            new AtomicReference<>();
    // Store the actual JNI release return before parsing/statistics allocation.
    // A failed post-release Java allocation must not lose the already-erased
    // Native owner's only receipt or attempt an unknown-handle release again.
    private volatile byte[] stagedExecutorReleaseReceipt;
    private final java.util.concurrent.atomic.AtomicBoolean cancellationReported =
            new java.util.concurrent.atomic.AtomicBoolean(false);
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

    // Canonical v2 TaskReport captured before the native handle is released.
    private final AtomicReference<byte[]> terminalTaskReport = new AtomicReference<>();

    // Canonical v2 NativeTaskStats captured before the native handle is released.
    private final AtomicReference<byte[]> terminalTaskStats = new AtomicReference<>();

    // Per-task shuffle server port (set by TaskLifecycleManager.startShuffleViaSpi)
    private final AtomicLong shufflePort = new AtomicLong(0);
    private volatile byte[] stagedCreationRecoveryReceipt;
    private final AtomicReference<ShuffleServerHandle> shuffleServerHandle =
            new AtomicReference<>();

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

    byte[] stagedCreationRecoveryReceipt() { return stagedCreationRecoveryReceipt; }
    void stageCreationRecoveryReceipt(byte[] receipt) { stagedCreationRecoveryReceipt = receipt; }

    boolean compareAndSetExecutorHandle(long expected, long update) {
        return executorHandle.compareAndSet(expected, update);
    }

    CompletableFuture<byte[]> claimExecutorRelease(CompletableFuture<byte[]> candidate) {
        return claimRelease(executorRelease, candidate);
    }

    byte[] executorReleaseReport() {
        var release = executorRelease.get();
        if (release == null) return null;
        byte[] receipt = release.join();
        return receipt == null ? null : receipt.clone();
    }

    CompletableFuture<Void> claimResourcesRelease(CompletableFuture<Void> candidate) {
        return claimRelease(resourcesRelease, candidate);
    }

    byte[] stagedExecutorReleaseReceipt() { return stagedExecutorReleaseReceipt; }
    void stageExecutorReleaseReceipt(byte[] receipt) { stagedExecutorReleaseReceipt = receipt; }
    boolean resourcesReleased() {
        var release = resourcesRelease.get();
        return release != null && release.isDone() && !release.isCompletedExceptionally();
    }

    private static <T> CompletableFuture<T> claimRelease(
            AtomicReference<CompletableFuture<T>> slot,
            CompletableFuture<T> candidate) {
        while (true) {
            CompletableFuture<T> current = slot.get();
            if (current != null && !current.isCompletedExceptionally()) {
                return current;
            }
            if (slot.compareAndSet(current, candidate)) {
                return candidate;
            }
        }
    }

    boolean markCancellationReported() {
        return cancellationReported.compareAndSet(false, true);
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

    // --- Canonical terminal status ---

    byte[] terminalTaskReport() {
        var release = executorRelease.get();
        if (release != null && release.isDone() && !release.isCompletedExceptionally()) {
            byte[] receipt = release.join();
            if (receipt != null) return receipt.clone();
        }
        byte[] value = terminalTaskReport.get();
        return value == null ? null : value.clone();
    }

    void setTerminalTaskReport(byte[] status) {
        if (status != null && status.length > 0) {
            terminalTaskReport.compareAndSet(null, status.clone());
        }
    }

    byte[] terminalTaskStats() {
        byte[] value = terminalTaskStats.get();
        return value == null ? null : value.clone();
    }

    void setTerminalTaskStats(byte[] stats) {
        if (stats != null && stats.length > 0) {
            terminalTaskStats.compareAndSet(null, stats.clone());
        }
    }

    void setReleasedTaskStats(byte[] stats) {
        terminalTaskStats.set(stats == null ? null : stats.clone());
    }

    // --- Shuffle port ---

    int shufflePort() { return (int) shufflePort.get(); }

    void setShufflePort(int port) { shufflePort.set(port); }

    ShuffleServerHandle shuffleServerHandle() { return shuffleServerHandle.get(); }

    void setShuffleServerHandle(ShuffleServerHandle handle) {
        shuffleServerHandle.set(handle);
        shufflePort.set(handle == null ? 0 : handle.port());
    }
}
