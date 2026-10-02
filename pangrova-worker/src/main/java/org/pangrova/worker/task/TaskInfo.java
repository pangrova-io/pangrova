package org.pangrova.worker.task;

import java.time.Instant;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.memory.TaskMemoryPool;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.worker.buffer.OutputBufferManager;

/**
 * Task tracking record: immutable metadata + delegated mutable execution state.
 *
 * <p>Metadata fields (taskId, queryId, request, descriptor, startTime) are
 * final and set at construction. Mutable execution state (executor handle,
 * memory accounting, output stats) is encapsulated in {@link TaskExecutionState}
 * and accessed via delegation methods.</p>
 */
public final class TaskInfo {

    // --- Immutable metadata ---
    private final String taskId;
    private final String queryId;
    private final SubmitTaskRequest request;
    private final TaskDescriptor descriptor;
    private final Instant startTime;

    // --- Mutable execution state (delegated) ---
    private final TaskExecutionState execution = new TaskExecutionState();
    private final TaskCleanupRetry cleanupRetry = new TaskCleanupRetry();

    TaskInfo(String taskId, String queryId, SubmitTaskRequest request,
             TaskDescriptor descriptor, Instant startTime) {
        this.taskId = taskId;
        this.queryId = queryId;
        this.request = request;
        this.descriptor = descriptor;
        this.startTime = startTime;
    }

    // --- Immutable accessors ---

    public String taskId() { return taskId; }
    public String queryId() { return queryId; }
    public SubmitTaskRequest request() { return request; }
    public TaskDescriptor descriptor() { return descriptor; }
    public Instant startTime() { return startTime; }

    void retainCleanupRetry(Runnable continuation) { cleanupRetry.retain(continuation); }
    boolean retryCleanup() { return cleanupRetry.retry(); }

    // --- State machine (delegated) ---

    public TaskManager.TaskState getState() { return execution.getState(); }

    public boolean compareAndSetState(TaskManager.TaskState expected, TaskManager.TaskState update) {
        return execution.compareAndSetState(expected, update);
    }

    /**
     * Blocks until the task enters a terminal state or timeout expires.
     * @return true if terminal state reached, false if timed out.
     */
    public boolean awaitTerminal(long timeout, java.util.concurrent.TimeUnit unit)
            throws InterruptedException {
        return execution.awaitTerminal(timeout, unit);
    }

    /** Wall-clock instant when this task first entered a terminal state
     * (FINISHED/FAILED/CANCELLED). null if still active. */
    public Instant terminalTime() { return execution.terminalTime(); }

    // --- Executor handle (delegated) ---

    public long executorHandle() { return execution.executorHandle(); }
    public void setExecutorHandle(long handle) { execution.setExecutorHandle(handle); }
    public boolean compareAndSetExecutorHandle(long expected, long update) {
        return execution.compareAndSetExecutorHandle(expected, update);
    }

    java.util.concurrent.CompletableFuture<byte[]> claimExecutorRelease(
            java.util.concurrent.CompletableFuture<byte[]> candidate) {
        return execution.claimExecutorRelease(candidate);
    }

    byte[] executorReleaseReport() { return execution.executorReleaseReport(); }

    java.util.concurrent.CompletableFuture<Void> claimResourcesRelease(
            java.util.concurrent.CompletableFuture<Void> candidate) {
        return execution.claimResourcesRelease(candidate);
    }

    boolean markCancellationReported() {
        return execution.markCancellationReported();
    }

    // --- Report sequencing (delegated) ---

    long nextReportSequence() { return execution.nextReportSequence(); }

    // --- Resource accounting (delegated) ---

    public long allocatedMemory() { return execution.allocatedMemory(); }
    public void setAllocatedMemory(long bytes) { execution.setAllocatedMemory(bytes); }
    public long allocatedBufferMemory() { return execution.allocatedBufferMemory(); }
    public void setAllocatedBufferMemory(long bytes) { execution.setAllocatedBufferMemory(bytes); }
    public TaskMemoryPool taskMemoryPool() { return execution.taskMemoryPool(); }
    public void setTaskMemoryPool(TaskMemoryPool pool) { execution.setTaskMemoryPool(pool); }

    // --- Output stats (delegated) ---

    public long outputBatches() { return execution.outputBatches(); }
    public void addOutputBatches(long count) { execution.addOutputBatches(count); }
    public long outputBytes() { return execution.outputBytes(); }
    public void addOutputBytes(long bytes) { execution.addOutputBytes(bytes); }

    // --- Buffer (delegated) ---

    public OutputBufferManager taskBuffer() { return execution.taskBuffer(); }
    public void setTaskBuffer(OutputBufferManager buffer) { execution.setTaskBuffer(buffer); }

    // --- Derived queries ---

    public boolean isWriteTask() {
        return request != null
                && request.taskRequest().hasExecutionOptions()
                && request.taskRequest().getExecutionOptions().hasWrite();
    }

    // --- Write commit metadata (delegated) ---

    public byte[] commitMetadata() { return execution.commitMetadata(); }
    public void setCommitMetadata(byte[] metadata) { execution.setCommitMetadata(metadata); }

    // --- Canonical terminal status (delegated) ---

    public byte[] terminalTaskReport() { return execution.terminalTaskReport(); }
    public void setTerminalTaskReport(byte[] status) {
        execution.setTerminalTaskReport(status);
    }
    public byte[] terminalTaskStats() { return execution.terminalTaskStats(); }
    public void setTerminalTaskStats(byte[] stats) {
        execution.setTerminalTaskStats(stats);
    }
    void setReleasedTaskStats(byte[] stats) {
        execution.setReleasedTaskStats(stats);
    }

    // --- Shuffle port (delegated) ---

    public int shufflePort() { return execution.shufflePort(); }
    public void setShufflePort(int port) { execution.setShufflePort(port); }
    public org.pangrova.shuffle.ShuffleServerHandle shuffleServerHandle() {
        return execution.shuffleServerHandle();
    }
    public void setShuffleServerHandle(org.pangrova.shuffle.ShuffleServerHandle handle) {
        execution.setShuffleServerHandle(handle);
    }
}
