package org.pangrova.worker.task;

import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.worker.enums.TaskState;

import java.time.Instant;

import org.pangrova.worker.task.memory.TaskMemoryAccount;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.worker.resource.memory.TaskMemoryPool;
import org.pangrova.protocol.execution.TaskDescriptor;

/**
 * Task tracking record: immutable metadata + delegated mutable execution state.
 *
 * <p>Metadata fields (taskId, queryId, request, descriptor, startTime) are final and set at
 * construction. Execution state (executor handle and output stats) is owned by
 * {@link TaskExecutionState}; admission memory reservations are owned by
 * {@link TaskMemoryAccount}. Both are accessed through the same per-task owners.
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
    private final TaskMemoryAccount memoryAccount;
    private final TaskCleanupRetry cleanupRetry = new TaskCleanupRetry();
    private final TaskNativeCreation nativeCreation = new TaskNativeCreation();
    private final TaskTerminalPublication terminalPublication = new TaskTerminalPublication();
    @org.pangrova.common.concurrent.GuardedBy("this")
    private TaskResultRelease resultRelease;

    TaskInfo(
            String taskId,
            String queryId,
            SubmitTaskRequest request,
            TaskDescriptor descriptor,
            Instant startTime) {
        this.taskId = taskId;
        this.queryId = queryId;
        this.request = request;
        this.descriptor = descriptor;
        this.startTime = startTime;
        this.memoryAccount = new TaskMemoryAccount(taskId, queryId, request, descriptor);
    }

    // --- Immutable accessors ---

    public String taskId() {
        return taskId;
    }

    public String queryId() {
        return queryId;
    }

    public SubmitTaskRequest request() {
        return request;
    }

    public TaskDescriptor descriptor() {
        return descriptor;
    }

    public Instant startTime() {
        return startTime;
    }

    void setPreNativeReadBytes(long bytes) { execution.setPreNativeReadBytes(bytes); }
    long preNativeReadBytes() { return execution.preNativeReadBytes(); }

    void retainCleanupRetry(Runnable continuation) {
        cleanupRetry.retain(continuation);
    }

    boolean retryCleanup() {
        return cleanupRetry.retry();
    }

    synchronized boolean installResultRelease(TaskResultRelease registration) {
        if (resultRelease != null) return false;
        resultRelease = registration;
        return true;
    }

    void cancelResultRelease() {
        TaskResultRelease registration;
        synchronized (this) {
            registration = resultRelease;
            resultRelease = null;
        }
        if (registration != null) registration.cancel();
    }

    boolean scheduleRegisteredResultCleanup(Instant cutoff) {
        TaskResultRelease registration;
        synchronized (this) {
            registration = resultRelease;
        }
        if (registration == null) return false;
        registration.retryOrExpire(TaskCleanupScheduler.terminalRetentionElapsed(this, cutoff));
        return true;
    }

    // --- State machine (delegated) ---

    public TaskState getState() {
        return execution.getState();
    }

    public boolean compareAndSetState(
            TaskState expected, TaskState update) {
        return execution.compareAndSetState(expected, update);
    }

    /**
     * Blocks until the task enters a terminal state or timeout expires.
     *
     * @return true if terminal state reached, false if timed out.
     */
    public boolean awaitTerminal(long timeout, java.util.concurrent.TimeUnit unit)
            throws InterruptedException {
        return execution.awaitTerminal(timeout, unit);
    }

    /**
     * Wall-clock instant when this task first entered a terminal state (FINISHED/FAILED/CANCELLED).
     * null if still active.
     */
    public Instant terminalTime() {
        return execution.terminalTime();
    }

    // --- Executor handle (delegated) ---

    public long executorHandle() {
        return execution.executorHandle();
    }

    public void setExecutorHandle(long handle) {
        nativeCreation.publish(handle, execution::setExecutorHandle);
    }

    void beginNativeCreation() {
        nativeCreation.begin();
    }

    void failNativeCreation(Throwable failure) {
        nativeCreation.failed(failure);
    }

    void nativeCreationRolledBackBeforeEntry() {
        nativeCreation.failedBeforeNativeEntry();
    }

    boolean sealNativeCreationWithoutOwnerOrAwaitExit() {
        return nativeCreation.sealWithoutOwnerOrAwaitExit();
    }

    void resolveNativeCreationFailure(long handle) {
        if (executorHandle() != 0 && executorHandle() != handle) {
            throw new IllegalStateException(
                    "Recovered Native owner differs from the recorded executor handle");
        }
        nativeCreation.resolveEnteredFailure(handle, execution::setExecutorHandle);
    }

    public boolean compareAndSetExecutorHandle(long expected, long update) {
        return execution.compareAndSetExecutorHandle(expected, update);
    }

    java.util.concurrent.CompletableFuture<byte[]> claimExecutorRelease(
            java.util.concurrent.CompletableFuture<byte[]> candidate) {
        return execution.claimExecutorRelease(candidate);
    }

    byte[] executorReleaseReport() {
        return execution.executorReleaseReport();
    }

    byte[] stagedExecutorReleaseReceipt() {
        return execution.stagedExecutorReleaseReceipt();
    }

    void stageExecutorReleaseReceipt(byte[] receipt) {
        execution.stageExecutorReleaseReceipt(receipt);
    }

    byte[] stagedCreationRecoveryReceipt() {
        return execution.stagedCreationRecoveryReceipt();
    }

    void stageCreationRecoveryReceipt(byte[] receipt) {
        execution.stageCreationRecoveryReceipt(receipt);
    }

    boolean resourcesReleased() {
        return execution.resourcesReleased();
    }

    org.pangrova.protocol.v2.ExecutionDrainState executionDrainState() {
        byte[] receipt = executorReleaseReport();
        if (receipt != null) {
            return TaskCompletionMonitor.requireCanonicalTaskReport(request.taskRequest(), receipt)
                    .getExecutionDrainState();
        }
        if (!nativeCreation.sealWithoutOwnerOrAwaitExit()) {
            throw new IllegalStateException("Created Native owner has no actual release receipt");
        }
        return org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER;
    }

    java.util.concurrent.CompletableFuture<Void> claimResourcesRelease(
            java.util.concurrent.CompletableFuture<Void> candidate) {
        return execution.claimResourcesRelease(candidate);
    }

    boolean markCancellationReported() {
        return execution.markCancellationReported();
    }

    // --- Report sequencing (delegated) ---

    long nextReportSequence() {
        return execution.nextReportSequence();
    }

    void publishTerminalStatus(
            boolean released,
            java.util.function.Supplier<org.pangrova.protocol.rest.task.TaskStatusReport> prepare,
            java.util.function.Consumer<org.pangrova.protocol.rest.task.TaskStatusReport> send) {
        if (released && !resourcesReleased()) {
            throw new IllegalStateException(
                    "Terminal publication precedes actual resource release");
        }
        terminalPublication.publish(released, prepare, send);
    }

    java.util.concurrent.CompletionStage<Void> publishResultStatus(
            boolean released,
            java.util.function.Supplier<org.pangrova.protocol.rest.task.TaskStatusReport> prepare,
            java.util.function.Function<org.pangrova.protocol.rest.task.TaskStatusReport,
                    java.util.concurrent.CompletionStage<Void>> send,
            Runnable onAcknowledgement) {
        if (released && !resourcesReleased()) {
            throw new IllegalStateException("Terminal publication precedes actual resource release");
        }
        return terminalPublication.publishAsync(released, prepare, send, onAcknowledgement);
    }

    void retryInitialResultStatus(java.util.function.Function<org.pangrova.protocol.rest.task.TaskStatusReport,
            java.util.concurrent.CompletionStage<Void>> send) {
        terminalPublication.retryFailedInitialDelivery(send);
    }

    // --- Resource accounting (delegated) ---

    /** Shares the one reservation account with this package's lifecycle and reporting owners. */
    TaskMemoryAccount memoryAccount() {
        return memoryAccount;
    }

    public long allocatedMemory() {
        return memoryAccount.allocatedMemory();
    }

    public void setAllocatedMemory(long bytes) {
        memoryAccount.setAllocatedMemory(bytes);
    }

    public long allocatedBufferMemory() {
        return memoryAccount.allocatedBufferMemory();
    }

    public void setAllocatedBufferMemory(long bytes) {
        memoryAccount.setAllocatedBufferMemory(bytes);
    }

    public TaskMemoryPool taskMemoryPool() {
        return execution.taskMemoryPool();
    }

    public void setTaskMemoryPool(TaskMemoryPool pool) {
        execution.setTaskMemoryPool(pool);
    }

    // --- Output stats (delegated) ---

    public long outputBatches() {
        return execution.outputBatches();
    }

    public void addOutputBatches(long count) {
        execution.addOutputBatches(count);
    }

    public long outputBytes() {
        return execution.outputBytes();
    }

    public void addOutputBytes(long bytes) {
        execution.addOutputBytes(bytes);
    }

    // --- Derived queries ---

    public boolean isWriteTask() {
        return request != null
                && request.taskRequest().hasExecutionOptions()
                && request.taskRequest().getExecutionOptions().hasWrite();
    }

    /** True when this task is executed by a Worker-JVM backend rather than Native. */
    public boolean isWorkerExecuted() {
        return request != null && request.taskRequest().getTaskKind()
                == org.pangrova.protocol.v2.TaskKind.TASK_KIND_CDC_INGEST;
    }

    // --- Write commit metadata (delegated) ---

    public byte[] commitMetadata() {
        return execution.commitMetadata();
    }

    public void setCommitMetadata(byte[] metadata) {
        execution.setCommitMetadata(metadata);
    }

    // --- Canonical terminal status (delegated) ---

    public byte[] terminalTaskReport() {
        return execution.terminalTaskReport();
    }

    public void setTerminalTaskReport(byte[] status) {
        execution.setTerminalTaskReport(status);
    }

    NativeTaskStats terminalTaskStats() {
        return execution.terminalTaskStats();
    }

    void setTerminalTaskStats(NativeTaskStats stats) {
        execution.setTerminalTaskStats(stats);
    }

    public java.util.List<byte[]> terminalCommitFragments() {
        return execution.terminalCommitFragments();
    }

    public void setTerminalCommitFragments(java.util.List<byte[]> fragments) {
        execution.setTerminalCommitFragments(fragments);
    }

    void setReleasedTaskStats(NativeTaskStats stats) {
        execution.setReleasedTaskStats(stats);
    }

    // --- Shuffle port (delegated) ---

    public int shufflePort() {
        return execution.shufflePort();
    }

    public void setShufflePort(int port) {
        execution.setShufflePort(port);
    }

    public org.pangrova.shuffle.api.ShuffleServerHandle shuffleServerHandle() {
        return execution.shuffleServerHandle();
    }

    public void setShuffleServerHandle(org.pangrova.shuffle.api.ShuffleServerHandle handle) {
        execution.setShuffleServerHandle(handle);
    }
}
