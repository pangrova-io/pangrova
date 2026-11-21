package org.pangrova.worker.task;

import org.pangrova.worker.shuffle.NativeShuffleProducerRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Releases task-scoped resources: shuffle producer registration, native executor, memory, and exchange client.
 * Extracted from {@link TaskLifecycleManager} to keep lifecycle orchestration focused on state
 * transitions.
 *
 * <p>Concurrent completion/error paths share one release future. Memory is released only after the
 * Native release call has joined the driver.
 */
final class TaskResourceReleaser {

    private static final Logger LOG = LoggerFactory.getLogger(TaskResourceReleaser.class);

    private final NativeShuffleProducerRegistration shuffleProducers;
    private final TaskManagerContext deps;
    private final Consumer<TaskInfo> memoryReleaser;

    /**
     * Creates the releaser with its collaborators.
     *
     * @param shuffleProducers shuffle transport used to stop per-executor servers
     * @param deps manager context providing the native task executor
     * @param memoryReleaser releases the task memory budget
     */
    TaskResourceReleaser(
            NativeShuffleProducerRegistration shuffleProducers,
            TaskManagerContext deps,
            Consumer<TaskInfo> memoryReleaser) {
        this.shuffleProducers = shuffleProducers;
        this.deps = deps;
        this.memoryReleaser = memoryReleaser;
    }

    /**
     * Releases all resources for a task: shuffle producer registration, native executor, memory, and exchange
     * client.
     */
    void releaseAllResources(TaskInfo info, String taskId, Runnable afterRelease) {
        var candidate = new java.util.concurrent.CompletableFuture<Void>();
        var release = info.claimResourcesRelease(candidate);
        if (release != candidate) {
            release.join();
            return;
        }
        try {
            unregisterShuffleProducerIfNeeded(info);
            releaseExecutorIfNeeded(info);
            afterRelease.run();
            memoryReleaser.accept(info);
            release.complete(null);
        } catch (RuntimeException | Error failure) {
            release.completeExceptionally(failure);
            throw failure;
        }
    }

    /**
     * Unregisters this executor from the shared Flight server before releasing its native handle.
     * Called before executor release on both completion and failure paths to prevent resource leaks
     * from orphaned registrations.
     */
    void unregisterShuffleProducerIfNeeded(TaskInfo info) {
        if (info.isWorkerExecuted()) {
            return;
        }
        byte[] receipt = info.stagedExecutorReleaseReceipt();
        if (receipt != null) {
            // This is the actual joined Native return, staged before Java reconciliation.
            // Validate its full attempt and drain before avoiding commands to a retired owner.
            validateFinalReceipt(info, receipt);
            return;
        }
        if (shuffleProducers != null) {
            long handle = info.executorHandle();
            if (handle != 0) {
                shuffleProducers.unregisterProducer(handle);
            }
        }
    }

    /**
     * Releases the Native executor once, allowing concurrent callers to wait for the same join
     * outcome before releasing task memory.
     */
    void releaseExecutorIfNeeded(TaskInfo info) {
        var candidate = new java.util.concurrent.CompletableFuture<byte[]>();
        var release = info.claimExecutorRelease(candidate);
        if (release != candidate) {
            release.join();
            return;
        }
        try {
            if (info.isWorkerExecuted()) {
                releaseWorkerExecutedExecutor(info, release);
                return;
            }
            if (TaskNativeCreationRecovery.seal(info, deps == null ? null : deps.taskExecutor())) {
                release.complete(null);
                return;
            }
            long handle = info.executorHandle();
            if (handle == 0) {
                throw new IllegalStateException("Actual Native owner has no release receipt");
            }
            byte[] receipt = releaseActualExecutor(info, handle);
            var report = validateFinalReceipt(info, receipt);
            info.setReleasedTaskStats(
                    report.hasNativeTaskStats() ? report.getNativeTaskStats().toByteArray() : null);
            if (!info.compareAndSetExecutorHandle(handle, 0)) {
                throw new IllegalStateException(
                        "Native executor handle changed during release: task=" + info.taskId());
            }
            release.complete(receipt);
            LOG.debug("event=executor_released taskId={} handle={}", info.taskId(), handle);
        } catch (RuntimeException | Error failure) {
            release.completeExceptionally(failure);
            LOG.error(
                    "event=executor_release_failed taskId={} handle={}",
                    info.taskId(),
                    info.executorHandle(),
                    failure);
            throw failure;
        }
    }

    private void releaseWorkerExecutedExecutor(
            TaskInfo info, java.util.concurrent.CompletableFuture<byte[]> release) {
        long handle = info.executorHandle();
        if (handle == 0) {
            // Creation failed before a backend owner existed; the failure report
            // will carry the no-Native-owner admission closure.
            release.complete(null);
            return;
        }
        byte[] receipt = deps.taskExecutor().releaseTask(handle)
                .orElseThrow(() -> new IllegalStateException(
                        "Worker-executed task release returned no final report: " + info.taskId()));
        info.stageExecutorReleaseReceipt(receipt);
        validateFinalReceipt(info, receipt);
        info.setReleasedTaskStats(null);
        if (!info.compareAndSetExecutorHandle(handle, 0)) {
            throw new IllegalStateException(
                    "Worker-executed handle changed during release: task=" + info.taskId());
        }
        release.complete(receipt);
        LOG.debug("event=worker_executed_released taskId={} handle={}", info.taskId(), handle);
    }

    private byte[] releaseActualExecutor(TaskInfo info, long handle) {
        if (deps == null || deps.taskExecutor() == null) {
            throw new IllegalStateException("Task executor is unavailable during release");
        }
        byte[] receipt = info.stagedExecutorReleaseReceipt();
        if (receipt != null) {
            validateFinalReceipt(info, receipt);
            return receipt;
        }
        if (deps.exchangeTransport() != null) {
            deps.exchangeTransport().releaseInputs(handle);
        }
        receipt =
                deps.taskExecutor()
                        .releaseTask(handle)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Native task release returned no final report: "
                                                        + info.taskId()));
        info.stageExecutorReleaseReceipt(receipt);
        return receipt;
    }

    private static org.pangrova.protocol.v2.TaskReport validateFinalReceipt(
            TaskInfo info, byte[] receipt) {
        var report =
                TaskCompletionMonitor.requireCanonicalTaskReport(
                        info.request().taskRequest(), receipt);
        var expectedDrain = !info.isWorkerExecuted()
                ? org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                : report.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                        ? org.pangrova.protocol.v2.ExecutionDrainState
                                .EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED
                        : org.pangrova.protocol.v2.ExecutionDrainState
                                .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER;
        if (report.getExecutionDrainState() != expectedDrain) {
            throw new IllegalStateException(
                    "Released task receipt has no actual executor drain evidence");
        }
        if (!info.isWorkerExecuted()
                && report.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                && !report.hasNativeTaskStats()) {
            throw new IllegalStateException(
                    "Finished Native task release returned no final statistics: " + info.taskId());
        }
        if (report.hasNativeTaskStats()
                && !report.getNativeTaskStats().getUnknownFields().asMap().isEmpty()) {
            throw new IllegalStateException(
                    "Released Native task statistics contain unknown fields: " + info.taskId());
        }
        return report;
    }
}
