package org.pangrova.worker.task;

import org.pangrova.shuffle.ShuffleTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Releases task-scoped resources: shuffle server, native executor, memory,
 * and exchange client. Extracted from {@link TaskLifecycleManager} to keep
 * lifecycle orchestration focused on state transitions.
 *
 * <p>Concurrent completion/error paths share one release future. Memory is
 * released only after the Native release call has joined the driver.</p>
 */
final class TaskResourceReleaser {

    private static final Logger LOG = LoggerFactory.getLogger(TaskResourceReleaser.class);

    private final ShuffleTransport shuffleTransport;
    private final TaskManagerContext deps;
    private final Consumer<TaskInfo> memoryReleaser;

    /**
     * Creates the releaser with its collaborators.
     *
     * @param shuffleTransport shuffle transport used to stop per-executor servers
     * @param deps             manager context providing the native task executor
     * @param memoryReleaser   releases the task memory budget
     */
    TaskResourceReleaser(ShuffleTransport shuffleTransport, TaskManagerContext deps,
                         Consumer<TaskInfo> memoryReleaser) {
        this.shuffleTransport = shuffleTransport;
        this.deps = deps;
        this.memoryReleaser = memoryReleaser;
    }

    /**
     * Releases all resources for a task: shuffle server, native executor,
     * memory, and exchange client.
     */
    void releaseAllResources(TaskInfo info, String taskId, Runnable afterRelease) {
        var candidate = new java.util.concurrent.CompletableFuture<Void>();
        var release = info.claimResourcesRelease(candidate);
        if (release != candidate) {
            release.join();
            return;
        }
        try {
            stopShuffleServerIfNeeded(info);
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
     * Stops the per-executor shuffle server to release the bound port and
     * associated resources. Called before executor release on both completion
     * and failure paths to prevent resource leaks from orphaned servers.
     */
    void stopShuffleServerIfNeeded(TaskInfo info) {
        if (info.shuffleServerHandle() != null
                && shuffleTransport instanceof org.pangrova.worker.shuffle.NativeShuffleTransport nst) {
            long handle = info.executorHandle();
            if (handle != 0) {
                nst.stopServer(handle);
                // Keep the successful stop checkpoint when a later Native/RPC join fails.
                // Reissuing task commands after release seals operation admission cannot drain it.
                info.setShuffleServerHandle(null);
            }
        }
    }

    /**
     * Releases the Native executor once, allowing concurrent callers to wait
     * for the same join outcome before releasing task memory.
     */
    void releaseExecutorIfNeeded(TaskInfo info) {
        var candidate = new java.util.concurrent.CompletableFuture<byte[]>();
        var release = info.claimExecutorRelease(candidate);
        if (release != candidate) {
            release.join();
            return;
        }
        try {
            if (TaskNativeCreationRecovery.seal(info, deps == null ? null : deps.taskExecutor())) {
                release.complete(null);
                return;
            }
            long handle = info.executorHandle();
            if (handle == 0) throw new IllegalStateException("Actual Native owner has no release receipt");
            byte[] receipt = releaseActualExecutor(info, handle);
            var report = validateFinalReceipt(info, receipt);
            info.setReleasedTaskStats(report.hasNativeTaskStats()
                    ? report.getNativeTaskStats().toByteArray() : null);
            if (!info.compareAndSetExecutorHandle(handle, 0)) {
                throw new IllegalStateException(
                        "Native executor handle changed during release: task=" + info.taskId());
            }
            release.complete(receipt);
            LOG.debug("event=executor_released taskId={} handle={}", info.taskId(), handle);
        } catch (RuntimeException | Error failure) {
            release.completeExceptionally(failure);
            LOG.error("event=executor_release_failed taskId={} handle={}",
                    info.taskId(), info.executorHandle(), failure);
            throw failure;
        }
    }

    private byte[] releaseActualExecutor(TaskInfo info, long handle) {
        if (deps == null || deps.taskExecutor() == null) {
            throw new IllegalStateException("Task executor is unavailable during release");
        }
        if (deps.exchangeTransport() != null) deps.exchangeTransport().releaseInputs(handle);
        byte[] receipt = info.stagedExecutorReleaseReceipt();
        if (receipt == null) {
            receipt = deps.taskExecutor().releaseTask(handle).orElseThrow(() ->
                    new IllegalStateException("Native task release returned no final report: " + info.taskId()));
            info.stageExecutorReleaseReceipt(receipt);
        }
        return receipt;
    }

    private static org.pangrova.protocol.v2.TaskReport validateFinalReceipt(TaskInfo info, byte[] receipt) {
        var report = TaskCompletionMonitor.requireCanonicalTaskReport(info.request().taskRequest(), receipt);
        if (report.getExecutionDrainState() != org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
            throw new IllegalStateException("Native release receipt has no actual executor drain evidence");
        }
        if (report.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                && !report.hasNativeTaskStats()) {
            throw new IllegalStateException(
                    "Finished Native task release returned no final statistics: " + info.taskId());
        }
        if (report.hasNativeTaskStats() && !report.getNativeTaskStats().getUnknownFields().asMap().isEmpty()) {
            throw new IllegalStateException("Released Native task statistics contain unknown fields: " + info.taskId());
        }
        return report;
    }
}
