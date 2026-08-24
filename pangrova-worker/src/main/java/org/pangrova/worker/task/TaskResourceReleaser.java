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
 * <p>Executor release uses CAS on the {@link TaskInfo} handle so concurrent
 * completion/error paths never double-release the native handle.</p>
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
    void releaseAllResources(TaskInfo info, String taskId) {
        stopShuffleServerIfNeeded(info);
        releaseExecutorIfNeeded(info);
        memoryReleaser.accept(info);
        destroyExchangeClientIfNeeded(taskId);
    }

    /**
     * Stops the per-executor shuffle server to release the bound port and
     * associated resources. Called before executor release on both completion
     * and failure paths to prevent resource leaks from orphaned servers.
     */
    void stopShuffleServerIfNeeded(TaskInfo info) {
        if (shuffleTransport instanceof org.pangrova.worker.shuffle.NativeShuffleTransport nst) {
            long handle = info.executorHandle();
            if (handle != 0) {
                nst.stopServer(handle);
            }
        }
    }

    /**
     * Cleanup hook for exchange resources on task completion/failure.
     * With native transport, exchange lifecycle is managed in C++.
     */
    void destroyExchangeClientIfNeeded(String taskId) {
        // No-op: native exchange manages connections in C++
    }

    /**
     * Releases the native executor handle using CAS to prevent concurrent double-release (R7.8).
     * Only the thread that successfully claims the handle via {@code compareAndSet(handle, 0)}
     * will call {@code releaseTask}, eliminating the race between onTaskCompleted and onTaskError.
     */
    void releaseExecutorIfNeeded(TaskInfo info) {
        long handle = info.executorHandle();
        if (handle == 0 || deps == null) {
            return;
        }
        // CAS: only the winner releases the native handle
        if (!info.compareAndSetExecutorHandle(handle, 0)) {
            LOG.debug("event=executor_release_skipped taskId={} reason=cas_lost handle={}",
                    info.taskId(), handle);
            return;
        }
        try {
            deps.taskExecutor().releaseTask(handle);
            LOG.debug("event=executor_released taskId={} handle={}", info.taskId(), handle);
        } catch (Exception e) {
            LOG.warn("event=executor_release_failed taskId={} handle={} error={}",
                    info.taskId(), handle, e.getMessage());
        }
    }
}
