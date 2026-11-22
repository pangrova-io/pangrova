package org.pangrova.worker.execution;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.Optional;

/**
 * SPI for executing tasks against the native engine.
 *
 * <p>Abstracts task execution behind a stable contract so that
 * {@code TaskManager} depends on this interface rather than static
 * {@code NativeRuntime} calls. The sole production implementation is
 * {@code NativeTaskExecutor} in {@code pangrova-worker}.</p>
 *
 * <p>All handle values returned by {@link #createTask} are opaque
 * native pointers managed by the underlying engine. Callers must
 * call {@link #releaseTask} when execution completes or fails.</p>
 *
 * <p>The executor owns task lifecycle and the fenced handoff to Native
 * transport ingress.</p>
 */
@Stable(since = "1.0")
public interface TaskExecutor {

    /**
     * Creates a task from the canonical request parsed at Worker ingress.
     *
     * @param request canonical task and execution bindings
     * @return a non-negative executor handle for subsequent calls
     */
    long createTask(TaskRequest request);

    /**
     * Starts execution of a previously created task.
     *
     * @param executorHandle handle returned by {@link #createTask}
     */
    void startTask(long executorHandle);

    /**
     * Releases all resources associated with the task.
     *
     * <p>Successful drain returns the actual canonical final TaskReport. A
     * failed drain retains ownership and throws so the caller can retry.
     * Absence means this executor no longer tracks the handle; it is not a
     * synthesized successful task or proof that another owner released it.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return the final canonical report, or empty when the handle is untracked
     */
    Optional<byte[]> releaseTask(long executorHandle);

    /**
     * Requests cancellation of a running task.
     *
     * <p>Cancellation is best-effort; the task may complete before
     * the cancellation takes effect.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     */
    void cancelTask(long executorHandle);

    /**
     * Executes a control-plane command against the native engine.
     * All low-frequency control operations (dynamic filter, memory limit,
     * prefetch, evict, metrics, cache stats) route through this channel.
     *
     * @param executorHandle task handle (0 for global commands)
     * @param commandProto   serialized NativeCommand proto bytes
     * @return serialized NativeCommandResponse proto bytes
     */
    byte[] executeCommand(long executorHandle, byte[] commandProto);

    /** Passes frame metadata and one owned exchange chunk separately to Native ingress. */
    byte[] pushExchangeChunk(long executorHandle, byte[] metadata, byte[] data);

    /**
     * Checks whether the task has finished execution (completed or failed).
     *
     * <p>Used by the autonomous stage completion monitor to poll task state
     * without starting Java data threads.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return true if the task has finished (completed, failed, or cancelled)
     */
    boolean isFinished(long executorHandle);

    /** Waits up to the supplied interval for this executor to publish a terminal state. */
    boolean awaitFinished(long executorHandle, int waitMillis) throws InterruptedException;

    /**
     * Identifies a handle owned by a Worker-JVM execution backend rather than
     * the Native engine. Lifecycle callers must skip Native-only memory and
     * drain commands for such handles.
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return true when the handle belongs to a Worker-executed backend
     */
    default boolean isWorkerExecuted(long executorHandle) {
        return false;
    }

    /**
     * Returns every commit fragment staged by a Worker-executed task.
     * Native tasks use their write-result metadata instead.
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return owned fragment copies, empty when none were staged
     */
    default java.util.List<byte[]> getTaskCommitFragments(long executorHandle) {
        return java.util.List.of();
    }

    /**
     * Returns the executor's canonical v2 TaskReport envelope.
     *
     * <p>The status is control-plane metadata only. Implementations must never
     * place result batches, candidate rows, vectors, or artifact bytes in this
     * envelope.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return the actual serialized canonical v2 TaskReport
     */
    byte[] getTaskStatus(long executorHandle);

    /**
     * Returns the executor's canonical v2 NativeTaskStats envelope.
     *
     * <p>This is control-plane telemetry only. Implementations must capture it
     * before releasing the executor handle; callers must not synthesize an
     * all-zero success record when it is unavailable.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return the actual serialized canonical v2 NativeTaskStats
     */
    byte[] getTaskStats(long executorHandle);

}
