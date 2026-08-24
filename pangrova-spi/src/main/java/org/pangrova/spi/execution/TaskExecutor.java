package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Stable;

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
 * <p>Data pull semantics: {@link #getNextBatchResult} and
 * {@link #getShuffleBatchResult} return discriminated {@link BatchResult}
 * values (DATA / EOF / NOT_READY / ERROR).</p>
 */
@Stable(since = "1.0")
public interface TaskExecutor {

    /**
     * Creates a task from serialized TaskRequest protobuf bytes.
     *
     * @param taskRequestBytes serialized TaskRequest protobuf
     * @return a non-negative executor handle for subsequent calls
     */
    long createTask(byte[] taskRequestBytes);

    /**
     * Starts execution of a previously created task.
     *
     * @param executorHandle handle returned by {@link #createTask}
     */
    void startTask(long executorHandle);

    /**
     * Releases all resources associated with the task.
     *
     * <p>Must be called exactly once per {@link #createTask} call,
     * regardless of whether the task succeeded or failed.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     */
    void releaseTask(long executorHandle);

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
    default byte[] executeCommand(long executorHandle, byte[] commandProto) {
        throw new UnsupportedOperationException("executeCommand not implemented");
    }

    /**
     * Pulls the next result batch with discriminated status.
     *
     * <p>Returns a {@link BatchResult} that preserves full status semantics:</p>
     * <ul>
     *   <li>{@code DATA} — valid batch bytes available</li>
     *   <li>{@code EOF} — end of stream, no more batches</li>
     *   <li>{@code NOT_READY} — temporary, output queue empty but task running</li>
     *   <li>{@code ERROR} — native execution error with message</li>
     * </ul>
     *
     * <p>Default returns EOF. In the current architecture, result data is
     * served by native transport servers; the production implementation
     * returns EOF immediately.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return discriminated batch result, never null
     */
    default BatchResult getNextBatchResult(long executorHandle) {
        return BatchResult.eof();
    }

    /**
     * Pulls the next shuffle batch with discriminated status.
     *
     * <p>Returns a {@link BatchResult} that preserves full status semantics:</p>
     * <ul>
     *   <li>{@code DATA} — valid shuffle batch bytes available</li>
     *   <li>{@code EOF} — end of stream, no more shuffle batches</li>
     *   <li>{@code NOT_READY} — temporary, shuffle queue empty but task running</li>
     *   <li>{@code ERROR} — native execution error with message</li>
     * </ul>
     *
     * <p>Default returns EOF. In the current architecture, shuffle data is
     * served by native transport servers; the production implementation
     * inherits this default.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return discriminated batch result, never null
     */
    default BatchResult getShuffleBatchResult(long executorHandle) {
        return BatchResult.eof();
    }

    /**
     * Checks whether the task has finished execution (completed or failed).
     *
     * <p>Used by the autonomous stage completion monitor to poll task state
     * without starting Java data threads.</p>
     *
     * @param executorHandle handle returned by {@link #createTask}
     * @return true if the task has finished (completed, failed, or cancelled)
     */
    default boolean isFinished(long executorHandle) {
        return false;
    }

    /**
     * Reports the execution capabilities of this executor.
     * The scheduler uses this information to match tasks to workers
     * with sufficient resources and compatible formats.
     *
     * <p>Default implementation returns {@link ExecutionCapabilities#unlimited()},
     * indicating no constraints. Implementations should override this to
     * report actual limits.</p>
     *
     * @return the executor's capabilities, never null
     */
    default ExecutionCapabilities capabilities() {
        return ExecutionCapabilities.unlimited();
    }

}
