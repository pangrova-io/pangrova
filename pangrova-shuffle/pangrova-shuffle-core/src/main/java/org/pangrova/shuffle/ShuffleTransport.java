package org.pangrova.shuffle;

/**
 * SPI for shuffle data transport between workers.
 *
 * <p>Abstracts the shuffle data plane so that {@code TaskLifecycleManager}
 * depends on this interface rather than concrete shuffle implementations.
 * Implementations may use Java HTTP (legacy) or native transport.</p>
 *
 * <p>Lifecycle per task:</p>
 * <ol>
 *   <li>{@link #startServer} — start serving shuffle output</li>
 *   <li>{@link #connectSource} — connect upstream shuffle sources (zero or more)</li>
 *   <li>{@link #noMoreSources} — signal all sources registered</li>
 *   <li>{@link #close} — release resources</li>
 * </ol>
 *
 * @see ShuffleServerHandle
 * @see ShuffleEndpoint
 */
public interface ShuffleTransport extends AutoCloseable {

    /**
     * Start serving shuffle output for a task. Called after task creation.
     *
     * @param executorHandle native executor handle from {@code TaskExecutor.createTask}
     * @param numPartitions  number of output partitions (must be positive)
     * @return handle with host and port for downstream connections
     */
    ShuffleServerHandle startServer(long executorHandle, int numPartitions);

    /**
     * Connect to an upstream shuffle source and feed data into the native executor.
     *
     * @param executorHandle native executor handle
     * @param sourceId       identifier of the upstream exchange source
     * @param upstream       endpoint of the upstream shuffle server
     */
    void connectSource(long executorHandle, String sourceId, ShuffleEndpoint upstream);

    /**
     * Signal that no more sources will be added for this task.
     *
     * @param executorHandle native executor handle
     */
    void noMoreSources(long executorHandle);

    /** Stop serving and release all resources. */
    @Override
    void close();
}
