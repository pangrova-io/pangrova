package org.pangrova.shuffle;

/**
 * SPI for shuffle data transport between workers.
 *
 * <p>Abstracts the shuffle data plane so that {@code TaskLifecycleManager}
 * depends on this interface rather than concrete shuffle implementations.
 * Implementations own only the producer-side Native transport lifecycle.</p>
 *
 * <p>Lifecycle per task:</p>
 * <ol>
 *   <li>{@link #startServer} — start serving shuffle output</li>
 *   <li>{@link #close} — release resources</li>
 * </ol>
 *
 * @see ShuffleServerHandle
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

    /** Stop serving and release all resources. */
    @Override
    void close();
}
