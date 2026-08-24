package org.pangrova.shuffle.exchange;

import org.pangrova.spi.annotation.Internal;

import java.util.List;

/**
 * SPI for exchange data input to a native executor.
 *
 * <p>Abstracts the exchange data plane so that {@code TaskLifecycleManager}
 * depends on this interface rather than concrete exchange implementations.
 * Implementations may use Java HTTP fetch + JNI inject (legacy)
 * or native-side transport fetch (target).</p>
 *
 * <p>Lifecycle per task:</p>
 * <ol>
 *   <li>{@link #startExchange} — begin fetching from initial sources</li>
 *   <li>{@link #addSources} — dynamically add sources (for lazy split assignment)</li>
 *   <li>{@link #noMoreSources} — signal all sources registered</li>
 * </ol>
 *
 * @see ExchangeEndpoint
 */
@Internal
public interface ExchangeTransport {

    /**
     * Feed exchange data from upstream sources into the executor.
     *
     * @param executorHandle native executor handle from {@code TaskExecutor.createTask}
     * @param sources        initial list of upstream exchange endpoints
     */
    void startExchange(long executorHandle, List<ExchangeEndpoint> sources);

    /**
     * Dynamically add new sources for lazy split assignment.
     *
     * @param executorHandle native executor handle
     * @param newSources     additional upstream exchange endpoints
     */
    void addSources(long executorHandle, List<ExchangeEndpoint> newSources);

    /**
     * Signal that no more sources will be added for this task.
     *
     * @param executorHandle native executor handle
     */
    void noMoreSources(long executorHandle);
}
