package org.pangrova.worker.resource.memory;

/**
 * Callback interface for memory consumers that can release memory on demand.
 *
 * <p>When the {@link MemoryArbitrator} detects memory pressure and needs to
 * reclaim bytes, it invokes {@link #shrink(long)} on registered consumers.
 * Consumers should release as many bytes as possible up to the requested amount.
 *
 * <p>Implementations must be thread-safe. The shrink callback may be invoked
 * from any thread performing an allocation that triggers pressure evaluation.
 */
public interface ShrinkableMemoryConsumer {

    /**
     * Unique identifier for this consumer (used for tracking and logging).
     */
    String consumerId();

    /**
     * Current bytes held by this consumer.
     */
    long currentUsageBytes();

    /**
     * Request this consumer to release up to {@code targetBytes} of memory.
     *
     * <p>The consumer should spill, evict, or otherwise free memory and return
     * the actual number of bytes released. Returning 0 means the consumer
     * cannot release any memory at this time.
     *
     * @param targetBytes desired number of bytes to reclaim (positive)
     * @return actual bytes released (0 if unable to shrink)
     */
    long shrink(long targetBytes);
}
