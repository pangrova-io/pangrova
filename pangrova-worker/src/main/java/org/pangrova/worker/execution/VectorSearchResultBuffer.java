package org.pangrova.worker.execution;

import org.pangrova.spi.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory buffer storing serialized vector search results keyed by taskId.
 *
 * <p>After the worker executes a vector ANN search via
 * {@link VectorSearchTaskInterceptor}, the serialized
 * {@link org.pangrova.protocol.v1.VectorSearchTaskResult} proto bytes are stored
 * here. The coordinator fetches them via the worker's HTTP API after task
 * completion, then applies cross-worker global Top-K merge.</p>
 *
 * <p>Entries are automatically removed after retrieval (single-consume semantics)
 * to prevent unbounded growth. A fallback eviction on task terminal state is
 * also expected from the task lifecycle manager.</p>
 *
 * <p>Thread-safe: backed by {@link ConcurrentHashMap}.</p>
 */
@ThreadSafe
public final class VectorSearchResultBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(VectorSearchResultBuffer.class);
    private static final int DEFAULT_MAX_CAPACITY = 10_000;

    private final ConcurrentHashMap<String, byte[]> results = new ConcurrentHashMap<>();
    private final int maxCapacity;

    public VectorSearchResultBuffer() {
        this(DEFAULT_MAX_CAPACITY);
    }

    /** Creates a buffer with the given maximum number of retained results. */
    public VectorSearchResultBuffer(int maxCapacity) {
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity must be positive");
        }
        this.maxCapacity = maxCapacity;
    }

    /**
     * Stores the serialized vector search result for a given task.
     *
     * @param taskId      the worker-internal task identifier
     * @param resultBytes serialized VectorSearchTaskResult proto bytes
     */
    public void put(String taskId, byte[] resultBytes) {
        results.compute(taskId, (key, existing) -> {
            if (existing == null && results.size() >= maxCapacity) {
                LOG.warn("event=vector_result_buffer_full taskId={} capacity={}", taskId, maxCapacity);
                throw new IllegalStateException(
                        "VectorSearchResultBuffer capacity exceeded: " + maxCapacity);
            }
            return resultBytes;
        });
        LOG.debug("event=vector_result_buffered taskId={} bytes={}", taskId, resultBytes.length);
    }

    /**
     * Retrieves and removes the serialized vector search result for a given task.
     * Returns null if no result is stored for that taskId.
     *
     * @param taskId the worker-internal task identifier
     * @return serialized result bytes, or null if not found
     */
    public byte[] take(String taskId) {
        byte[] data = results.remove(taskId);
        if (data != null) {
            LOG.debug("event=vector_result_consumed taskId={} bytes={}", taskId, data.length);
        }
        return data;
    }

    /**
     * Retrieves the serialized vector search result without removing it.
     *
     * @param taskId the worker-internal task identifier
     * @return serialized result bytes, or null if not found
     */
    public byte[] peek(String taskId) {
        return results.get(taskId);
    }

    /**
     * Removes any buffered result for the given task (cleanup on failure/cancel).
     */
    public void remove(String taskId) {
        results.remove(taskId);
    }

    /**
     * Returns the number of currently buffered results.
     */
    public int size() {
        return results.size();
    }
}
