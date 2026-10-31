package org.pangrova.worker.buffer;

import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query-scoped output buffer registry.
 *
 * <p>Each query gets its own {@link OutputBufferManager}, keyed by queryId.
 * This replaces the global {@link ActiveOutputBuffer#swap} pattern that caused
 * cross-query contamination when multiple queries ran concurrently on the
 * same worker (P0-FPV-5).</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap provides thread-safe per-key
 * operations; no external synchronization required.</p>
 */
@ThreadSafe
public final class QueryOutputBufferRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(QueryOutputBufferRegistry.class);

    private final ConcurrentHashMap<String, OutputBufferManager> buffers = new ConcurrentHashMap<>();

    /**
     * Registers a buffer for the given queryId.
     *
     * @param queryId the query identifier
     * @param buffer  the output buffer for this query
     * @throws IllegalStateException if a buffer is already registered for this queryId
     */
    public void register(String queryId, OutputBufferManager buffer) {
        OutputBufferManager prev = buffers.putIfAbsent(queryId, buffer);
        if (prev != null) {
            throw new IllegalStateException(
                    "Output buffer already registered for queryId=" + queryId);
        }
        LOG.debug("event=buffer_registered queryId={}", queryId);
    }

    /**
     * Returns the buffer for the given queryId, or null if not registered.
     */
    public OutputBufferManager get(String queryId) {
        return buffers.get(queryId);
    }

    /**
     * Removes and closes the buffer for the given queryId.
     *
     * @return the removed buffer, or null if no buffer was registered
     */
    public OutputBufferManager removeAndClose(String queryId) {
        OutputBufferManager removed = buffers.remove(queryId);
        if (removed != null) {
            removed.close();
            LOG.debug("event=buffer_removed_and_closed queryId={}", queryId);
        }
        return removed;
    }

    /**
     * Returns the number of currently registered buffers.
     */
    public int size() {
        return buffers.size();
    }

    /**
     * Returns true if a buffer is registered for the given queryId.
     */
    public boolean contains(String queryId) {
        return buffers.containsKey(queryId);
    }
}
