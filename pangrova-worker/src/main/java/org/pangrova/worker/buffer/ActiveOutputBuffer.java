package org.pangrova.worker.buffer;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe holder for the currently active OutputBufferManager.
 *
 * <p>Since P0-FPV-5, the primary buffer lookup path is through
 * {@link QueryOutputBufferRegistry} (query-scoped isolation).
 * The global {@link #get()} fallback remains for backward compatibility
 * with components that have not yet migrated to query-scoped lookup.</p>
 *
 * <p>The {@code queryId} binding allows DoGet consumers to detect when a swap
 * has occurred mid-stream by comparing the queryId they locked at stream start
 * against the current queryId.</p>
 */
@ThreadSafe
public final class ActiveOutputBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveOutputBuffer.class);

    private final AtomicReference<OutputBufferManager> ref;
    private volatile String queryId;
    private volatile QueryOutputBufferRegistry registry;

    public ActiveOutputBuffer(OutputBufferManager initial) {
        this.ref = new AtomicReference<>(initial);
        this.queryId = null;
    }

    /**
     * Attaches a query-scoped buffer registry.
     * Once attached, {@link #getForQuery(String)} routes through the registry.
     */
    public void setRegistry(QueryOutputBufferRegistry registry) {
        this.registry = registry;
    }

    /**
     * Returns the registry, or null if not attached.
     */
    public QueryOutputBufferRegistry registry() {
        return registry;
    }

    /**
     * Returns the buffer for a specific queryId from the registry.
     * Falls back to the global buffer if the registry is not attached
     * or the queryId is not found.
     */
    public OutputBufferManager getForQuery(String requestedQueryId) {
        QueryOutputBufferRegistry reg = this.registry;
        if (reg != null && requestedQueryId != null) {
            OutputBufferManager scoped = reg.get(requestedQueryId);
            if (scoped != null) {
                return scoped;
            }
        }
        LOG.warn("event=output_buffer_fallback_to_global queryId={} "
                + "reason=registry_not_attached_or_query_not_found", requestedQueryId);
        return ref.get();
    }

    /**
     * Returns the currently active global buffer (legacy fallback).
     */
    public OutputBufferManager get() {
        return ref.get();
    }

    /**
     * Returns the queryId bound to the current global buffer.
     */
    public String queryId() {
        return queryId;
    }

    /**
     * Returns true if the current buffer is still bound to the given queryId.
     * Used by DoGet consumers to detect mid-stream swaps.
     */
    public boolean isBoundTo(String expectedQueryId) {
        return expectedQueryId != null && expectedQueryId.equals(this.queryId);
    }

    /**
     * Atomically replaces the active buffer with a new one and closes the previous buffer.
     * Returns the previous buffer.
     *
     * @param newBuffer the replacement buffer
     * @return the previous buffer (now closed)
     */
    public OutputBufferManager swap(OutputBufferManager newBuffer) {
        OutputBufferManager prev = ref.getAndSet(newBuffer);
        this.queryId = null;
        prev.close();
        return prev;
    }
}
