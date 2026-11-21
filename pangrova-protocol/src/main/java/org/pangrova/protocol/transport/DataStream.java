package org.pangrova.protocol.transport;

import org.pangrova.common.api.annotation.Internal;

/**
 * Request-scoped cursor over serialized batches. The transport owns one cursor
 * per request and serializes its reads, including handoff between its threads.
 * Returned bytes must remain immutable while the transport decodes them.
 */
@Internal
@FunctionalInterface
public interface DataStream {
    /**
     * Reads the next batch from this request, not from a global query cursor.
     *
     * @return data bytes; an empty array at EOF; null only while not ready
     */
    byte[] next();
}
