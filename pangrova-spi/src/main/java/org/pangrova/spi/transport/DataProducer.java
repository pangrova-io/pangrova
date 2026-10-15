package org.pangrova.spi.transport;

import org.pangrova.spi.annotation.Internal;

/**
 * Functional interface for handling DoGet requests on the data transport server.
 */
@Internal
@FunctionalInterface
public interface DataProducer {

    /**
     * Produces data for the given ticket.
     *
     * @param ticket the data ticket identifying the requested partition
     * @return raw data bytes if available; empty array if stream ended (no more data);
     *         null if data is not yet ready (caller should retry)
     */
    byte[] doGet(DataTicket ticket);

    /**
     * Whether one non-empty response contains the complete result for a DoGet request.
     * Snapshot producers can retain bytes for independent retries; streaming producers
     * continue to return batches until they return an empty array for EOF.
     */
    default boolean singleResponsePerRequest() {
        return false;
    }
}
