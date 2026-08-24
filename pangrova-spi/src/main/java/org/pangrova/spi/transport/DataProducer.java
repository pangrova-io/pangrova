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
}
