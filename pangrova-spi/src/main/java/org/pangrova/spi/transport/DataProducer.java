package org.pangrova.spi.transport;

import org.pangrova.spi.annotation.Internal;

/**
 * Opens independent request-scoped cursors on the data transport server.
 */
@Internal
@FunctionalInterface
public interface DataProducer {

    /**
     * Opens one read request for the given ticket. Reopening retained results
     * must not reuse an exhausted cursor from a previous request.
     *
     * @param ticket the data ticket identifying the requested partition
     * @return the non-null cursor owned by this request
     */
    DataStream openStream(DataTicket ticket);
}
