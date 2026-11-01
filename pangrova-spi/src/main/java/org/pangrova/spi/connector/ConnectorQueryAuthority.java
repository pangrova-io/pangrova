package org.pangrova.spi.connector;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Engine-issued connector lifetime backed by the actual query creation owner.
 * Session properties and a query identifier alone cannot issue this authority.
 */
@Experimental
public interface ConnectorQueryAuthority {
    /** Returns the immutable query identifier of the creation owner. */
    String queryId();

    /** Returns whether the actual query owner has closed admission; closure is irreversible. */
    boolean isClosed();

    /**
     * Claims cleanup against this exact owner only after its issued tasks and operation permits exit.
     * Failed claims retain the owner for retry; a completed claim is idempotent.
     */
    void requireDrained();

    /**
     * Atomically enters an operation against the actual open query owner.
     * The returned permit must remain owned until all resources acquired by that operation exit.
     *
     * @return an exactly-once operation permit
     * @throws IllegalStateException if query admission is already closed
     */
    ConnectorQueryOperation enterOperation();
}
