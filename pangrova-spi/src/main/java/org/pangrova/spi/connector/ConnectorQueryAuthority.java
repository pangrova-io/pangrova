package org.pangrova.spi.connector;

import org.pangrova.spi.annotation.Experimental;

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

    /** Retains and begins the actual resolved metadata instance with this same public lifetime. */
    org.pangrova.spi.connector.metadata.ConnectorMetadata beginMetadata(String resolvedCatalog,
            java.util.function.Supplier<org.pangrova.spi.connector.metadata.ConnectorMetadata> lookup);

    /** Releases retained metadata after physical drain; failed cleanup remains owned for retry. */
    org.pangrova.spi.connector.metadata.ConnectorQueryCleanupResult cleanupMetadata();

    /**
     * Atomically enters an operation against the actual open query owner.
     * The returned permit must remain owned until all resources acquired by that operation exit.
     *
     * @return an exactly-once operation permit
     * @throws IllegalStateException if query admission is already closed
     */
    ConnectorQueryOperation enterOperation();
}
