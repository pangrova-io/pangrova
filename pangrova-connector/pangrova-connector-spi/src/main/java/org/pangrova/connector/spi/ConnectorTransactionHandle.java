package org.pangrova.connector.spi;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Marker interface for connector-specific transaction handle.
 *
 * <p>Represents an active transaction within a connector. Connectors that do not
 * support transactions may use {@link #empty()}.</p>
 */
@Experimental
public interface ConnectorTransactionHandle {

    /**
     * Returns the shared handle for connectors that do not support transactions.
     *
     * @return the API-owned no-op transaction handle
     */
    static ConnectorTransactionHandle empty() {
        return NoopTransactionHandle.INSTANCE;
    }
}
