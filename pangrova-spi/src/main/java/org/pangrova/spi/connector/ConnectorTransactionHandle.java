package org.pangrova.spi.connector;

import org.pangrova.spi.annotation.Experimental;

/**
 * Marker interface for connector-specific transaction handle.
 *
 * <p>Represents an active transaction within a connector. Connectors that do not
 * support transactions may use the {@link #EMPTY} singleton constant.</p>
 */
@Experimental
public interface ConnectorTransactionHandle {

    /**
     * Empty transaction handle for connectors that do not support transactions.
     */
    ConnectorTransactionHandle EMPTY = new ConnectorTransactionHandle() {};
}
