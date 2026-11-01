package org.pangrova.spi.connector;

/**
 * API-owned null object for connectors that do not support transactions.
 */
final class NoopTransactionHandle implements ConnectorTransactionHandle {

    /** Shared no-op transaction handle. */
    static final NoopTransactionHandle INSTANCE = new NoopTransactionHandle();

    private NoopTransactionHandle() {
    }
}
