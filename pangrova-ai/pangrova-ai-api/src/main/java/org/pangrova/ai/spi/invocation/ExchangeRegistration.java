package org.pangrova.ai.spi.invocation;

import org.pangrova.common.api.annotation.Experimental;

/** Task-owned registration, borrowed by the transport until local delivery finalizes. */
@Experimental
public interface ExchangeRegistration extends AutoCloseable {
    /**
     * Idempotently unregisters the exchange. Does not cancel it, close its attempt, or
     * imply a remote commit. The transport must finish delivery before unregistering.
     */
    @Override
    void close();
}
