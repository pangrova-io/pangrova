package org.pangrova.governance.api.lineage;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for sending lineage events to a backend (Marquez, DataHub, etc.).
 */
@Experimental
public interface LineageBackend extends AutoCloseable {
    void send(LineageEvent event);

    /** Releases transport resources after all sends finish; called once by the owner on retirement. */
    @Override
    void close() throws java.io.IOException;
}
