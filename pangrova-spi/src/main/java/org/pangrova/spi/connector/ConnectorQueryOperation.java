package org.pangrova.spi.connector;

import org.pangrova.spi.annotation.Experimental;

/** Permit retained by an actual connector operation, including an opened SDK reader. */
@Experimental
public interface ConnectorQueryOperation extends AutoCloseable {
    /** Records physical operation exit against the same owner without resolving its query ID again. */
    @Override
    void close();
}
