package org.pangrova.connector.spi;

import org.pangrova.common.api.annotation.Experimental;

/** Permit retained by an actual connector operation, including an opened SDK reader. */
@Experimental
public interface ConnectorQueryOperation extends AutoCloseable {
    /** Records physical operation exit against the same owner without resolving its query ID again. */
    @Override
    void close();
}
