package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Internal;

/**
 * SPI for managing the result transport lifecycle on a worker node.
 *
 * <p>In the current architecture, query results flow entirely within C++:
 * the native engine serializes results and serves them via a data exchange server.
 * This interface provides Java-side lifecycle control (start/stop) for
 * the native result server.</p>
 *
 * @see ResultBatch
 */
@Internal
public interface ResultTransport extends AutoCloseable {

    /**
     * Starts the result transport server on the given port.
     *
     * @param preferredPort preferred port (0 for auto-assign)
     * @return the actual bound port
     */
    int startServer(int preferredPort);

    /** Stops the result transport server gracefully. */
    void stopServer();

    /**
     * Returns the bound port of the result server, or -1 if not started.
     */
    int boundPort();

    /** Release all resources associated with this transport. */
    @Override
    void close();
}
