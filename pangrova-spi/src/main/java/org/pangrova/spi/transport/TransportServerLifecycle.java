package org.pangrova.spi.transport;

import org.pangrova.spi.annotation.Internal;

/**
 * SPI contract for data transport server lifecycle management.
 *
 * <p>Allows modules (e.g., Coordinator) to manage a transport server
 * without depending on the concrete implementation in pangrova-worker.
 * Implementations must be thread-safe: start/stop are idempotent.</p>
 */
@Internal
public interface TransportServerLifecycle {

    /**
     * Starts the transport server. Idempotent — calling start on an
     * already-running server is a no-op.
     *
     * @throws IllegalStateException if the server cannot bind its port
     */
    void start();

    /**
     * Stops the transport server and releases associated resources.
     * Idempotent — calling stop on an already-stopped server is a no-op.
     */
    void stop();

    /**
     * Returns {@code true} if the server is currently accepting connections.
     */
    boolean isRunning();

    /**
     * Returns the actual bound port. Useful when the server was configured
     * with port 0 (random port assignment).
     *
     * @return the port the server is listening on, or the requested port
     *         if the server has not started yet
     */
    int getPort();
}
