package org.pangrova.spi.execution.eventlistener;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;

/**
 * Factory for creating {@link QueryEventListener} instances.
 *
 * <p>Each event listener plugin registers an {@code EventListenerFactory} via
 * {@link java.util.ServiceLoader}. The engine discovers factories at startup
 * and uses them to create listener instances that observe the query lifecycle.</p>
 *
 * <p>Multiple factories may be registered; the engine will create one listener
 * per factory and dispatch events to all of them. Listener invocation is
 * fault-isolated: a throwing listener does not affect other listeners or the
 * query execution path.</p>
 *
 * @see QueryEventListener
 */
@Experimental(reason = "G-SPI-08: event listener plugin SPI")
public interface EventListenerFactory {

    /**
     * Returns the unique name of this event listener type.
     *
     * <p>This name is used in configuration to select the listener
     * implementation (e.g., {@code "audit-log"}, {@code "lineage"}).</p>
     *
     * @return the listener name, never null
     */
    String getName();

    /**
     * Creates a new event listener instance.
     *
     * @param config the listener configuration properties
     * @return a new event listener instance, never null
     */
    QueryEventListener create(Map<String, String> config);
}
