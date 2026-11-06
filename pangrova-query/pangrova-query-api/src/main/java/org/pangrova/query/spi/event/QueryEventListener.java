package org.pangrova.query.spi.event;

import org.pangrova.query.api.event.QueryCompletedEvent;
import org.pangrova.query.api.event.QueryCreatedEvent;
import org.pangrova.query.api.event.SplitCompletedEvent;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for observing query lifecycle events. Modeled on Trino's {@code EventListener}
 * ({@code queryCreated}/{@code queryCompleted}).
 *
 * <p>Events describe admission, completion, and split execution. Delivery is local to
 * the running process: this SPI does not guarantee persistence, replay, deduplication,
 * or exactly-once delivery across retries and restarts. A security denial may produce
 * a completion event without an admission event. Statistics contain only the measurements
 * available to the publisher; their presence does not imply a complete accounting of
 * execution resources. Durable audit and publication must use their domain contracts.</p>
 *
 * <p>Implementations must be thread-safe and must not block the calling (query
 * execution) thread for long; the dispatcher fault-isolates each callback so a throwing
 * listener cannot abort the query or other listeners.</p>
 */
@Experimental
public interface QueryEventListener {

    /**
     * Observes a query admitted for execution.
     *
     * @param event the created-query event
     */
    default void queryCreated(QueryCreatedEvent event) {
        // intentional no-op: implementations override as needed
    }

    /**
     * Observes a query reaching a terminal state, including cancellation or denial.
     *
     * @param event the completed-query event
     */
    default void queryCompleted(QueryCompletedEvent event) {
        // intentional no-op: implementations override as needed
    }

    /**
     * Called when a split completes processing (succeeded or failed).
     *
     * <p>This callback carries split-level timing and I/O metrics. The publisher
     * determines the dispatch thread; implementations must not assume asynchronous
     * delivery or any ordering relative to
     * {@link #queryCompleted(QueryCompletedEvent)} for the same query.</p>
     *
     * @param event the split-completed event
     */
    default void splitCompleted(SplitCompletedEvent event) {
        // intentional no-op: implementations override as needed
    }
}
