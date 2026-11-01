package org.pangrova.spi.observability.event;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for observing query lifecycle as discrete, once-per-query events carrying a
 * complete statistics payload. Modeled on Trino's {@code EventListener}
 * ({@code queryCreated}/{@code queryCompleted}).
 *
 * <p>This SPI fires exactly once when a query is admitted and once when it reaches a
 * terminal state, with a {@link QueryCompletedEvent} that carries the final state, timing,
 * and {@link QueryStatistics}. It is the single integration point intended for audit,
 * lineage, and cost subsystems.</p>
 *
 * <p>Implementations must be thread-safe and must not block the calling (query
 * execution) thread for long; the dispatcher fault-isolates each callback so a throwing
 * listener cannot abort the query or other listeners.</p>
 */
@Experimental
public interface QueryEventListener {

    /**
     * Called exactly once when a query is admitted for execution.
     *
     * @param event the created-query event
     */
    default void queryCreated(QueryCreatedEvent event) {
        // intentional no-op: implementations override as needed
    }

    /**
     * Called exactly once when a query reaches a terminal state (succeeded or failed).
     *
     * @param event the completed-query event
     */
    default void queryCompleted(QueryCompletedEvent event) {
        // intentional no-op: implementations override as needed
    }

    /**
     * Called when a split completes processing (succeeded or failed).
     *
     * <p>This callback fires once per split and carries split-level timing and I/O
     * metrics. It is dispatched asynchronously from the task execution thread, so
     * implementations must not assume any ordering relative to
     * {@link #queryCompleted(QueryCompletedEvent)} for the same query.</p>
     *
     * @param event the split-completed event
     */
    default void splitCompleted(SplitCompletedEvent event) {
        // intentional no-op: implementations override as needed
    }
}
