package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Internal;

/**
 * SPI for observing query lifecycle transitions.
 * Implementations can perform auditing, metrics collection,
 * or resource management in response to query state changes.
 */
@Internal
public interface QueryLifecycleListener {

    /**
     * Called when a query begins execution.
     *
     * @param queryId unique query identifier
     * @param sql     the SQL text being executed
     */
    void onQueryStarted(String queryId, String sql);

    /**
     * Called when a query completes successfully.
     *
     * @param queryId unique query identifier
     */
    void onQueryCompleted(String queryId);

    /**
     * Called when a query fails with an error.
     *
     * @param queryId unique query identifier
     * @param error   the cause of the failure
     */
    void onQueryFailed(String queryId, Throwable error);

    /**
     * Called when a query ends regardless of outcome (success, failure, or abnormal termination).
     * Implementations should use this callback for resource cleanup that must happen
     * on every query termination path.
     *
     * <p>Default implementation is a no-op. Implementations that need guaranteed cleanup
     * (e.g., removing tracking entries from in-memory collections) should override this method.</p>
     *
     * @param queryId unique query identifier
     */
    default void onQueryEnd(String queryId) {
        // intentional no-op: subclasses override for cleanup
    }
}
