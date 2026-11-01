package org.pangrova.spi.query.state;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.session.SessionContext;
import org.pangrova.spi.observability.event.QueryState;
import org.pangrova.spi.query.identity.QueryId;

/**
 * SPI for managing the lifecycle of queries in the Pangrova engine.
 *
 * <p>Provides operations to create a query from SQL text, inspect its
 * current state, and request cancellation. Implementations coordinate
 * query planning, task scheduling, and result collection.</p>
 */
@Internal
public interface QueryLifecycle {

    /**
     * Creates a new query from the given SQL statement.
     *
     * @param session the session context providing user identity and properties
     * @param sql     the SQL statement to execute
     * @return a unique query identifier for tracking the query
     * @throws NullPointerException if session or sql is null
     */
    QueryId createQuery(SessionContext session, String sql);

    /**
     * Returns the current state of a query.
     *
     * @param queryId the query identifier returned by {@link #createQuery}
     * @return the current query state, never null
     * @throws NullPointerException if queryId is null
     */
    QueryState getQueryState(QueryId queryId);

    /**
     * Requests cancellation of a running query.
     *
     * <p>Cancellation is best-effort; the query may complete before the
     * cancellation takes effect. All associated tasks will also be
     * cancelled.</p>
     *
     * @param queryId the query identifier returned by {@link #createQuery}
     * @throws NullPointerException if queryId is null
     */
    void cancelQuery(QueryId queryId);
}
