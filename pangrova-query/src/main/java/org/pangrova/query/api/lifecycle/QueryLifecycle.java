package org.pangrova.query.api.lifecycle;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.session.SessionContext;
import org.pangrova.execution.api.identity.QueryId;

/**
 * Caller API for managing the lifecycle of queries in the Pangrova engine.
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
     * <p>Each successful call submits a distinct query; callers must not blindly retry
     * an unknown submission outcome. The implementation must establish trusted
     * authentication and tenant identity before accepting work. Session properties
     * alone do not confer that authority; unsupported session carriers are rejected.</p>
     *
     * @param session the session context providing user identity and properties
     * @param sql     the SQL statement to execute
     * @return a unique query identifier for tracking the query
     * @throws NullPointerException if session or sql is null
     * @throws IllegalArgumentException if the implementation cannot establish trusted session authority
     */
    QueryId createQuery(SessionContext session, String sql);

    /**
     * Returns the current state of a query.
     *
     * @param queryId the query identifier returned by {@link #createQuery}
     * @return the current authoritative query state, including scheduling, never null
     * @throws java.util.NoSuchElementException if the query is unknown or no longer retained
     * @throws NullPointerException if queryId is null
     */
    QueryState getQueryState(QueryId queryId);

    /**
     * Requests cancellation of a running query.
     *
     * <p>Cancellation is best-effort; the query may complete before the
     * cancellation takes effect. The implementation requests cancellation of
     * associated tasks; return does not certify physical task drain or resource
     * release. Repeated cancellation does not restart a terminal query.</p>
     *
     * @param queryId the query identifier returned by {@link #createQuery}
     * @throws NullPointerException if queryId is null
     */
    void cancelQuery(QueryId queryId);
}
