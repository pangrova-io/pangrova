package org.pangrova.coordinator.query.lifecycle;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryStateMachine;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.UUID;

import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.QueryLifecycle;
import org.pangrova.spi.execution.QueryState;
import org.pangrova.spi.execution.SessionContext;

/**
 * Coordinator-side implementation of {@link QueryLifecycle} SPI.
 *
 * <p>Delegates to {@link QueryManager} for query submission, state retrieval,
 * and cancellation. Maps internal {@link QueryStateMachine.State} to SPI
 * {@link QueryState}, collapsing states absent from the SPI contract:
 * {@code SCHEDULING → PLANNING}. Terminal cancellation remains distinct from failure.</p>
 */
public class CoordinatorQueryLifecycle implements QueryLifecycle {

    private final QueryManager queryManager;

    public CoordinatorQueryLifecycle(QueryManager queryManager) {
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
    }

    @Override
    public QueryId createQuery(SessionContext session, String sql) {
        checkNotNull(session, "session must not be null");
        checkNotNull(sql, "sql must not be null");
        String id = UUID.randomUUID().toString();
        org.pangrova.coordinator.query.session.CoordinatorSessionContext coordinatorSession =
                SessionContextConverter.fromSpiSessionContext(id, session);
        if (coordinatorSession.userIdentity() == null || coordinatorSession.tenantId() == null) {
            throw new IllegalArgumentException("Query submission requires an authenticated tenant and principal");
        }
        queryManager.submitQuery(id, sql, coordinatorSession);
        return new QueryId(id);
    }

    @Override
    public QueryState getQueryState(QueryId queryId) {
        checkNotNull(queryId, "queryId must not be null");
        return queryManager.getQueryState(queryId.id())
                .map(CoordinatorQueryLifecycle::toSpiState)
                .orElse(QueryState.FAILED);
    }

    @Override
    public void cancelQuery(QueryId queryId) {
        checkNotNull(queryId, "queryId must not be null");
        queryManager.cancelQuery(queryId.id());
    }

    /**
     * Maps {@link QueryStateMachine.State} to SPI {@link QueryState}.
     *
     * <p>SPI {@code QueryState} lacks {@code SCHEDULING}:
     * <ul>
     *   <li>{@code SCHEDULING → PLANNING} — still in planning phase from SPI perspective</li>
     * </ul>
     * Uses exhaustive switch expression for compile-time safety.</p>
     */
    private static QueryState toSpiState(QueryStateMachine.State state) {
        return switch (state) {
            case QUEUED -> QueryState.QUEUED;
            case PLANNING -> QueryState.PLANNING;
            case SCHEDULING -> QueryState.PLANNING;
            case RUNNING -> QueryState.RUNNING;
            case FINISHING -> QueryState.FINISHING;
            case FINISHED -> QueryState.FINISHED;
            case FAILED -> QueryState.FAILED;
            case CANCELLED -> QueryState.CANCELLED;
        };
    }
}
