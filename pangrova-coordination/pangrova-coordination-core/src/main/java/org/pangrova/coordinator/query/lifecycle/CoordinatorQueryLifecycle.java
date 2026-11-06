package org.pangrova.coordinator.query.lifecycle;

import org.pangrova.observability.spi.tracing.ExecutionTraceContextFactory;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.session.SessionContextConverter;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.UUID;

import org.pangrova.query.api.identity.QueryId;
import org.pangrova.query.api.lifecycle.QueryLifecycle;
import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.common.session.SessionContext;

/**
 * Coordinator-side implementation of {@link QueryLifecycle} SPI.
 *
 * <p>Delegates to {@link QueryManager} for query submission, state retrieval,
 * and cancellation. Returns the authoritative query state without collapsing scheduling,
 * failure or cancellation. An unknown query is distinct from a failed query.</p>
 */
public class CoordinatorQueryLifecycle implements QueryLifecycle {
    private final ExecutionTraceContextFactory traceContexts;

    private final QueryManager queryManager;

    public CoordinatorQueryLifecycle(QueryManager queryManager, ExecutionTraceContextFactory traceContexts) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
    }

    @Override
    public QueryId createQuery(SessionContext session, String sql) {
        checkNotNull(session, "session must not be null");
        checkNotNull(sql, "sql must not be null");
        String id = UUID.randomUUID().toString();
        org.pangrova.query.api.session.QuerySession coordinatorSession =
                SessionContextConverter.fromSpiSessionContext(id, session, traceContexts);
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
                .orElseThrow(() -> new java.util.NoSuchElementException("Unknown query: " + queryId));
    }

    @Override
    public void cancelQuery(QueryId queryId) {
        checkNotNull(queryId, "queryId must not be null");
        queryManager.cancelQuery(queryId.id());
    }

}
