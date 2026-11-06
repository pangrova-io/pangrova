package org.pangrova.coordinator.task;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.connector.spi.metadata.ConnectorMetadataScope;
import org.pangrova.connector.api.session.ConnectorSession;

import java.util.Map;

/** Component-test admission through the real query state machine and issued operation owner. */
public final class TestConnectorQuery {
    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final QueryStateMachine state = new QueryStateMachine();
    private final ConnectorMetadataScope authority;
    private final QuerySession session;

    /** Creates a fresh engine owner, not an authority decoded from session properties. */
    public TestConnectorQuery(String queryId) {
        this(queryId, "component-reader", "component-tenant", Map.of());
    }

    /** Preserves the actual test actor and options while binding a fresh engine owner. */
    public TestConnectorQuery(String queryId, String user, String tenant, Map<String, String> properties) {
        authority = state.bindConnectorAuthority(tracker.openQueryIssuance(queryId));
        var options = new java.util.HashMap<>(properties);
        options.put("user", user);
        session = new QuerySession(queryId, options, null, tenant, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()).withQueryAuthority(authority);
    }

    /** Returns the same actual authority used by all session copies. */
    public ConnectorMetadataScope authority() { return authority; }

    /** Returns an authenticated connector actor bound to this engine lifetime. */
    public ConnectorSession session(String catalog, String schema) {
        return SessionContextConverter.toConnectorSession(session, catalog, schema);
    }

    /** Returns the actual actor and lifetime for metadata-service admission. */
    public MetadataSessionContext metadataSession() {
        return SessionContextConverter.toMetadataSessionContext(session);
    }

    /** Closes admission, but deliberately does not pretend that active readers have exited. */
    public void seal() {
        state.tryTransition(QueryState.CANCELLED);
        tracker.sealQueryIssuance(authority.queryId());
    }

    /** Retires only after the test has released its actual connector resources. */
    public void retire() {
        tracker.requireQueryExecutionDrained(authority.queryId());
        tracker.retireQueryIssuance(authority.queryId());
    }
}
