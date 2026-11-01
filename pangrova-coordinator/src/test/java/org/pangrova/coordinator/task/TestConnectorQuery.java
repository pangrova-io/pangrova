package org.pangrova.coordinator.task;

import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.spi.connector.ConnectorQueryAuthority;
import org.pangrova.spi.connector.ConnectorSession;

import java.util.Map;

/** Component-test admission through the real query state machine and issued operation owner. */
public final class TestConnectorQuery {
    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final QueryStateMachine state = new QueryStateMachine();
    private final ConnectorQueryAuthority authority;
    private final CoordinatorSessionContext session;

    /** Creates a fresh engine owner, not an authority decoded from session properties. */
    public TestConnectorQuery(String queryId) {
        this(queryId, "component-reader", "component-tenant", Map.of());
    }

    /** Preserves the actual test actor and options while binding a fresh engine owner. */
    public TestConnectorQuery(String queryId, String user, String tenant, Map<String, String> properties) {
        authority = state.bindConnectorAuthority(tracker.openQueryIssuance(queryId));
        var options = new java.util.HashMap<>(properties);
        options.put("user", user);
        session = new CoordinatorSessionContext(queryId, options, null, tenant).withQueryAuthority(authority);
    }

    /** Returns the same actual authority used by all session copies. */
    public ConnectorQueryAuthority authority() { return authority; }

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
        state.tryTransition(QueryStateMachine.State.CANCELLED);
        tracker.sealQueryIssuance(authority.queryId());
    }

    /** Retires only after the test has released its actual connector resources. */
    public void retire() {
        tracker.requireQueryExecutionDrained(authority.queryId());
        tracker.retireQueryIssuance(authority.queryId());
    }
}
