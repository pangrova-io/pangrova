package org.pangrova.coordinator.query.session;

import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.connector.ConnectorQueryAuthority;
import org.pangrova.observability.tracing.ExecutionTraceContext;

import java.util.Map;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Minimal value object carrying query context through the orchestration pipeline.
 * Distinct from SPI SessionContext — this is coordinator-internal.
 */
public record CoordinatorSessionContext(
        String queryId,
        Map<String, String> sessionProperties,
        UserIdentity userIdentity,
        String tenantId,
        Optional<ConnectorQueryAuthority> queryAuthority,
        ExecutionTraceContext traceContext
) {

    public CoordinatorSessionContext {
        checkNotNull(queryId, "queryId must not be null");
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
        queryAuthority = checkNotNull(queryAuthority, "queryAuthority must not be null");
        traceContext = checkNotNull(traceContext, "traceContext must not be null");
        queryAuthority.ifPresent(authority -> {
            if (!queryId.equals(authority.queryId())) {
                throw new IllegalArgumentException("Session queryId differs from its issued authority");
            }
        });
    }

    /** Creates an authenticated request context before engine query admission. */
    public CoordinatorSessionContext(String queryId, Map<String, String> sessionProperties,
            UserIdentity userIdentity, String tenantId) {
        this(queryId, sessionProperties, userIdentity, tenantId, Optional.empty(), ExecutionTraceContext.capture());
    }

    public CoordinatorSessionContext(String queryId, Map<String, String> sessionProperties,
            UserIdentity userIdentity, String tenantId, Optional<ConnectorQueryAuthority> queryAuthority) {
        this(queryId, sessionProperties, userIdentity, tenantId, queryAuthority, ExecutionTraceContext.capture());
    }

    public CoordinatorSessionContext withTraceContext(ExecutionTraceContext context) {
        return new CoordinatorSessionContext(
                queryId, sessionProperties, userIdentity, tenantId, queryAuthority, context);
    }

    /** Binds the actual engine-owned query lifetime without altering the authenticated actor. */
    public CoordinatorSessionContext withQueryAuthority(ConnectorQueryAuthority authority) {
        if (queryAuthority.isPresent() && queryAuthority.orElseThrow() != authority) {
            throw new IllegalStateException("Session already belongs to another query lifetime");
        }
        return new CoordinatorSessionContext(queryId, sessionProperties, userIdentity, tenantId,
                Optional.of(authority), traceContext);
    }

    /**
     * Returns a copy with a different queryId, preserving all other fields.
     */
    public CoordinatorSessionContext withQueryId(String newQueryId) {
        if (queryAuthority.isPresent() && !queryId.equals(newQueryId)) {
            throw new IllegalStateException("An admitted session cannot change its query identity");
        }
        return new CoordinatorSessionContext(newQueryId, sessionProperties, userIdentity, tenantId, queryAuthority,
                traceContext);
    }
}
