package org.pangrova.coordinator.metadata;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.connector.api.session.ConnectorSession;

import java.util.Map;

/** Explicit authenticated planning identity used by schema boundary tests. */
public final class MetadataPlanningTestSessions {
    private MetadataPlanningTestSessions() {}

    public static ConnectorSession session() {
        return SessionContextConverter.toConnectorSession(new QuerySession(
                "metadata-query", Map.of("user", "metadata-user"), null, "metadata-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
    }
}
