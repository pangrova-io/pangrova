package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.connector.spi.ConnectorSession;

import java.util.Map;

/** Explicit authenticated planning identity used by schema boundary tests. */
public final class MetadataPlanningTestSessions {
    private MetadataPlanningTestSessions() {}

    public static ConnectorSession session() {
        return SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "metadata-query", Map.of("user", "metadata-user"), null, "metadata-tenant"));
    }
}
