package org.pangrova.coordinator.query;

import org.pangrova.spi.security.UserIdentity;

import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Minimal value object carrying query context through the orchestration pipeline.
 * Distinct from SPI SessionContext — this is coordinator-internal.
 */
public record CoordinatorSessionContext(
        String queryId,
        Map<String, String> sessionProperties,
        UserIdentity userIdentity,
        String tenantId
) {

    public CoordinatorSessionContext {
        checkNotNull(queryId, "queryId must not be null");
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
    }

    /**
     * Returns a copy with a different queryId, preserving all other fields.
     */
    public CoordinatorSessionContext withQueryId(String newQueryId) {
        return new CoordinatorSessionContext(newQueryId, sessionProperties, userIdentity, tenantId);
    }
}
