package org.pangrova.planner.cache;

import java.util.Map;

/**
 * Holds all context fields needed for query fingerprint computation.
 */
public record QueryContext(
        String catalog,
        String schema,
        String defaultSchemaPath,
        Map<String, String> sessionProperties,
        String dataVersion,
        String tenantId,
        String userPrincipal,
        String activeRolesHash,
        String rlsPolicyHash,
        String columnMaskHash
) {
    /** Freezes the session identity used to compute cache fingerprints. */
    public QueryContext {
        sessionProperties = Map.copyOf(sessionProperties);
    }
}
