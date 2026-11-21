package org.pangrova.query.api.audit;

import java.util.Objects;
import org.pangrova.common.api.annotation.Internal;

/**
 * Bounded attribution copied from an already-authorized query operation.
 * This is audit metadata, not an authentication token or authority to execute.
 * A null tenant denotes an operation without tenant attribution.
 */
@Internal
public record QueryAuditAttribution(String queryId, String principal, String tenantId) {
    public QueryAuditAttribution {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(principal, "principal");
    }
}
