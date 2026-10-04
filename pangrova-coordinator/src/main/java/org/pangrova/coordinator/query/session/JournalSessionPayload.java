package org.pangrova.coordinator.query.session;

import org.pangrova.common.json.JsonMapperFactory;

/** Minimal journal recovery context. Planning's exact session configuration remains a separate contract. */
public record JournalSessionPayload(String queryId, String tenantId) {
    /**
     * Serializes the original minimal fields with JSON escaping, preserving null-tenant as empty.
     *
     * @param session actual submitting session; planning configuration is not added to this payload
     * @return JSON containing the exact original query and tenant strings
     * @throws NullPointerException if session is null
     * @throws IllegalStateException if serialization fails before journal ownership transfer
     */
    public static String encode(CoordinatorSessionContext session) {
        java.util.Objects.requireNonNull(session, "session");
        try {
            return JsonMapperFactory.writer().writeValueAsString(new JournalSessionPayload(session.queryId(),
                    session.tenantId() != null ? session.tenantId() : ""));
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("Query journal session serialization failed before event transfer", failure);
        }
    }
}
