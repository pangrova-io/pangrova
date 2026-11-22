package org.pangrova.query.session;

import org.pangrova.query.api.session.QuerySession;

import org.pangrova.common.json.JsonMapperFactory;

import java.util.Optional;

/** Minimal journal recovery context. Planning's exact session configuration remains a separate contract. */
public record JournalSessionPayload(String queryId, String tenantId, String principal) {
    /**
     * Serializes the original minimal fields with JSON escaping, preserving null-tenant as empty.
     *
     * @param session actual submitting session; planning configuration is not added to this payload
     * @return JSON containing the exact original query, tenant and principal
     * @throws NullPointerException if session is null
     * @throws IllegalStateException if serialization fails before journal ownership transfer
     */
    public static String encode(QuerySession session) {
        java.util.Objects.requireNonNull(session, "session");
        try {
            String principal = session.userIdentity() == null ? "anonymous" : session.userIdentity().principal();
            return JsonMapperFactory.writer().writeValueAsString(new JournalSessionPayload(session.queryId(),
                    session.tenantId() != null ? session.tenantId() : "", principal));
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException(
                    "Query journal session serialization failed before event transfer", failure);
        }
    }

    /** Reads only the principal captured by the canonical admission of this exact query. */
    public static Optional<String> admittedPrincipal(String queryId, String payload) {
        if (payload == null) return Optional.empty();
        try {
            var session = JsonMapperFactory.shared().readTree(payload);
            if (session == null || !queryId.equals(session.path("queryId").textValue())) return Optional.empty();
            return Optional.ofNullable(session.path("principal").textValue()).filter(value -> !value.isBlank());
        } catch (com.fasterxml.jackson.core.JsonProcessingException malformed) {
            return Optional.empty();
        }
    }
}
