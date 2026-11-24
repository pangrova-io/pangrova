package org.pangrova.query.api.session;

import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.json.CanonicalJsonMapper;

/** Minimal session identity persisted with query admission. */
@Internal
public record JournalSessionPayload(String queryId, String tenantId, String principal) {
    /** Writes the admitted identity without persisting planning configuration. */
    public static String encode(QuerySession session) {
        Objects.requireNonNull(session, "session");
        String principal = session.userIdentity() == null ? "anonymous" : session.userIdentity().principal();
        var payload = new JournalSessionPayload(session.queryId(),
                session.tenantId() != null ? session.tenantId() : "", principal);
        try {
            return CanonicalJsonMapper.writeValueAsStringUnchecked(payload);
        } catch (UncheckedIOException failure) {
            throw new IllegalStateException(
                    "Query journal session serialization failed before event transfer", failure.getCause());
        }
    }

    /** Reads the principal only for the exact query captured by admission. */
    public static Optional<String> admittedPrincipal(String queryId, String payload) {
        return CanonicalJsonMapper.readObjectTextFields(payload)
                .filter(fields -> queryId.equals(fields.get("queryId")))
                .map(fields -> fields.get("principal"))
                .filter(principal -> !principal.isBlank());
    }
}
