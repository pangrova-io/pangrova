package org.pangrova.query.api.lineage;

import org.pangrova.governance.api.enums.LineageEventType;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

import org.pangrova.common.api.annotation.Stable;

/** Frozen logical query lineage; delivery retries retain the same identity, time and payload. */
@Stable(since = "1.0")
public record QueryLineageFact(String namespace, String queryId, LineageEventType phase, long timestampMs,
        String sql, String failureMessage, QueryLineageMetadata lineage) {
    /** Rejects ambiguous logical identities and invalid terminal payloads before any durable append. */
    public QueryLineageFact {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(lineage, "lineage");
        if (namespace.isBlank() || queryId.isBlank() || namespace.indexOf(0) >= 0
                || queryId.indexOf(0) >= 0 || timestampMs < 0) {
            throw new IllegalArgumentException("Lineage requires namespace, query identity and original time");
        }
        if (phase != LineageEventType.FAIL && failureMessage != null) {
            throw new IllegalArgumentException("Only failure lineage may carry a failure message");
        }
    }

    /** START and the sole terminal fact have separate stable identities across coordinator recovery. */
    public UUID eventId() {
        String category = phase == LineageEventType.START ? "START" : "TERMINAL";
        return UUID.nameUUIDFromBytes((namespace + '\0' + queryId + '\0' + category)
                .getBytes(StandardCharsets.UTF_8));
    }

}
