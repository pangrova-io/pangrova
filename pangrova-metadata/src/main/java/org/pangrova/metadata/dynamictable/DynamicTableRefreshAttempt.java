package org.pangrova.metadata.dynamictable;

import java.util.Objects;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;

/** Durable Native candidate identity; a replacement publisher cannot replace its original I/O scope. */
public record DynamicTableRefreshAttempt(String tenantId, String logicalName, String incarnationId,
                                         long baseRevision, DynamicTableRefreshIssuer issuer,
                                         String candidateTable, State state, String commitFingerprint,
                                         NativeWriteFinalization publication) {
    public enum State { PREPARING, PUBLISHED }

    public DynamicTableRefreshAttempt {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(logicalName, "logicalName");
        Objects.requireNonNull(incarnationId, "incarnationId");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(candidateTable, "candidateTable");
        Objects.requireNonNull(state, "state");
        if (baseRevision < 0) throw new IllegalArgumentException("Refresh base revision must be nonnegative");
        if ((state == State.PUBLISHED) != (commitFingerprint != null && publication != null)
                || state == State.PREPARING && (commitFingerprint != null || publication != null)) {
            throw new IllegalArgumentException("Refresh outcome contradicts its Native publication receipt");
        }
        if (state == State.PUBLISHED && (!commitFingerprint.matches("[0-9a-f]{64}")
                || publication.outcome() != NativeWriteFinalization.Outcome.COMMITTED)) {
            throw new IllegalArgumentException("Published refresh requires exact committed Native evidence");
        }
    }

    public String executionId() { return issuer.owner().executionId(); }
}
