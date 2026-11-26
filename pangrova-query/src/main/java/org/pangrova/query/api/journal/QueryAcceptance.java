package org.pangrova.query.api.journal;

import java.time.Duration;
import java.util.Objects;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.submission.QuerySubmissionResult;

/** Immutable acceptance fact carried by the initial QUEUED journal event. */
@Experimental
public record QueryAcceptance(
        String principal,
        String idempotencyKeyDigest,
        String requestFingerprint,
        QuerySubmissionResult receipt,
        long retentionMs) {
    public static final long DEFAULT_RETENTION_MS = Duration.ofMinutes(10).toMillis();

    public QueryAcceptance {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal must not be null or blank");
        }
        QueryAcceptanceRequest.requireSha256(idempotencyKeyDigest, "idempotencyKeyDigest");
        QueryAcceptanceRequest.requireSha256(requestFingerprint, "requestFingerprint");
        Objects.requireNonNull(receipt, "receipt must not be null");
        if (receipt.state() != QueryState.QUEUED || receipt.resultDisposition() != ResultDisposition.PENDING
                || receipt.resultEndpoint() != null) {
            throw new IllegalArgumentException("Acceptance receipt must be QUEUED with PENDING disposition");
        }
        if (retentionMs <= 0) {
            throw new IllegalArgumentException("retentionMs must be positive");
        }
    }

    /** Creates the deterministic receipt stored in the initial QUEUED acceptance row. */
    public static QueryAcceptance pending(
            QueryAcceptanceRequest request, String queryId, long retentionMs) {
        Objects.requireNonNull(request, "request must not be null");
        return new QueryAcceptance(request.principal(), request.idempotencyKeyDigest(),
                request.requestFingerprint(),
                new QuerySubmissionResult(queryId, QueryState.QUEUED, ResultDisposition.PENDING,
                        null, java.util.List.of()), retentionMs);
    }
}
