package org.pangrova.query.api.journal;

import java.util.Objects;
import java.util.regex.Pattern;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.client.query.QueryOwnerEndpoint;

/** Principal-scoped idempotency material supplied before a candidate query is published. */
@Experimental
public record QueryAcceptanceRequest(String principal, String idempotencyKeyDigest, String requestFingerprint,
        QueryOwnerEndpoint ownerEndpoint, boolean explicitKey) {
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    public QueryAcceptanceRequest(String principal, String idempotencyKeyDigest, String requestFingerprint) {
        this(principal, idempotencyKeyDigest, requestFingerprint, null, true);
    }

    public QueryAcceptanceRequest(String principal, String idempotencyKeyDigest, String requestFingerprint,
            QueryOwnerEndpoint ownerEndpoint) {
        this(principal, idempotencyKeyDigest, requestFingerprint, ownerEndpoint, true);
    }

    public QueryAcceptanceRequest {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal must not be null or blank");
        }
        requireSha256(idempotencyKeyDigest, "idempotencyKeyDigest");
        requireSha256(requestFingerprint, "requestFingerprint");
    }

    static void requireSha256(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (!SHA256.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256 digest");
        }
    }
}
