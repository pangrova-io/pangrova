package org.pangrova.spi.security.credential;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;
import java.util.Objects;

/**
 * Request context for credential vending.
 *
 * <p>Carries the catalog identity (IAM role ARN or service-account), the
 * storage prefix to scope the credential to, the requesting tenant/user,
 * and optional session tags for audit attribution.</p>
 *
 * @param catalogName     the logical catalog name
 * @param roleIdentity    IAM role ARN, GCS service-account, or Azure managed-identity
 *                        (the "who" to assume/impersonate)
 * @param storagePrefix   the S3/GCS/ADLS prefix to scope the credential to
 *                        (e.g., {@code s3://bucket/warehouse/db.db/table/})
 * @param tenantId        the requesting tenant (for audit and session-tag attribution)
 * @param queryId         the requesting query (for blast-radius bounding)
 * @param sessionTags     optional key-value tags to attach to the STS session
 *                        (e.g., {@code pangrova.tenant=t1, pangrova.query=q42})
 */
@Experimental(reason = "G-META-03: credential vending request context")
public record CredentialRequest(
        String catalogName,
        String roleIdentity,
        String storagePrefix,
        String tenantId,
        String queryId,
        Map<String, String> sessionTags) {

    public CredentialRequest {
        Objects.requireNonNull(catalogName, "catalogName must not be null");
        Objects.requireNonNull(roleIdentity, "roleIdentity must not be null");
        Objects.requireNonNull(storagePrefix, "storagePrefix must not be null");
        tenantId = tenantId != null ? tenantId : "";
        queryId = queryId != null ? queryId : "";
        sessionTags = sessionTags != null ? Map.copyOf(sessionTags) : Map.of();
    }

    /** Creates a minimal request without session tags. */
    public static CredentialRequest of(String catalogName, String roleIdentity,
                                       String storagePrefix, String tenantId, String queryId) {
        return new CredentialRequest(catalogName, roleIdentity, storagePrefix,
                tenantId, queryId, Map.of());
    }
}
