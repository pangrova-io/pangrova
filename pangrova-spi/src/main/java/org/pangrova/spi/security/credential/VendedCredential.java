package org.pangrova.spi.security.credential;

import org.pangrova.spi.annotation.Experimental;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * A short-lived, scoped storage credential issued by a {@link CredentialVendor}.
 *
 * <p>Modeled after the Iceberg REST Catalog {@code loadCredentials} response
 * and AWS STS {@code AssumeRole} output. Contains temporary access credentials
 * with an expiration time and a scope that limits the credential to a specific
 * storage prefix (e.g., an S3 prefix, GCS bucket path, or ADLS container).</p>
 *
 * <p>Resolves G-META-03: replaces static, long-lived cloud keys embedded in
 * {@code CatalogDescriptor.properties} with scoped, short-lived credentials.</p>
 *
 * @param accessKeyId     the temporary access key identifier
 * @param secretAccessKey the temporary secret access key
 * @param sessionToken    the session token (STS token, may be empty for non-STS providers)
 * @param expiration      when this credential expires (UTC)
 * @param scope           the storage path prefix this credential is scoped to
 * @param additionalProperties extra provider-specific properties (e.g., region, endpoint)
 */
@Experimental(reason = "G-META-03: scoped credential vending SPI")
public record VendedCredential(
        String accessKeyId,
        String secretAccessKey,
        String sessionToken,
        Instant expiration,
        String scope,
        Map<String, String> additionalProperties) {

    public VendedCredential {
        Objects.requireNonNull(accessKeyId, "accessKeyId must not be null");
        Objects.requireNonNull(secretAccessKey, "secretAccessKey must not be null");
        sessionToken = sessionToken != null ? sessionToken : "";
        Objects.requireNonNull(expiration, "expiration must not be null");
        scope = scope != null ? scope : "";
        additionalProperties = additionalProperties != null
                ? Map.copyOf(additionalProperties) : Map.of();
    }

    /** @return true if this credential has expired. */
    public boolean isExpired() {
        return Instant.now().isAfter(expiration);
    }

    /** @return remaining lifetime in seconds (may be negative if expired). */
    public long remainingSeconds() {
        return java.time.Duration.between(Instant.now(), expiration).getSeconds();
    }
}
