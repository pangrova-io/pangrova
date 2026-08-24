package org.pangrova.spi.security.credential;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI for vending short-lived, prefix-scoped storage credentials.
 *
 * <p>Replaces the anti-pattern of embedding static, long-lived cloud keys
 * in {@code CatalogDescriptor.properties}. Implementations call the
 * cloud provider's token service (AWS STS AssumeRole, GCS
 * generateAccessToken, Azure AD token exchange) to mint credentials that are:
 * <ul>
 *   <li><b>Short-lived</b> — default 1 hour, max as configured by the IAM policy</li>
 *   <li><b>Prefix-scoped</b> — the inline policy restricts access to the
 *       specific S3 prefix / GCS path / ADLS container for the table(s) being
 *       accessed</li>
 *   <li><b>Per-query</b> — a new credential is minted per query or per
 *       split batch, ensuring blast radius is bounded</li>
 * </ul>
 *
 * <p>The vended credential is surfaced to workers via the Iceberg REST
 * {@code X-Iceberg-Access-Delegation} header or the {@code loadCredentials}
 * endpoint, and to native workers via the task request's credential fields.</p>
 *
 * <h3>Integration points</h3>
 * <ul>
 *   <li>{@code CatalogDescriptor} stores an IAM role ARN / service-account
 *       reference instead of static keys</li>
 *   <li>{@code IcebergSplitManager} calls {@link #vendCredential} when
 *       enumerating splits, attaching the credential to each split batch</li>
 *   <li>{@code IcebergPageSourceProvider} passes the credential through to
 *       the native file reader via the columnar data interface task request</li>
 * </ul>
 *
 * <p>Resolves G-META-03.</p>
 *
 * <p>Thread-safety: implementations must be thread-safe; credential vending
 * is on the split-enumeration hot path and may be called concurrently.</p>
 */
@Experimental(reason = "G-META-03: scoped credential vending — replaces static cloud keys")
public interface CredentialVendor {

    /**
     * Vends a short-lived credential scoped to the given storage prefix.
     *
     * <p>The implementation should call the cloud provider's STS / token
     * service to mint a credential with an inline policy restricting access
     * to {@code storagePrefix}. The credential lifetime should default to
     * 1 hour or the maximum allowed by the IAM policy, whichever is shorter.</p>
     *
     * @param request the credential request containing catalog identity,
     *                storage prefix, and optional session tags
     * @return a vended credential with expiration and scope
     * @throws CredentialVendingException if the STS call fails
     */
    VendedCredential vendCredential(CredentialRequest request);

    /**
     * Returns the provider type this vendor handles (e.g., "aws", "gcs", "azure").
     *
     * @return the provider identifier
     */
    String providerType();
}
