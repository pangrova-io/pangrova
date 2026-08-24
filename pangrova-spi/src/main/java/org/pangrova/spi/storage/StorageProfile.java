package org.pangrova.spi.storage;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable configuration for an external storage location.
 * Represents a named set of credentials and endpoint configuration for accessing
 * object storage buckets/containers beyond the catalog's default warehouse path.
 *
 * <p>Analogous to Databricks External Locations / Snowflake External Stages.</p>
 *
 * @param name        unique profile name (used in SQL: {@code STORAGE 'name'})
 * @param type        storage backend type (S3, OSS, GCS, ADLS, LOCAL)
 * @param bucket      bucket or container name
 * @param prefix      optional path prefix within the bucket (restricts accessible scope)
 * @param endpoint    optional custom endpoint URL (for S3-compatible services)
 * @param region      cloud region identifier
 * @param credentials authentication credentials (access-key-id, secret-access-key, role-arn, session-token)
 * @param options     additional configuration (max-connections, sse-type, path-style-access, etc.)
 */
@Experimental
public record StorageProfile(
        String name,
        StorageType type,
        String bucket,
        String prefix,
        String endpoint,
        String region,
        Map<String, String> credentials,
        Map<String, String> options
) {

    public StorageProfile {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(bucket, "bucket must not be null");
        if (prefix == null) prefix = "";
        if (endpoint == null) endpoint = "";
        if (region == null) region = "";
        credentials = credentials == null ? Map.of() : Map.copyOf(credentials);
        options = options == null ? Map.of() : Map.copyOf(options);
    }

    /**
     * Returns true if the given URL (scheme://bucket/key) matches this profile's
     * bucket and prefix scope.
     */
    public boolean matchesUrl(String url) {
        if (url == null) return false;
        // Parse: s3://bucket/prefix/...
        int schemeEnd = url.indexOf("://");
        if (schemeEnd < 0) return false;
        String remaining = url.substring(schemeEnd + 3);
        int slashIdx = remaining.indexOf('/');
        String urlBucket = slashIdx > 0 ? remaining.substring(0, slashIdx) : remaining;
        String urlKey = slashIdx > 0 ? remaining.substring(slashIdx + 1) : "";
        return urlBucket.equals(bucket) && (prefix.isEmpty() || urlKey.startsWith(prefix));
    }

    /**
     * Validates that the given key path is within this profile's prefix scope.
     *
     * @param key the object key (path within bucket)
     * @return true if the key is within the allowed prefix
     */
    public boolean isKeyWithinPrefix(String key) {
        return prefix.isEmpty() || (key != null && key.startsWith(prefix));
    }

    /**
     * Returns whether path-style access should be used (required for MinIO).
     */
    public boolean isPathStyleAccess() {
        return "true".equalsIgnoreCase(options.getOrDefault("path-style-access", "false"));
    }

    /**
     * Returns the access key ID from credentials, or null if not configured.
     */
    public String accessKeyId() {
        return credentials.get("access-key-id");
    }

    /**
     * Returns the secret access key from credentials, or null if not configured.
     */
    public String secretAccessKey() {
        return credentials.get("secret-access-key");
    }

    /**
     * Returns the IAM role ARN for AssumeRole authentication, or null if not configured.
     */
    public String roleArn() {
        return credentials.get("role-arn");
    }

    /**
     * Returns the session token for temporary credentials, or null if not configured.
     */
    public String sessionToken() {
        return credentials.get("session-token");
    }

    /**
     * Returns a string representation that redacts sensitive credential values.
     * Never logs access keys or secrets.
     */
    @Override
    public String toString() {
        return "StorageProfile[name=" + name
                + ", type=" + type
                + ", bucket=" + bucket
                + ", prefix=" + prefix
                + ", endpoint=" + endpoint
                + ", region=" + region
                + ", credentials=[REDACTED:" + credentials.size() + " keys]"
                + ", options=" + options + "]";
    }
}
