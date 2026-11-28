package org.pangrova.persistence.file.api.claim;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.common.api.annotation.Experimental;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import org.pangrova.common.file.CopyFileCommitBinding;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Immutable identity and admission evidence for one versioned S3 file load. */
@Experimental
public record CopyFileClaim(
        String tenantId,
        String tableUuid,
        String sourceUrl,
        String sourceVersionId,
        String sourceSha256,
        long sourceSize,
        String profileName,
        String profileIncarnationId,
        long profileRevision,
        long loadGeneration,
        String executionId) {

    private static final int MAX_S3_KEY_BYTES = 1024;
    private static final int MAX_S3_VERSION_BYTES = 1024;
    private static final int MAX_S3_BUCKET_BYTES = 63;

    public CopyFileClaim {
        requireText(tenantId, "tenantId");
        requireUuid(tableUuid, "tableUuid");
        requireSourceUrl(sourceUrl);
        requireText(sourceVersionId, "sourceVersionId");
        if ("null".equals(sourceVersionId)) {
            throw new IllegalArgumentException("sourceVersionId must be a non-null S3 version");
        }
        if (sourceVersionId.getBytes(StandardCharsets.UTF_8).length > MAX_S3_VERSION_BYTES) {
            throw new IllegalArgumentException("sourceVersionId exceeds the S3 version byte limit");
        }
        if (!Sha256Digests.isSha256Hex(sourceSha256)) {
            throw new IllegalArgumentException("sourceSha256 must be lowercase SHA-256 hex");
        }
        if (sourceSize < 0) {
            throw new IllegalArgumentException("sourceSize must not be negative");
        }
        requireText(profileName, "profileName");
        requireUuid(profileIncarnationId, "profileIncarnationId");
        if (profileRevision <= 0) {
            throw new IllegalArgumentException("profileRevision must be positive");
        }
        if (loadGeneration < 0) {
            throw new IllegalArgumentException("loadGeneration must not be negative");
        }
        requireText(executionId, "executionId");
    }

    /** Stable SHA-256 of domain-tagged, four-byte-length-prefixed UTF-8 identity fields. */
    public String claimKey() {
        return digest("pangrova.copy.file-key.v1", tenantId, tableUuid, sourceUrl, sourceVersionId);
    }

    /** Exact non-secret file and profile evidence to place in an Iceberg snapshot marker. */
    public String claimDigest() {
        return digest("pangrova.copy.file-evidence.v1", claimKey(), sourceSha256,
                Long.toString(sourceSize), profileName, profileIncarnationId,
                Long.toString(profileRevision));
    }

    /** One claim generation; generation zero is the normal first load. */
    public CopyFileCommitBinding commitBinding() {
        return new CopyFileCommitBinding(claimKey(), loadGeneration, claimDigest(), tableUuid);
    }

    private static void requireSourceUrl(String value) {
        requireText(value, "sourceUrl");
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("sourceUrl must be a canonical S3 object URI", invalid);
        }
        if (!"s3".equals(uri.getScheme()) || uri.isOpaque()
                || !canonicalAuthority(uri) || uri.getRawQuery() != null
                || uri.getRawFragment() != null || uri.getRawPath() == null
                || !value.equals("s3://" + uri.getRawAuthority() + uri.getRawPath())) {
            throw new IllegalArgumentException("sourceUrl must be a canonical S3 object URI");
        }
        if (!safeObjectKey(uri.getRawPath())) {
            throw new IllegalArgumentException("sourceUrl contains an unsafe object key");
        }
    }

    private static boolean canonicalAuthority(URI uri) {
        String authority = uri.getRawAuthority();
        return authority != null && !authority.isBlank()
                && uri.getHost() != null && uri.getHost().equals(authority)
                && authority.getBytes(StandardCharsets.UTF_8).length <= MAX_S3_BUCKET_BYTES
                && uri.getRawUserInfo() == null && uri.getPort() == -1;
    }

    private static boolean safeObjectKey(String path) {
        if (!path.startsWith("/") || path.length() <= 1
                || path.indexOf('%') >= 0 || path.indexOf('\\') >= 0
                || path.substring(1).getBytes(StandardCharsets.UTF_8).length > MAX_S3_KEY_BYTES) {
            return false;
        }
        for (String segment : path.substring(1).split("/", -1)) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    private static void requireUuid(String value, String field) {
        requireText(value, field);
        try {
            if (!UUID.fromString(value).toString().equals(value)) {
                throw new IllegalArgumentException(field + " must be a canonical UUID");
            }
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(field + " must be a canonical UUID", invalid);
        }
    }

    private static String digest(String domain, String... components) {
        MessageDigest hash = Sha256Digests.sha256();
        byte[] domainBytes = domain.getBytes(StandardCharsets.UTF_8);
        hash.update(ByteBuffer.allocate(Integer.BYTES).putInt(domainBytes.length).array());
        hash.update(domainBytes);
        for (String component : components) {
            byte[] bytes = component.getBytes(StandardCharsets.UTF_8);
            hash.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            hash.update(bytes);
        }
        return HexFormat.of().formatHex(hash.digest());
    }
}
