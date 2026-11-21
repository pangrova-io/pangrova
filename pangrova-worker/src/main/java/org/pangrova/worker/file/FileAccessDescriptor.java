package org.pangrova.worker.file;

import java.util.Map;
import java.util.Objects;

import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;

/**
 * Credential-free description of one file operation, issued by the coordinator.
 *
 * <p>The coordinator authorizes the user and resolves the space root; the Worker
 * receives only the physical binding and supplies credentials from its own secret
 * boundary. Descriptors are short-lived and never carry access keys.</p>
 *
 * @param profileName     profile identity for diagnostics
 * @param type            backend type
 * @param bucket          bucket, container, or local root
 * @param prefix          profile prefix scope
 * @param endpoint        S3-compatible endpoint, or empty
 * @param region          cloud region, or empty
 * @param pathStyleAccess whether path-style addressing is required
 * @param baseKey         bucket-relative space root
 * @param path            space-relative object path; empty for listings at the root
 * @param operation       head, list, read, write, or delete
 * @param expiresAtEpochMillis absolute expiry in epoch milliseconds
 * @param spaceId namespace incarnation, retained through rename and replaced after DROP
 * @param subject authenticated principal that received the grant
 * @param tenantId authenticated tenant that received the grant
 * @param profileIncarnationId exact storage-profile incarnation
 * @param profileRevision exact storage-profile definition revision
 */
public record FileAccessDescriptor(
        String profileName,
        StorageType type,
        String bucket,
        String prefix,
        String endpoint,
        String region,
        boolean pathStyleAccess,
        String baseKey,
        String path,
        String operation,
        long expiresAtEpochMillis,
        String spaceId,
        String subject,
        String tenantId,
        String profileIncarnationId,
        long profileRevision
) {

    public FileAccessDescriptor {
        Objects.requireNonNull(profileName, "profileName must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(bucket, "bucket must not be null");
        Objects.requireNonNull(baseKey, "baseKey must not be null");
        operation = org.pangrova.file.api.enums.FileOperation.parse(operation).wireName();
        prefix = prefix == null ? "" : prefix;
        endpoint = endpoint == null ? "" : endpoint;
        region = region == null ? "" : region;
        path = path == null ? "" : path;
        java.util.UUID.fromString(Objects.requireNonNull(spaceId, "spaceId"));
        java.util.UUID.fromString(Objects.requireNonNull(profileIncarnationId, "profileIncarnationId"));
        if (subject == null || subject.isBlank() || tenantId == null || tenantId.isBlank()
                || profileRevision <= 0) {
            throw new IllegalArgumentException("File descriptor requires a subject, tenant and profile revision");
        }
    }

    /** Rebuilds the credential-free profile the Worker will access. */
    public StorageProfile profile() {
        return new StorageProfile(profileName, type, bucket, prefix, endpoint, region,
                Map.of(), Map.of("path-style-access", Boolean.toString(pathStyleAccess)));
    }

    /** Requires the descriptor to still be valid at the given epoch millisecond. */
    public FileAccessDescriptor requireUnexpired(long nowEpochMillis) {
        if (expiresAtEpochMillis <= nowEpochMillis) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.enums.PangrovaErrorCode.ACCESS_DENIED,
                    "File access descriptor has expired");
        }
        return this;
    }
}
