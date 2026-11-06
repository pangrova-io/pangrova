package org.pangrova.connector.api.commit;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Durable identity of one coordinator-owned distributed catalog commit.
 *
 * <p>The same instance values must be used for the first catalog attempt and every recovery
 * attempt. Connectors use {@code commitFingerprint} as an externally persisted idempotency marker;
 * {@code baseSnapshotId} is the table snapshot captured when the write began, never a snapshot
 * looked up during recovery.</p>
 */
@Experimental
public record DistributedWriteCommitContext(
        String executionId,
        String tenantId,
        long baseSnapshotId,
        byte[] writeOutputBinding,
        String commitFingerprint,
        String principal,
        org.pangrova.connector.api.commit.WriteAuthorizationReceipt authorizationReceipt,
        org.pangrova.common.file.CopyFileCommitBinding copyFile) {

    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    public DistributedWriteCommitContext {
        requireNonBlank(executionId, "executionId");
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(principal, "principal");
        Objects.requireNonNull(authorizationReceipt, "authorizationReceipt");
        if (baseSnapshotId < -1) {
            throw new IllegalArgumentException("baseSnapshotId must be -1 or non-negative");
        }
        writeOutputBinding = Objects.requireNonNull(
                writeOutputBinding, "writeOutputBinding must not be null").clone();
        if (writeOutputBinding.length == 0) {
            throw new IllegalArgumentException("writeOutputBinding must not be empty");
        }
        Objects.requireNonNull(commitFingerprint, "commitFingerprint must not be null");
        if (!SHA_256.matcher(commitFingerprint).matches()) {
            throw new IllegalArgumentException(
                    "commitFingerprint must be a lowercase SHA-256 hex digest");
        }
    }

    @Override
    public byte[] writeOutputBinding() {
        return writeOutputBinding.clone();
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
