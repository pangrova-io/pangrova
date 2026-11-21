package org.pangrova.connector.api.commit;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

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
        org.pangrova.common.file.CopyFileCommitBinding copyFile,
        CdcSourceCommitIdentity cdcSource) {

    /** Creates a non-CDC distributed commit context. */
    public DistributedWriteCommitContext(
            String executionId,
            String tenantId,
            long baseSnapshotId,
            byte[] writeOutputBinding,
            String commitFingerprint,
            String principal,
            WriteAuthorizationReceipt authorizationReceipt,
            org.pangrova.common.file.CopyFileCommitBinding copyFile) {
        this(executionId, tenantId, baseSnapshotId, writeOutputBinding, commitFingerprint,
                principal, authorizationReceipt, copyFile, null);
    }

    public DistributedWriteCommitContext {
        requireText(executionId, "executionId");
        requireText(tenantId, "tenantId");
        requireText(principal, "principal");
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
        if (!DigestUtils.isSha256Hex(commitFingerprint)) {
            throw new IllegalArgumentException(
                    "commitFingerprint must be a lowercase SHA-256 hex digest");
        }
    }

    @Override
    public byte[] writeOutputBinding() {
        return writeOutputBinding.clone();
    }
}
