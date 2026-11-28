package org.pangrova.common.file;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** Verified Iceberg snapshot publication for one immutable COPY file generation. */
@Experimental
public record CopyFilePublication(
        CopyFileCommitBinding binding,
        String executionId,
        String commitFingerprint,
        long snapshotId) {


    public CopyFilePublication {
        Objects.requireNonNull(binding, "binding must not be null");
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (!Sha256Digests.isSha256Hex(commitFingerprint)) {
            throw new IllegalArgumentException("commitFingerprint must be lowercase SHA-256 hex");
        }
        if (snapshotId <= 0) {
            throw new IllegalArgumentException("snapshotId must be positive");
        }
    }
}
