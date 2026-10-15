package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;
import java.util.regex.Pattern;

/** Verified Iceberg snapshot publication for one immutable COPY file generation. */
@Experimental
public record CopyFilePublication(
        CopyFileCommitBinding binding,
        String executionId,
        String commitFingerprint,
        long snapshotId) {

    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    public CopyFilePublication {
        Objects.requireNonNull(binding, "binding must not be null");
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (commitFingerprint == null || !SHA_256.matcher(commitFingerprint).matches()) {
            throw new IllegalArgumentException("commitFingerprint must be lowercase SHA-256 hex");
        }
        if (snapshotId <= 0) {
            throw new IllegalArgumentException("snapshotId must be positive");
        }
    }
}
