package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Experimental;

import java.util.UUID;
import java.util.regex.Pattern;

/** Exact admitted COPY file generation bound to one distributed write commit. */
@Experimental
public record CopyFileCommitBinding(
        String fileKey,
        long loadGeneration,
        String claimDigest,
        String tableUuid) {

    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    public CopyFileCommitBinding {
        if (fileKey == null || !SHA_256.matcher(fileKey).matches()) {
            throw new IllegalArgumentException("fileKey must be lowercase SHA-256 hex");
        }
        if (loadGeneration < 0) {
            throw new IllegalArgumentException("loadGeneration must not be negative");
        }
        if (claimDigest == null || !SHA_256.matcher(claimDigest).matches()) {
            throw new IllegalArgumentException("claimDigest must be lowercase SHA-256 hex");
        }
        if (tableUuid == null) {
            throw new IllegalArgumentException("tableUuid must be a canonical UUID");
        }
        try {
            if (!UUID.fromString(tableUuid).toString().equals(tableUuid)) {
                throw new IllegalArgumentException("tableUuid must be a canonical UUID");
            }
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("tableUuid must be a canonical UUID", invalid);
        }
    }
}
