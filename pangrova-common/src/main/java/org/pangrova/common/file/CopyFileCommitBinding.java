package org.pangrova.common.file;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.common.api.annotation.Experimental;

import java.util.UUID;

/** Exact admitted COPY file generation bound to one distributed write commit. */
@Experimental
public record CopyFileCommitBinding(
        String fileKey,
        long loadGeneration,
        String claimDigest,
        String tableUuid) {


    public CopyFileCommitBinding {
        if (!Sha256Digests.isSha256Hex(fileKey)) {
            throw new IllegalArgumentException("fileKey must be lowercase SHA-256 hex");
        }
        if (loadGeneration < 0) {
            throw new IllegalArgumentException("loadGeneration must not be negative");
        }
        if (!Sha256Digests.isSha256Hex(claimDigest)) {
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
