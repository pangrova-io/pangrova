package org.pangrova.worker.ai.blob;

import java.time.Instant;

/**
 * Metadata for a blob file discovered during storage scanning.
 *
 * @param uri          absolute storage URI of the blob file
 * @param size         file size in bytes
 * @param lastModified timestamp when the file was last modified
 */
public record BlobFileInfo(String uri, long size, Instant lastModified) {

    public BlobFileInfo {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("uri must not be null or blank");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative, got: " + size);
        }
        if (lastModified == null) {
            throw new IllegalArgumentException("lastModified must not be null");
        }
    }
}
