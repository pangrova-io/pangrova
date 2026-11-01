package org.pangrova.filesystem.api.object;

import org.pangrova.common.api.annotation.Experimental;

/**
 * One uploaded multipart part and the ETag the provider assigned to it.
 *
 * @param partNumber 1-based part number
 * @param etag       provider part ETag
 * @param size       bytes accepted for the part
 */
@Experimental
public record UploadedPart(int partNumber, String etag, long size) {

    public UploadedPart {
        if (partNumber < 1) {
            throw new IllegalArgumentException("partNumber must be positive: " + partNumber);
        }
        if (etag == null || etag.isEmpty()) {
            throw new IllegalArgumentException("etag must not be empty");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative: " + size);
        }
    }
}
