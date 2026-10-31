package org.pangrova.file.api.upload;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.file.api.space.ObjectLocation;

/**
 * Handle for one multipart upload session.
 *
 * <p>The handle is opaque to callers; it exists so a retry can resume the same
 * provider session instead of starting a second, invisible upload.</p>
 *
 * @param uploadId provider multipart upload identifier
 * @param location resolved space root
 * @param path     space-relative target path
 */
@Experimental
public record MultipartUpload(String uploadId, ObjectLocation location, String path) {

    public MultipartUpload {
        Objects.requireNonNull(uploadId, "uploadId must not be null");
        Objects.requireNonNull(location, "location must not be null");
        Objects.requireNonNull(path, "path must not be null");
    }
}
