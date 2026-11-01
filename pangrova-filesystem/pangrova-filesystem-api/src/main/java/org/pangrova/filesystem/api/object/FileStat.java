package org.pangrova.filesystem.api.object;

import java.time.Instant;
import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Observed metadata for one object inside a Volume or Stage.
 *
 * <p>{@code path} is relative to the space root and never starts with a slash.
 * {@code etag}, {@code contentType}, {@code providerVersion}, and {@code md5} may
 * be null when the backend cannot report them; implementations must not fabricate
 * values. A multipart ETag in particular is never reported as an MD5 digest.</p>
 *
 * @param path            space-relative path
 * @param size            object size in bytes
 * @param lastModified    provider last-modified timestamp
 * @param etag            provider ETag, or null
 * @param contentType     stored or inferred content type, or null
 * @param providerVersion opaque version identifier usable for conditional reads
 * @param md5             content MD5 when the backend can prove it, otherwise null
 */
@Experimental
public record FileStat(
        String path,
        long size,
        Instant lastModified,
        String etag,
        String contentType,
        String providerVersion,
        String md5
) {

    public FileStat {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(lastModified, "lastModified must not be null");
        if (path.startsWith("/") || path.isEmpty()) {
            throw new IllegalArgumentException("path must be relative and non-empty: " + path);
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative: " + size);
        }
    }

    /** Returns the ETag or throws when the backend did not provide one. */
    public String requireEtag() {
        if (etag == null || etag.isEmpty()) {
            throw new IllegalStateException("object storage reported no ETag for " + path);
        }
        return etag;
    }
}
