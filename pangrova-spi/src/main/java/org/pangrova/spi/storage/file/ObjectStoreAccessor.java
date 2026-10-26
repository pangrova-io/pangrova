package org.pangrova.spi.storage.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.storage.StorageType;

/**
 * Byte-level access to one object storage backend.
 *
 * <p>This is the shared transfer layer behind the Files API and the Stage
 * commands. Implementations never buffer a whole object in memory, never invent
 * hashes, and bind range reads to the provider version they were planned with.</p>
 *
 * <p>Failures use {@code FILE-*} error codes: missing objects, failed
 * preconditions, unavailable storage, and exceeded limits are distinguishable so
 * callers can map them to HTTP status without inspecting messages.</p>
 */
@Experimental
public interface ObjectStoreAccessor {

    /** Returns the backend this accessor serves. */
    StorageType storageType();

    /** Returns object metadata, or throws {@code FILE-0001} when absent. */
    FileStat head(ObjectLocation location, String path);

    /**
     * Opens a byte range of an object.
     *
     * @param offset          first byte to read
     * @param length          bytes to read, or a negative value to read to the end
     * @param expectedVersion version returned by {@link #head}; a mismatch fails
     *                        with {@code FILE-0002} instead of splicing versions
     */
    InputStream openRange(ObjectLocation location, String path, long offset, long length,
            String expectedVersion) throws IOException;

    /** Writes a complete object and returns its observed metadata. */
    FileStat put(ObjectLocation location, String path, InputStream data, long size,
            String contentType) throws IOException;

    /** Starts a multipart upload for objects larger than the single-request limit. */
    MultipartUpload beginMultipart(ObjectLocation location, String path, String contentType);

    /** Uploads one part of an open multipart upload. */
    UploadedPart uploadPart(MultipartUpload upload, int partNumber, InputStream data, long size)
            throws IOException;

    /** Completes a multipart upload and returns the resulting object metadata. */
    FileStat completeMultipart(MultipartUpload upload, List<UploadedPart> parts);

    /** Aborts a multipart upload, releasing staged parts. */
    void abortMultipart(MultipartUpload upload);

    /**
     * Lists objects under a space-relative prefix.
     *
     * <p>Empty pages with a non-null token are legal; absence of an object is only
     * meaningful after the final page.</p>
     *
     * @param prefix    space-relative prefix, or null/empty for the space root
     * @param pageToken continuation token from the previous page, or null to start
     * @param pageSize  maximum entries requested per page
     */
    FileListing list(ObjectLocation location, String prefix, String pageToken, int pageSize);

    /**
     * Deletes an object.
     *
     * @param expectedVersion version returned by {@link #head}; a mismatch fails
     *                        with {@code FILE-0002}
     */
    void delete(ObjectLocation location, String path, String expectedVersion);

    /**
     * Returns a time-limited URL for one object, or fails when the backend cannot
     * sign URLs.
     *
     * <p>The URL grants exactly one method and one key until the TTL elapses. It
     * is a delivery convenience, not an authorization replacement: callers must
     * still authorize the request before asking for a URL.</p>
     *
     * @param ttl  positive requested lifetime of the signed URL
     * @param read true for a download URL, false for an upload URL
     * @return the actual provider URL and its signing-operation expiry bound
     */
    default PresignedObjectUrl presign(ObjectLocation location, String path, java.time.Duration ttl,
            boolean read) {
        throw new org.pangrova.common.exception.PangrovaException(
                org.pangrova.common.exception.PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                storageType() + " storage cannot issue signed URLs");
    }
}
