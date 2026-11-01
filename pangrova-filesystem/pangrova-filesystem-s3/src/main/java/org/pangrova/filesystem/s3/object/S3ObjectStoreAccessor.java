package org.pangrova.filesystem.s3.object;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.filesystem.api.object.MultipartUpload;
import org.pangrova.filesystem.api.object.ObjectLocation;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;
import org.pangrova.filesystem.api.object.PresignedObjectUrl;
import org.pangrova.filesystem.api.object.UploadedPart;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

/**
 * S3-compatible implementation of the file-plane transfer layer.
 *
 * <p>Range reads first pin the current version with a HEAD, then request that
 * exact version so two plans never splice different object versions. Write
 * receipts likewise pin the provider's acknowledged version. ETags remain
 * provider identity evidence and are never inferred to be content MD5.</p>
 */
public final class S3ObjectStoreAccessor implements ObjectStoreAccessor {
    private static final Pattern CONTENT_RANGE = Pattern.compile(
            "bytes ([0-9]+)-([0-9]+)/([0-9]+)", Pattern.CASE_INSENSITIVE);

    private static final int NOT_FOUND = 404;
    private static final int PRECONDITION_FAILED = 412;

    private final S3Client client;
    private final S3Presigner presigner;
    private final java.util.Set<String> conditionalDeleteEndpoints;

    public S3ObjectStoreAccessor(S3Client client) {
        this(client, null);
    }

    /**
     * Borrows the owner's thread-safe SDK client and optional presigner; the owner closes them.
     * Without a presigner, {@code presign} stays unavailable.
     */
    public S3ObjectStoreAccessor(S3Client client, S3Presigner presigner) {
        this(client, presigner, java.util.Set.of());
    }

    /**
     * Borrows SDK resources and installs exact custom endpoints qualified to enforce conditional DELETE.
     * Other custom endpoints can delete only immutable version IDs; an If-Match header alone is not proof.
     */
    public S3ObjectStoreAccessor(S3Client client, S3Presigner presigner,
            java.util.Set<String> conditionalDeleteEndpoints) {
        this.client = java.util.Objects.requireNonNull(client, "client");
        this.presigner = presigner;
        this.conditionalDeleteEndpoints = java.util.Set.copyOf(conditionalDeleteEndpoints);
    }

    @Override
    public PresignedObjectUrl presign(ObjectLocation location, String path, java.time.Duration ttl,
            boolean read) {
        if (presigner == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "S3 signed URLs are not enabled by this owner");
        }
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new PangrovaException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Signed URL lifetime must be positive");
        }
        String key = location.key(path);
        String bucket = location.profile().bucket();
        try {
            var signed = read
                    ? presigner.presignGetObject(GetObjectPresignRequest.builder()
                        .signatureDuration(ttl)
                        .getObjectRequest(request -> request.bucket(bucket).key(key))
                        .build())
                    : presigner.presignPutObject(PutObjectPresignRequest.builder()
                    .signatureDuration(ttl)
                    .putObjectRequest(request -> request.bucket(bucket).key(key))
                    .build());
            return new PresignedObjectUrl(signed.url().toString(), signed.expiration().toEpochMilli());
        } catch (SdkException failure) {
            throw storageFailure("Failed to sign a URL for '" + path + "'", failure);
        }
    }

    @Override
    public StorageType storageType() {
        return StorageType.S3;
    }

    @Override
    public FileStat head(ObjectLocation location, String path) {
        String key = location.key(path);
        return stat(path, headObject(location, key));
    }

    @Override
    public InputStream openRange(ObjectLocation location, String path, long offset, long length,
            String expectedVersion) throws IOException {
        String key = location.key(path);
        HeadObjectResponse pinned = headObject(location, key);
        FileStat current = stat(path, pinned);
        if (expectedVersion != null && !expectedVersion.equals(current.providerVersion())) {
            throw precondition("Version precondition failed for '" + path + "'");
        }
        if (offset < 0 || offset > current.size() || length < -1 || length > current.size() - offset) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "Requested range is outside '" + path + "'");
        }
        long expectedLength = length < 0 ? current.size() - offset : length;
        if (expectedLength == 0) return InputStream.nullInputStream();
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(location.profile().bucket())
                .key(key);
        String versionId = realVersionId(pinned.versionId());
        if (versionId != null) {
            request.versionId(versionId);
        } else if (current.etag() != null) {
            request.ifMatch(current.etag());
        }
        long end = length < 0 ? current.size() - 1 : offset + length - 1;
        if (current.size() > 0) {
            request.range("bytes=" + offset + "-" + end);
        }
        try {
            ResponseInputStream<GetObjectResponse> stream =
                    client.getObject(request.build());
            GetObjectResponse response = stream.response();
            if (versionId != null && !versionId.equals(response.versionId())) {
                abort(stream);
                throw precondition("Object version changed while opening '" + path + "'");
            }
            if (versionId == null && current.etag() != null
                    && response.eTag() != null && !current.etag().equals(response.eTag())) {
                abort(stream);
                throw precondition("Object ETag changed while opening '" + path + "'");
            }
            if (!Long.valueOf(expectedLength).equals(response.contentLength())
                    || !matchesRange(response.contentRange(), offset, end, current.size())) {
                abort(stream);
                throw precondition("Object response did not prove the requested byte window");
            }
            return new S3ObjectReadStream(stream, expectedLength);
        } catch (S3Exception failure) {
            throw translate("Failed to read '" + path + "'", failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to read '" + path + "'", failure);
        }
    }

    private static boolean matchesRange(String header, long offset, long end, long size) {
        if (header == null) return false;
        var range = CONTENT_RANGE.matcher(header);
        if (!range.matches()) return false;
        try {
            return Long.parseLong(range.group(1)) == offset && Long.parseLong(range.group(2)) == end
                    && Long.parseLong(range.group(3)) == size;
        } catch (NumberFormatException invalid) {
            return false;
        }
    }

    @Override
    public FileStat put(ObjectLocation location, String path, InputStream data, long size,
            String contentType) throws IOException {
        if (size < 0) {
            throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                    "Upload size must be known before writing '" + path + "'");
        }
        String key = location.key(path);
        var request = software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                .bucket(location.profile().bucket())
                .key(key)
                .contentLength(size);
        if (contentType != null && !contentType.isBlank()) {
            request.contentType(contentType);
        }
        PutObjectResponse response;
        try {
            response = client.putObject(request.build(), RequestBody.fromInputStream(data, size));
        } catch (S3Exception failure) {
            throw translate("Failed to write '" + path + "'", failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to write '" + path + "'", failure);
        }
        return committedStat(location, path, response.versionId(), response.eTag());
    }

    @Override
    public MultipartUpload beginMultipart(ObjectLocation location, String path,
            String contentType) {
        String key = location.key(path);
        var request = CreateMultipartUploadRequest.builder()
                .bucket(location.profile().bucket())
                .key(key);
        if (contentType != null && !contentType.isBlank()) {
            request.contentType(contentType);
        }
        try {
            String uploadId = client.createMultipartUpload(request.build()).uploadId();
            return new MultipartUpload(uploadId, location, path);
        } catch (SdkException failure) {
            throw storageFailure("Cannot start multipart upload for '" + path + "'", failure);
        }
    }

    @Override
    public UploadedPart uploadPart(MultipartUpload upload, int partNumber, InputStream data,
            long size) throws IOException {
        if (size < 0) {
            throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                    "Multipart part size must be known");
        }
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(upload.location().profile().bucket())
                .key(upload.location().key(upload.path()))
                .uploadId(upload.uploadId())
                .partNumber(partNumber)
                .contentLength(size)
                .build();
        try {
            String etag = client.uploadPart(request, RequestBody.fromInputStream(data, size))
                    .eTag();
            if (etag == null || etag.isEmpty()) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                        "S3 returned no ETag for multipart part " + partNumber);
            }
            return new UploadedPart(partNumber, etag, size);
        } catch (S3Exception failure) {
            throw translate("Failed to upload multipart part " + partNumber, failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to upload multipart part " + partNumber, failure);
        }
    }

    @Override
    public FileStat completeMultipart(MultipartUpload upload, List<UploadedPart> parts) {
        if (parts.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Multipart upload has no parts: " + upload.uploadId());
        }
        List<CompletedPart> completed = new ArrayList<>(parts.size());
        long expectedPart = 1;
        for (UploadedPart part : parts.stream()
                .sorted(Comparator.comparingInt(UploadedPart::partNumber)).toList()) {
            if (part.partNumber() != expectedPart) {
                throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                        "Multipart parts are not contiguous at part " + expectedPart);
            }
            completed.add(CompletedPart.builder()
                    .partNumber(part.partNumber()).eTag(part.etag()).build());
            expectedPart++;
        }
        CompleteMultipartUploadResponse response;
        try {
            response = client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(upload.location().profile().bucket())
                    .key(upload.location().key(upload.path()))
                    .uploadId(upload.uploadId())
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
                    .build());
        } catch (S3Exception failure) {
            throw translate("Failed to complete multipart upload for '" + upload.path() + "'",
                    failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to complete multipart upload for '" + upload.path() + "'",
                    failure);
        }
        return committedStat(upload.location(), upload.path(), response.versionId(), response.eTag());
    }

    @Override
    public void abortMultipart(MultipartUpload upload) {
        try {
            client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(upload.location().profile().bucket())
                    .key(upload.location().key(upload.path()))
                    .uploadId(upload.uploadId())
                    .build());
        } catch (SdkException failure) {
            throw storageFailure("Failed to abort multipart upload " + upload.uploadId(), failure);
        }
    }

    @Override
    public FileListing list(ObjectLocation location, String prefix, String pageToken, int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be positive");
        }
        String keyPrefix = location.key(prefix == null ? "" : prefix);
        String rawPrefix = keyPrefix;
        final String scopedPrefix = rawPrefix.isEmpty() || rawPrefix.endsWith("/")
                ? rawPrefix : rawPrefix + "/";
        try {
            ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
                    .bucket(location.profile().bucket())
                    .prefix(scopedPrefix)
                    .maxKeys(pageSize);
            if (pageToken != null && !pageToken.isEmpty()) {
                request.continuationToken(pageToken);
            }
            ListObjectsV2Response response = client.listObjectsV2(request.build());
            List<FileStat> files = response.contents().stream()
                    .filter(object -> !object.key().equals(scopedPrefix))
                    .map(object -> listing(location, object))
                    .toList();
            return new FileListing(files, response.isTruncated() != null
                    && response.isTruncated() ? response.nextContinuationToken() : null);
        } catch (S3Exception failure) {
            throw translate("Failed to list '" + scopedPrefix + "'", failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to list '" + scopedPrefix + "'", failure);
        }
    }

    @Override
    public void delete(ObjectLocation location, String path, String expectedVersion) {
        String key = location.key(path);
        HeadObjectResponse pinned = headObject(location, key);
        FileStat current = stat(path, pinned);
        // ListObjectsV2 reports no version id, so a caller that listed the object
        // holds its ETag instead; both identify the same bytes.
        if (expectedVersion != null && !expectedVersion.equals(current.providerVersion())
                && !expectedVersion.equals(current.etag())) {
            throw precondition("Version precondition failed for '" + path + "'");
        }
        var request = DeleteObjectRequest.builder()
                .bucket(location.profile().bucket())
                .key(key);
        String versionId = realVersionId(pinned.versionId());
        if (versionId != null) {
            request.versionId(versionId);
        } else if (!location.profile().endpoint().isEmpty()
                && !conditionalDeleteEndpoints.contains(location.profile().endpoint())) {
            throw storageFailure("Mutable-version deletion requires a qualified conditional-delete endpoint", null);
        } else if (current.etag() != null && !current.etag().isBlank()) {
            // The pinned SDK predates modeled conditional deletes; this header
            // participates in its normal signing and HTTP request pipeline.
            request.overrideConfiguration(config -> config.putHeader("If-Match", current.etag()));
        } else {
            throw storageFailure("S3 returned no identity for conditional deletion of '" + path + "'", null);
        }
        try {
            client.deleteObject(request.build());
        } catch (S3Exception failure) {
            throw translate("Failed to delete '" + path + "'", failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to delete '" + path + "'", failure);
        }
    }

    private HeadObjectResponse headObject(ObjectLocation location, String key) {
        try {
            return client.headObject(HeadObjectRequest.builder()
                    .bucket(location.profile().bucket()).key(key).build());
        } catch (S3Exception failure) {
            throw translate("Object not found: " + key, failure);
        } catch (SdkException failure) {
            throw storageFailure("Failed to stat '" + key + "'", failure);
        }
    }

    /** Reads the completed write's identity; a later writer's current HEAD is not its receipt. */
    private FileStat committedStat(ObjectLocation location, String path, String reportedVersion, String etag) {
        String version = realVersionId(reportedVersion);
        if (version == null && (etag == null || etag.isBlank())) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "S3 committed the write but returned no object identity for '" + path + "'");
        }
        var request = HeadObjectRequest.builder().bucket(location.profile().bucket()).key(location.key(path));
        if (version != null) request.versionId(version);
        if (etag != null && !etag.isBlank()) request.ifMatch(etag);
        HeadObjectResponse metadata;
        try {
            metadata = client.headObject(request.build());
        } catch (SdkException failure) {
            // The provider already acknowledged the write. Metadata failure is not
            // rollback evidence and must not cause this method to replay the write.
            throw storageFailure("S3 committed the write but its metadata could not be verified for '"
                    + path + "'", failure);
        }
        if ((version != null && !version.equals(realVersionId(metadata.versionId())))
                || (etag != null && !etag.isBlank() && !etag.equals(metadata.eTag()))
                || metadata.contentLength() == null || metadata.lastModified() == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "S3 committed the write but its returned identity could not be verified for '" + path + "'");
        }
        return stat(path, metadata);
    }

    private static FileStat stat(String path, HeadObjectResponse response) {
        Long size = response.contentLength();
        Instant modified = response.lastModified();
        if (size == null || modified == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                    "S3 returned incomplete metadata for '" + path + "'");
        }
        String etag = response.eTag();
        String version = realVersionId(response.versionId());
        // ETag shape does not prove content MD5: encryption, multipart and
        // compatible providers have different semantics. Keep checksum unknown.
        return new FileStat(path, size, modified, etag, response.contentType(),
                version != null ? version : etag, null);
    }

    private static FileStat listing(ObjectLocation location, S3Object object) {
        String key = object.key();
        String base = location.baseKey();
        String path = base.isEmpty() ? key : key.substring(base.length() + 1);
        // ListObjectsV2 does not report version ids, so the ETag is the only
        // provider identity available for recovery until a HEAD pins one.
        return new FileStat(path, object.size(), object.lastModified(), object.eTag(),
                null, object.eTag(), null);
    }

    private static String realVersionId(String versionId) {
        return versionId == null || versionId.isBlank() || "null".equals(versionId)
                ? null : versionId;
    }

    private static void abort(ResponseInputStream<?> stream) {
        try {
            stream.abort();
        } catch (RuntimeException ignored) {
            // Rejecting the response is best-effort; the precondition error wins.
        }
    }

    private static PangrovaException translate(String message, S3Exception failure) {
        if (failure.statusCode() == NOT_FOUND) {
            return new PangrovaException(PangrovaErrorCode.FILE_OBJECT_NOT_FOUND, message, failure);
        }
        if (failure.statusCode() == PRECONDITION_FAILED) {
            return precondition(message);
        }
        return storageFailure(message, failure);
    }

    private static PangrovaException precondition(String message) {
        return new PangrovaException(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED, message);
    }

    private static PangrovaException storageFailure(String message, Throwable cause) {
        return new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE, message, cause);
    }
}
