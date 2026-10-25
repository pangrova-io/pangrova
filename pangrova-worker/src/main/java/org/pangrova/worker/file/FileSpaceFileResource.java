package org.pangrova.worker.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileStat;
import org.pangrova.spi.storage.file.MultipartUpload;
import org.pangrova.spi.storage.file.ObjectLocation;
import org.pangrova.spi.storage.file.ObjectStoreAccessor;
import org.pangrova.spi.storage.file.UploadedPart;

/**
 * Worker data-plane endpoint for Volume and Stage file bytes.
 *
 * <p>Every request carries a short-lived, credential-free descriptor signed by
 * the coordinator after authorization. The Worker supplies backend credentials
 * from its own secret boundary, so a leaked descriptor grants only the one
 * operation it names until it expires, and credentials never cross the control
 * plane.</p>
 */
@Path("/v1/file-space")
public final class FileSpaceFileResource {

    static final long MAX_SINGLE_PUT_BYTES = 5L * 1024 * 1024 * 1024;
    static final long MULTIPART_PART_SIZE = 64L * 1024 * 1024;
    static final long DEFAULT_URL_TTL_SECONDS = 3600;
    static final long MAX_URL_TTL_SECONDS = 7L * 24 * 60 * 60;
    static final int MAX_LIST_PAGE_SIZE = 1000;
    static final String SIGNATURE_HEADER = "X-Pangrova-File-Signature";

    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC);

    private final ObjectStoreAccessorFactory accessors;
    private final String signingSecret;
    private final ObjectMapper mapper;
    private final LongSupplier clock;
    private final Map<String, MultipartUpload> multipartUploads = new ConcurrentHashMap<>();
    private final Map<String, Long> multipartDeadlines = new ConcurrentHashMap<>();

    /** Creates the endpoint; descriptors must be signed with the cluster internal secret. */
    public FileSpaceFileResource(ObjectStoreAccessorFactory accessors, String signingSecret) {
        this(accessors, signingSecret, JsonMapperFactory.create(), System::currentTimeMillis);
    }

    /** Creates the endpoint with explicit JSON mapping and clock, for tests and embedding. */
    public FileSpaceFileResource(ObjectStoreAccessorFactory accessors, String signingSecret,
            ObjectMapper mapper, LongSupplier clock) {
        if (signingSecret == null || signingSecret.isBlank()) {
            throw new IllegalArgumentException(
                    "File space endpoint requires a descriptor signing secret");
        }
        this.accessors = accessors;
        this.signingSecret = signingSecret;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Returns object metadata for a signed descriptor. */
    @POST
    @Path("/head")
    @Produces(MediaType.APPLICATION_JSON)
    public FileStat head(@QueryParam("descriptor") String descriptor,
            @QueryParam("signature") String signature) {
        return head(verified(descriptor, signature));
    }

    /** Returns one page of a recursive listing for a signed descriptor. */
    @POST
    @Path("/list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public FileListing list(ListRequest request) {
        FileAccessDescriptor descriptor = verified(request.descriptor(), request.signature());
        int pageSize = Math.max(1, Math.min(request.pageSize(), MAX_LIST_PAGE_SIZE));
        return list(descriptor, request.pageToken(), pageSize);
    }

    /** Writes a complete object; larger objects use the multipart endpoints. */
    @PUT
    @Path("/object")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public FileStat put(@HeaderParam("X-Pangrova-File-Descriptor") String descriptor,
            @HeaderParam(SIGNATURE_HEADER) String signature,
            @QueryParam("length") Long declaredLength,
            @HeaderParam("Content-Length") Long contentLength,
            @HeaderParam("Content-Type") String contentType, InputStream body) throws IOException {
        FileAccessDescriptor checked = verified(descriptor, signature);
        long length = declaredLength != null ? declaredLength
                : contentLength == null ? -1 : contentLength;
        if (length < 0 || length > MAX_SINGLE_PUT_BYTES) {
            throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                    "Single-request upload must declare at most 5 GiB");
        }
        return put(checked, length, contentType, body);
    }

    /** Streams an object or byte range, honoring a version precondition. */
    @GET
    @Path("/object")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response get(@QueryParam("descriptor") String descriptor,
            @QueryParam("signature") String signature,
            @QueryParam("offset") Long offset, @QueryParam("length") Long length,
            @QueryParam("version") String version,
            @HeaderParam("Range") String range,
            @HeaderParam("If-Match") String ifMatch,
            @HeaderParam("If-Range") String ifRange) throws IOException {
        RangeRead read = read(verified(descriptor, signature), offset, length, version,
                range, ifMatch, ifRange);
        Response.ResponseBuilder response = readMetadata(read.stat(),
                read.partial() ? Response.Status.PARTIAL_CONTENT : Response.Status.OK)
                .entity((jakarta.ws.rs.core.StreamingOutput) output -> {
                    try (InputStream in = read.stream()) {
                        in.transferTo(output);
                    }
                });
        if (read.partial()) {
            long end = read.offset() + read.stat().size() - 1;
            response.header("Content-Range", "bytes " + read.offset() + "-" + end + "/"
                    + read.totalSize());
        }
        return response.build();
    }

    /** HEAD does not open a body stream that the HTTP runtime would discard. */
    @HEAD
    @Path("/object")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response headObject(@QueryParam("descriptor") String descriptor,
            @QueryParam("signature") String signature,
            @QueryParam("version") String version,
            @HeaderParam("If-Match") String ifMatch) {
        FileAccessDescriptor checked = require(verified(descriptor, signature), "read");
        FileStat stat = accessor(checked).head(location(checked), checked.path());
        FileReadPreconditions.check(stat, version, ifMatch);
        return readMetadata(stat, Response.Status.OK).build();
    }

    private static Response.ResponseBuilder readMetadata(FileStat stat, Response.Status status) {
        return Response.status(status)
                .header("Content-Length", stat.size())
                .header("Last-Modified", HTTP_DATE.format(stat.lastModified()))
                .header("Accept-Ranges", "bytes")
                .header("ETag", FileReadPreconditions.entityTag(stat));
    }

    /** Deletes one object with a version precondition. */
    @DELETE
    @Path("/object")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response delete(@QueryParam("descriptor") String descriptor,
            @QueryParam("signature") String signature, DeleteRequest request) {
        FileAccessDescriptor checked = verified(descriptor, signature);
        delete(checked, request == null ? null : request.expectedVersion());
        return Response.noContent().build();
    }

    /** Starts a multipart upload for objects above the single-request limit. */
    @POST
    @Path("/multipart/begin")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MultipartSession beginMultipart(BeginMultipartRequest request) {
        FileAccessDescriptor checked = verified(request.descriptor(), request.signature());
        MultipartUpload upload = beginMultipart(checked, request.contentType());
        return new MultipartSession(sessionToken(checked, upload.uploadId()),
                request.descriptor(), request.signature(), MULTIPART_PART_SIZE);
    }

    /** Accepts one part of an open multipart upload. */
    @PUT
    @Path("/multipart/part")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public UploadedPart uploadPart(@QueryParam("descriptor") String descriptor,
            @QueryParam("signature") String signature,
            @QueryParam("uploadId") String uploadId,
            @QueryParam("partNumber") int partNumber,
            @QueryParam("length") Long declaredLength,
            @HeaderParam("Content-Length") Long contentLength,
            InputStream body) throws IOException {
        FileAccessDescriptor checked = verified(descriptor, signature);
        long length = declaredLength != null ? declaredLength
                : contentLength == null ? -1 : contentLength;
        if (length < 0) {
            throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                    "Multipart part must declare its length");
        }
        return uploadPart(checked, uploadId, partNumber, length, body);
    }

    /** Completes a multipart upload from the accepted parts. */
    @POST
    @Path("/multipart/complete")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public FileStat completeMultipart(CompleteMultipartRequest request) {
        FileAccessDescriptor checked = verified(request.descriptor(), request.signature());
        List<UploadedPart> parts = request.parts() == null ? List.of()
                : request.parts().stream()
                        .map(part -> new UploadedPart(part.partNumber(), part.etag(), part.size()))
                        .toList();
        return completeMultipart(checked, request.uploadId(), parts);
    }

    /** Aborts a multipart upload and releases its staged parts. */
    @DELETE
    @Path("/multipart")
    public Response abortMultipart(@QueryParam("descriptor") String descriptor,
            @QueryParam("signature") String signature,
            @QueryParam("uploadId") String uploadId) {
        abortMultipart(verified(descriptor, signature), uploadId);
        return Response.noContent().build();
    }

    /** Issues a time-limited provider URL for one object. */
    @POST
    @Path("/presign")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public PresignedUrl presign(PresignRequest request) {
        boolean read = request.read();
        FileAccessDescriptor checked = require(
                verified(request.descriptor(), request.signature()),
                read ? "read" : "write");
        long ttlSeconds = request.ttlSeconds() <= 0
                ? DEFAULT_URL_TTL_SECONDS : Math.min(request.ttlSeconds(), MAX_URL_TTL_SECONDS);
        String url = accessor(checked).presign(location(checked), checked.path(),
                java.time.Duration.ofSeconds(ttlSeconds), read);
        return new PresignedUrl(url, clock.getAsLong() + ttlSeconds * 1000);
    }

    FileStat head(FileAccessDescriptor descriptor) {
        FileAccessDescriptor checked = require(descriptor, "head");
        return accessor(checked).head(location(checked), checked.path());
    }

    FileListing list(FileAccessDescriptor descriptor, String pageToken, int pageSize) {
        FileAccessDescriptor checked = require(descriptor, "list");
        return accessor(checked).list(location(checked), checked.path(), pageToken, pageSize);
    }

    FileStat put(FileAccessDescriptor descriptor, long contentLength, String contentType,
            InputStream body) throws IOException {
        FileAccessDescriptor checked = require(descriptor, "write");
        return accessor(checked).put(location(checked), checked.path(),
                body, contentLength, contentType);
    }

    RangeRead read(FileAccessDescriptor descriptor, long offset, long length, String version)
            throws IOException {
        return read(descriptor, offset, length, version, null, null, null);
    }

    private RangeRead read(FileAccessDescriptor descriptor, Long offset, Long length,
            String version, String range, String ifMatch, String ifRange) throws IOException {
        FileAccessDescriptor checked = require(descriptor, "read");
        ObjectStoreAccessor accessor = accessor(checked);
        ObjectLocation location = location(checked);
        FileStat stat = accessor.head(location, checked.path());
        FileReadPreconditions.check(stat, version, ifMatch);
        long total = stat.size();
        // Dates are not strong validators here; only an exact strong tag enables resumption.
        boolean ignoreRange = range != null && ifRange != null
                && (ifRange.startsWith("W/")
                    || !ifRange.equals(FileReadPreconditions.entityTag(stat)));
        FileReadWindow requested = ignoreRange && offset == null && length == null
                ? new FileReadWindow(0, total, false)
                : FileReadWindow.resolve(total, offset, length, range);
        FileStat window = new FileStat(stat.path(), requested.length(), stat.lastModified(),
                stat.etag(), stat.contentType(), stat.providerVersion(), stat.md5());
        InputStream stream = accessor.openRange(location, checked.path(), requested.offset(),
                requested.length(), stat.providerVersion());
        return new RangeRead(window, stream, requested.partial(), total, requested.offset());
    }

    void delete(FileAccessDescriptor descriptor, String expectedVersion) {
        FileAccessDescriptor checked = require(descriptor, "delete");
        accessor(checked).delete(location(checked), checked.path(), expectedVersion);
    }

    MultipartUpload beginMultipart(FileAccessDescriptor descriptor, String contentType) {
        FileAccessDescriptor checked = require(descriptor, "write");
        evictExpired();
        MultipartUpload upload = accessor(checked).beginMultipart(
                location(checked), checked.path(), contentType);
        multipartUploads.put(upload.uploadId(), upload);
        multipartDeadlines.put(upload.uploadId(), checked.expiresAtEpochMillis());
        return upload;
    }

    UploadedPart uploadPart(FileAccessDescriptor descriptor, String uploadId, int partNumber,
            long length, InputStream body) throws IOException {
        MultipartUpload upload = requireUpload(descriptor, uploadId);
        return accessor(descriptor).uploadPart(upload, partNumber, body, length);
    }

    FileStat completeMultipart(FileAccessDescriptor descriptor, String uploadId,
            List<UploadedPart> parts) {
        MultipartUpload upload = requireUpload(descriptor, uploadId);
        FileStat stat = accessor(descriptor).completeMultipart(upload, parts);
        forget(uploadId, upload.uploadId());
        return stat;
    }

    void abortMultipart(FileAccessDescriptor descriptor, String uploadId) {
        MultipartUpload upload = requireUpload(descriptor, uploadId);
        try {
            accessor(descriptor).abortMultipart(upload);
        } finally {
            forget(uploadId, upload.uploadId());
        }
    }

    /**
     * Resolves the session opened for this exact space and path. The descriptor
     * is verified on every call, so a leaked upload id never outlives the
     * authorization that created it and never retargets another path.
     *
     * <p>The client normally sends a signed session token, so a part upload,
     * completion, or abort may land on any Worker in the cluster. Tokens are
     * bound to the space root and path, which is what stops a retarget. Raw
     * provider ids are accepted only from the Worker that opened the session.</p>
     */
    private MultipartUpload requireUpload(FileAccessDescriptor descriptor, String uploadId) {
        FileAccessDescriptor checked = require(descriptor, "write");
        String providerUploadId = providerUploadId(uploadId, checked);
        if (providerUploadId != null) {
            return new MultipartUpload(providerUploadId, location(checked), checked.path());
        }
        MultipartUpload upload = uploadId == null ? null : multipartUploads.get(uploadId);
        if (upload == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Unknown or already finished multipart upload");
        }
        Long deadline = multipartDeadlines.get(uploadId);
        if (deadline != null && deadline <= clock.getAsLong()) {
            forget(uploadId);
            abortQuietly(upload);
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    "Multipart upload authorization has expired");
        }
        if (!upload.location().baseKey().equals(checked.baseKey())
                || !upload.path().equals(checked.path())) {
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    "Multipart upload belongs to a different space or path");
        }
        return upload;
    }

    /** Wraps a provider upload id in a path-bound token so any Worker can resume it. */
    private String sessionToken(FileAccessDescriptor descriptor, String providerUploadId) {
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(providerUploadId.getBytes(StandardCharsets.UTF_8));
        return encoded + '.' + FileAccessSignature.sign(signingSecret,
                sessionPayload(descriptor, providerUploadId));
    }

    /**
     * Returns the provider upload id when the token was issued for this exact
     * space and path, or {@code null} when the value is not a session token.
     */
    private String providerUploadId(String uploadId, FileAccessDescriptor descriptor) {
        if (uploadId == null) {
            return null;
        }
        int separator = uploadId.indexOf('.');
        if (separator <= 0 || separator == uploadId.length() - 1) {
            return null;
        }
        String providerId;
        try {
            providerId = new String(
                    Base64.getUrlDecoder().decode(uploadId.substring(0, separator)),
                    StandardCharsets.UTF_8);
        } catch (IllegalArgumentException notBase64) {
            return null;
        }
        try {
            FileAccessSignature.requireValid(signingSecret,
                    sessionPayload(descriptor, providerId),
                    uploadId.substring(separator + 1));
            return providerId;
        } catch (PangrovaException tamperedOrRetargeted) {
            return null;
        }
    }

    private static String sessionPayload(FileAccessDescriptor descriptor,
            String providerUploadId) {
        return "multipart\n" + descriptor.baseKey() + "\n" + descriptor.path() + "\n"
                + providerUploadId;
    }

    /**
     * Releases provider parts for sessions whose authorization elapsed without a
     * completion or abort, so expired attempts do not accumulate objects.
     */
    private void evictExpired() {
        long now = clock.getAsLong();
        multipartDeadlines.forEach((uploadId, deadline) -> {
            if (deadline == null || deadline > now) {
                return;
            }
            MultipartUpload expired = multipartUploads.remove(uploadId);
            if (multipartDeadlines.remove(uploadId) != null && expired != null) {
                abortQuietly(expired);
            }
        });
    }

    private void abortQuietly(MultipartUpload upload) {
        try {
            accessors.accessor(upload.location().profile()).abortMultipart(upload);
        } catch (RuntimeException ignored) {
            // The session is already unreachable; a failed abort must not mask that.
        }
    }

    private void forget(String... uploadIds) {
        for (String uploadId : uploadIds) {
            if (uploadId == null) {
                continue;
            }
            multipartUploads.remove(uploadId);
            multipartDeadlines.remove(uploadId);
        }
    }

    private FileAccessDescriptor require(FileAccessDescriptor descriptor, String operation) {
        FileAccessDescriptor checked = descriptor.requireUnexpired(clock.getAsLong());
        if (!operation.equals(checked.operation())) {
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    "File access descriptor allows '" + checked.operation()
                            + "', not '" + operation + "'");
        }
        return checked;
    }

    private ObjectStoreAccessor accessor(FileAccessDescriptor descriptor) {
        return accessors.accessor(descriptor.profile());
    }

    private static ObjectLocation location(FileAccessDescriptor descriptor) {
        return new ObjectLocation(descriptor.profile(), descriptor.baseKey());
    }

    /** Verifies the signature before parsing, so unsigned input never reaches storage. */
    FileAccessDescriptor verified(String encoded, String signature) {
        FileAccessSignature.requireValid(signingSecret, encoded, signature);
        return decode(encoded);
    }

    FileAccessDescriptor decode(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    "File access descriptor is required");
        }
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(encoded);
            return mapper.readValue(decoded, FileAccessDescriptor.class);
        } catch (IOException | IllegalArgumentException failure) {
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    "File access descriptor is not valid", failure);
        }
    }

    /** Listing request body; the descriptor path is the prefix. */
    public record ListRequest(String descriptor, String signature, String pageToken,
            int pageSize) {
    }

    /** Delete request body with its version precondition. */
    public record DeleteRequest(String expectedVersion) {
    }

    /** Multipart begin request; {@code contentType} may be null. */
    public record BeginMultipartRequest(String descriptor, String signature,
            String contentType) {
    }

    /** Multipart session handed back to the client for part uploads. */
    public record MultipartSession(String uploadId, String descriptor, String signature,
            long partSize) {
    }

    /** Multipart completion request with the provider ETags of each part. */
    public record CompleteMultipartRequest(String descriptor, String signature, String uploadId,
            List<MultipartPart> parts) {
    }

    /** One accepted part as reported by the client. */
    public record MultipartPart(int partNumber, String etag, long size) {
    }

    /** Signed-URL request; {@code read=false} asks for an upload URL. */
    public record PresignRequest(String descriptor, String signature, boolean read,
            long ttlSeconds) {
    }

    /** Signed URL and the epoch millisecond at which it stops working. */
    public record PresignedUrl(String url, long expiresAtEpochMillis) {
    }

    /** One resolved range with metadata sized to the returned window. */
    public record RangeRead(FileStat stat, InputStream stream, boolean partial,
            long totalSize, long offset) {
    }
}
