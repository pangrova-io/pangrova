package org.pangrova.worker.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileStat;
import org.pangrova.spi.storage.file.MultipartUpload;
import org.pangrova.spi.storage.file.UploadedPart;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSpaceFileResourceTest {

    private static final long EXPIRY = 2_000_000_000_000L;
    private static final String SECRET = "file-plane-test-secret";

    @TempDir
    Path root;

    private final ObjectMapper mapper = JsonMapperFactory.create();
    private FileSpaceFileResource resource;

    @BeforeEach
    void setUp() {
        resource = new FileSpaceFileResource(new ObjectStoreAccessorFactory(), SECRET,
                mapper, () -> 1_000L);
    }

    @Test
    void descriptorDrivesPutHeadRangeAndList() throws IOException {
        FileAccessDescriptor descriptor = descriptor("images/a.bin", "write");
        FileStat written = resource.put(descriptor, 10, "text/plain", stream("0123456789"));

        assertThat(written.size()).isEqualTo(10);
        FileStat head = resource.head(descriptor("images/a.bin", "head"));
        assertThat(head.providerVersion()).isEqualTo(written.providerVersion());

        FileSpaceFileResource.RangeRead read =
                resource.read(descriptor("images/a.bin", "read"), 2, 3, null);
        assertThat(read.partial()).isTrue();
        assertThat(read.totalSize()).isEqualTo(10);
        assertThat(readValue(read.stream())).isEqualTo("234");

        FileListing listing = resource.list(descriptor("images", "list"), null, 10);
        assertThat(listing.files()).extracting(FileStat::path).containsExactly("images/a.bin");
    }

    @Test
    void signedDescriptorIsAcceptedAndEveryOtherFormIsRejected() throws IOException {
        resource.put(descriptor("images/a.bin", "write"), 1, "text/plain", stream("x"));
        String encoded = encode(descriptor("images/a.bin", "head"));
        String signature = FileAccessSignature.sign(SECRET, encoded);

        assertThat(resource.head(encoded, signature).path()).isEqualTo("images/a.bin");

        assertThatThrownBy(() -> resource.head(encoded, "forged"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
        assertThatThrownBy(() -> resource.head(encoded, null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void signatureDoesNotCarryOverToAnotherDescriptor() {
        String read = encode(descriptor("images/a.bin", "read"));
        String write = encode(descriptor("images/a.bin", "write"));

        assertThatThrownBy(() ->
                resource.verified(write, FileAccessSignature.sign(SECRET, read)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void expiredDescriptorIsRejectedBeforeAnyStorageAccess() {
        FileAccessDescriptor expired = new FileAccessDescriptor("local", StorageType.LOCAL,
                root.toString(), "", "", "", false, "volumes/media/raw/landing",
                "images/a.bin", "read", 500L);

        assertThatThrownBy(() -> resource.head(expired))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void oversizeSingleRequestIsRejected() {
        assertThatThrownBy(() -> resource.put(descriptor("images/a.bin", "write"),
                FileSpaceFileResource.MAX_SINGLE_PUT_BYTES + 1, null, stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_LIMIT_EXCEEDED));
    }

    @Test
    void malformedDescriptorIsRejected() {
        assertThatThrownBy(() -> resource.decode("not-base64-json"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void multipartUploadRoundTripsIntoOneReadableObject() throws IOException {
        FileAccessDescriptor write = descriptor("videos/clip.bin", "write");
        MultipartUpload upload = resource.beginMultipart(write, "application/octet-stream");
        UploadedPart first = resource.uploadPart(write, upload.uploadId(), 1, 5,
                stream("hello"));
        UploadedPart second = resource.uploadPart(write, upload.uploadId(), 2, 6,
                stream(" world"));

        FileStat completed = resource.completeMultipart(write, upload.uploadId(),
                java.util.List.of(first, second));

        assertThat(completed.size()).isEqualTo(11);
        FileSpaceFileResource.RangeRead read =
                resource.read(descriptor("videos/clip.bin", "read"), 0, -1, null);
        assertThat(readValue(read.stream())).isEqualTo("hello world");
    }

    @Test
    void descriptorOperationIsEnforcedPerEndpoint() {
        FileAccessDescriptor readOnly = descriptor("images/a.bin", "read");
        assertThatThrownBy(() -> resource.put(readOnly, 1, "text/plain", stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void multipartSessionCannotBeRetargetedToAnotherPath() {
        FileAccessDescriptor upload = descriptor("videos/a.bin", "write");
        MultipartUpload session = resource.beginMultipart(upload, null);

        assertThatThrownBy(() -> resource.uploadPart(descriptor("videos/b.bin", "write"),
                session.uploadId(), 1, 1, stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void unknownMultipartSessionIsRejected() {
        assertThatThrownBy(() -> resource.completeMultipart(
                descriptor("videos/a.bin", "write"), "missing", java.util.List.of()))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
    }

    @Test
    void expiredMultipartSessionsAreEvictedWhenTheNextUploadStarts() {
        java.util.concurrent.atomic.AtomicLong now =
                new java.util.concurrent.atomic.AtomicLong(1_000L);
        FileSpaceFileResource clocked = new FileSpaceFileResource(
                new ObjectStoreAccessorFactory(), SECRET, mapper, now::get);
        FileAccessDescriptor expiring = new FileAccessDescriptor("local", StorageType.LOCAL,
                root.toString(), "", "", "", false, "volumes/media/raw/landing",
                "videos/stale.bin", "write", 2_000L);
        MultipartUpload stale = clocked.beginMultipart(expiring, null);

        now.set(3_000L);
        FileAccessDescriptor later = new FileAccessDescriptor("local", StorageType.LOCAL,
                root.toString(), "", "", "", false, "volumes/media/raw/landing",
                "videos/fresh.bin", "write", 9_000L);
        clocked.beginMultipart(later, null);

        assertThatThrownBy(() -> clocked.uploadPart(later, stale.uploadId(), 1, 1,
                stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
    }

    @Test
    void multipartSessionResumesOnAnotherWorker() throws IOException {
        FileAccessDescriptor upload = descriptor("videos/multi.bin", "write");
        String encoded = encode(upload);
        FileSpaceFileResource.MultipartSession session = resource.beginMultipart(
                new FileSpaceFileResource.BeginMultipartRequest(encoded,
                        FileAccessSignature.sign(SECRET, encoded), "video/mp4"));
        FileSpaceFileResource anotherWorker = new FileSpaceFileResource(
                new ObjectStoreAccessorFactory(), SECRET, mapper, () -> 1_000L);

        UploadedPart first = anotherWorker.uploadPart(upload, session.uploadId(), 1, 2,
                stream("ab"));
        UploadedPart second = anotherWorker.uploadPart(upload, session.uploadId(), 2, 2,
                stream("cd"));
        FileStat completed = anotherWorker.completeMultipart(upload, session.uploadId(),
                java.util.List.of(first, second));

        assertThat(completed.size()).isEqualTo(4);
        assertThat(readValue(anotherWorker
                .read(descriptor("videos/multi.bin", "read"), 0, -1, null).stream()))
                .isEqualTo("abcd");
    }

    @Test
    void multipartSessionTokenCannotBeReplayedForAnotherPath() {
        FileAccessDescriptor upload = descriptor("videos/a.bin", "write");
        String encoded = encode(upload);
        FileSpaceFileResource.MultipartSession session = resource.beginMultipart(
                new FileSpaceFileResource.BeginMultipartRequest(encoded,
                        FileAccessSignature.sign(SECRET, encoded), null));

        assertThatThrownBy(() -> resource.uploadPart(descriptor("videos/b.bin", "write"),
                session.uploadId(), 1, 1, stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
    }

    @Test
    void presignFailsClosedOnBackendsWithoutUrlSupport() {
        String encoded = encode(descriptor("images/a.bin", "read"));
        String signature = FileAccessSignature.sign(SECRET, encoded);

        assertThatThrownBy(() -> resource.presign(new FileSpaceFileResource.PresignRequest(
                encoded, signature, true, 60)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
    }

    @Test
    void endpointWithoutSigningSecretIsRefused() {
        assertThatThrownBy(() -> new FileSpaceFileResource(
                new ObjectStoreAccessorFactory(), " ", mapper, () -> 1_000L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private String encode(FileAccessDescriptor descriptor) {
        try {
            return Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(mapper.writeValueAsBytes(descriptor));
        } catch (IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private FileAccessDescriptor descriptor(String path, String operation) {
        return new FileAccessDescriptor("local", StorageType.LOCAL, root.toString(),
                "", "", "", false, "volumes/media/raw/landing", path, operation, EXPIRY);
    }

    private static InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readValue(InputStream input) throws IOException {
        try (input) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
