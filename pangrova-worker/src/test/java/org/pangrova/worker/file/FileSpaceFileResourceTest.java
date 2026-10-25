package org.pangrova.worker.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
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

        now.set(1_000L + FileSpaceFileResource.MULTIPART_SESSION_MILLIS);
        FileAccessDescriptor later = new FileAccessDescriptor("local", StorageType.LOCAL,
                root.toString(), "", "", "", false, "volumes/media/raw/landing",
                "videos/fresh.bin", "write", EXPIRY);
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
    void renewedGrantSurvivesOriginCleanupAndResumesOnAnotherWorker() throws IOException {
        AtomicLong now = new AtomicLong(1_000L);
        FileSpaceFileResource origin = clocked(now);
        FileSpaceFileResource next = clocked(now);
        FileAccessDescriptor initial = writeGrant(2_000L);
        FileSpaceFileResource.MultipartSession session = begin(origin, initial);
        UploadedPart first = origin.uploadPart(initial, session.uploadId(), 1, 2, stream("ab"));

        now.set(3_000L);
        origin.beginMultipart(descriptor("videos/other.bin", "write"), null);
        assertThatThrownBy(() -> next.uploadPart(initial, session.uploadId(), 2, 2, stream("cd")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
        FileAccessDescriptor renewed = writeGrant(9_000L);
        UploadedPart second = next.uploadPart(renewed, session.uploadId(), 2, 2, stream("cd"));
        next.completeMultipart(renewed, session.uploadId(), java.util.List.of(first, second));
        assertThat(readValue(next.read(descriptor("videos/session.bin", "read"),
                0, -1, null).stream())).isEqualTo("abcd");
    }

    @Test
    void sessionLifetimeCannotBeExtendedByRenewingGrantOrTamperingToken() {
        AtomicLong now = new AtomicLong(1_000L);
        FileSpaceFileResource origin = clocked(now);
        FileSpaceFileResource next = clocked(now);
        FileSpaceFileResource.MultipartSession session = begin(origin, writeGrant(2_000L));
        assertThat(session.expiresAtEpochMillis())
                .isEqualTo(1_000L + FileSpaceFileResource.MULTIPART_SESSION_MILLIS);
        String tampered = session.uploadId().replace("." + session.expiresAtEpochMillis() + ".",
                "." + EXPIRY + ".");
        assertThatThrownBy(() -> next.uploadPart(writeGrant(EXPIRY), tampered, 1, 1, stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
        now.set(session.expiresAtEpochMillis());
        assertThatThrownBy(() -> next.uploadPart(writeGrant(EXPIRY), session.uploadId(),
                1, 1, stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void multipartTokenBindsEveryStorageScopeField() {
        FileAccessDescriptor original = writeGrant(EXPIRY);
        String token = begin(resource, original).uploadId();
        Object[] scope = {original.profileName(), original.type(), original.bucket(), original.prefix(),
                original.endpoint(), original.region(), original.pathStyleAccess(), original.baseKey(),
                original.path()};
        Object[] replacements = {"other-profile", StorageType.S3, root.resolve("other").toString(),
                "different-prefix", "http://127.0.0.1:1", "eu-west-1", true, "other-root", "other.bin"};
        for (int field = 0; field < scope.length; field++) {
            Object[] changed = scope.clone();
            changed[field] = replacements[field];
            FileAccessDescriptor retargeted = new FileAccessDescriptor((String) changed[0],
                    (StorageType) changed[1], (String) changed[2], (String) changed[3],
                    (String) changed[4], (String) changed[5], (boolean) changed[6],
                    (String) changed[7], (String) changed[8], "write", EXPIRY);
            assertThatThrownBy(() -> resource.uploadPart(retargeted, token, 1, 1, stream("x")))
                    .as("storage scope field %s", field)
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
        }
    }

    @Test
    void failedExpiredCleanupRetainsOwnershipForRetry() throws IOException {
        AtomicLong now = new AtomicLong(1_000L);
        FileSpaceFileResource origin = clocked(now);
        MultipartUpload stale = origin.beginMultipart(writeGrant(EXPIRY), null);
        origin.uploadPart(writeGrant(EXPIRY), stale.uploadId(), 1, 1, stream("x"));
        Path parts = root.resolve(".pangrova-multipart").resolve(stale.uploadId());
        Path saved = root.resolve("saved-parts");
        Files.move(parts, saved);
        Files.createSymbolicLink(parts, saved);
        now.set(1_000L + FileSpaceFileResource.MULTIPART_SESSION_MILLIS);
        origin.beginMultipart(descriptor("videos/other.bin", "write"), null);
        assertThat(saved.resolve("00001.part")).exists();
        Files.delete(parts);
        Files.move(saved, parts);
        origin.beginMultipart(descriptor("videos/another.bin", "write"), null);
        assertThat(parts).doesNotExist();
    }

    private FileSpaceFileResource clocked(AtomicLong now) {
        return new FileSpaceFileResource(new ObjectStoreAccessorFactory(), SECRET, mapper, now::get);
    }

    private FileAccessDescriptor writeGrant(long expiry) {
        return new FileAccessDescriptor("local", StorageType.LOCAL, root.toString(), "", "", "",
                false, "volumes/media/raw/landing", "videos/session.bin", "write", expiry);
    }

    private FileSpaceFileResource.MultipartSession begin(FileSpaceFileResource worker,
            FileAccessDescriptor descriptor) {
        String encoded = encode(descriptor);
        return worker.beginMultipart(new FileSpaceFileResource.BeginMultipartRequest(encoded,
                FileAccessSignature.sign(SECRET, encoded), null));
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
    void providerUrlCannotOutliveTheAuthorizingGrant() {
        var now = new AtomicLong(System.currentTimeMillis());
        try (var accessors = new ObjectStoreAccessorFactory(
                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("test", "test-secret")))) {
            var worker = new FileSpaceFileResource(accessors, SECRET, mapper, now::get);
            var grant = new FileAccessDescriptor("s3", StorageType.S3, "test-bucket", "", "http://localhost:1",
                    "us-east-1", true, "stage", "private.bin", "read", now.get() + 5_999);
            var encoded = encode(grant);
            var result = worker.presign(new FileSpaceFileResource.PresignRequest(encoded,
                    FileAccessSignature.sign(SECRET, encoded), true, 3600));
            assertThat(result.expiresAtEpochMillis()).isEqualTo(now.get() + 5_000);
            assertThat(java.net.URI.create(result.url()).getRawQuery()).contains("X-Amz-Expires=5");
            now.addAndGet(5_000);
            assertThatThrownBy(() -> worker.presign(new FileSpaceFileResource.PresignRequest(encoded,
                    FileAccessSignature.sign(SECRET, encoded), true, 60)))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
        }
    }

    @Test
    void anOpenedStorageReadCannotContinuePastItsDeliveryDeadline() throws IOException {
        var now = new AtomicLong(1_000);
        var worker = clocked(now);
        worker.put(descriptor("private.bin", "write"), 10, "text/plain", stream("0123456789"));
        var grant = new FileAccessDescriptor("local", StorageType.LOCAL, root.toString(), "", "", "", false,
                "volumes/media/raw/landing", "private.bin", "read", 2_000);
        try (var opened = worker.read(grant, 0, -1, null).stream()) {
            assertThat(opened.read()).isEqualTo('0');
            now.set(2_000);
            assertThatThrownBy(opened::read).isInstanceOf(IOException.class).hasMessageContaining("expired");
            assertThatThrownBy(() -> opened.read(new byte[4])).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> opened.skip(1)).isInstanceOf(IOException.class);
        }
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
