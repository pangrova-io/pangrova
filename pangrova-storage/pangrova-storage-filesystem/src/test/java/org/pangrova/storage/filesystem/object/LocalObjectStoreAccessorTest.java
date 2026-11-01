package org.pangrova.storage.filesystem.object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.file.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.directory.FileListing;
import org.pangrova.file.api.directory.FileStat;
import org.pangrova.file.api.object.ObjectLocation;
import org.pangrova.file.api.object.UploadedPart;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalObjectStoreAccessorTest {

    @TempDir
    Path root;

    @TempDir
    Path externalRoot;

    private LocalObjectStoreAccessor accessor;
    private ObjectLocation location;

    @BeforeEach
    void setUp() {
        accessor = new LocalObjectStoreAccessor(root);
        StorageProfile profile = new StorageProfile("local", StorageType.LOCAL,
                root.toString(), "", "", "", Map.of(), Map.of());
        location = new ObjectLocation(profile, "volumes/media/raw/landing");
    }

    @Test
    void invalidNegativeRangeLengthIsRejectedConsistentlyAcrossBackends() throws IOException {
        accessor.put(location, "range.bin", stream("data"), 4, null);
        assertThatThrownBy(() -> accessor.openRange(location, "range.bin", 0, -2, null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));
    }

    @Test
    void putHeadRangeAndVersionedDeleteRoundTrip() throws IOException {
        FileStat written = accessor.put(location, "images/a.bin",
                stream("0123456789"), 10, "application/octet-stream");

        assertThat(written.size()).isEqualTo(10);
        assertThat(written.path()).isEqualTo("images/a.bin");
        assertThat(written.providerVersion()).isNotBlank();
        assertThat(written.md5()).isNull();
        assertThat(read(accessor.openRange(location, "images/a.bin", 2, 3, null)))
                .isEqualTo("234");
        assertThat(read(accessor.openRange(location, "images/a.bin", 8, -1, null)))
                .isEqualTo("89");

        assertThatThrownBy(() -> accessor.openRange(location, "images/a.bin", 0, 1, "stale"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));

        accessor.delete(location, "images/a.bin", written.providerVersion());
        assertThatThrownBy(() -> accessor.head(location, "images/a.bin"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_OBJECT_NOT_FOUND));
    }

    @Test
    void sameSizeOverwriteWithRestoredTimestampInvalidatesReadPrecondition() throws IOException {
        FileStat original = accessor.put(location, "same.bin", stream("before"), 6, null);
        Path file = root.resolve(location.baseKey()).resolve("same.bin");
        var timestamp = Files.getLastModifiedTime(file);
        accessor.put(location, "same.bin", stream("after!"), 6, null);
        Files.setLastModifiedTime(file, timestamp);

        assertThat(accessor.head(location, "same.bin").etag()).isNotEqualTo(original.etag());
        assertThatThrownBy(() -> accessor.openRange(location, "same.bin", 0, -1,
                original.providerVersion()))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));
    }

    @Test
    void openReadRetainsOriginalBytesAcrossAtomicReplacement() throws IOException {
        FileStat original = accessor.put(location, "replace.bin", stream("original"), 8, null);
        try (InputStream opened = accessor.openRange(location, "replace.bin", 0, -1,
                original.providerVersion())) {
            accessor.put(location, "replace.bin", stream("new"), 3, null);
            assertThat(read(opened)).isEqualTo("original");
        }
    }

    @Test
    void multipartStitchesPartsInOrder() throws IOException {
        var upload = accessor.beginMultipart(location, "video/clip.mov", "video/quicktime");
        UploadedPart first = accessor.uploadPart(upload, 1, stream("hello "), 6);
        UploadedPart second = accessor.uploadPart(upload, 2, stream("world"), 5);

        FileStat completed = accessor.completeMultipart(upload, List.of(second, first));

        assertThat(completed.size()).isEqualTo(11);
        assertThat(read(accessor.openRange(location, "video/clip.mov", 0, -1, null)))
                .isEqualTo("hello world");
        assertThat(Files.exists(root.resolve(".pangrova-multipart").resolve(upload.uploadId())))
                .isFalse();
    }

    @Test
    void multipartRejectsNonContiguousParts() throws IOException {
        var upload = accessor.beginMultipart(location, "video/clip.mov", "video/quicktime");
        UploadedPart second = accessor.uploadPart(upload, 2, stream("b"), 1);

        assertThatThrownBy(() -> accessor.completeMultipart(upload, List.of(second)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("contiguous");
        UploadedPart first = accessor.uploadPart(upload, 1, stream("a"), 1);
        accessor.completeMultipart(upload, List.of(first, second));
        assertThat(read(accessor.openRange(location, "video/clip.mov", 0, -1, null))).isEqualTo("ab");
    }

    @Test
    void stalePartEtagRejectsSameLengthReplacementWithoutChangingPublishedObject() throws IOException {
        accessor.put(location, "clip.bin", stream("published"), 9, null);
        var upload = accessor.beginMultipart(location, "clip.bin", null);
        UploadedPart old = accessor.uploadPart(upload, 1, stream("old"), 3);
        UploadedPart replacement = accessor.uploadPart(upload, 1, stream("new"), 3);
        assertThat(replacement.etag()).isNotEqualTo(old.etag());
        assertThatThrownBy(() -> accessor.completeMultipart(upload, List.of(old)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID));
        assertThat(read(accessor.openRange(location, "clip.bin", 0, -1, null))).isEqualTo("published");
        assertThatThrownBy(() -> accessor.completeMultipart(upload,
                List.of(new UploadedPart(1, replacement.etag(), 4))))
                .isInstanceOf(PangrovaException.class);
        accessor.completeMultipart(upload, List.of(replacement));
        assertThat(read(accessor.openRange(location, "clip.bin", 0, -1, null))).isEqualTo("new");
    }

    @Test
    void incompleteOrOversizeReplacementRetainsPreviouslyAcceptedPart() throws IOException {
        var upload = accessor.beginMultipart(location, "clip.bin", null);
        UploadedPart original = accessor.uploadPart(upload, 1, stream("original"), 8);
        assertThatThrownBy(() -> accessor.uploadPart(upload, 1, stream("short"), 8))
                .isInstanceOf(PangrovaException.class);
        assertThatThrownBy(() -> accessor.uploadPart(upload, 1, stream("too-long"), 2))
                .isInstanceOf(PangrovaException.class);
        InputStream interrupted = new InputStream() {
            private int remaining = 3;
            @Override
            public int read() throws IOException {
                if (remaining-- > 0) return 'x';
                throw new IOException("connection interrupted");
            }
        };
        assertThatThrownBy(() -> accessor.uploadPart(upload, 1, interrupted, 8))
                .isInstanceOf(IOException.class);
        accessor.completeMultipart(upload, List.of(original));
        assertThat(read(accessor.openRange(location, "clip.bin", 0, -1, null))).isEqualTo("original");
    }

    @Test
    void listingPaginatesWithoutLossOrDuplicates() throws IOException {
        for (int index = 0; index < 5; index++) {
            accessor.put(location, "parts/part-" + index + ".bin", stream("x"), 1, null);
        }
        accessor.put(location, "other/skip.bin", stream("x"), 1, null);

        List<String> seen = new ArrayList<>();
        String token = null;
        do {
            FileListing page = accessor.list(location, "parts", token, 2);
            page.files().forEach(file -> seen.add(file.path()));
            token = page.nextPageToken();
        } while (token != null);

        assertThat(seen).containsExactly(
                "parts/part-0.bin", "parts/part-1.bin", "parts/part-2.bin",
                "parts/part-3.bin", "parts/part-4.bin");
    }

    @Test
    void listingContinuesAfterThePreviousPageLastObjectWasDeleted() throws IOException {
        for (String path : List.of("z.txt", "a.txt", "nested/c.txt", "b.txt")) {
            accessor.put(location, path, stream(path), path.length(), null);
        }
        FileListing first = accessor.list(location, "", null, 2);
        assertThat(first.files()).extracting(FileStat::path).containsExactly("a.txt", "b.txt");
        assertThat(first.nextPageToken()).isEqualTo("b.txt");
        accessor.delete(location, "b.txt", null);
        accessor.put(location, "aa.txt", stream("x"), 1, null);

        FileListing second = accessor.list(location, "", first.nextPageToken(), 2);
        assertThat(second.files()).extracting(FileStat::path).containsExactly("nested/c.txt", "z.txt");
        assertThat(second.nextPageToken()).isNull();
        assertThat(accessor.list(location, "", "z.txt", 2).files()).isEmpty();
    }

    @Test
    void listingReadsContentOnlyForTheRequestedPage() throws IOException {
        accessor.put(location, "a.txt", stream("a"), 1, null);
        accessor.put(location, "z.txt", stream("z"), 1, null);
        Path later = root.resolve(location.baseKey()).resolve("z.txt");
        var permissions = Files.getPosixFilePermissions(later);
        try {
            Files.setPosixFilePermissions(later, java.util.Set.of());
            FileListing first = accessor.list(location, "", null, 1);
            assertThat(first.files()).extracting(FileStat::path).containsExactly("a.txt");
            assertThat(first.nextPageToken()).isEqualTo("a.txt");
        } finally {
            Files.setPosixFilePermissions(later, permissions);
        }
        assertThat(accessor.list(location, "", "a.txt", 1).files())
                .extracting(FileStat::path).containsExactly("z.txt");
    }

    @Test
    void traversalAndSymlinksCannotEscapeTheSpaceRoot() throws IOException {
        assertThatThrownBy(() -> accessor.head(location, "../../etc/passwd"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));
        assertThatThrownBy(() -> accessor.head(location, "a/%2e%2e/b"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));

        Path outside = externalRoot.resolve("outside.txt");
        Files.writeString(outside, "secret");
        Path spaceRoot = root.resolve("volumes/media/raw/landing");
        Files.createDirectories(spaceRoot);
        Files.createSymbolicLink(spaceRoot.resolve("link.txt"), outside);

        assertThatThrownBy(() -> accessor.head(location, "link.txt"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));
    }

    @Test
    void putRejectsMoreBytesThanDeclared() {
        assertThatThrownBy(() -> accessor.put(location, "images/big.bin",
                stream("0123456789"), 4, null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_LIMIT_EXCEEDED));
    }

    @Test
    void directorySymlinkCannotReadListDeleteOrWriteAnotherSpace() throws IOException {
        Path space = Files.createDirectories(root.resolve(location.baseKey()));
        Path otherSpace = Files.createDirectories(root.resolve("other-space"));
        Path secret = Files.writeString(otherSpace.resolve("secret.txt"), "secret");
        Files.createSymbolicLink(space.resolve("linked"), otherSpace);

        assertInvalidPath(() -> accessor.head(location, "linked/secret.txt"));
        assertInvalidPath(() -> accessor.openRange(location, "linked/secret.txt", 0, -1, null));
        assertInvalidPath(() -> accessor.list(location, "linked", null, 10));
        assertInvalidPath(() -> accessor.delete(location, "linked/secret.txt", null));
        assertInvalidPath(() -> accessor.put(location, "linked/new/nested.txt", stream("x"), 1, null));
        assertThat(Files.readString(secret)).isEqualTo("secret");
        assertThat(otherSpace.resolve("new")).doesNotExist();
    }

    @Test
    void spaceRootSymlinkCannotEscapeTheProfile() throws IOException {
        Path external = externalRoot;
        Path secret = Files.writeString(external.resolve("secret.txt"), "secret");
        Path space = root.resolve(location.baseKey());
        Files.createDirectories(space.getParent());
        Files.createSymbolicLink(space, external);

        assertInvalidPath(() -> accessor.head(location, "secret.txt"));
        assertInvalidPath(() -> accessor.list(location, "", null, 10));
        assertInvalidPath(() -> accessor.put(location, "new.txt", stream("x"), 1, null));
        assertThat(Files.readString(secret)).isEqualTo("secret");
        assertThat(external.resolve("new.txt")).doesNotExist();
    }

    @Test
    void multipartStagingSymlinkCannotWriteOutsideTheProfile() throws IOException {
        Path elsewhere = Files.createDirectories(root.resolve("elsewhere"));
        Files.createSymbolicLink(root.resolve(".pangrova-multipart"), elsewhere);

        assertInvalidPath(() -> accessor.beginMultipart(location, "large.bin", null));
        try (var files = Files.list(elsewhere)) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void multipartPartSymlinkCannotReadOrOverwriteAnotherFile() throws IOException {
        var upload = accessor.beginMultipart(location, "large.bin", null);
        Path secret = Files.writeString(externalRoot.resolve("secret.txt"), "secret");
        Path part = root.resolve(".pangrova-multipart").resolve(upload.uploadId())
                .resolve("00001.part");
        Files.createSymbolicLink(part, secret);

        assertInvalidPath(() -> accessor.uploadPart(upload, 1, stream("changed"), 7));
        assertInvalidPath(() -> accessor.completeMultipart(upload,
                List.of(new UploadedPart(1, "6", 6))));
        assertThat(Files.readString(secret)).isEqualTo("secret");
        assertThat(root.resolve(location.baseKey()).resolve("large.bin")).doesNotExist();
        try (var files = Files.list(root.resolve(location.baseKey()))) {
            assertThat(files).isEmpty();
        }
    }

    private static void assertInvalidPath(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(PangrovaException.class, failure ->
                assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));
    }

    private static InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(InputStream input) throws IOException {
        try (input) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
