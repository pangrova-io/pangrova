package org.pangrova.filesystem.local.blob;

import org.pangrova.filesystem.api.blob.BlobObjectId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemBlobStorageTest {
    private static final String SCOPE = "a".repeat(64);
    @TempDir Path directory;

    @Test
    void publishedContentDoesNotAliasCallerStagingFile() throws Exception {
        Path draft = Files.write(directory.resolve("draft"), new byte[] {1, 2, 3});
        String hash = checksum(draft);
        var storage = new FileSystemBlobStorage(directory.resolve("objects"));
        storage.publish(new BlobObjectId(SCOPE, hash, 3), draft);
        Files.write(draft, new byte[] {4, 5, 6});
        try (var input = storage.open(new BlobObjectId(SCOPE, hash, 3))) {
            assertThat(input.readAllBytes()).containsExactly(1, 2, 3);
        }
    }

    @Test
    void invalidDraftCannotPoisonPublishedNamespace() throws Exception {
        Path draft = Files.write(directory.resolve("draft"), new byte[] {1, 2, 3});
        String expectedHash = "b".repeat(64);
        var storage = new FileSystemBlobStorage(directory.resolve("objects"));
        assertThatThrownBy(() -> storage.publish(new BlobObjectId(SCOPE, expectedHash, 3), draft))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> storage.open(new BlobObjectId(SCOPE, expectedHash, 3)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void readsRejectWrongLengthAndSameLengthCorruption() throws Exception {
        Path draft = Files.write(directory.resolve("draft"), new byte[] {1, 2, 3});
        String hash = checksum(draft);
        Path root = directory.resolve("objects");
        var storage = new FileSystemBlobStorage(root);
        storage.publish(new BlobObjectId(SCOPE, hash, 3), draft);
        assertThatThrownBy(() -> storage.open(new BlobObjectId(SCOPE, hash, 4))).isInstanceOf(IOException.class);
        Path object;
        try (var files = Files.walk(root)) {
            object = files.filter(Files::isRegularFile).findFirst().orElseThrow();
        }
        Files.write(object, new byte[] {7, 8, 9});
        assertThatThrownBy(() -> storage.open(new BlobObjectId(SCOPE, hash, 3))).isInstanceOf(IOException.class);
    }

    @Test
    void tenantScopeCannotEscapeConfiguredRoot() throws Exception {
        Path draft = Files.write(directory.resolve("draft"), new byte[] {1});
        var storage = new FileSystemBlobStorage(directory.resolve("objects"));
        assertThatThrownBy(() -> storage.publish(new BlobObjectId("../outside", checksum(draft), 1), draft))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(directory.resolve("outside")).doesNotExist();
    }

    @Test
    void tenantDirectoryCannotBeASymbolicLink() throws Exception {
        Path outside = Files.createDirectory(directory.resolve("outside"));
        Path root = Files.createDirectory(directory.resolve("objects"));
        Files.createSymbolicLink(root.resolve(SCOPE), outside);
        Path draft = Files.write(directory.resolve("draft"), new byte[] {1});
        var storage = new FileSystemBlobStorage(root);
        assertThatThrownBy(() -> storage.publish(new BlobObjectId(SCOPE, checksum(draft), 1), draft))
                .isInstanceOf(IOException.class);
        try (var files = Files.list(outside)) { assertThat(files).isEmpty(); }
    }

    @Test
    void emptyPublicationAndDuplicatePublicationRemainReadable() throws Exception {
        Path draft = Files.write(directory.resolve("empty"), new byte[0]);
        var id = new BlobObjectId(SCOPE, checksum(draft), 0);
        var first = new FileSystemBlobStorage(directory.resolve("objects"));
        var second = new FileSystemBlobStorage(directory.resolve("objects"));
        first.publish(id, draft);
        second.publish(id, draft);
        try (var input = second.open(id)) { assertThat(input.read()).isEqualTo(-1); }
    }

    @Test
    void concurrentProvidersPublishOneVerifiedObjectWithoutKeepingDrafts() throws Exception {
        byte[] content = new byte[131_072];
        java.util.Arrays.fill(content, (byte) 7);
        Path draft = Files.write(directory.resolve("concurrent"), content);
        var id = new BlobObjectId(SCOPE, checksum(draft), content.length);
        Path objects = directory.resolve("objects");
        var first = new FileSystemBlobStorage(objects);
        var second = new FileSystemBlobStorage(objects);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            var one = pool.submit(() -> { start.await(); first.publish(id, draft); return null; });
            var two = pool.submit(() -> { start.await(); second.publish(id, draft); return null; });
            start.countDown();
            one.get(5, java.util.concurrent.TimeUnit.SECONDS);
            two.get(5, java.util.concurrent.TimeUnit.SECONDS);
            try (var input = first.open(id)) { assertThat(input.readAllBytes()).isEqualTo(content); }
            try (var files = Files.walk(objects)) { assertThat(files.filter(Files::isRegularFile)).hasSize(1); }
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void stagingSymlinksAreRejectedWithoutFollowingTheirTarget() throws Exception {
        Path content = Files.write(directory.resolve("content"), new byte[] {1});
        Path symlink = Files.createSymbolicLink(directory.resolve("symlink"), content);
        var storage = new FileSystemBlobStorage(directory.resolve("objects"));
        assertThatThrownBy(() -> storage.publish(new BlobObjectId(SCOPE, checksum(content), 1), symlink))
                .isInstanceOf(IOException.class);
    }

    private static String checksum(Path path) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(Files.readAllBytes(path)));
    }
}
