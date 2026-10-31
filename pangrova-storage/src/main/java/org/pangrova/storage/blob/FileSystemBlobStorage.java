package org.pangrova.storage.blob;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Filesystem storage backend for deterministic tests of the governed FILE contract. */
public final class FileSystemBlobStorage implements BlobStorage {

    private static final int BUFFER_BYTES = 64 * 1024;

    private final Path root;

    /** Creates a test backend rooted at the given directory. */
    public FileSystemBlobStorage(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        this.root = root.toAbsolutePath().normalize();
        try {
            Files.createDirectories(this.root);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed to initialize FILE test storage", failure);
        }
    }

    @Override
    public void publish(
            String tenantScope, String checksum, long size, Path stagedContent)
            throws IOException {
        Path target = objectPath(tenantScope, checksum);
        Path parent = target.getParent();
        if (parent == null) {
            throw new IOException("Published FILE path requires a parent directory");
        }
        Files.createDirectories(parent);
        try {
            Files.createLink(target, stagedContent);
        } catch (FileAlreadyExistsException race) {
            // A concurrent publisher won. Integrity is verified below.
        } catch (UnsupportedOperationException failure) {
            throw new IOException("Filesystem does not support atomic FILE publication", failure);
        }
        verify(target, checksum, size);
    }

    @Override
    public InputStream open(String tenantScope, String checksum, long size) throws IOException {
        Path target = objectPath(tenantScope, checksum);
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Governed FILE object is missing or is not a regular file");
        }
        return Files.newInputStream(target, StandardOpenOption.READ);
    }

    private void verify(Path target, String expectedChecksum, long expectedSize)
            throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Governed FILE object is missing or is not a regular file");
        }
        MessageDigest digest = sha256();
        long size = 0;
        try (InputStream input = Files.newInputStream(target, StandardOpenOption.READ)) {
            byte[] buffer = new byte[BUFFER_BYTES];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                if (count > 0) {
                    size = Math.addExact(size, count);
                    digest.update(buffer, 0, count);
                }
            }
        }
        String checksum = HexFormat.of().formatHex(digest.digest());
        if (size != expectedSize || !checksum.equals(expectedChecksum)) {
            throw new IOException("Existing content-addressed object failed integrity validation");
        }
    }

    private Path objectPath(String tenantScope, String checksum) {
        return root.resolve(tenantScope).resolve(checksum.substring(0, 2))
                .resolve(checksum + ".blob");
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
