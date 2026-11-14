package org.pangrova.filesystem.local.blob;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.filesystem.api.blob.BlobObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

import org.pangrova.filesystem.api.blob.BlobStorage;

/**
 * Filesystem content publication with private copies and atomic no-replace linking.
 * The configured directory and ancestors must be administrator-controlled. Symbolic links at
 * or below the root are rejected; concurrent external namespace edits are outside this boundary.
 * Workers sharing governed references must use the same persistent filesystem namespace.
 */
public final class FileSystemBlobStorage implements BlobStorage {
    private static final int BUFFER_BYTES = 64 * 1024;
    private final Path root;

    /** Opens a persistent administrator-controlled publication root. */
    public FileSystemBlobStorage(Path root) {
        Path requested = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        try {
            requireDirectory(requested, true);
            this.root = requested.toRealPath();
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot initialize filesystem blob storage", failure);
        }
    }

    @Override
    public void publish(BlobObjectId object, Path stagedContent) throws IOException {
        Objects.requireNonNull(object, "object");
        Path target = objectPath(object, true);
        Path parent = target.getParent();
        if (parent == null) throw new IOException("Blob publication requires a parent directory");
        requireRegularFile(stagedContent);
        try (var draft = new DraftFile(Files.createTempFile(parent, ".publish-", ".part"))) {
            copyAndVerify(stagedContent, draft.path(), object);
            try {
                Files.createLink(target, draft.path());
                forceDirectory(parent);
            } catch (FileAlreadyExistsException race) {
                // A concurrent publisher may win, but its bytes must satisfy the same identity.
            } catch (UnsupportedOperationException failure) {
                throw new IOException("Filesystem does not support atomic blob publication", failure);
            }
            try (var published = openVerified(target, object)) {
                // Verification uses the actual opened handle, including when another publisher won.
            }
        }
    }

    @Override
    public InputStream open(BlobObjectId object) throws IOException {
        Objects.requireNonNull(object, "object");
        return Channels.newInputStream(openVerified(objectPath(object, false), object));
    }

    private Path objectPath(BlobObjectId object, boolean create) throws IOException {
        requireDirectory(root, false);
        Path tenant = root.resolve(object.tenantScope());
        requireDirectory(tenant, create);
        Path shard = tenant.resolve(object.checksum().substring(0, 2));
        requireDirectory(shard, create);
        return shard.resolve(object.checksum() + ".blob");
    }

    private static void requireDirectory(Path path, boolean create) throws IOException {
        if (create) {
            if (Files.isSymbolicLink(path)) throw new IOException("Blob directory is a symbolic link");
            var missing = new java.util.ArrayList<Path>();
            for (Path entry = path; entry != null && !Files.exists(entry, LinkOption.NOFOLLOW_LINKS);
                    entry = entry.getParent()) {
                missing.add(entry);
            }
            Files.createDirectories(path);
            for (Path entry : missing) {
                forceDirectory(entry);
                Path parent = entry.getParent();
                if (parent != null) forceDirectory(parent);
            }
        }
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Blob directory is missing or is not a real directory");
        }
    }

    private static void requireRegularFile(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Blob content is missing or is not a regular file");
        }
    }

    private static FileChannel openVerified(Path path, BlobObjectId object) throws IOException {
        requireRegularFile(path);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        try {
            if (channel.size() != object.size()) throw new IOException("Blob length differs from its identity");
            MessageDigest digest = DigestUtils.sha256();
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_BYTES);
            while (channel.read(buffer) != -1) {
                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }
            requireChecksum(digest, object);
            channel.position(0);
            return channel;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                channel.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static void copyAndVerify(Path source, Path draft, BlobObjectId object) throws IOException {
        MessageDigest digest = DigestUtils.sha256();
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
             FileChannel output = FileChannel.open(draft, StandardOpenOption.WRITE)) {
            if (input.size() != object.size()) throw new IOException("Staged blob length differs from its identity");
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_BYTES);
            long copied = 0;
            int read;
            while ((read = input.read(buffer)) != -1) {
                if (read == 0) continue;
                copied = Math.addExact(copied, read);
                if (copied > object.size()) throw new IOException("Staged blob exceeds its identity length");
                buffer.flip();
                digest.update(buffer.asReadOnlyBuffer());
                while (buffer.hasRemaining()) output.write(buffer);
                buffer.clear();
            }
            if (copied != object.size()) throw new IOException("Staged blob ended before its identity length");
            requireChecksum(digest, object);
            output.force(true);
        }
    }

    private static void requireChecksum(MessageDigest digest, BlobObjectId object) throws IOException {
        if (!object.checksum().equals(HexFormat.of().formatHex(digest.digest()))) {
            throw new IOException("Blob content failed SHA-256 integrity verification");
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private record DraftFile(Path path) implements AutoCloseable {
        @Override public void close() throws IOException { Files.deleteIfExists(path); }
    }
}
