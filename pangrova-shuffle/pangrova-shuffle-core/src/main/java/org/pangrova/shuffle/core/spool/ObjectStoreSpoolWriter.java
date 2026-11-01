package org.pangrova.shuffle.core.spool;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.pangrova.shuffle.api.spool.SpoolWriter;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;

/**
 * Object-storage spool segment writer: buffers one producer attempt's partition output into a
 * local temp file while computing its SHA-256, then publishes it as an immutable object on
 * {@link #commit()}. Closing without committing aborts the segment.
 */
final class ObjectStoreSpoolWriter implements SpoolWriter {

    private final ShuffleObjectStorage objects;
    private final String key;
    private final Path tempFile;
    private final DigestOutputStream out;
    private boolean committed;

    ObjectStoreSpoolWriter(ShuffleObjectStorage objects, String key, Path tempDir) {
        this.objects = objects;
        this.key = key;
        try {
            Files.createDirectories(tempDir);
            this.tempFile = Files.createTempFile(tempDir, ".s3-spool-", ".arrow");
            this.out = new DigestOutputStream(
                    Files.newOutputStream(tempFile), MessageDigest.getInstance("SHA-256"));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open object-storage spool segment " + key, e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    @Override
    public void write(ByteBuffer arrowIpc) {
        try {
            ByteBuffer remaining = arrowIpc.duplicate();
            byte[] chunk = new byte[64 * 1024];
            while (remaining.hasRemaining()) {
                int count = Math.min(remaining.remaining(), chunk.length);
                remaining.get(chunk, 0, count);
                out.write(chunk, 0, count);
            }
            arrowIpc.position(arrowIpc.limit());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write object-storage spool segment " + key, e);
        }
    }

    @Override
    public void commit() {
        try {
            out.close();
            long size = Files.size(tempFile);
            byte[] sha256 = out.getMessageDigest().digest();
            try (InputStream in = Files.newInputStream(tempFile)) {
                objects.putImmutable(key, () -> in, size, sha256);
            }
            committed = true;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to commit object-storage spool segment " + key, e);
        } finally {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException cleanupFailure) {
                // Segment is already published (or failed); temp cleanup is best-effort.
            }
        }
    }

    @Override
    public void close() {
        if (!committed) {
            try {
                out.close();
            } catch (IOException ignored) {
                // Aborting: the pending segment is discarded.
            }
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException ignored) {
                // Best-effort temp cleanup.
            }
        }
    }
}
