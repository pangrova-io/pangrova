package org.pangrova.shuffle.storage.local;

import org.pangrova.shuffle.api.spool.SpoolWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Writes one producer attempt's output for a single partition to the local filesystem.
 *
 * <p>Data is written to a {@code .tmp} file first, then atomically moved to its final path on
 * {@link #commit()}, ensuring readers never see partially-written segments. Closing without
 * committing deletes the temp file (abort semantics).
 */
final class LocalFsSpoolWriter implements SpoolWriter {

    private final Path tmpPath;
    private final Path finalPath;
    private FileChannel channel;
    private boolean committed;

    LocalFsSpoolWriter(Path finalPath) {
        this.finalPath = finalPath;
        this.tmpPath = finalPath.resolveSibling(finalPath.getFileName() + ".tmp");
        try {
            Path parent = finalPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.channel = FileChannel.open(tmpPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open spool writer at " + tmpPath, e);
        }
    }

    @Override
    public void write(ByteBuffer arrowIpc) {
        if (committed) {
            throw new IllegalStateException("Writer already committed");
        }
        if (channel == null) {
            throw new IllegalStateException("Writer already closed");
        }
        try {
            while (arrowIpc.hasRemaining()) {
                channel.write(arrowIpc);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write to " + tmpPath, e);
        }
    }

    @Override
    public void commit() {
        if (committed) {
            return;
        }
        if (channel == null) {
            throw new IllegalStateException("Writer already closed without commit");
        }
        try {
            channel.force(true);
            channel.close();
            channel = null;
            Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
            committed = true;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to commit spool file " + finalPath, e);
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                // best-effort
            }
            channel = null;
        }
        // If not committed, clean up the temp file (abort)
        if (!committed) {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (IOException e) {
                // best-effort cleanup
            }
        }
    }
}
