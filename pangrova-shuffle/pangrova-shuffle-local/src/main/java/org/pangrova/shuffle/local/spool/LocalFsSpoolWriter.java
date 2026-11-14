package org.pangrova.shuffle.local.spool;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.LinkOption;

import org.pangrova.shuffle.api.spool.SpoolWriter;

/** One exclusive attempt. Completion publishes a complete framed segment, never a whole exchange. */
final class LocalFsSpoolWriter implements SpoolWriter {
    private final LocalFsSpoolStore owner;
    private final LocalSpoolPaths.Segment segment;
    private Path temporary;
    private FileChannel channel;
    private DataOutputStream output;
    private long batches;
    private long bytes;
    private boolean committed;
    private boolean failed;
    private boolean closed;

    LocalFsSpoolWriter(LocalFsSpoolStore owner, LocalSpoolPaths.Segment segment) {
        this.owner = owner;
        this.segment = segment;
    }

    void initialize() throws IOException {
        temporary = owner.paths.temporary(segment);
        channel = FileChannel.open(temporary, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        owner.paths.requireCurrent(segment);
        output = new DataOutputStream(Channels.newOutputStream(channel));
        output.writeInt(LocalSpoolFormat.MAGIC);
        output.writeInt(LocalSpoolFormat.VERSION);
    }

    @Override
    public void write(ByteBuffer batch) {
        java.util.Objects.requireNonNull(batch, "batch");
        synchronized (owner.monitor) {
            requireWritable();
            int length = batch.remaining();
            if (length <= 0 || length > LocalSpoolFormat.MAX_BATCH_BYTES) {
                throw new IllegalArgumentException(
                        "Encoded batch size must be in [1, " + LocalSpoolFormat.MAX_BATCH_BYTES + "]");
            }
            long newBytes = Math.addExact(bytes, length);
            long newBatches = Math.addExact(batches, 1);
            byte[] digest = LocalSpoolFormat.digest(batch);
            try {
                output.writeInt(length);
                output.write(digest);
                while (batch.hasRemaining()) channel.write(batch);
                bytes = newBytes;
                batches = newBatches;
            } catch (IOException failure) {
                failed = true;
                throw new UncheckedIOException("Cannot write local spool batch", failure);
            }
        }
    }

    @Override
    public void commit() {
        synchronized (owner.monitor) {
            if (committed) return;
            requireWritable();
            try {
                output.writeInt(LocalSpoolFormat.END);
                output.writeLong(batches);
                output.writeLong(bytes);
                channel.force(true);
                channel.close();
                owner.paths.requireCurrent(segment);
                if (Files.exists(segment.file(), LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Completed spool attempt must not be overwritten");
                }
                // No fallback to a non-atomic move. The directory has one locked owner.
                Files.move(temporary, segment.file(), StandardCopyOption.ATOMIC_MOVE);
                committed = true;
            } catch (IOException failure) {
                failed = true;
                throw new UncheckedIOException(
                        "Spool completion failed; outcome must be confirmed before publication", failure);
            }
        }
    }

    @Override
    public void close() {
        synchronized (owner.monitor) {
            if (closed) return;
            IOException failure = null;
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure = closeFailure;
                }
            }
            if (!committed && temporary != null) {
                try {
                    owner.paths.requireCurrent(segment);
                    Files.deleteIfExists(temporary);
                } catch (IOException deleteFailure) {
                    if (failure == null) failure = deleteFailure;
                    else failure.addSuppressed(deleteFailure);
                }
            }
            if (failure != null) throw new UncheckedIOException("Cannot close local spool writer", failure);
            closed = true;
            owner.release(this, segment.file());
        }
    }

    private void requireWritable() {
        owner.requireOpen();
        if (closed || committed || failed) {
            throw new IllegalStateException("Spool writer is closed, completed or failed");
        }
    }
}
