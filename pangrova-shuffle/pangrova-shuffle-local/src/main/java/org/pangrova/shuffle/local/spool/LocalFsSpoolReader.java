package org.pangrova.shuffle.local.spool;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.LinkOption;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

import org.pangrova.shuffle.spi.spool.SpoolReader;

/** Reads only the owner's fixed attempt selection, one bounded verified batch at a time. */
final class LocalFsSpoolReader implements SpoolReader {
    private final LocalFsSpoolStore owner;
    private final Deque<FileChannel> segments = new ArrayDeque<>();
    private DataInputStream input;
    private int nextLength = -1;
    private long batches;
    private long bytes;
    private boolean closed;
    private RuntimeException failed;

    LocalFsSpoolReader(LocalFsSpoolStore owner) {
        this.owner = owner;
    }

    void pin(LocalSpoolPaths.Segment segment) throws IOException {
        FileChannel channel = FileChannel.open(segment.file(), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        segments.addLast(channel);
        owner.paths.requireCurrent(segment);
    }

    @Override
    public boolean hasNext() {
        synchronized (owner.monitor) {
            requireReadable();
            try {
                while (true) {
                    if (nextLength > 0) return true;
                    if (input == null) {
                        if (segments.isEmpty()) return false;
                        input = new DataInputStream(new BufferedInputStream(
                                Channels.newInputStream(segments.getFirst())));
                        segments.removeFirst();
                        if (input.readInt() != LocalSpoolFormat.MAGIC || input.readInt() != LocalSpoolFormat.VERSION) {
                            throw new IOException("Unsupported local spool format");
                        }
                        batches = 0;
                        bytes = 0;
                    }
                    int length = input.readInt();
                    if (length == LocalSpoolFormat.END) {
                        if (input.readLong() != batches || input.readLong() != bytes || input.read() != -1) {
                            throw new IOException("Invalid local spool completion trailer");
                        }
                        input.close();
                        input = null;
                    } else {
                        if (length <= 0 || length > LocalSpoolFormat.MAX_BATCH_BYTES) {
                            throw new IOException("Invalid local spool batch size: " + length);
                        }
                        nextLength = length;
                        return true;
                    }
                }
            } catch (IOException failure) {
                failed = new UncheckedIOException("Corrupt or incomplete selected spool segment", failure);
                throw failed;
            }
        }
    }

    @Override
    public ByteBuffer next() {
        synchronized (owner.monitor) {
            if (!hasNext()) throw new NoSuchElementException("No more selected spool batches");
            try {
                byte[] expected = new byte[LocalSpoolFormat.DIGEST_BYTES];
                input.readFully(expected);
                byte[] batch = new byte[nextLength];
                input.readFully(batch);
                if (!MessageDigest.isEqual(expected, LocalSpoolFormat.digest(ByteBuffer.wrap(batch)))) {
                    throw new IOException("Local spool batch checksum mismatch");
                }
                batches = Math.addExact(batches, 1);
                bytes = Math.addExact(bytes, nextLength);
                nextLength = -1;
                return ByteBuffer.wrap(batch).asReadOnlyBuffer();
            } catch (IOException failure) {
                failed = new UncheckedIOException("Corrupt or incomplete selected spool batch", failure);
                throw failed;
            }
        }
    }

    @Override
    public void close() {
        synchronized (owner.monitor) {
            if (closed) return;
            IOException failure = null;
            if (input != null) {
                try {
                    input.close();
                    input = null;
                } catch (IOException closeFailure) {
                    failure = closeFailure;
                }
            }
            for (var iterator = segments.iterator(); iterator.hasNext();) {
                try {
                    iterator.next().close();
                    iterator.remove();
                } catch (IOException closeFailure) {
                    if (failure == null) failure = closeFailure;
                    else failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) throw new UncheckedIOException("Cannot close spool reader", failure);
            closed = true;
            owner.release(this, null);
        }
    }

    private void requireReadable() {
        owner.requireOpen();
        if (closed) throw new IllegalStateException("Spool reader is closed");
        if (failed != null) throw failed;
    }
}
