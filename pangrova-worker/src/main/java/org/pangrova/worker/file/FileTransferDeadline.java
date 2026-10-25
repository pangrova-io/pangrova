package org.pangrova.worker.file;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.LongSupplier;

/** Enforces the admitted delivery deadline without extending it when an operation begins. */
final class FileTransferDeadline extends FilterInputStream {
    private final long deadline;
    private final LongSupplier clock;

    FileTransferDeadline(InputStream input, long deadline, LongSupplier clock) {
        super(input);
        this.deadline = deadline;
        this.clock = clock;
    }

    @Override
    public int read() throws IOException {
        requireActive();
        int value = in.read();
        requireActive();
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        requireActive();
        int count = in.read(bytes, offset, length);
        requireActive();
        return count;
    }

    @Override
    public long skip(long count) throws IOException {
        requireActive();
        long skipped = in.skip(count);
        requireActive();
        return skipped;
    }

    @Override
    public int available() throws IOException {
        requireActive();
        return in.available();
    }

    private void requireActive() throws IOException {
        if (clock.getAsLong() >= deadline) {
            var expired = new IOException("File delivery authorization expired");
            try {
                close();
            } catch (IOException closeFailure) {
                expired.addSuppressed(closeFailure);
            }
            throw expired;
        }
    }
}
