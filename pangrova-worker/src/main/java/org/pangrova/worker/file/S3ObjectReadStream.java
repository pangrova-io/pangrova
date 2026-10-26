package org.pangrova.worker.file;

import software.amazon.awssdk.core.ResponseInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Returns completed responses to the pool, but aborts unread bodies without draining provider bytes. */
final class S3ObjectReadStream extends FilterInputStream {
    private final ResponseInputStream<?> response;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile long remaining;
    private volatile boolean failed;

    S3ObjectReadStream(ResponseInputStream<?> response, long contentLength) {
        super(response);
        this.response = response;
        this.remaining = contentLength;
    }

    @Override
    public int read() throws IOException {
        requireOpen();
        try {
            int value = in.read();
            consumed(value < 0 ? -1 : 1);
            return value;
        } catch (IOException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        requireOpen();
        try {
            int count = in.read(bytes, offset, length);
            consumed(count);
            return count;
        } catch (IOException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    @Override
    public long skip(long count) throws IOException {
        requireOpen();
        try {
            long skipped = in.skip(count);
            consumed(skipped);
            return skipped;
        } catch (IOException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    private void consumed(long count) throws IOException {
        requireOpen();
        if (count < 0) {
            if (remaining > 0) throw new IOException("S3 response ended before its declared length");
        } else {
            if (count > remaining) throw new IOException("S3 response exceeded its declared length");
            remaining -= count;
        }
    }

    private void requireOpen() throws IOException {
        if (closed.get()) throw new IOException("S3 object read is closed");
    }

    @Override public boolean markSupported() { return false; }
    @Override public void mark(int readLimit) { }
    @Override public void reset() throws IOException { throw new IOException("S3 object read cannot reset"); }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;
        if (!failed && remaining == 0) {
            response.close();
        } else {
            response.abort();
        }
    }
}
