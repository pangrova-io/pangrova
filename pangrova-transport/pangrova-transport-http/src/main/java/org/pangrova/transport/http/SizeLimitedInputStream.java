package org.pangrova.transport.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/** Counts a request body and distinguishes an exact-length EOF from overflow. */
public final class SizeLimitedInputStream extends InputStream {

    private final InputStream delegate;
    private final int maxBytes;
    private final IntFunction<? extends RuntimeException> overflow;
    private final IntConsumer observedBytes;
    private int bytesRead;
    private boolean exceeded;

    /** Wraps a request body and supplies its transport-specific overflow exception. */
    public SizeLimitedInputStream(
            InputStream delegate, int maxBytes, IntFunction<? extends RuntimeException> overflow) {
        this(delegate, maxBytes, overflow, ignored -> {});
    }

    /** Calls the byte observer before delivering each newly read range to the parser. */
    public SizeLimitedInputStream(InputStream delegate, int maxBytes,
            IntFunction<? extends RuntimeException> overflow, IntConsumer observedBytes) {
        this.delegate = requireNonNull(delegate, "delegate");
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative");
        }
        this.maxBytes = maxBytes;
        this.overflow = requireNonNull(overflow, "overflow");
        this.observedBytes = requireNonNull(observedBytes, "observedBytes");
    }

    /** Number of body bytes passed through to the reader. */
    public int bytesRead() {
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        if (exceeded) {
            throw overflow.apply(maxBytes);
        }
        if (bytesRead >= maxBytes) {
            if (delegate.read() == -1) return -1;
            exceeded = true;
            throw overflow.apply(maxBytes);
        }
        int b = delegate.read();
        if (b != -1) {
            observedBytes.accept(++bytesRead);
        }
        return b;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        checkFromIndexSize(off, len, buf.length);
        if (len == 0) return 0;
        if (exceeded) {
            throw overflow.apply(maxBytes);
        }
        if (bytesRead >= maxBytes) {
            if (delegate.read() == -1) return -1;
            exceeded = true;
            throw overflow.apply(maxBytes);
        }
        int allowedLen = Math.min(len, maxBytes - bytesRead);
        int n = delegate.read(buf, off, allowedLen);
        if (n > 0) {
            bytesRead += n;
            observedBytes.accept(bytesRead);
        }
        return n;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

}
