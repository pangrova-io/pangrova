package org.pangrova.storage.api.blob;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.pangrova.common.concurrent.NotThreadSafe;

/** Caller-owned managed read whose abort releases transport without draining rejected content. */
@NotThreadSafe
public final class BlobInputStream extends FilterInputStream {
    private final Runnable abort;
    private boolean ended;

    /** Owns the supplied body and its mutually exclusive close/abort actions. */
    public BlobInputStream(InputStream body, Runnable abort) {
        super(Objects.requireNonNull(body, "body"));
        this.abort = Objects.requireNonNull(abort, "abort");
    }

    /** Aborts a failed read; a subsequent close must not drain the aborted body. */
    public void abort() {
        if (!ended) {
            ended = true;
            abort.run();
        }
    }

    @Override
    public void close() throws IOException {
        if (!ended) {
            ended = true;
            super.close();
        }
    }
}
