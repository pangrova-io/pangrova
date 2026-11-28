package org.pangrova.worker.file;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.pangrova.common.concurrency.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single-reader stream; cancellation/close may race with that reader without extending its deadline. */
@NotThreadSafe
final class FileTransferDeadline extends FilterInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FileTransferDeadline.class);
    private final long deadline;
    private final LongSupplier clock;
    private final FileReadWatchdog owner;
    private final AtomicBoolean expired = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicReference<ScheduledFuture<?>> alarm = new AtomicReference<>();
    private final CompletableFuture<Void> closeComplete = new CompletableFuture<>();

    FileTransferDeadline(InputStream input, long deadline, LongSupplier clock, FileReadWatchdog owner) {
        super(java.util.Objects.requireNonNull(input, "input"));
        this.deadline = deadline;
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
        this.owner = java.util.Objects.requireNonNull(owner, "owner");
    }

    void arm(ScheduledExecutorService scheduler) throws IOException {
        requireActive();
        long remaining = deadline - clock.getAsLong();
        var scheduled = scheduler.schedule(this::expire, Math.max(0, remaining), TimeUnit.MILLISECONDS);
        alarm.set(scheduled);
        // Expiry or endpoint shutdown can close this stream before schedule returns.
        if (closing.get()) scheduled.cancel(false);
        requireActive();
    }

    private void expire() {
        expired.set(true);
        try {
            close();
        } catch (IOException | RuntimeException failure) {
            // The owner also retains this failure and fails its terminal close.
            LOG.warn("event=file_read_expiry_cleanup_failed failureType={}", failure.getClass().getName());
        }
    }

    @Override
    public int read() throws IOException {
        requireActive();
        int value;
        try {
            value = in.read();
        } catch (IOException failure) {
            throw readFailure(failure);
        }
        requireActive();
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        requireActive();
        int count;
        try {
            count = in.read(bytes, offset, length);
        } catch (IOException failure) {
            throw readFailure(failure);
        }
        requireActive();
        return count;
    }

    @Override
    public long skip(long count) throws IOException {
        requireActive();
        long skipped;
        try {
            skipped = in.skip(count);
        } catch (IOException failure) {
            throw readFailure(failure);
        }
        requireActive();
        return skipped;
    }

    @Override
    public int available() throws IOException {
        requireActive();
        int count = in.available();
        requireActive();
        return count;
    }

    private IOException readFailure(IOException failure) {
        return expired.get() ? new IOException("File delivery authorization expired", failure) : failure;
    }

    private void requireActive() throws IOException {
        if (expired.get() || clock.getAsLong() >= deadline) {
            expired.set(true);
            var denied = new IOException("File delivery authorization expired");
            try {
                close();
            } catch (IOException closeFailure) {
                denied.addSuppressed(closeFailure);
            }
            throw denied;
        }
        if (closing.get()) throw new IOException("File read is closed");
    }

    @Override
    public void close() throws IOException {
        if (closing.compareAndSet(false, true)) {
            var scheduled = alarm.getAndSet(null);
            if (scheduled != null) scheduled.cancel(false);
            Throwable failure = null;
            try {
                in.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                failure = closeFailure;
            }
            owner.released(this, failure);
            if (failure == null) closeComplete.complete(null);
            else closeComplete.completeExceptionally(failure);
        }
        try {
            closeComplete.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof IOException cause) throw cause;
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw new IOException("File read cleanup failed", failure.getCause());
        }
    }
}
