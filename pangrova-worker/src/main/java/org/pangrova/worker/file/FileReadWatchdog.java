package org.pangrova.worker.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;

/** Owns opened read streams and their absolute-expiry alarms for one file endpoint. */
@ThreadSafe
final class FileReadWatchdog implements AutoCloseable {
    private final ScheduledExecutorService alarms;
    private final Set<FileTransferDeadline> reads = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
    private final CompletableFuture<Void> closeComplete = new CompletableFuture<>();

    FileReadWatchdog() {
        this(ManagedExecutorFactory.newSingleThreadScheduledExecutor("file-read-expiry"));
    }

    /** Takes ownership of the supplied scheduler, including its shutdown. */
    FileReadWatchdog(ScheduledExecutorService alarms) {
        this.alarms = java.util.Objects.requireNonNull(alarms, "alarms");
    }

    /** Takes ownership of the stream even when registration or alarm admission fails. */
    InputStream open(InputStream input, long deadline, LongSupplier clock) throws IOException {
        var read = new FileTransferDeadline(input, deadline, clock, this);
        reads.add(read);
        try {
            requireOpen();
            read.arm(alarms);
            requireOpen();
            return read;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                read.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                if (closeFailure != failure) failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    void released(FileTransferDeadline read, Throwable failure) {
        if (failure != null) rememberFailure(failure);
        reads.remove(read);
    }

    private void requireOpen() throws IOException {
        if (stopped.get()) throw new IOException("File read endpoint is closed");
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            try {
                retire();
                closeComplete.complete(null);
            } catch (RuntimeException | Error failure) {
                closeComplete.completeExceptionally(failure);
            }
        }
        try {
            closeComplete.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw new IllegalStateException("File read cleanup failed", failure.getCause());
        }
    }

    private void retire() {
        try {
            alarms.shutdownNow();
        } catch (RuntimeException failure) {
            rememberFailure(failure);
        }
        for (var read : reads) {
            try {
                read.close();
            } catch (IOException | RuntimeException | Error failure) {
                rememberFailure(failure);
            }
        }
        Throwable failure = cleanupFailure.get();
        if (failure instanceof Error error) throw error;
        if (failure != null) throw new IllegalStateException("File read cleanup failed", failure);
    }

    private void rememberFailure(Throwable failure) {
        cleanupFailure.updateAndGet(previous -> {
            if (previous == null || (failure instanceof Error && !(previous instanceof Error))) return failure;
            return previous;
        });
    }
}
