package org.pangrova.worker.dataset;

import org.pangrova.common.concurrent.ThreadSafe;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bounds synchronous metadata commands and drains them before Native shutdown. */
@ThreadSafe
final class NativeDatasetControlAdmission implements AutoCloseable {
    static final int MAX_CONCURRENT_COMMANDS = 8;
    private static final int DRAIN_SECONDS = 30;
    private final Semaphore permits = new Semaphore(MAX_CONCURRENT_COMMANDS);
    private final AtomicBoolean closed = new AtomicBoolean();

    Lease acquire() {
        if (closed.get()) throw new IllegalStateException("Native dataset control is closed");
        if (!permits.tryAcquire()) throw new ControlAdmissionRejectedException();
        if (closed.get()) {
            permits.release();
            throw new IllegalStateException("Native dataset control is closed");
        }
        return new Lease();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try {
            if (!permits.tryAcquire(MAX_CONCURRENT_COMMANDS, DRAIN_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Native dataset control failed to drain before shutdown");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted draining Native dataset control", interrupted);
        }
    }

    final class Lease implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) permits.release();
        }
    }
}
