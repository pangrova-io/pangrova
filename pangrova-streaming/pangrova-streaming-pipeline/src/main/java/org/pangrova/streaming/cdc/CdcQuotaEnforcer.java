package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrency.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces CDC quota: pauses consumer when limits exceeded.
 *
 * <p>Thread-safe via atomic fields. Note: {@link #shouldPause()} reads two
 * atomics non-atomically, which is acceptable for a heuristic backpressure check.</p>
 */
@ThreadSafe
public class CdcQuotaEnforcer {

    private final int maxConcurrentWrites;
    private final long maxPendingBytes;
    private final AtomicInteger activeWrites = new AtomicInteger(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);

    public CdcQuotaEnforcer(int maxConcurrentWrites, long maxPendingBytes) {
        this.maxConcurrentWrites = maxConcurrentWrites;
        this.maxPendingBytes = maxPendingBytes;
    }

    public boolean shouldPause() {
        return activeWrites.get() >= maxConcurrentWrites
            || pendingBytes.get() >= maxPendingBytes;
    }

    public void onWriteStart(long bytes) {
        activeWrites.incrementAndGet();
        pendingBytes.addAndGet(bytes);
    }

    public void onWriteComplete(long bytes) {
        activeWrites.decrementAndGet();
        pendingBytes.addAndGet(-bytes);
    }

    public int activeWrites() {
        return activeWrites.get();
    }

    public long pendingBytes() {
        return pendingBytes.get();
    }
}
