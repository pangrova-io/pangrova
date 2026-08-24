package org.pangrova.coordinator.dynamictable.quota;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded memory quota backed by an {@link AtomicLong} counter.
 *
 * <p>Concurrency strategy: CAS loop on {@link AtomicLong} for
 * lock-free allocation and release.</p>
 */
@ThreadSafe
public final class BoundedRefreshMemoryQuota implements RefreshMemoryQuota {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedRefreshMemoryQuota.class);

    private final long quotaBytes;
    private final AtomicLong used = new AtomicLong(0);

    public BoundedRefreshMemoryQuota(long quotaBytes) {
        if (quotaBytes <= 0) {
            throw new IllegalArgumentException("quotaBytes must be positive");
        }
        this.quotaBytes = quotaBytes;
    }

    @Override
    public void allocate(long bytes) {
        while (true) {
            long current = used.get();
            long next = current + bytes;
            if (next > quotaBytes) {
                LOG.error("event=dt_refresh_oom requested={} used={} quota={}",
                        bytes, current, quotaBytes);
                throw new RefreshMemoryQuotaExceededException(bytes, quotaBytes - current);
            }
            if (used.compareAndSet(current, next)) {
                return;
            }
        }
    }

    @Override
    public void release(long bytes) {
        long result = used.addAndGet(-bytes);
        if (result < 0) {
            LOG.warn("event=dt_refresh_quota_underflow released={} resultUsed={}", bytes, result);
            used.set(0);
        }
    }

    @Override
    public long usedBytes() {
        return used.get();
    }

    @Override
    public long totalQuotaBytes() {
        return quotaBytes;
    }
}
