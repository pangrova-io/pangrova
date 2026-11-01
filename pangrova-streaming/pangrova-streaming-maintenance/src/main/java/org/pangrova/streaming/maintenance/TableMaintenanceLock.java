package org.pangrova.streaming.maintenance;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Per-table maintenance guard shared by the scheduler and CDC writers.
 *
 * <p>Implemented as a semaphore rather than a {@link java.util.concurrent.locks.ReentrantLock}
 * because the scheduler releases it from an asynchronous completion callback, which
 * may run on a different thread than the one that acquired it.</p>
 */
public final class TableMaintenanceLock {

    private final Semaphore permit = new Semaphore(1);

    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        return permit.tryAcquire(timeout, unit);
    }

    public void unlock() {
        permit.release();
    }
}
