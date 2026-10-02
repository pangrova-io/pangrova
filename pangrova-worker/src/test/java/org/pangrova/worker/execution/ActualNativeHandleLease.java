package org.pangrova.worker.execution;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Actual handle-operation lease used to assert release exclusion, not task execution. */
public final class ActualNativeHandleLease implements AutoCloseable {
    private final ReentrantReadWriteLock lock;
    private boolean held = true;

    public ActualNativeHandleLease(NativeTaskExecutor executor, long handle) {
        lock = executor.acquireHandleReadLock(handle);
    }

    public boolean blocks(Thread releaseThread) { return lock.hasQueuedThread(releaseThread); }

    @Override
    public void close() {
        if (!held) return;
        lock.readLock().unlock();
        held = false;
    }
}
