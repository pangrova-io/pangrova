package org.pangrova.worker.execution;

/** Actual handle-operation lease used to assert release exclusion, not task execution. */
public final class ActualNativeHandleLease implements AutoCloseable {
    private final NativeOperationLifetime.Lease lease;

    public ActualNativeHandleLease(NativeTaskExecutor executor, long handle) {
        lease = executor.acquireHandleLease(handle);
    }

    public boolean blocks(Thread releaseThread) { return lease.blocks(releaseThread); }

    @Override
    public void close() {
        lease.close();
    }
}
