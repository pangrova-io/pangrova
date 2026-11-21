package org.pangrova.worker.copy;

/** One admitted task's requested bytes, consumed before a managed GET enters the backend. */
public final class CopyTaskReadBudget {
    private final long grantedBytes;
    private long requestedBytes;

    public CopyTaskReadBudget(long grantedBytes) {
        if (grantedBytes < 0) throw new IllegalArgumentException("COPY read grant must not be negative");
        this.grantedBytes = grantedBytes;
    }

    /** A rejected request starts no backend read and consumes no allowance. Entered reads are not refunded. */
    public synchronized void reserve(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("COPY requested bytes must not be negative");
        if (bytes > grantedBytes - requestedBytes) {
            throw new IllegalStateException("COPY task requested-read grant exhausted");
        }
        requestedBytes += bytes;
    }

    public synchronized long requestedBytes() { return requestedBytes; }
    public synchronized long remainingBytes() { return grantedBytes - requestedBytes; }
}
