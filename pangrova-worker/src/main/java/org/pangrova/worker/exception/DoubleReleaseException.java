package org.pangrova.worker.exception;

/**
 * Thrown when a memory pool detects a double-release: the caller attempts
 * to free more bytes than currently reserved, indicating an accounting bug
 * or duplicate release of the same allocation.
 * <p>
 * This is a fail-fast signal — the pool refuses to let usage go negative,
 * preserving memory accounting integrity.
 */
public final class DoubleReleaseException extends RuntimeException {

    private final String poolName;
    private final long releaseBytes;
    private final long currentUsed;

    /**
     * Creates an exception describing the failed release attempt.
     */
    public DoubleReleaseException(String poolName, long releaseBytes, long currentUsed) {
        super(formatMessage(poolName, releaseBytes, currentUsed));
        this.poolName = poolName;
        this.releaseBytes = releaseBytes;
        this.currentUsed = currentUsed;
    }

    public String getPoolName() {
        return poolName;
    }

    public long getReleaseBytes() {
        return releaseBytes;
    }

    public long getCurrentUsed() {
        return currentUsed;
    }

    private static String formatMessage(String poolName, long releaseBytes, long currentUsed) {
        return String.format(
                "Double-release detected in pool '%s': attempted to free %d bytes but only %d bytes are in use",
                poolName, releaseBytes, currentUsed);
    }
}
