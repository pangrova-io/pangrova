package org.pangrova.coordinator.dynamictable.quota;

/**
 * Thrown when a refresh operation exceeds its allocated memory quota.
 *
 * <p>Triggers the {@code dt_refresh_oom} metric (R16 AC3, R16 AC5).</p>
 */
public final class RefreshMemoryQuotaExceededException extends RuntimeException {

    private final long requested;
    private final long available;

    public RefreshMemoryQuotaExceededException(long requested, long available) {
        super("Memory quota exceeded: requested=" + requested
                + " available=" + available);
        this.requested = requested;
        this.available = available;
    }

    public long requested() { return requested; }

    public long available() { return available; }
}
