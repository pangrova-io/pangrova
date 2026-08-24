package org.pangrova.common.exception;

/**
 * Thrown when a conditional state update fails because the epoch has changed
 * since the caller last read the state. Indicates a concurrent writer
 * (typically another coordinator after HA failover) has advanced the epoch.
 *
 * <p>Callers should treat this as a non-retryable conflict: the current
 * operation is stale and must be abandoned.</p>
 */
public final class StaleEpochException extends PangrovaException {

    private final String tableName;
    private final long expectedEpoch;

    /**
     * Creates a stale epoch conflict.
     *
     * @param tableName table whose epoch changed
     * @param expectedEpoch epoch observed by the caller
     */
    public StaleEpochException(String tableName, long expectedEpoch) {
        super(PangrovaErrorCode.METADATA_STALE_EPOCH,
                "Stale epoch for tableName=" + tableName + " expectedEpoch=" + expectedEpoch);
        this.tableName = tableName;
        this.expectedEpoch = expectedEpoch;
    }

    public String tableName() {
        return tableName;
    }

    public long expectedEpoch() {
        return expectedEpoch;
    }
}
