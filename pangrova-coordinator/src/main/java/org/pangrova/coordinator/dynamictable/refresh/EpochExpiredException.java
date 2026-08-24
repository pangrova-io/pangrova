package org.pangrova.coordinator.dynamictable.refresh;

/**
 * Thrown when a refresh attempt detects that the HA epoch has changed
 * since the refresh started, indicating a leader switchover occurred.
 *
 * <p>The refresh must be rejected to prevent stale writes (R13 AC4).</p>
 */
public final class EpochExpiredException extends RuntimeException {

    private final long stateEpoch;
    private final long currentEpoch;

    public EpochExpiredException(String dtName, long stateEpoch, long currentEpoch) {
        super("Epoch expired for dtName=" + dtName
                + " stateEpoch=" + stateEpoch
                + " currentEpoch=" + currentEpoch);
        this.stateEpoch = stateEpoch;
        this.currentEpoch = currentEpoch;
    }

    public long stateEpoch() { return stateEpoch; }

    public long currentEpoch() { return currentEpoch; }
}
