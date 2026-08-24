package org.pangrova.coordinator.dynamictable.refresh;

/**
 * Thrown when a DT refresh lock renewal fails during a long-running
 * refresh cycle, indicating the lease has expired or been stolen by
 * another coordinator.
 *
 * <p>BUG-D03 fix: this exception aborts the refresh before the
 * irreversible {@code writer.write()} call to prevent concurrent
 * DT writes and data corruption.</p>
 */
public final class LockRenewalFailedException extends RuntimeException {

    private final String dtName;
    private final String refreshId;

    public LockRenewalFailedException(String dtName, String refreshId) {
        super("Lock renewal failed for dtName=" + dtName + " refreshId=" + refreshId
                + " — aborting refresh to prevent concurrent writes");
        this.dtName = dtName;
        this.refreshId = refreshId;
    }

    public String dtName() {
        return dtName;
    }

    public String refreshId() {
        return refreshId;
    }
}
