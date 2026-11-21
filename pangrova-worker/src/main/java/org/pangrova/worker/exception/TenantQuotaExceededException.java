package org.pangrova.worker.exception;

/**
 * Exception indicating a tenant quota violation during execution.
 */
public final class TenantQuotaExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String tenantId;
    private final String violationType;

    public TenantQuotaExceededException(String tenantId, String violationType, String message) {
        super(message);
        this.tenantId = tenantId;
        this.violationType = violationType;
    }

    public String tenantId() { return tenantId; }
    public String violationType() { return violationType; }
}
