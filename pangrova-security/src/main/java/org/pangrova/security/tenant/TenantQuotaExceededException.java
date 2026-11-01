package org.pangrova.security.tenant;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a tenant exceeds a runtime quota limit (CPU time, scan bytes).
 */
public class TenantQuotaExceededException extends PangrovaException {

    public TenantQuotaExceededException(String message) {
        super(PangrovaErrorCode.TENANT_QUOTA_EXCEEDED, message);
    }
}
