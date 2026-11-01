package org.pangrova.security.tenant.store;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Raised when durable tenant store persistence fails. */
public class TenantStoreException extends PangrovaException {

    public TenantStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.TENANT_STORE_ERROR, message, cause);
    }
}
