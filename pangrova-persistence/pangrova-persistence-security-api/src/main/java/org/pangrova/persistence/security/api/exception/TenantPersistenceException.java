package org.pangrova.persistence.security.api.exception;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Tenant persistence or encoding failed; the original backend failure remains the cause. */
@Experimental
public final class TenantPersistenceException extends PangrovaException {
    public TenantPersistenceException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
