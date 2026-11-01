package org.pangrova.governance.quality.store;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Raised when durable quality rule persistence fails. */
public class QualityStoreException extends PangrovaException {

    public QualityStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.GOVERNANCE_QUALITY_STORE_ERROR, message, cause);
    }
}
