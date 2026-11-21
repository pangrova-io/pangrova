package org.pangrova.governance.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Raised when a quality-rule store cannot complete a durable operation. */
public class QualityRuleStoreException extends PangrovaException {

    public QualityRuleStoreException(String message) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message);
    }

    public QualityRuleStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_METASTORE_ERROR, message, cause);
    }
}
