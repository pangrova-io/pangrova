package org.pangrova.planner.mv;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception for materialized view operations (creation, refresh).
 */
public class MaterializedViewException extends PangrovaException {

    public MaterializedViewException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public MaterializedViewException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
