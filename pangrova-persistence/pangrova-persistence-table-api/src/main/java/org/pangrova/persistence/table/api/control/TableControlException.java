package org.pangrova.persistence.table.api.control;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Durable table-control state could not be read or written, or an optimistic
 * concurrency check rejected the write.
 */
public class TableControlException extends PangrovaException {

    public TableControlException(String message) {
        this(PangrovaErrorCode.TABLE_CONTROL_PERSISTENCE_FAILED, message);
    }

    public TableControlException(String message, Throwable cause) {
        this(PangrovaErrorCode.TABLE_CONTROL_PERSISTENCE_FAILED, message, cause);
    }

    public TableControlException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public TableControlException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    /** A write lost the metadata revision compare-and-set. */
    public static TableControlException staleWrite(String message) {
        return new TableControlException(PangrovaErrorCode.TABLE_CONTROL_STALE_WRITE, message);
    }
}
