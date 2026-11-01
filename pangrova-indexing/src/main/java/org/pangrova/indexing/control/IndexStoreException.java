package org.pangrova.indexing.control;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Raised when durable index metadata persistence fails. */
public class IndexStoreException extends PangrovaException {

    public IndexStoreException(String message) {
        super(PangrovaErrorCode.INDEX_STORE_ERROR, message);
    }

    public IndexStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.INDEX_STORE_ERROR, message, cause);
    }
}
