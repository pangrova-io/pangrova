package org.pangrova.security.storage.store;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Raised when the durable file space or storage profile store fails. */
public class StorageStoreException extends PangrovaException {

    public StorageStoreException(String message) {
        super(PangrovaErrorCode.STORAGE_STORE_ERROR, message);
    }

    public StorageStoreException(String message, Throwable cause) {
        super(PangrovaErrorCode.STORAGE_STORE_ERROR, message, cause);
    }
}
