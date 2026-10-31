package org.pangrova.file.core;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Thrown when durable file-space, directory, profile, or COPY state cannot be read or written. */
public class FileStorageException extends PangrovaException {

    public FileStorageException(String message) {
        this(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE, message);
    }

    public FileStorageException(String message, Throwable cause) {
        this(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE, message, cause);
    }

    public FileStorageException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public FileStorageException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
