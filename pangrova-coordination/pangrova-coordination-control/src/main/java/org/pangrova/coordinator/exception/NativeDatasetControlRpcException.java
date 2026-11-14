package org.pangrova.coordinator.exception;

import org.pangrova.common.api.annotation.Internal;

/** External publication is uncertain; authentication and authoritative rejection do not use this exception. */
@Internal
public final class NativeDatasetControlRpcException extends RuntimeException {

    public NativeDatasetControlRpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
