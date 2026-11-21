package org.pangrova.client.cli.exception;

import org.pangrova.common.api.annotation.Internal;

/**
 * Thrown when Arrow IPC stream decoding fails.
 * Replaces silent degradation to empty results, ensuring the caller
 * can display a meaningful error message to the user.
 */
@Internal
public final class ResultDecodeException extends RuntimeException {

    public ResultDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
