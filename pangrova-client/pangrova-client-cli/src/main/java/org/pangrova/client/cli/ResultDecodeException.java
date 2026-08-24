package org.pangrova.client.cli;

/**
 * Thrown when Arrow IPC stream decoding fails.
 * Replaces silent degradation to empty results, ensuring the caller
 * can display a meaningful error message to the user.
 */
final class ResultDecodeException extends RuntimeException {

    ResultDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
