package org.pangrova.worker.shuffle;

/**
 * Thrown when the native shuffle drain encounters an error condition.
 */
public class ShuffleDrainException extends RuntimeException {

    public ShuffleDrainException(String message) {
        super(message);
    }

    public ShuffleDrainException(String message, Throwable cause) {
        super(message, cause);
    }
}
