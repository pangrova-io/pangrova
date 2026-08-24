package org.pangrova.common.exception;

/**
 * Thrown when configuration loading or validation fails.
 */
public class ConfigurationException extends PangrovaException {

    public ConfigurationException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ConfigurationException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
