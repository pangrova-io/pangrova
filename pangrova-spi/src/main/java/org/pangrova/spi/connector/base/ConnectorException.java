package org.pangrova.spi.connector.base;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Base exception for all connector-level errors (Iceberg, Paimon, etc.).
 * Translates connector-specific exceptions into unified PangrovaErrorCode semantics
 * at the module boundary.
 */
@Experimental
public class ConnectorException extends PangrovaException {

    public ConnectorException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ConnectorException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
