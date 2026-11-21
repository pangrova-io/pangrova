package org.pangrova.connector.iceberg.exception;

import org.pangrova.common.api.annotation.Internal;

/** Signals an expression that cannot be pushed down to the native reader. */
@Internal
public final class UnsupportedIcebergExpressionException extends RuntimeException {
    public UnsupportedIcebergExpressionException(String message) {
        super(message);
    }
}
