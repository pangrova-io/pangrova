package org.pangrova.streaming.api.cdc;

/**
 * Thrown when a raw CDC payload cannot be parsed into a {@link CdcEnvelope}.
 *
 * <p>This is a format-specific parsing error (e.g., malformed JSON, missing required
 * fields, unsupported schema version). It extends {@link RuntimeException} rather than
 * {@code PangrovaException} because it represents a data-level parsing failure, not a
 * system-level operational error.</p>
 */
public class CdcRecordConversionException extends RuntimeException {

    /**
     * @param message descriptive error message including format and failure details
     * @param cause   the underlying parsing exception
     */
    public CdcRecordConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
