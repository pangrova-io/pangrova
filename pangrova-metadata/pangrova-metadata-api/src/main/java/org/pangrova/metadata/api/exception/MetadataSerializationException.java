package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when metadata serialization or deserialization fails.
 */
public class MetadataSerializationException extends MetadataException {

    public MetadataSerializationException(String message) {
        super(PangrovaErrorCode.METADATA_SERIALIZATION_ERROR, message);
    }

    public MetadataSerializationException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_SERIALIZATION_ERROR, message, cause);
    }
}
