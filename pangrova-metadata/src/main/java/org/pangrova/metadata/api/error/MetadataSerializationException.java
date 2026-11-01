package org.pangrova.metadata.api.error;

import org.pangrova.common.exception.PangrovaErrorCode;

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
