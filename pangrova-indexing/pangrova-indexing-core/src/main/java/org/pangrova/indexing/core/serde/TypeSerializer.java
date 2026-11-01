package org.pangrova.indexing.core.serde;

import org.pangrova.spi.indexing.data.IndexData;

/**
 * Type-specific serializer for a particular {@link IndexData} implementation.
 *
 * @param <T> the concrete IndexData type
 */
public interface TypeSerializer<T extends IndexData> {

    /**
     * Serializes the type-specific payload (excluding common header).
     */
    byte[] serialize(T data);

    /**
     * Deserializes the type-specific payload back to an IndexData instance.
     */
    T deserialize(byte[] payload);
}
