package org.pangrova.indexing.api.codec;

import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Type-specific serializer for a particular {@link IndexData} implementation.
 *
 * <p>Implementations must support concurrent calls without mutating their inputs.
 * Serialized bytes are owned by the caller. A deserializer borrows its input only
 * for the duration of the call; returned data must not depend on subsequent input
 * mutation. Malformed or unsupported payloads must throw, never synthesize an
 * empty successful index. This contract owns only the type payload; the codec
 * module owns the versioned envelope and checksum.</p>
 *
 * @param <T> the concrete IndexData type
 */
@Experimental
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
