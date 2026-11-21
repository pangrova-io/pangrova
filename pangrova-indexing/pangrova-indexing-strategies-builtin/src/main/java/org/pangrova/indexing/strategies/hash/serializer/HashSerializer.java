package org.pangrova.indexing.strategies.hash.serializer;

import org.pangrova.indexing.api.data.HashIndexData;
import org.pangrova.indexing.codec.data.SerdeCodec;
import org.pangrova.indexing.api.codec.TypeSerializer;

/**
 * Serializer for {@link HashIndexData}.
 * Format: [entryCount:4B] [capacity:4B] [table:varlen]
 */
public final class HashSerializer implements TypeSerializer<HashIndexData> {

    @Override
    public byte[] serialize(HashIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeInt(data.entryCount());
            out.writeInt(data.capacity());
            SerdeCodec.writeByteArray(out, data.table());
        });
    }

    @Override
    public HashIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            int entryCount = in.readInt();
            int capacity = in.readInt();
            byte[] table = SerdeCodec.readByteArray(in);
            return new HashIndexData(table, entryCount, capacity);
        });
    }
}
