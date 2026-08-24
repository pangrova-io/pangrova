package org.pangrova.indexing.serde;

import org.pangrova.spi.index.data.BloomFilterIndexData;

/**
 * Serializer for {@link BloomFilterIndexData}.
 * Format: [numHashFunctions:4B] [numBits:8B] [filterData:varlen]
 */
final class BloomFilterSerializer implements TypeSerializer<BloomFilterIndexData> {

    @Override
    public byte[] serialize(BloomFilterIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeInt(data.numHashFunctions());
            out.writeLong(data.numBits());
            SerdeCodec.writeByteArray(out, data.filterData());
        });
    }

    @Override
    public BloomFilterIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            int numHash = in.readInt();
            long numBits = in.readLong();
            byte[] filterData = SerdeCodec.readByteArray(in);
            return new BloomFilterIndexData(filterData, numHash, numBits);
        });
    }
}
