package org.pangrova.indexing.serde;

import org.pangrova.spi.index.data.text.NGramBloomIndexData;

/**
 * Serializer for {@link NGramBloomIndexData}.
 * Format: [numHashFunctions:4B] [numBits:8B] [ngramSize:4B] [filterData:varlen]
 */
final class NGramBloomSerializer implements TypeSerializer<NGramBloomIndexData> {

    @Override
    public byte[] serialize(NGramBloomIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeInt(data.numHashFunctions());
            out.writeLong(data.numBits());
            out.writeInt(data.ngramSize());
            SerdeCodec.writeByteArray(out, data.filterData());
        });
    }

    @Override
    public NGramBloomIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            int numHash = in.readInt();
            long numBits = in.readLong();
            int ngramSize = in.readInt();
            byte[] filterData = SerdeCodec.readByteArray(in);
            return new NGramBloomIndexData(filterData, numHash, numBits, ngramSize);
        });
    }
}
