package org.pangrova.indexing.engine.ngram.serializer;

import org.pangrova.spi.indexing.data.text.NGramBloomIndexData;
import org.pangrova.indexing.serde.SerdeCodec;
import org.pangrova.indexing.serde.TypeSerializer;

/**
 * Serializer for {@link NGramBloomIndexData}.
 * Format: [numHashFunctions:4B] [numBits:8B] [ngramSize:4B] [filterData:varlen]
 */
public final class NGramBloomSerializer implements TypeSerializer<NGramBloomIndexData> {

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
