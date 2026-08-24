package org.pangrova.indexing.serde;

import org.pangrova.spi.index.data.text.InvertedIndexData;

/**
 * Serializer for {@link InvertedIndexData}.
 * Format: [storePositions:1B] [postings:map]
 */
final class InvertedSerializer implements TypeSerializer<InvertedIndexData> {

    @Override
    public byte[] serialize(InvertedIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeBoolean(data.storePositions());
            SerdeCodec.writeStringPostings(out, data.postings(), data.storePositions());
        });
    }

    @Override
    public InvertedIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            boolean storePositions = in.readBoolean();
            var postings = SerdeCodec.readStringPostings(in, storePositions);
            return new InvertedIndexData(postings, storePositions);
        });
    }
}
