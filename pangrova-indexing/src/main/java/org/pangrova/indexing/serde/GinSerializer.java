package org.pangrova.indexing.serde;

import org.pangrova.spi.index.data.text.GinIndexData;

/**
 * Serializer for {@link GinIndexData}.
 * Format: [storePositions:1B] [postings:map]
 */
final class GinSerializer implements TypeSerializer<GinIndexData> {

    @Override
    public byte[] serialize(GinIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeBoolean(data.storePositions());
            SerdeCodec.writeStringPostings(out, data.elementPostings(), data.storePositions());
        });
    }

    @Override
    public GinIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            boolean storePositions = in.readBoolean();
            var postings = SerdeCodec.readStringPostings(in, storePositions);
            return new GinIndexData(postings, storePositions);
        });
    }
}
