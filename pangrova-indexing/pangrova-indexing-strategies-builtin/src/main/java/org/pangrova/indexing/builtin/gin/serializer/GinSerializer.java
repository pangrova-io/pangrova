package org.pangrova.indexing.builtin.gin.serializer;

import org.pangrova.indexing.api.data.text.GinIndexData;
import org.pangrova.indexing.codec.data.SerdeCodec;
import org.pangrova.indexing.api.codec.TypeSerializer;

/**
 * Serializer for {@link GinIndexData}.
 * Format: [storePositions:1B] [postings:map]
 */
public final class GinSerializer implements TypeSerializer<GinIndexData> {

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
