package org.pangrova.indexing.engine.gin.serializer;

import org.pangrova.spi.indexing.data.text.GinIndexData;
import org.pangrova.indexing.serde.SerdeCodec;
import org.pangrova.indexing.serde.TypeSerializer;

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
