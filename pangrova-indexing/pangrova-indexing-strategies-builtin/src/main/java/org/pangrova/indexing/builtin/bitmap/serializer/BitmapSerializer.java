package org.pangrova.indexing.builtin.bitmap.serializer;

import org.pangrova.indexing.api.data.bitmap.BitmapIndexData;

import java.util.LinkedHashMap;
import org.pangrova.indexing.codec.data.SerdeCodec;
import org.pangrova.indexing.api.codec.TypeSerializer;

/**
 * Serializer for {@link BitmapIndexData}.
 * Format: [partial:1B] [count:4B] {[key:comparable] [bitmap:varlen]}*
 */
public final class BitmapSerializer implements TypeSerializer<BitmapIndexData> {

    @Override
    public byte[] serialize(BitmapIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeBoolean(data.partial());
            var bitmaps = data.bitmaps();
            out.writeInt(bitmaps.size());
            for (var entry : bitmaps.entrySet()) {
                SerdeCodec.writeComparable(out, entry.getKey());
                SerdeCodec.writeByteArray(out, entry.getValue());
            }
        });
    }

    @Override
    public BitmapIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            boolean partial = in.readBoolean();
            int count = in.readInt();
            var bitmaps = new LinkedHashMap<Comparable<?>, byte[]>(count);
            for (int i = 0; i < count; i++) {
                Comparable<?> key = SerdeCodec.readComparable(in);
                byte[] bitmap = SerdeCodec.readByteArray(in);
                bitmaps.put(key, bitmap);
            }
            return new BitmapIndexData(bitmaps, partial);
        });
    }
}
