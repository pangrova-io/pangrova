package org.pangrova.indexing.serde;

import org.pangrova.spi.index.data.bitmap.BitmapIndexData;

import java.util.LinkedHashMap;

/**
 * Serializer for {@link BitmapIndexData}.
 * Format: [partial:1B] [count:4B] {[key:comparable] [bitmap:varlen]}*
 */
final class BitmapSerializer implements TypeSerializer<BitmapIndexData> {

    @Override
    public byte[] serialize(BitmapIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeBoolean(data.partial());
            out.writeInt(data.bitmaps().size());
            for (var entry : data.bitmaps().entrySet()) {
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
