package org.pangrova.indexing.serde;

import org.pangrova.spi.index.data.text.PrefixEntry;
import org.pangrova.spi.index.data.text.PrefixIndexData;

import java.util.ArrayList;

/**
 * Serializer for {@link PrefixIndexData}.
 * Format: [prefixLength:4B] [entryCount:4B] {[prefix:UTF] [rowOffset:8B]}*
 */
final class PrefixSerializer implements TypeSerializer<PrefixIndexData> {

    @Override
    public byte[] serialize(PrefixIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeInt(data.prefixLength());
            out.writeInt(data.entries().size());
            for (PrefixEntry entry : data.entries()) {
                out.writeUTF(entry.prefix());
                out.writeLong(entry.rowOffset());
            }
        });
    }

    @Override
    public PrefixIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            int prefixLength = in.readInt();
            int count = in.readInt();
            var entries = new ArrayList<PrefixEntry>(count);
            for (int i = 0; i < count; i++) {
                entries.add(new PrefixEntry(in.readUTF(), in.readLong()));
            }
            return new PrefixIndexData(entries, prefixLength);
        });
    }
}
