package org.pangrova.indexing.engine.prefix.serializer;

import org.pangrova.indexing.api.data.text.PrefixEntry;
import org.pangrova.indexing.api.data.text.PrefixIndexData;

import java.util.ArrayList;
import org.pangrova.indexing.core.serde.SerdeCodec;
import org.pangrova.indexing.core.serde.TypeSerializer;

/**
 * Serializer for {@link PrefixIndexData}.
 * Format: [prefixLength:4B] [entryCount:4B] {[prefix:UTF] [rowOffset:8B]}*
 */
public final class PrefixSerializer implements TypeSerializer<PrefixIndexData> {

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
