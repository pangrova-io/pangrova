package org.pangrova.indexing.builtin.sparse.serializer;

import org.pangrova.indexing.api.data.spatial.SparseEntry;
import org.pangrova.indexing.api.data.spatial.SparseIndexData;

import java.util.ArrayList;
import org.pangrova.indexing.codec.data.SerdeCodec;
import org.pangrova.indexing.spi.codec.TypeSerializer;

/**
 * Serializer for {@link SparseIndexData}.
 * Format: [stride:4B] [entryCount:4B] {[key:comparable] [rowOffset:8B]}*
 */
public final class SparseSerializer implements TypeSerializer<SparseIndexData> {

    @Override
    public byte[] serialize(SparseIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeInt(data.stride());
            out.writeInt(data.entries().size());
            for (SparseEntry entry : data.entries()) {
                SerdeCodec.writeComparable(out, entry.key());
                out.writeLong(entry.rowOffset());
            }
        });
    }

    @Override
    public SparseIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            int stride = in.readInt();
            int count = in.readInt();
            var entries = new ArrayList<SparseEntry>(count);
            for (int i = 0; i < count; i++) {
                entries.add(new SparseEntry(SerdeCodec.readComparable(in), in.readLong()));
            }
            return new SparseIndexData(entries, stride);
        });
    }
}
