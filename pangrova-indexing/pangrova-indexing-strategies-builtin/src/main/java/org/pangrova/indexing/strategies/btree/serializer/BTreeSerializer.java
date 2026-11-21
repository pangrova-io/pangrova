package org.pangrova.indexing.strategies.btree.serializer;

import org.pangrova.indexing.api.data.BTreeIndexData;
import org.pangrova.indexing.api.data.BTreePage;

import java.util.ArrayList;
import java.util.List;
import org.pangrova.indexing.codec.data.SerdeCodec;
import org.pangrova.indexing.api.codec.TypeSerializer;

/**
 * Serializer for {@link BTreeIndexData}.
 * Format: [storePositions:1B] [pageCount:4B] {page}*
 * Page: [keyCount:4B] {key:comparable}* [minKey:comparable] [maxKey:comparable]
 *       if storePositions: {[posCount:4B] {pos:4B}*}*
 */
public final class BTreeSerializer implements TypeSerializer<BTreeIndexData> {

    @Override
    public byte[] serialize(BTreeIndexData data) {
        return SerdeCodec.toBytes(out -> {
            out.writeBoolean(data.storePositions());
            out.writeInt(data.leafPages().size());
            for (BTreePage page : data.leafPages()) {
                serializePage(out, page, data.storePositions());
            }
        });
    }

    private static void serializePage(java.io.DataOutputStream out, BTreePage page,
                                      boolean storePositions) throws java.io.IOException {
        if (storePositions && (page.rowPositions() == null || page.rowPositions().size() != page.keys().size())) {
            throw new IllegalArgumentException("BTree positions must align with every key");
        }
        out.writeInt(page.keys().size());
        for (Comparable<?> key : page.keys()) {
            SerdeCodec.writeComparable(out, key);
        }
        SerdeCodec.writeComparable(out, page.minKey());
        SerdeCodec.writeComparable(out, page.maxKey());
        if (storePositions && page.rowPositions() != null) {
            for (List<Integer> positions : page.rowPositions()) {
                SerdeCodec.writeIntList(out, positions);
            }
        }
    }

    @Override
    public BTreeIndexData deserialize(byte[] payload) {
        return SerdeCodec.fromBytes(payload, in -> {
            boolean storePositions = in.readBoolean();
            int pageCount = readCount(in, 6);
            var pages = new ArrayList<BTreePage>(pageCount);
            for (int p = 0; p < pageCount; p++) {
                pages.add(deserializePage(in, storePositions));
            }
            if (in.read() != -1) throw new java.io.IOException("Trailing BTree payload bytes");
            return new BTreeIndexData(pages, storePositions);
        });
    }

    private static BTreePage deserializePage(java.io.DataInputStream in,
                                             boolean storePositions) throws java.io.IOException {
        int keyCount = readCount(in, 1);
        var keys = new ArrayList<Comparable<?>>(keyCount);
        for (int k = 0; k < keyCount; k++) {
            keys.add(SerdeCodec.readComparable(in));
        }
        Comparable<?> minKey = SerdeCodec.readComparable(in);
        Comparable<?> maxKey = SerdeCodec.readComparable(in);
        List<List<Integer>> rowPositions = null;
        if (storePositions) {
            rowPositions = new ArrayList<>(keyCount);
            for (int k = 0; k < keyCount; k++) {
                int count = readCount(in, Integer.BYTES);
                var positions = new ArrayList<Integer>(count);
                for (int i = 0; i < count; i++) positions.add(in.readInt());
                rowPositions.add(positions);
            }
        }
        return new BTreePage(keys, rowPositions, minKey, maxKey);
    }

    // Input is always the bounded ByteArrayInputStream created by fromBytes.
    // Reject impossible declared counts before allocating their collections.
    private static int readCount(java.io.DataInputStream in, int minimumBytes) throws java.io.IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available() / minimumBytes) {
            throw new java.io.IOException("Invalid BTree collection size: " + count);
        }
        return count;
    }
}
