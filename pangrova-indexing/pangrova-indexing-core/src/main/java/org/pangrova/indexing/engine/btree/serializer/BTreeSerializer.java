package org.pangrova.indexing.engine.btree.serializer;

import org.pangrova.spi.indexing.data.BTreeIndexData;
import org.pangrova.spi.indexing.data.BTreePage;

import java.util.ArrayList;
import java.util.List;
import org.pangrova.indexing.core.serde.SerdeCodec;
import org.pangrova.indexing.core.serde.TypeSerializer;

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
            int pageCount = in.readInt();
            var pages = new ArrayList<BTreePage>(pageCount);
            for (int p = 0; p < pageCount; p++) {
                pages.add(deserializePage(in, storePositions));
            }
            return new BTreeIndexData(pages, storePositions);
        });
    }

    private static BTreePage deserializePage(java.io.DataInputStream in,
                                             boolean storePositions) throws java.io.IOException {
        int keyCount = in.readInt();
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
                rowPositions.add(SerdeCodec.readIntList(in));
            }
        }
        return new BTreePage(keys, rowPositions, minKey, maxKey);
    }
}
