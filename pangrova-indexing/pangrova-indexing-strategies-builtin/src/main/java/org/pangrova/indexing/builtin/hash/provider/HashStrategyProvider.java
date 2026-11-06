package org.pangrova.indexing.builtin.hash.provider;

import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.indexing.spi.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.HashIndexData;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IndexPredicate;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Hash index strategy using open-addressing hash table with linear probing
 * and 8-byte fingerprints for collision disambiguation.
 *
 * <p>Each slot is 9 bytes: 1 status byte + 8 fingerprint bytes.
 * Status: 0=empty, 1=occupied.</p>
 */
public final class HashStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_LOAD_FACTOR = "hash.load_factor";
    private static final double DEFAULT_LOAD_FACTOR = 0.7;
    private static final int SLOT_SIZE = 9; // 1 status + 8 fingerprint
    private static final int INITIAL_CAPACITY = 16;
    private static final byte EMPTY = 0;
    private static final byte OCCUPIED = 1;

    /** Mask that keeps a long hash in the non-negative int range. */
    private static final long POSITIVE_HASH_MASK = 0x7FFFFFFFL;
    /** FNV-1a 64-bit offset basis. */
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    /** FNV-1a 64-bit prime. */
    private static final long FNV_PRIME = 0x100000001b3L;
    /** Mask to widen a signed byte to unsigned. */
    private static final long BYTE_MASK = 0xFFL;
    /** Avalanche mix constant (MurmurHash3 finalizer). */
    private static final long MIX_CONSTANT_1 = 0xff51afd7ed558ccdL;
    /** Avalanche mix constant (MurmurHash3 finalizer). */
    private static final long MIX_CONSTANT_2 = 0xc4ceb9fe1a85ec53L;
    /** Shift width for the avalanche mixing steps. */
    private static final int MIX_SHIFT = 33;
    /** Independent seed for the fingerprint hash. */
    private static final long FINGERPRINT_SEED = 0x517cc1b727220a95L;

    @Override
    public IndexType supportedType() {
        return IndexType.HASH;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        double loadFactor = doubleProp(properties, PROP_LOAD_FACTOR, DEFAULT_LOAD_FACTOR);
        HashTableState state = new HashTableState(INITIAL_CAPACITY);

        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (row.isEmpty() || row.get(0) == null) {
                continue;
            }
            byte[] bytes = row.get(0).toString().getBytes(StandardCharsets.UTF_8);
            long hash = hash64(bytes);
            long fingerprint = fingerprint64(bytes);
            if (probeAndInsert(state.table, state.slotHashes, state.capacity, hash, fingerprint)) {
                state.entryCount++;
                state = resizeIfNeeded(state, loadFactor);
            }
        }
        return new HashIndexData(state.table, state.entryCount, state.capacity);
    }

    private static final class HashTableState {
        byte[] table;
        long[] slotHashes;
        int capacity;
        int entryCount;

        HashTableState(int capacity) {
            this.table = new byte[capacity * SLOT_SIZE];
            this.slotHashes = new long[capacity];
            this.capacity = capacity;
        }
    }

    private static HashTableState resizeIfNeeded(HashTableState state, double loadFactor) {
        if (state.entryCount < (int) (state.capacity * loadFactor)) {
            return state;
        }
        int newCapacity = state.capacity << 1;
        HashTableState newState = new HashTableState(newCapacity);
        newState.entryCount = state.entryCount;
        rehash(state.table, state.slotHashes, state.capacity,
                newState.table, newState.slotHashes, newCapacity);
        return newState;
    }

    /**
     * Probes the table for the given fingerprint. Inserts into the first
     * empty slot if absent. Returns true if a new entry was inserted.
     */
    private static boolean probeAndInsert(byte[] table, long[] slotHashes,
                                          int capacity, long hash, long fingerprint) {
        int slot = (int) (hash & POSITIVE_HASH_MASK) % capacity;
        for (int probe = 0; probe < capacity; probe++) {
            int pos = (slot + probe) % capacity;
            int idx = pos * SLOT_SIZE;
            if (table[idx] == EMPTY) {
                table[idx] = OCCUPIED;
                writeFingerprint(table, idx + 1, fingerprint);
                slotHashes[pos] = hash;
                return true;
            }
            if (readFingerprint(table, idx + 1) == fingerprint) {
                return false; // already present
            }
        }
        return false;
    }

    /**
     * Rehashes all occupied entries from the old table into a new table.
     * Uses the parallel hash array to compute correct slot positions.
     */
    private static void rehash(byte[] oldTable, long[] oldHashes, int oldCapacity,
                               byte[] newTable, long[] newHashes, int newCapacity) {
        for (int i = 0; i < oldCapacity; i++) {
            int oldIdx = i * SLOT_SIZE;
            if (oldTable[oldIdx] != OCCUPIED) {
                continue;
            }
            long fp = readFingerprint(oldTable, oldIdx + 1);
            long hash = oldHashes[i];
            int slot = (int) (hash & POSITIVE_HASH_MASK) % newCapacity;
            for (int probe = 0; probe < newCapacity; probe++) {
                int pos = (slot + probe) % newCapacity;
                int newIdx = pos * SLOT_SIZE;
                if (newTable[newIdx] == EMPTY) {
                    newTable[newIdx] = OCCUPIED;
                    writeFingerprint(newTable, newIdx + 1, fp);
                    newHashes[pos] = hash;
                    break;
                }
            }
        }
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof HashIndexData hashData)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (predicate instanceof EqualTo eq) {
            return probeTable(hashData, eq.value().toString())
                    ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
        }
        if (predicate instanceof InList inList) {
            return evaluateInList(hashData, inList);
        }
        return IndexEvalResult.UNKNOWN;
    }

    private static IndexEvalResult evaluateInList(HashIndexData hashData, InList inList) {
        for (Comparable<?> val : inList.values()) {
            if (probeTable(hashData, val.toString())) {
                return IndexEvalResult.KEEP;
            }
        }
        return IndexEvalResult.SKIP;
    }

    private static boolean probeTable(HashIndexData data, String value) {
        byte[] table = data.table();
        int capacity = data.capacity();
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        long hash = hash64(bytes);
        long fingerprint = fingerprint64(bytes);
        int slot = (int) (hash & POSITIVE_HASH_MASK) % capacity;

        for (int probe = 0; probe < capacity; probe++) {
            int idx = ((slot + probe) % capacity) * SLOT_SIZE;
            if (table[idx] == EMPTY) {
                return false;
            }
            if (readFingerprint(table, idx + 1) == fingerprint) {
                return true;
            }
        }
        return false;
    }

    private static long hash64(byte[] bytes) {
        long h = FNV_OFFSET_BASIS;
        for (byte b : bytes) {
            h ^= (b & BYTE_MASK);
            h *= FNV_PRIME;
        }
        h ^= h >>> MIX_SHIFT;
        h *= MIX_CONSTANT_1;
        h ^= h >>> MIX_SHIFT;
        return h;
    }

    private static long fingerprint64(byte[] bytes) {
        // Different seed for fingerprint to be independent of slot hash
        long h = FINGERPRINT_SEED;
        for (byte b : bytes) {
            h ^= (b & BYTE_MASK);
            h *= FNV_PRIME;
        }
        h ^= h >>> MIX_SHIFT;
        h *= MIX_CONSTANT_2;
        h ^= h >>> MIX_SHIFT;
        return h;
    }

    private static void writeFingerprint(byte[] table, int offset, long fingerprint) {
        ByteBuffer.wrap(table, offset, 8).order(ByteOrder.BIG_ENDIAN).putLong(fingerprint);
    }

    private static long readFingerprint(byte[] table, int offset) {
        return ByteBuffer.wrap(table, offset, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static double doubleProp(Map<String, String> props, String key, double defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Double.parseDouble(val);
    }
}
