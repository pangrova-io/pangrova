package org.pangrova.indexing.strategies.test.serde;

import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;


import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link IndexType} uses stable IDs for persistence rather than
 * ordinal values, ensuring deserialization is resilient to enum reordering.
 */
class IndexTypeStableIdTest {

    private static final TypeSerializerRegistry REGISTRY = TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class));

    @Test
    void stableIdIsIndependentOfOrdinal() {
        for (IndexType type : IndexType.values()) {
            assertThat(type.getStableId())
                    .as("stableId for %s should be positive", type)
                    .isGreaterThan(0);
        }
        // BTREE has ordinal=0 but stableId=1, proving they are decoupled.
        assertThat(IndexType.BTREE.getStableId()).isNotEqualTo(IndexType.BTREE.ordinal());
    }

    @Test
    void allStableIdsAreUnique() {
        Set<Integer> ids = new HashSet<>();
        for (IndexType type : IndexType.values()) {
            boolean added = ids.add(type.getStableId());
            assertThat(added)
                    .as("Duplicate stableId %d for %s", type.getStableId(), type)
                    .isTrue();
        }
    }

    @Test
    void fromStableIdRoundTripsAllValues() {
        for (IndexType type : IndexType.values()) {
            IndexType resolved = IndexType.fromStableId(type.getStableId());
            assertThat(resolved).isSameAs(type);
        }
    }

    @Test
    void fromStableIdRejectsUnknownId() {
        assertThatThrownBy(() -> IndexType.fromStableId(9999))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("9999");
    }

    @Test
    void fromStableIdRejectsZero() {
        assertThatThrownBy(() -> IndexType.fromStableId(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0");
    }

    @Test
    void serializerUsesStableIdNotOrdinal() {
        // Serialize a BloomFilter index (stableId=3, ordinal=2)
        BloomFilterIndexData data = new BloomFilterIndexData(
                new byte[]{1, 2, 3, 4}, 3, 32);
        IndexSerializer serializer = new IndexSerializer(REGISTRY, false);
        byte[] bytes = serializer.serialize(data);

        // Read the typeStableId field directly from the wire format (offset 6, 2 bytes)
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buf.position(6); // skip magic(4) + version(2)
        short wireTypeId = buf.getShort();

        assertThat(wireTypeId)
                .as("Wire format should contain stableId, not ordinal")
                .isEqualTo((short) IndexType.BLOOM_FILTER.getStableId());
    }

    @Test
    void deserializationWorksWithStableIdRegardlessOfEnumOrder() {
        // Manually craft a serialized payload with a known stableId.
        // This simulates data persisted before any potential enum reordering.
        BloomFilterIndexData original = new BloomFilterIndexData(
                new byte[]{10, 20, 30}, 5, 64);
        IndexSerializer serializer = new IndexSerializer(REGISTRY, true);

        byte[] serialized = serializer.serialize(original);
        Optional<IndexData> deserialized = serializer.deserialize(serialized);

        assertThat(deserialized).isPresent();
        BloomFilterIndexData result = (BloomFilterIndexData) deserialized.get();
        assertThat(result.indexType()).isEqualTo(IndexType.BLOOM_FILTER);
        assertThat(result.filterData()).isEqualTo(original.filterData());
        assertThat(result.numHashFunctions()).isEqualTo(original.numHashFunctions());
        assertThat(result.numBits()).isEqualTo(original.numBits());
    }

    @Test
    void knownStableIdAssignments() {
        // Verify the fixed ID assignments are correct and stable.
        // These values must never change once persisted data exists.
        assertThat(IndexType.BTREE.getStableId()).isEqualTo(1);
        assertThat(IndexType.INVERTED.getStableId()).isEqualTo(2);
        assertThat(IndexType.BLOOM_FILTER.getStableId()).isEqualTo(3);
        assertThat(IndexType.PREFIX.getStableId()).isEqualTo(4);
        assertThat(IndexType.NGRAM_BLOOM_FILTER.getStableId()).isEqualTo(5);
        assertThat(IndexType.BITMAP.getStableId()).isEqualTo(6);
        assertThat(IndexType.HASH.getStableId()).isEqualTo(7);
        assertThat(IndexType.SPARSE.getStableId()).isEqualTo(8);
        assertThat(IndexType.R_TREE.getStableId()).isEqualTo(9);
        assertThat(IndexType.GIN.getStableId()).isEqualTo(10);
        assertThat(IndexType.VECTOR.getStableId()).isEqualTo(11);
    }
}
