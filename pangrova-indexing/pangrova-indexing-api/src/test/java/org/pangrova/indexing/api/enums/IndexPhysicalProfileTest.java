package org.pangrova.indexing.api.enums;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexPhysicalProfileTest {
    @Test
    void everyPersistedTypeRetainsItsStableIdAndPhysicalFamily() {
        Map<IndexType, Integer> stableIds = Map.ofEntries(
                Map.entry(IndexType.BTREE, 1), Map.entry(IndexType.INVERTED, 2),
                Map.entry(IndexType.BLOOM_FILTER, 3), Map.entry(IndexType.PREFIX, 4),
                Map.entry(IndexType.NGRAM_BLOOM_FILTER, 5), Map.entry(IndexType.BITMAP, 6),
                Map.entry(IndexType.HASH, 7), Map.entry(IndexType.SPARSE, 8),
                Map.entry(IndexType.R_TREE, 9), Map.entry(IndexType.GIN, 10),
                Map.entry(IndexType.VECTOR, 11));
        var nativeTypes = EnumSet.of(IndexType.VECTOR, IndexType.INVERTED);
        assertThat(stableIds.keySet()).containsExactlyInAnyOrder(IndexType.values());
        for (IndexType type : IndexType.values()) {
            assertThat(type.getStableId()).isEqualTo(stableIds.get(type));
            assertThat(IndexType.fromStableId(stableIds.get(type))).isSameAs(type);
            assertThat(type.physicalProfile()).isSameAs(nativeTypes.contains(type)
                    ? IndexPhysicalProfile.NATIVE_RETRIEVAL : IndexPhysicalProfile.SCALAR_FILE_PRUNING);
            assertThat(type.requiresNativeArtifact()).isEqualTo(nativeTypes.contains(type));
        }
    }
}
