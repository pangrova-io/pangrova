package org.pangrova.planner.runtimefilter;

import org.junit.jupiter.api.Test;
import org.pangrova.common.bloom.SplitBlockBloomFilter;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeFilterOwnershipTest {
    @Test
    void borrowedBloomIsImmutableAndCannotBeChangedThroughItsBuilderOrEncodedBytes() {
        var builder = SplitBlockBloomFilter.builder(100, 0.01);
        builder.put("retained");
        var bloom = builder.build();
        var filter = RuntimeFilter.bloom("id", bloom);
        var snapshot = bloom.toByteArray();
        builder.put("later");
        byte[] externallyModified = filter.bloomFilter().toByteArray();
        java.util.Arrays.fill(externallyModified, (byte) 0);
        assertThat(filter.bloomFilter()).isSameAs(bloom);
        assertThat(filter.bloomFilter().toByteArray()).containsExactly(snapshot);
        assertThat(filter.mightContain("retained")).isTrue();
    }
}
