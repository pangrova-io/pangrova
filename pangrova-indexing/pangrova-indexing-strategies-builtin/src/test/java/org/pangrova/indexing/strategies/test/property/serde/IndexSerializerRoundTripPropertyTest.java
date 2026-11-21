package org.pangrova.indexing.strategies.test.property.serde;


import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 7: IndexSerializer serialize-then-deserialize roundtrip consistency.
 *
 * <p>For any legal index data and any checksumEnabled value,
 * serialize followed by deserialize produces data equivalent to the original.</p>
 *
 * <p><b>Validates: Requirement 5.4</b></p>
 */
class IndexSerializerRoundTripPropertyTest {

    private static final TypeSerializerRegistry REGISTRY = TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class));

    @Property(tries = 200)
    void roundTripWithChecksumEnabled(@ForAll("bloomFilters") BloomFilterIndexData original) {
        IndexSerializer serializer = new IndexSerializer(REGISTRY, true);
        byte[] bytes = serializer.serialize(original);
        Optional<IndexData> result = serializer.deserialize(bytes);

        assertThat(result).isPresent();
        assertBloomFilterEquals(original, (BloomFilterIndexData) result.get());
    }

    @Property(tries = 200)
    void roundTripWithChecksumDisabled(@ForAll("bloomFilters") BloomFilterIndexData original) {
        IndexSerializer serializer = new IndexSerializer(REGISTRY, false);
        byte[] bytes = serializer.serialize(original);
        Optional<IndexData> result = serializer.deserialize(bytes);

        assertThat(result).isPresent();
        assertBloomFilterEquals(original, (BloomFilterIndexData) result.get());
    }

    @Property(tries = 100)
    void checksumEnabledAndDisabledProduceDifferentSizes(
            @ForAll("bloomFilters") BloomFilterIndexData original) {
        IndexSerializer withCrc = new IndexSerializer(REGISTRY, true);
        IndexSerializer noCrc = new IndexSerializer(REGISTRY, false);

        byte[] bytesWithCrc = withCrc.serialize(original);
        byte[] bytesNoCrc = noCrc.serialize(original);

        assertThat(bytesWithCrc.length)
                .as("CRC-enabled output should be 4 bytes longer")
                .isEqualTo(bytesNoCrc.length + 4);
    }

    private static void assertBloomFilterEquals(BloomFilterIndexData expected,
                                                BloomFilterIndexData actual) {
        assertThat(actual.numHashFunctions()).isEqualTo(expected.numHashFunctions());
        assertThat(actual.numBits()).isEqualTo(expected.numBits());
        assertThat(actual.filterData()).isEqualTo(expected.filterData());
    }

    @Provide
    Arbitrary<BloomFilterIndexData> bloomFilters() {
        Arbitrary<byte[]> filterData = Arbitraries.bytes().array(byte[].class)
                .ofMinSize(1).ofMaxSize(256);
        Arbitrary<Integer> numHash = Arbitraries.integers().between(1, 20);
        Arbitrary<Long> numBits = Arbitraries.longs().between(8, 2048);
        return Combinators.combine(filterData, numHash, numBits)
                .as(BloomFilterIndexData::new);
    }
}
