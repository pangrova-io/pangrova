package org.pangrova.testing.e2e.generator;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

import java.util.List;

/**
 * jqwik generators for Iceberg delete scenario property tests.
 * Covers position delete and equality delete key generation.
 */
public final class DeleteGenerators {

    private DeleteGenerators() {}

    /** Random row positions a user might delete from a data file. */
    @Provide
    public static Arbitrary<List<Long>> deletePositions(int maxRows) {
        return Arbitraries.longs().between(0, maxRows - 1)
                .list().ofMinSize(1).ofMaxSize(maxRows / 2)
                .map(list -> list.stream().distinct().sorted().toList());
    }

    /** Random equality delete key values matching a primary key column. */
    @Provide
    public static Arbitrary<List<Integer>> deleteKeyValues(int maxKey) {
        return Arbitraries.integers().between(1, maxKey)
                .list().ofMinSize(1).ofMaxSize(maxKey / 3)
                .map(list -> list.stream().distinct().sorted().toList());
    }
}
