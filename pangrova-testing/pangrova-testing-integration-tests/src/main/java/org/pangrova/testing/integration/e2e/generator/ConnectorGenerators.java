package org.pangrova.testing.integration.e2e.generator;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.types.value.TypedRow;

/**
 * jqwik generators for connector-level data scenarios.
 *
 * <p>Provides {@link Arbitrary} instances for generating Iceberg rows,
 * ORC data type subsets, and Paimon write operation sequences
 * used in connector property-based testing.
 */
public final class ConnectorGenerators {

    private static final List<String> ORC_DATA_TYPES =
            List.of("BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP");

    private static final List<String> PAIMON_OPS =
            List.of("INSERT", "UPDATE", "DELETE");

    private static final List<String> SAMPLE_NAMES =
            List.of("alpha", "bravo", "charlie", "delta", "echo",
                     "foxtrot", "golf", "hotel", "india", "juliet");

    private ConnectorGenerators() {}

    /**
     * Generate random Iceberg row data with unique id, name, and value columns.
     *
     * @param count the number of rows to generate; must be positive
     * @return arbitrary producing lists of typed rows
     */
    @Provide
    public static Arbitrary<List<TypedRow>> icebergRows(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive, got: " + count);
        }
        return Arbitraries.integers().between(1, 10000)
                .list().ofSize(count)
                .map(ids -> ids.stream().distinct()
                        .map(id -> TypedRow.builder()
                                .putInt("id", id)
                                .putString("name", SAMPLE_NAMES.get(
                                        Math.abs(id) % SAMPLE_NAMES.size()))
                                .putRaw("value", id * 1.5)
                                .build())
                        .toList());
    }

    /**
     * Generate random ORC data type subset for format equivalence tests.
     *
     * @return arbitrary producing lists of distinct ORC type names
     */
    @Provide
    public static Arbitrary<List<String>> orcDataTypes() {
        return Arbitraries.of(ORC_DATA_TYPES)
                .list().ofMinSize(2).ofMaxSize(ORC_DATA_TYPES.size())
                .map(list -> list.stream().distinct().toList());
    }

    /**
     * Generate random Paimon write sequence (INSERT/UPDATE/DELETE), always starts with INSERT.
     *
     * @param maxOps the maximum number of operations; must be at least 2
     * @return arbitrary producing lists of operation strings
     */
    @Provide
    public static Arbitrary<List<String>> paimonWriteSequence(int maxOps) {
        if (maxOps < 2) {
            throw new IllegalArgumentException("maxOps must be at least 2, got: " + maxOps);
        }
        return Arbitraries.of(PAIMON_OPS)
                .list().ofMinSize(2).ofMaxSize(maxOps)
                .map(ops -> {
                    if ("INSERT".equals(ops.get(0))) {
                        return ops;
                    }
                    List<String> fixed = new ArrayList<>(ops);
                    fixed.set(0, "INSERT");
                    return List.copyOf(fixed);
                });
    }
}
