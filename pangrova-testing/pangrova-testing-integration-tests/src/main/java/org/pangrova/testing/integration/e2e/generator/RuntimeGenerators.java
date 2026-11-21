package org.pangrova.testing.integration.e2e.generator;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * jqwik generators for runtime execution scenarios (query types, concurrency, JNI handles, wide tables).
 */
public final class RuntimeGenerators {

    private static final List<String> QUERY_TYPES =
            List.of("aggregation", "sort", "join");

    private static final List<String> COLUMN_TYPES =
            List.of("BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP");

    private RuntimeGenerators() {}

    /** Random query type for spill/shuffle metamorphic tests. */
    @Provide
    public static Arbitrary<String> queryTypes() {
        return Arbitraries.of(QUERY_TYPES);
    }

    /** Random wide table column definitions for I/O coalesced load tests. */
    @Provide
    public static Arbitrary<List<Map<String, String>>> wideTableColumns(int count) {
        return Arbitraries.of(COLUMN_TYPES)
                .list().ofSize(count)
                .map(types -> {
                    List<Map<String, String>> columns = new ArrayList<>();
                    for (int i = 0; i < types.size(); i++) {
                        columns.add(Map.of("name", "col_" + i, "type", types.get(i)));
                    }
                    return List.copyOf(columns);
                });
    }

    /** Random concurrent query count (2-8) for concurrency tests. */
    @Provide
    public static Arbitrary<Integer> concurrentQueryCount() {
        return Arbitraries.integers().between(2, 8);
    }

    /** Random JNI handle count (1-16) for native resource lifecycle tests. */
    @Provide
    public static Arbitrary<Integer> jniHandleCount() {
        return Arbitraries.integers().between(1, 16);
    }
}
