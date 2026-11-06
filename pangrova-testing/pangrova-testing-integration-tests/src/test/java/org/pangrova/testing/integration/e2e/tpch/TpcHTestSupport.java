package org.pangrova.testing.integration.e2e.tpch;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.testkit.ResultValidator;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Compares complete results with an independent engine on identical input rows. */
final class TpcHTestSupport {
    // Q15 orders only by supplier key; duplicate supplier rows with the same
    // key have no specified order, so compare their complete row multiset.
    private static final Set<String> UNORDERED_QUERIES = Set.of(
            "q06.sql", "q14.sql", "q15.sql", "q17.sql", "q19.sql");
    private static final double NUMERIC_TOLERANCE = 1e-6;

    private TpcHTestSupport() {}

    static void assertMatchesReference(List<TypedRow> actual, List<TypedRow> expected,
                                       String queryFile, String... expectedColumns) {
        assertThat(actual).as("%s row count", queryFile).hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i).columnNames())
                    .as("%s columns at row %s", queryFile, i)
                    .containsExactlyInAnyOrderElementsOf(expected.get(i).columnNames())
                    .contains(expectedColumns);
        }
        if ("q15.sql".equals(queryFile)) {
            long previousKey = Long.MIN_VALUE;
            for (TypedRow row : actual) {
                long key = ((Number) row.getRaw("s_suppkey")).longValue();
                assertThat(key).as("Q15 supplier key order").isGreaterThanOrEqualTo(previousKey);
                previousKey = key;
            }
        }
        if (UNORDERED_QUERIES.contains(queryFile)) {
            ResultValidator.assertEqualsUnorderedWithTolerance(expected, actual, NUMERIC_TOLERANCE);
        } else {
            for (int i = 0; i < expected.size(); i++) {
                ResultValidator.assertEqualsUnorderedWithTolerance(
                        List.of(expected.get(i)), List.of(actual.get(i)), NUMERIC_TOLERANCE);
            }
        }
    }
}
