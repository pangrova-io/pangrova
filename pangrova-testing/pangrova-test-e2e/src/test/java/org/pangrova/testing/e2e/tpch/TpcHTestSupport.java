package org.pangrova.testing.e2e.tpch;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.ResultValidator;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Compares complete results with an independent engine on identical input rows. */
final class TpcHTestSupport {
    private static final Set<String> UNORDERED_QUERIES = Set.of(
            "q06.sql", "q14.sql", "q17.sql", "q19.sql");
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
