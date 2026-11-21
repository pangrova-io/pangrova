package org.pangrova.testing.integration.e2e.tpch;

import org.junit.jupiter.api.Test;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TpcHTestSupportTest {
    @Test
    void rejectsEmptyActualWhenReferenceHasRows() {
        assertThatThrownBy(() -> compare(List.of(), List.of(row(1)), "q01.sql"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void acceptsEmptyOnlyWhenReferenceIsAlsoEmpty() {
        compare(List.of(), List.of(), "q01.sql");
    }

    @Test
    void rejectsWrongValuesEvenWhenSchemaMatches() {
        assertThatThrownBy(() -> compare(List.of(row(9)), List.of(row(1)), "q01.sql"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsExtraColumns() {
        var extra = TypedRow.fromRawMap(Map.of("value", 1, "unexpected", 2));
        assertThatThrownBy(() -> compare(List.of(extra), List.of(row(1)), "q01.sql"))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void orderedQueriesMustRetainRowOrder() {
        assertThatThrownBy(() -> compare(List.of(row(2), row(1)),
                List.of(row(1), row(2)), "q01.sql")).isInstanceOf(AssertionError.class);
    }

    @Test
    void unorderedComparisonPreservesMultiplicity() {
        compare(List.of(row(2), row(1)), List.of(row(1), row(2)), "q06.sql");
        assertThatThrownBy(() -> compare(List.of(row(1), row(1)),
                List.of(row(1), row(2)), "q06.sql")).isInstanceOf(AssertionError.class);
    }

    @Test
    void numericToleranceDoesNotAcceptMateriallyWrongAggregates() {
        compare(List.of(row(1.00000001)), List.of(row(1.0)), "q01.sql");
        assertThatThrownBy(() -> compare(List.of(row(1.01)), List.of(row(1.0)), "q01.sql"))
                .isInstanceOf(AssertionError.class);
    }

    private static TypedRow row(Number value) {
        return TypedRow.fromRawMap(Map.of("value", value));
    }

    private static void compare(List<TypedRow> actual, List<TypedRow> expected, String query) {
        TpcHTestSupport.assertMatchesReference(actual, expected, query, "value");
    }
}
