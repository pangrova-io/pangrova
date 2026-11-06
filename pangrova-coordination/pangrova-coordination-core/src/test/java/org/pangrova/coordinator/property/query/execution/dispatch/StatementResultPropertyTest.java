package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.List;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;

import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.types.value.TypedRow;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for StatementResult immutability.
 *
 * <p><b>Validates: Requirement 2.6</b></p>
 */
class StatementResultPropertyTest {

    /**
     * Property 7: StatementResult immutability — rows().
     * For any StatementResult, rows() must reject add/remove/set mutations.
     */
    @Property(tries = 30)
    void rowsListIsImmutable(@ForAll("statementResults") StatementResult result) {
        List<TypedRow> rows = result.rows();

        assertThatThrownBy(() -> rows.add(TypedRow.builder().putString("x", "y").build()))
                .isInstanceOf(UnsupportedOperationException.class);

        if (!rows.isEmpty()) {
            assertThatThrownBy(() -> rows.remove(0))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> rows.set(0, TypedRow.builder().putString("x", "y").build()))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    /**
     * Property 7 (continued): StatementResult immutability — columnNames().
     * For any StatementResult, columnNames() must reject add/remove/set mutations.
     */
    @Property(tries = 30)
    void columnNamesListIsImmutable(@ForAll("statementResults") StatementResult result) {
        List<String> names = result.columnNames();

        assertThatThrownBy(() -> names.add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);

        if (!names.isEmpty()) {
            assertThatThrownBy(() -> names.remove(0))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> names.set(0, "replaced"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Provide
    Arbitrary<StatementResult> statementResults() {
        Arbitrary<List<TypedRow>> rowsArb = Arbitraries.integers().between(0, 3)
                .map(count -> {
                    java.util.ArrayList<TypedRow> list = new java.util.ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        list.add(TypedRow.builder()
                                .putString("col" + i, "val" + i)
                                .putInt("num", i)
                                .build());
                    }
                    return list;
                });

        Arbitrary<List<String>> colNamesArb = Arbitraries.integers().between(0, 4)
                .map(count -> {
                    java.util.ArrayList<String> list = new java.util.ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        list.add("col" + i);
                    }
                    return list;
                });

        Arbitrary<StatementCategory> categoryArb = Arbitraries.of(StatementCategory.values());

        Arbitrary<String> tableArb = Arbitraries.of("catalog.schema.table", null);

        return Combinators.combine(rowsArb, colNamesArb, tableArb, categoryArb)
                .as(StatementResult::new);
    }
}
