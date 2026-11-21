package org.pangrova.testing.integration.e2e.generator;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for SqlGenerators.
 */
class SqlGeneratorsTest {

    @Property(tries = 100)
    void wherePredicatesProducesNonBlankSql(
            @ForAll("predicates") String predicate) {
        assertThat(predicate).isNotBlank();
        assertThat(predicate).doesNotContain("null")
                .as("Predicate should use SQL NULL keyword, not Java null");
    }

    @Property(tries = 100)
    void tpchColumnNamesReturnsValidColumns(
            @ForAll("ordersColumns") String column) {
        assertThat(column).startsWith("o_");
    }

    @Property(tries = 100)
    void orderByClausesContainsDirection(
            @ForAll("ordersOrderBy") String clause) {
        assertThat(clause).containsAnyOf("ASC", "DESC");
    }

    @Property(tries = 100)
    void limitValuesInRange(@ForAll("limits") int limit) {
        assertThat(limit).isBetween(1, 1000);
    }

    @Provide
    Arbitrary<String> predicates() {
        return SqlGenerators.wherePredicates();
    }

    @Provide
    Arbitrary<String> ordersColumns() {
        return SqlGenerators.tpchColumnNames("orders");
    }

    @Provide
    Arbitrary<String> ordersOrderBy() {
        return SqlGenerators.orderByClauses("orders");
    }

    @Provide
    Arbitrary<Integer> limits() {
        return SqlGenerators.limitValues();
    }
}
