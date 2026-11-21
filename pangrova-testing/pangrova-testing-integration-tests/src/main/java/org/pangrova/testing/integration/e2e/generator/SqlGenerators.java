package org.pangrova.testing.integration.e2e.generator;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;
import org.pangrova.testing.testkit.TestDataGenerator;

import java.util.List;

/**
 * jqwik generators for random SQL fragments used in property-based testing.
 */
public final class SqlGenerators {

    private static final int NUMERIC_EQUALITY_MAX = 10000;
    private static final int NUMERIC_RANGE_MAX = 5000;

    private static final List<String> ORDERS_STRING_COLUMNS =
            List.of("o_orderstatus", "o_orderpriority", "o_clerk", "o_comment");

    private static final List<String> ORDERS_NUMERIC_COLUMNS =
            List.of("o_orderkey", "o_custkey", "o_totalprice", "o_shippriority");

    private static final List<String> SAMPLE_STRING_VALUES =
            List.of("F", "O", "P", "1-URGENT", "2-HIGH", "val_100");

    private SqlGenerators() {}

    /** Generates predicates for property-based SQL tests. */
    @Provide
    public static Arbitrary<String> wherePredicates() {
        return Arbitraries.oneOf(
                equalityPredicate(),
                rangePredicate(),
                inPredicate(),
                likePredicate(),
                isNullPredicate(),
                compoundPredicate()
        );
    }

    /** Generates a column name from the requested TPC-H table. */
    @Provide
    public static Arbitrary<String> tpchColumnNames(String tableName) {
        List<String> columns = TestDataGenerator.getSchemas()
                .get(tableName).columns().stream()
                .map(TestDataGenerator.ColumnDef::name)
                .toList();
        return Arbitraries.of(columns);
    }

    @Provide
    public static Arbitrary<String> orderByClauses(String tableName) {
        return tpchColumnNames(tableName).flatMap(col ->
                Arbitraries.of(" ASC", " DESC").map(dir -> col + dir));
    }

    @Provide
    public static Arbitrary<Integer> limitValues() {
        return Arbitraries.integers().between(1, 1000);
    }

    private static Arbitrary<String> equalityPredicate() {
        return Arbitraries.oneOf(
                stringEqualityPredicate(),
                numericEqualityPredicate()
        );
    }

    private static Arbitrary<String> stringEqualityPredicate() {
        return Arbitraries.of(ORDERS_STRING_COLUMNS).flatMap(col ->
                Arbitraries.of(SAMPLE_STRING_VALUES).map(val ->
                        col + " = '" + val + "'"));
    }

    private static Arbitrary<String> numericEqualityPredicate() {
        return Arbitraries.of(ORDERS_NUMERIC_COLUMNS).flatMap(col ->
                Arbitraries.integers().between(1, NUMERIC_EQUALITY_MAX).map(val ->
                        col + " = " + val));
    }

    private static Arbitrary<String> rangePredicate() {
        return Arbitraries.of(ORDERS_NUMERIC_COLUMNS).flatMap(col ->
                Arbitraries.integers().between(1, NUMERIC_RANGE_MAX).flatMap(lo ->
                        Arbitraries.integers().between(lo + 1, lo + NUMERIC_RANGE_MAX).map(hi ->
                                col + " > " + lo + " AND " + col + " < " + hi)));
    }

    private static Arbitrary<String> inPredicate() {
        return Arbitraries.of(ORDERS_STRING_COLUMNS).flatMap(col ->
                Arbitraries.of(SAMPLE_STRING_VALUES).list().ofMinSize(1).ofMaxSize(4)
                        .map(vals -> col + " IN ("
                                + String.join(", ", vals.stream().map(v -> "'" + v + "'").toList())
                                + ")"));
    }

    private static Arbitrary<String> likePredicate() {
        return Arbitraries.of(ORDERS_STRING_COLUMNS).flatMap(col ->
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(5).map(prefix ->
                        col + " LIKE '" + prefix + "%'"));
    }

    private static Arbitrary<String> isNullPredicate() {
        return Arbitraries.of(ORDERS_STRING_COLUMNS).flatMap(col ->
                Arbitraries.of(" IS NULL", " IS NOT NULL").map(suffix -> col + suffix));
    }

    private static Arbitrary<String> compoundPredicate() {
        Arbitrary<String> simple = Arbitraries.oneOf(
                equalityPredicate(),
                rangePredicate(),
                isNullPredicate()
        );
        return simple.flatMap(left ->
                simple.flatMap(right ->
                        Arbitraries.of(" AND ", " OR ").map(op ->
                                left + op + right)));
    }
}
