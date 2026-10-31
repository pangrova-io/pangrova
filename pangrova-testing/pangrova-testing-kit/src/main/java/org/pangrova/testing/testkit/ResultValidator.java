package org.pangrova.testing.testkit;

import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates query results by comparing expected vs actual {@link TypedRow} sets.
 *
 * <p>Provides ordered and unordered comparison with detailed mismatch reporting
 * including row index and column name.
 */
public final class ResultValidator {

    private ResultValidator() {}

    /**
     * Assert that expected and actual row lists are equal (ordered comparison).
     *
     * @param expected expected rows; must not be null
     * @param actual   actual rows; must not be null
     * @throws AssertionError if row count or any cell value differs
     */
    public static void assertEquals(List<TypedRow> expected, List<TypedRow> actual) {
        assertEqualsOrdered(expected, actual);
    }

    /**
     * Assert that expected and actual row lists are equal in order.
     *
     * @param expected expected rows; must not be null
     * @param actual   actual rows; must not be null
     * @throws AssertionError if row count or any cell value differs
     */
    public static void assertEqualsOrdered(List<TypedRow> expected, List<TypedRow> actual) {
        Objects.requireNonNull(expected, "expected must not be null");
        Objects.requireNonNull(actual, "actual must not be null");
        assertSizeMatch(expected, actual);
        for (int i = 0; i < expected.size(); i++) {
            assertRowMatch(expected.get(i), actual.get(i), i);
        }
    }

    /**
     * Assert that expected and actual row lists contain the same rows (order-independent).
     *
     * @param expected expected rows; must not be null
     * @param actual   actual rows; must not be null
     * @throws AssertionError if row count or any cell value differs after sorting
     */
    public static void assertEqualsUnordered(List<TypedRow> expected, List<TypedRow> actual) {
        Objects.requireNonNull(expected, "expected must not be null");
        Objects.requireNonNull(actual, "actual must not be null");
        assertSizeMatch(expected, actual);
        List<TypedRow> sortedExpected = sortRows(expected);
        List<TypedRow> sortedActual = sortRows(actual);
        assertEqualsOrdered(sortedExpected, sortedActual);
    }

    /**
     * Assert that expected and actual row lists contain the same rows (order-independent)
     * with tolerance for floating-point comparisons.
     *
     * <p>Numeric values are compared with the given relative tolerance to handle
     * ULP-level differences from non-deterministic partial aggregation order in
     * distributed/native execution engines.
     *
     * @param expected  expected rows; must not be null
     * @param actual    actual rows; must not be null
     * @param tolerance relative tolerance for numeric comparisons (e.g. 1e-6)
     * @throws AssertionError if row count or any cell value differs beyond tolerance
     */
    public static void assertEqualsUnorderedWithTolerance(
            List<TypedRow> expected, List<TypedRow> actual, double tolerance) {
        Objects.requireNonNull(expected, "expected must not be null");
        Objects.requireNonNull(actual, "actual must not be null");
        assertSizeMatch(expected, actual);
        List<TypedRow> sortedExpected = sortRows(expected);
        List<TypedRow> sortedActual = sortRows(actual);
        for (int i = 0; i < sortedExpected.size(); i++) {
            assertRowMatchWithTolerance(sortedExpected.get(i), sortedActual.get(i), i, tolerance);
        }
    }

    private static void assertSizeMatch(List<TypedRow> expected, List<TypedRow> actual) {
        if (expected.size() != actual.size()) {
            throw new AssertionError(
                "Row count mismatch: expected " + expected.size()
                    + " but got " + actual.size());
        }
    }

    private static void assertRowMatch(TypedRow expectedRow, TypedRow actualRow, int rowIndex) {
        for (String column : expectedRow.columnNames()) {
            ColumnValue expectedVal = expectedRow.get(column);
            ColumnValue actualVal = actualRow.get(column);
            if (!Objects.equals(expectedVal, actualVal)) {
                throw new AssertionError(
                    "Mismatch at row " + rowIndex + ", column '"
                        + column + "': expected "
                        + expectedVal + " but got " + actualVal);
            }
        }
    }

    private static void assertRowMatchWithTolerance(
            TypedRow expectedRow, TypedRow actualRow, int rowIndex, double tolerance) {
        for (String column : expectedRow.columnNames()) {
            Object expectedRaw = expectedRow.getRaw(column);
            Object actualRaw = actualRow.getRaw(column);
            if (!valuesEqualWithTolerance(expectedRaw, actualRaw, tolerance)) {
                throw new AssertionError(
                    "Mismatch at row " + rowIndex + ", column '"
                        + column + "': expected "
                        + expectedRaw + " but got " + actualRaw);
            }
        }
    }

    private static boolean valuesEqualWithTolerance(Object expected, Object actual, double tolerance) {
        if (Objects.equals(expected, actual)) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }
        if (expected instanceof Number && actual instanceof Number) {
            return numbersEqualWithTolerance((Number) expected, (Number) actual, tolerance);
        }
        return expected.equals(actual);
    }

    private static boolean numbersEqualWithTolerance(Number expected, Number actual, double tolerance) {
        double e = expected.doubleValue();
        double a = actual.doubleValue();
        if (Double.isNaN(e) && Double.isNaN(a)) {
            return true;
        }
        double diff = Math.abs(e - a);
        double magnitude = Math.max(Math.abs(e), Math.abs(a));
        return magnitude == 0.0 ? diff == 0.0 : diff / magnitude <= tolerance;
    }

    private static List<TypedRow> sortRows(List<TypedRow> rows) {
        if (rows.isEmpty()) return List.of();
        // Use a stable column order for comparison: sort column names alphabetically
        // to avoid non-determinism from varying column insertion order across runs.
        List<String> sortedCols = rows.get(0).columnNames().stream().sorted().toList();
        List<TypedRow> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> {
            for (String col : sortedCols) {
                Object va = a.getRaw(col);
                Object vb = b.getRaw(col);
                int cmp = compareValues(va, vb);
                if (cmp != 0) return cmp;
            }
            return 0;
        });
        return sorted;
    }

    private static int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return compareNonNullValues(a, b);
    }

    private static int compareNonNullValues(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(
                    ((Number) a).doubleValue(),
                    ((Number) b).doubleValue());
        }
        if (a.getClass() == b.getClass() && a instanceof Comparable<?>) {
            @SuppressWarnings("unchecked")
            Comparable<Object> ca = (Comparable<Object>) a;
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }
}
