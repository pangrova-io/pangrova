package org.pangrova.connector.api.index.statistics;

import org.pangrova.connector.api.enums.ColumnComparisonOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

/**
 * Tests for {@link FileColumnStatistics} pruning logic, focusing on:
 * <ul>
 *   <li>F-NATIVE-WRITE-07: null/missing min/max must never cause incorrect pruning</li>
 *   <li>Type mismatch between statistics and predicates must be conservative (no pruning)</li>
 * </ul>
 */
class FileColumnStatisticsTest {

    // ── Null min/max: never prune ──────────────────────────────────────

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void noRowsSatisfy_nullMinValue_neverPrunes(ColumnComparisonOperator op) {
        var stats = new FileColumnStatistics("col", null, 100L, 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(op, 50L),
                "null minValue must never prune for " + op);
    }

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void noRowsSatisfy_nullMaxValue_neverPrunes(ColumnComparisonOperator op) {
        var stats = new FileColumnStatistics("col", 0L, null, 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(op, 50L),
                "null maxValue must never prune for " + op);
    }

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void noRowsSatisfy_bothNull_neverPrunes(ColumnComparisonOperator op) {
        var stats = new FileColumnStatistics("col", null, null, 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(op, 50L),
                "null min+max must never prune for " + op);
    }

    // ── Type mismatch: never prune (F-NATIVE-WRITE-07) ─────────────────

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void noRowsSatisfy_stringStatsWithLongPredicate_neverPrunes(ColumnComparisonOperator op) {
        // Simulates JDBC path: min/max stored as strings from DB
        var stats = new FileColumnStatistics("col", "10", "200", 0, 100, 10, 1024);
        // Long predicate vs String stats => ClassCastException => conservative false
        assertFalse(stats.noRowsSatisfy(op, 50L),
                "String stats with Long predicate must never prune for " + op);
    }

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void noRowsSatisfy_stringStatsWithDoublePredicate_neverPrunes(ColumnComparisonOperator op) {
        var stats = new FileColumnStatistics("col", "10.5", "200.5", 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(op, 50.0),
                "String stats with Double predicate must never prune for " + op);
    }

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void allRowsSatisfy_nullStats_neverAsserts(ColumnComparisonOperator op) {
        var stats = new FileColumnStatistics("col", null, null, 0, 100, 10, 1024);
        assertFalse(stats.allRowsSatisfy(op, 50L),
                "null stats must never assert all match for " + op);
    }

    @ParameterizedTest
    @EnumSource(ColumnComparisonOperator.class)
    void allRowsSatisfy_typeMismatch_neverAsserts(ColumnComparisonOperator op) {
        var stats = new FileColumnStatistics("col", "10", "200", 0, 100, 10, 1024);
        assertFalse(stats.allRowsSatisfy(op, 50L),
                "Type mismatch must never assert all match for " + op);
    }

    // ── Valid stats: correct pruning ───────────────────────────────────

    @Test
    void noRowsSatisfy_equal_outsideRange_prunes() {
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 5L),
                "value below min should prune");
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 105L),
                "value above max should prune");
    }

    @Test
    void noRowsSatisfy_equal_insideRange_doesNotPrune() {
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 50L),
                "value inside range should not prune");
    }

    @Test
    void noRowsSatisfy_greaterThan_allBelowQuery_prunes() {
        // File has values in [10, 100]. Query: col > 100. No rows satisfy.
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.GREATER_THAN, 100L));
    }

    @Test
    void noRowsSatisfy_greaterThan_someAboveQuery_doesNotPrune() {
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.GREATER_THAN, 50L));
    }

    @Test
    void noRowsSatisfy_lessThan_allAboveQuery_prunes() {
        // File has values in [10, 100]. Query: col < 10. No rows satisfy.
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.LESS_THAN, 10L));
    }

    @Test
    void noRowsSatisfy_lessThan_someBelowQuery_doesNotPrune() {
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.LESS_THAN, 50L));
    }

    // ── Properly typed stats from JDBC parse path ──────────────────────

    @Test
    void noRowsSatisfy_longStatsFromJdbcParse_correctPruning() {
        // Simulates correctly parsed JDBC values (Long, not String)
        var stats = new FileColumnStatistics("col", 10L, 100L, 0, 100, 10, 1024);
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 5L));
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 50L));
    }

    @Test
    void noRowsSatisfy_doubleStatsWithDoublePredicate_correctPruning() {
        var stats = new FileColumnStatistics("col", 1.5, 9.5, 0, 100, 10, 1024);
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 0.5));
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 5.0));
    }

    @Test
    void stringBoundsUseUtf8SqlOrdering() {
        String bmp = "\uE000";
        String supplementary = "\uD800\uDC00";
        var stats = new FileColumnStatistics("col", bmp, supplementary, 0, 2, 2, 16);
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, supplementary));
        assertFalse(stats.noRowsSatisfy(ColumnComparisonOperator.GREATER_THAN, bmp));
        assertTrue(stats.noRowsSatisfy(ColumnComparisonOperator.LESS_THAN, bmp));
        assertTrue(stats.allRowsSatisfy(ColumnComparisonOperator.LESS_THAN_OR_EQUAL, supplementary));
    }

    @Test
    void invalidUnicodeOrInconsistentBoundsCannotProvePruning() {
        var invalid = new FileColumnStatistics("col", "\uD800", "z", 0, 1, 1, 8);
        assertFalse(invalid.noRowsSatisfy(ColumnComparisonOperator.EQUAL, "a"));
        assertFalse(invalid.allRowsSatisfy(ColumnComparisonOperator.LESS_THAN, "z"));
        var backwards = new FileColumnStatistics("col", "z", "a", 0, 1, 1, 8);
        assertFalse(backwards.noRowsSatisfy(ColumnComparisonOperator.EQUAL, "m"));
        assertFalse(backwards.allRowsSatisfy(ColumnComparisonOperator.LESS_THAN, "z"));
    }

    @Test
    void signedZeroAndUnknownNanCoverageRemainConservative() {
        var doubles = new FileColumnStatistics("col", -0.0d, -0.0d, 0, 1, 1, 8);
        var floats = new FileColumnStatistics("col", 0.0f, 0.0f, 0, 1, 1, 8);
        assertFalse(doubles.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 0.0d));
        assertFalse(floats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, -0.0f));
        assertFalse(doubles.noRowsSatisfy(ColumnComparisonOperator.EQUAL, Double.NaN));
        assertFalse(doubles.noRowsSatisfy(ColumnComparisonOperator.GREATER_THAN, 1.0d));
        assertFalse(doubles.allRowsSatisfy(ColumnComparisonOperator.EQUAL, 0.0d));
        assertTrue(doubles.noRowsSatisfy(ColumnComparisonOperator.EQUAL, 1.0d));
    }
}
