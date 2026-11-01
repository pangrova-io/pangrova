package org.pangrova.connector.iceberg.read;

import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;
import org.pangrova.spi.indexing.statistics.RowGroupSkipInfo;
import org.pangrova.spi.indexing.statistics.RowGroupStatistics;
import org.pangrova.spi.indexing.statistics.RowGroupStatistics.ColumnStats;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RowGroupPruner} — sub-file pruning using row-group-level
 * min/max statistics (zone maps).
 *
 * <p>Validates: G-INDEX-06 — row-group-level skip index / zone map support.</p>
 */
class RowGroupPrunerTest {

    // ── Simple predicate tests ─────────────────────────────────────────

    @Test
    void equalPredicate_skipsRowGroupWhenValueOutOfRange() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("id", colStats("id", 1, 100))),
                rowGroup(1, 1000, Map.of("id", colStats("id", 101, 200))),
                rowGroup(2, 1000, Map.of("id", colStats("id", 201, 300)))
        );

        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("id",
                        FileColumnStatistics.ComparisonOp.EQUAL, 150)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.totalRowGroups()).isEqualTo(3);
        assertThat(result.skippedCount()).isEqualTo(2);
        assertThat(result.shouldRead(0)).isFalse(); // 150 not in [1, 100]
        assertThat(result.shouldRead(1)).isTrue();  // 150 in [101, 200]
        assertThat(result.shouldRead(2)).isFalse(); // 150 not in [201, 300]
    }

    @Test
    void greaterThanPredicate_skipsRowGroupsBelowThreshold() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("price", colStats("price", 10.0, 50.0))),
                rowGroup(1, 1000, Map.of("price", colStats("price", 40.0, 90.0))),
                rowGroup(2, 1000, Map.of("price", colStats("price", 80.0, 120.0)))
        );

        // WHERE price > 80.0
        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("price",
                        FileColumnStatistics.ComparisonOp.GREATER_THAN, 80.0)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(1);
        assertThat(result.shouldRead(0)).isFalse(); // max=50 < 80
        assertThat(result.shouldRead(1)).isTrue();  // max=90 >= 80
        assertThat(result.shouldRead(2)).isTrue();  // max=120 > 80
    }

    @Test
    void lessThanPredicate_skipsRowGroupsAboveThreshold() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("age", colStats("age", 10, 25))),
                rowGroup(1, 1000, Map.of("age", colStats("age", 30, 45))),
                rowGroup(2, 1000, Map.of("age", colStats("age", 50, 65)))
        );

        // WHERE age < 30
        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("age",
                        FileColumnStatistics.ComparisonOp.LESS_THAN, 30)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(2);
        assertThat(result.shouldRead(0)).isTrue();  // min=10 < 30
        assertThat(result.shouldRead(1)).isFalse(); // min=30, not < 30
        assertThat(result.shouldRead(2)).isFalse(); // min=50, not < 30
    }

    @Test
    void greaterThanOrEqualPredicate_correctBoundaryBehavior() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("x", colStats("x", 10, 20))),
                rowGroup(1, 1000, Map.of("x", colStats("x", 20, 30)))
        );

        // WHERE x >= 20 — both row groups could contain x=20
        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("x",
                        FileColumnStatistics.ComparisonOp.GREATER_THAN_OR_EQUAL, 20)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(0);
        assertThat(result.shouldRead(0)).isTrue();  // max=20, could have x=20
        assertThat(result.shouldRead(1)).isTrue();  // min=20, could have x=20
    }

    // ── Range predicate tests ──────────────────────────────────────────

    @Test
    void rangePredicate_skipsNonOverlappingRowGroups() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("ts", colStats("ts", 100L, 200L))),
                rowGroup(1, 1000, Map.of("ts", colStats("ts", 300L, 400L))),
                rowGroup(2, 1000, Map.of("ts", colStats("ts", 500L, 600L)))
        );

        // WHERE ts BETWEEN 250 AND 350 (inclusive range)
        List<RowGroupPruner.RangePredicate> ranges = List.of(
                new RowGroupPruner.RangePredicate("ts", 250L, 350L, true, true)
        );

        RowGroupSkipInfo result = RowGroupPruner.pruneByRange("test.parquet", stats, ranges);

        assertThat(result.skippedCount()).isEqualTo(2);
        assertThat(result.shouldRead(0)).isFalse(); // max=200 < 250
        assertThat(result.shouldRead(1)).isTrue();  // overlaps [250, 350]
        assertThat(result.shouldRead(2)).isFalse(); // min=500 > 350
    }

    @Test
    void rangePredicate_exclusiveBounds() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("val", colStats("val", 10, 20))),
                rowGroup(1, 1000, Map.of("val", colStats("val", 20, 30))),
                rowGroup(2, 1000, Map.of("val", colStats("val", 30, 40)))
        );

        // WHERE val > 20 AND val < 30 (exclusive)
        List<RowGroupPruner.RangePredicate> ranges = List.of(
                new RowGroupPruner.RangePredicate("val", 20, 30, false, false)
        );

        RowGroupSkipInfo result = RowGroupPruner.pruneByRange("test.parquet", stats, ranges);

        assertThat(result.skippedCount()).isEqualTo(2);
        assertThat(result.shouldRead(0)).isFalse(); // max=20, exclusive lower
        assertThat(result.shouldRead(1)).isTrue();  // range [20,30] overlaps (20,30)
        assertThat(result.shouldRead(2)).isFalse(); // min=30, exclusive upper
    }

    @Test
    void rangePredicate_unboundedLower() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("val", colStats("val", 10, 20))),
                rowGroup(1, 1000, Map.of("val", colStats("val", 30, 40)))
        );

        // WHERE val < 25 (no lower bound)
        List<RowGroupPruner.RangePredicate> ranges = List.of(
                new RowGroupPruner.RangePredicate("val", null, 25, true, false)
        );

        RowGroupSkipInfo result = RowGroupPruner.pruneByRange("test.parquet", stats, ranges);

        assertThat(result.skippedCount()).isEqualTo(1);
        assertThat(result.shouldRead(0)).isTrue();  // min=10 < 25
        assertThat(result.shouldRead(1)).isFalse(); // min=30 >= 25
    }

    // ── Combined pruning tests ─────────────────────────────────────────

    @Test
    void combinedPruning_mergesSimpleAndRangePredicates() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of(
                        "id", colStats("id", 1, 100),
                        "status", colStats("status", "A", "C"))),
                rowGroup(1, 1000, Map.of(
                        "id", colStats("id", 101, 200),
                        "status", colStats("status", "D", "F"))),
                rowGroup(2, 1000, Map.of(
                        "id", colStats("id", 201, 300),
                        "status", colStats("status", "A", "B")))
        );

        // WHERE id BETWEEN 50 AND 150 AND status = 'E'
        List<RowGroupPruner.Predicate> simple = List.of(
                new RowGroupPruner.Predicate("status",
                        FileColumnStatistics.ComparisonOp.EQUAL, "E")
        );
        List<RowGroupPruner.RangePredicate> range = List.of(
                new RowGroupPruner.RangePredicate("id", 50, 150, true, true)
        );

        RowGroupSkipInfo result = RowGroupPruner.pruneAll(
                "test.parquet", stats, simple, range);

        // rg0: status E not in [A,C] -> skip by simple
        // rg1: id [101,200] overlaps [50,150], status E in [D,F] -> keep
        // rg2: id [201,300] not in [50,150] -> skip by range
        assertThat(result.skippedCount()).isEqualTo(2);
        assertThat(result.shouldRead(0)).isFalse();
        assertThat(result.shouldRead(1)).isTrue();
        assertThat(result.shouldRead(2)).isFalse();
    }

    // ── Conservative behavior tests ────────────────────────────────────

    @Test
    void missingColumnStats_rowGroupRetained() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("id", colStats("id", 1, 100))),
                rowGroup(1, 1000, Map.of()) // no stats for any column
        );

        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("id",
                        FileColumnStatistics.ComparisonOp.EQUAL, 500)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(1);
        assertThat(result.shouldRead(0)).isFalse(); // 500 not in [1, 100]
        assertThat(result.shouldRead(1)).isTrue();  // no stats -> conservative keep
    }

    @Test
    void nullStatsBounds_rowGroupRetained() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("id", new ColumnStats("id", null, null, 500)))
        );

        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("id",
                        FileColumnStatistics.ComparisonOp.EQUAL, 50)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(0);
        assertThat(result.shouldRead(0)).isTrue(); // null bounds -> conservative
    }

    @Test
    void emptyInputs_noPruning() {
        assertThat(RowGroupPruner.prune("test.parquet", List.of(), List.of(
                new RowGroupPruner.Predicate("id",
                        FileColumnStatistics.ComparisonOp.EQUAL, 1)
        ))).isEqualTo(RowGroupSkipInfo.NONE);

        assertThat(RowGroupPruner.prune("test.parquet",
                List.of(rowGroup(0, 100, Map.of())),
                List.of())).isEqualTo(RowGroupSkipInfo.NONE);

        assertThat(RowGroupPruner.prune("test.parquet", null, null))
                .isEqualTo(RowGroupSkipInfo.NONE);
    }

    @Test
    void noRowGroupsSkipped_allRetained() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("id", colStats("id", 1, 1000))),
                rowGroup(1, 1000, Map.of("id", colStats("id", 1, 1000)))
        );

        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("id",
                        FileColumnStatistics.ComparisonOp.EQUAL, 500)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(0);
        assertThat(result.hasSkips()).isFalse();
        assertThat(result.retainedCount()).isEqualTo(2);
    }

    // ── RowGroupSkipInfo tests ─────────────────────────────────────────

    @Test
    void skipInfo_retainedIndices() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 100, Map.of("id", colStats("id", 1, 10))),
                rowGroup(1, 100, Map.of("id", colStats("id", 11, 20))),
                rowGroup(2, 100, Map.of("id", colStats("id", 21, 30))),
                rowGroup(3, 100, Map.of("id", colStats("id", 31, 40)))
        );

        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("id",
                        FileColumnStatistics.ComparisonOp.EQUAL, 15)
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.retainedIndices()).containsExactly(1);
        assertThat(result.skippedCount()).isEqualTo(3);
        assertThat(result.retainedCount()).isEqualTo(1);
    }

    @Test
    void stringColumnPruning_worksWithLexicographicComparison() {
        List<RowGroupStatistics> stats = List.of(
                rowGroup(0, 1000, Map.of("name", colStats("name", "Alice", "Charlie"))),
                rowGroup(1, 1000, Map.of("name", colStats("name", "Dave", "Frank"))),
                rowGroup(2, 1000, Map.of("name", colStats("name", "George", "Ivan")))
        );

        // WHERE name = 'Eve'
        List<RowGroupPruner.Predicate> predicates = List.of(
                new RowGroupPruner.Predicate("name",
                        FileColumnStatistics.ComparisonOp.EQUAL, "Eve")
        );

        RowGroupSkipInfo result = RowGroupPruner.prune("test.parquet", stats, predicates);

        assertThat(result.skippedCount()).isEqualTo(2);
        assertThat(result.shouldRead(0)).isFalse(); // "Eve" > "Charlie"
        assertThat(result.shouldRead(1)).isTrue();  // "Eve" in ["Dave", "Frank"]
        assertThat(result.shouldRead(2)).isFalse(); // "Eve" < "George"
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private static RowGroupStatistics rowGroup(
            int index, long rowCount, Map<String, ColumnStats> stats) {
        return new RowGroupStatistics(index, rowCount, stats);
    }

    private static ColumnStats colStats(String name, Comparable<?> min, Comparable<?> max) {
        return new ColumnStats(name, min, max, 0);
    }
}
