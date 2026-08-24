package org.pangrova.worker.execution.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v1.ColumnStatisticsInfo;
import org.pangrova.protocol.v1.SplitConstraintInfo;
import org.pangrova.protocol.v1.SplitInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies constraint-based split filtering using partition values and
 * file-level column statistics from SplitConstraintInfo.
 *
 * <p>Validates: Requirement A.8 — Split objects carry Constraint predicates
 * (partition values, file-level min/max) so the execution engine can skip
 * irrelevant data without re-evaluating filters.</p>
 */
class ConstraintBasedSplitFilterTest {

    @Test
    void partitionFilter_skipsSplitsOutsideRange() {
        SplitInfo matchingSplit = buildSplitWithPartitionConstraint("s1", "country", "US");
        SplitInfo nonMatchingSplit = buildSplitWithPartitionConstraint("s2", "country", "JP");
        SplitInfo alsoMatching = buildSplitWithPartitionConstraint("s3", "country", "UK");

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "f1", "MIN_MAX",
                "country|UA|US".getBytes(StandardCharsets.UTF_8),
                "0", "country", 1);

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(matchingSplit, nonMatchingSplit, alsoMatching),
                        List.of(filter), "q-test", 0);

        assertThat(result.skippedSplits()).isEqualTo(1);
        assertThat(result.retainedSplits()).hasSize(2);
        assertThat(result.retainedSplits().stream().map(SplitInfo::getSplitId).toList())
                .containsExactly("s1", "s3");
    }

    @Test
    void columnStatistics_skipsSplitsWithNonOverlappingRange() {
        SplitInfo splitInRange = buildSplitWithColumnStats("s1", "amount", "100", "500");
        SplitInfo splitOutOfRange = buildSplitWithColumnStats("s2", "amount", "600", "900");
        SplitInfo splitPartialOverlap = buildSplitWithColumnStats("s3", "amount", "400", "700");

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "f1", "MIN_MAX",
                "amount|200|550".getBytes(StandardCharsets.UTF_8),
                "0", "amount", 1);

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(splitInRange, splitOutOfRange, splitPartialOverlap),
                        List.of(filter), "q-stats", 0);

        assertThat(result.skippedSplits()).isEqualTo(1);
        assertThat(result.retainedSplits()).hasSize(2);
        assertThat(result.retainedSplits().stream().map(SplitInfo::getSplitId).toList())
                .containsExactly("s1", "s3");
    }

    @Test
    void noConstraint_allSplitsRetained() {
        SplitInfo splitNoConstraint = SplitInfo.newBuilder()
                .setSplitId("s1")
                .setConnectorId("iceberg")
                .setFilePath("/data/s1.parquet")
                .build();

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "f1", "MIN_MAX",
                "col|10|20".getBytes(StandardCharsets.UTF_8),
                "0", "col", 1);

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(splitNoConstraint), List.of(filter), "q-none", 0);

        assertThat(result.skippedSplits()).isEqualTo(0);
        assertThat(result.retainedSplits()).hasSize(1);
    }

    @Test
    void noFilters_allSplitsRetained() {
        SplitInfo split = buildSplitWithPartitionConstraint("s1", "region", "EU");

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(split), List.of(), "q-empty", 0);

        assertThat(result.skippedSplits()).isEqualTo(0);
        assertThat(result.retainedSplits()).hasSize(1);
    }

    @Test
    void nullFilters_allSplitsRetained() {
        SplitInfo split = buildSplitWithPartitionConstraint("s1", "region", "EU");

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(split), null, "q-null", 0);

        assertThat(result.skippedSplits()).isEqualTo(0);
        assertThat(result.retainedSplits()).hasSize(1);
    }

    @Test
    void multipleFilters_allMustPass() {
        SplitInfo split = SplitInfo.newBuilder()
                .setSplitId("s1")
                .setConnectorId("iceberg")
                .setFilePath("/data/s1.parquet")
                .setSplitConstraint(SplitConstraintInfo.newBuilder()
                        .putPartitionValues("year", "2024")
                        .addColumnStatistics(ColumnStatisticsInfo.newBuilder()
                                .setColumnName("amount")
                                .setMinValue("100")
                                .setMaxValue("500")
                                .build())
                        .build())
                .build();

        DynamicFilterPayload yearFilter = new DynamicFilterPayload(
                "f1", "MIN_MAX",
                "year|2023|2025".getBytes(StandardCharsets.UTF_8),
                "0", "year", 1);
        DynamicFilterPayload amountFilter = new DynamicFilterPayload(
                "f2", "MIN_MAX",
                "amount|600|900".getBytes(StandardCharsets.UTF_8),
                "0", "amount", 1);

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(split), List.of(yearFilter, amountFilter), "q-multi", 0);

        // year passes (2024 in [2023,2025]) but amount fails (split max 500 < filter min 600)
        assertThat(result.skippedSplits()).isEqualTo(1);
        assertThat(result.retainedSplits()).isEmpty();
    }

    @Test
    void bloomFilter_ignoredByConstraintFilter() {
        SplitInfo split = buildSplitWithPartitionConstraint("s1", "region", "EU");

        DynamicFilterPayload bloomFilter = new DynamicFilterPayload(
                "f1", "BLOOM",
                new byte[]{1, 2, 3},
                "0", "region", 1);

        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(
                        List.of(split), List.of(bloomFilter), "q-bloom", 0);

        // BLOOM filters are not evaluated by ConstraintBasedSplitFilter
        assertThat(result.skippedSplits()).isEqualTo(0);
        assertThat(result.retainedSplits()).hasSize(1);
    }

    private static SplitInfo buildSplitWithPartitionConstraint(
            String splitId, String partitionKey, String partitionValue) {
        return SplitInfo.newBuilder()
                .setSplitId(splitId)
                .setConnectorId("iceberg")
                .setFilePath("/data/" + splitId + ".parquet")
                .putPartitionValues(partitionKey, partitionValue)
                .setSplitConstraint(SplitConstraintInfo.newBuilder()
                        .putPartitionValues(partitionKey, partitionValue)
                        .build())
                .build();
    }

    private static SplitInfo buildSplitWithColumnStats(
            String splitId, String column, String min, String max) {
        return SplitInfo.newBuilder()
                .setSplitId(splitId)
                .setConnectorId("iceberg")
                .setFilePath("/data/" + splitId + ".parquet")
                .setSplitConstraint(SplitConstraintInfo.newBuilder()
                        .addColumnStatistics(ColumnStatisticsInfo.newBuilder()
                                .setColumnName(column)
                                .setMinValue(min)
                                .setMaxValue(max)
                                .build())
                        .build())
                .build();
    }
}
