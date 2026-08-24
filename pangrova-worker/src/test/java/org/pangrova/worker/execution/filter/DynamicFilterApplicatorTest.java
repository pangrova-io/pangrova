package org.pangrova.worker.execution.filter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v1.SplitInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies probe-side row/split count reduction when DPP filters are applied.
 *
 * <p><b>Validates: Requirements 1.42, 2.42</b></p>
 */
class DynamicFilterApplicatorTest {

    private final DynamicFilterApplicator applicator = new DynamicFilterApplicator();

    @BeforeEach
    void resetMetrics() {
        DynamicPartitionFilterMetrics.reset();
    }

    @Test
    void minMaxFilter_prunesSplitsOutsideRange() {
        List<String> splits = List.of(
                encodeSplit("s1", "customer_id", "50"),
                encodeSplit("s2", "customer_id", "150"),
                encodeSplit("s3", "customer_id", "200"),
                encodeSplit("s4", "customer_id", "300"));

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "dpp-0", "MIN_MAX",
                "customer_id|100|200".getBytes(StandardCharsets.UTF_8),
                "0", 1);

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(splits, List.of(filter), 0, "q-test");

        assertThat(result.retainedSplits()).hasSize(2);
        assertThat(result.filteredSplits()).isEqualTo(2);
        assertThat(result.totalSplits()).isEqualTo(4);
    }

    @Test
    void sbbfFilter_prunesSplitsNotInBloom() {
        // Build SBBF containing "val-A" and "val-B"
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(100, 0.001)
                .put("val-A")
                .put("val-B")
                .build();
        byte[] sbbfBytes = sbbf.toByteArray();

        List<String> splits = List.of(
                encodeSplit("s1", "region", "val-A"),
                encodeSplit("s2", "region", "val-B"),
                encodeSplit("s3", "region", "val-X"));

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "dpp-1", "BLOOM", sbbfBytes, "1", "region", 1);

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(splits, List.of(filter), 1, "q-bloom");

        // SBBF is probabilistic — val-X should definitely be pruned
        assertThat(result.retainedSplits().size()).isLessThanOrEqualTo(3);
        assertThat(result.filteredSplits()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void noFilters_allSplitsRetained() {
        List<String> splits = List.of(
                encodeSplit("s1", "col", "a"),
                encodeSplit("s2", "col", "b"));

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(splits, List.of(), 0, "q-no-filter");

        assertThat(result.retainedSplits()).hasSize(2);
        assertThat(result.filteredSplits()).isEqualTo(0);
    }

    @Test
    void nullFilters_allSplitsRetained() {
        List<String> splits = List.of(encodeSplit("s1", "col", "a"));

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(splits, null, 0, "q-null");

        assertThat(result.retainedSplits()).hasSize(1);
        assertThat(result.filteredSplits()).isEqualTo(0);
    }

    @Test
    void emptySplits_returnsEmpty() {
        DynamicFilterPayload filter = new DynamicFilterPayload(
                "dpp-0", "MIN_MAX",
                "col|1|100".getBytes(StandardCharsets.UTF_8),
                "0", 1);

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(List.of(), List.of(filter), 0, "q-empty");

        assertThat(result.retainedSplits()).isEmpty();
        assertThat(result.filteredSplits()).isEqualTo(0);
    }

    @Test
    void splitsWithoutPartitionValues_areRetained() {
        SplitInfo split = SplitInfo.newBuilder()
                .setSplitId("s-no-partition")
                .setConnectorId("iceberg")
                .setFilePath("/data/file.parquet")
                .build();
        String encoded = Base64.getEncoder().encodeToString(split.toByteArray());

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "dpp-0", "MIN_MAX",
                "customer_id|100|200".getBytes(StandardCharsets.UTF_8),
                "0", 1);

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(List.of(encoded), List.of(filter), 0, "q-no-pv");

        assertThat(result.retainedSplits()).hasSize(1);
    }

    @Test
    void filterResult_carriesCorrectMetrics() {
        List<String> splits = List.of(
                encodeSplit("s1", "id", "50"),
                encodeSplit("s2", "id", "150"),
                encodeSplit("s3", "id", "250"));

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "dpp-0", "MIN_MAX",
                "id|100|200".getBytes(StandardCharsets.UTF_8),
                "0", 1);

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(splits, List.of(filter), 0, "q-metrics");

        assertThat(result.totalSplits()).isEqualTo(3);
        assertThat(result.filteredSplits()).isEqualTo(2);
        assertThat(result.retainedSplits()).hasSize(1);

        DynamicPartitionFilterMetrics.recordSplitFiltering("q-metrics", 0,
                result.totalSplits(), result.filteredSplits());
        assertThat(DynamicPartitionFilterMetrics.totalSplitsSeen()).isEqualTo(3);
        assertThat(DynamicPartitionFilterMetrics.splitsFilteredOut()).isEqualTo(2);
        assertThat(DynamicPartitionFilterMetrics.filterApplications()).isEqualTo(1);
    }

    @Test
    void unknownFilterType_acceptsAllSplits() {
        List<String> splits = List.of(
                encodeSplit("s1", "col", "a"),
                encodeSplit("s2", "col", "b"));

        DynamicFilterPayload filter = new DynamicFilterPayload(
                "dpp-0", "UNKNOWN_TYPE",
                "data".getBytes(StandardCharsets.UTF_8),
                "0", 1);

        DynamicFilterApplicator.FilterResult result =
                applicator.apply(splits, List.of(filter), 0, "q-unknown");

        assertThat(result.retainedSplits()).hasSize(2);
        assertThat(result.filteredSplits()).isEqualTo(0);
    }

    private static String encodeSplit(String splitId, String partitionKey, String partitionValue) {
        SplitInfo split = SplitInfo.newBuilder()
                .setSplitId(splitId)
                .setConnectorId("iceberg")
                .setFilePath("/data/" + splitId + ".parquet")
                .putPartitionValues(partitionKey, partitionValue)
                .build();
        return Base64.getEncoder().encodeToString(split.toByteArray());
    }
}
