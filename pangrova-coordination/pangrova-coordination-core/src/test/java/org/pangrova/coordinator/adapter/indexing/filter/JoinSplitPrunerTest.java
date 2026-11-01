package org.pangrova.coordinator.adapter.indexing.filter;

import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.indexing.pruning.JoinPruningSummary;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link JoinSplitPruner}. */
class JoinSplitPrunerTest {

    private final JoinSplitPruner pruner = new JoinSplitPruner();

    @Test
    void prunesDisjointRanges() {
        var splits = List.of(split("f1"), split("f2"), split("f3"));
        var stats = Map.of(
                "f1", List.of(colStats("jcol", 1L, 10L)),
                "f2", List.of(colStats("jcol", 50L, 100L)),
                "f3", List.of(colStats("jcol", 200L, 300L)));
        var summary = new JoinPruningSummary("jcol", 40L, 120L, 50);

        var result = pruner.applyBuildSideSummary(splits, stats, summary);

        assertThat(result).extracting(s -> s.dataFilePaths().get(0))
                .containsExactly("f2");
    }

    @Test
    void keepsOverlappingRanges() {
        var splits = List.of(split("f1"));
        var stats = Map.of("f1", List.of(colStats("jcol", 5L, 50L)));
        var summary = new JoinPruningSummary("jcol", 10L, 100L, 50);

        var result = pruner.applyBuildSideSummary(splits, stats, summary);

        assertThat(result).hasSize(1);
    }

    @Test
    void keepsSplitsWithNoStats() {
        var splits = List.of(split("f1"));
        var summary = new JoinPruningSummary("jcol", 10L, 100L, 50);

        var result = pruner.applyBuildSideSummary(splits, Map.of(), summary);

        assertThat(result).hasSize(1);
    }

    @Test
    void nullBuildSideRangeKeepsAll() {
        var splits = List.of(split("f1"));
        var stats = Map.of("f1", List.of(colStats("jcol", 1L, 10L)));
        var summary = new JoinPruningSummary("jcol", null, null, 0);

        var result = pruner.applyBuildSideSummary(splits, stats, summary);

        assertThat(result).hasSize(1);
    }

    @Test
    void emptyInputReturnsEmpty() {
        var summary = new JoinPruningSummary("jcol", 10L, 100L, 50);
        assertThat(pruner.applyBuildSideSummary(List.of(), Map.of(), summary)).isEmpty();
    }

    private static ConnectorSplit split(String path) {
        return new TestSplit(path);
    }

    private static FileColumnStatistics colStats(String col, Long min, Long max) {
        return new FileColumnStatistics(col, min, max, 0, 100, 50, 1024);
    }

    record TestSplit(String filePath) implements ConnectorSplit {
        @Override public List<String> dataFilePaths() { return List.of(filePath); }
        @Override public Map<String, String> splitProperties() { return Map.of("file_path", filePath); }
    }
}
