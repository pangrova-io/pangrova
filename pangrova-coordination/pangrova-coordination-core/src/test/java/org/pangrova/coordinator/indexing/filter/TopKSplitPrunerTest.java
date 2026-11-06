package org.pangrova.coordinator.indexing.filter;

import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.pruning.TopKBoundary;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TopKSplitPruner}. */
class TopKSplitPrunerTest {

    private final TopKSplitPruner pruner = new TopKSplitPruner();

    @Test
    void applyBoundaryDescPrunesFilesWithMaxBelowBoundary() {
        var splits = List.of(split("f1"), split("f2"), split("f3"));
        var stats = Map.of(
                "f1", List.of(colStats("col", 1L, 50L)),
                "f2", List.of(colStats("col", 60L, 100L)),
                "f3", List.of(colStats("col", 10L, 30L)));
        var boundary = new TopKBoundary("col", 40L, true, 10);

        var result = pruner.applyBoundary(splits, stats, boundary);

        assertThat(result).extracting(s -> s.dataFilePaths().get(0))
                .containsExactlyInAnyOrder("f1", "f2");
    }

    @Test
    void applyBoundaryAscPrunesFilesWithMinAboveBoundary() {
        var splits = List.of(split("f1"), split("f2"));
        var stats = Map.of(
                "f1", List.of(colStats("col", 1L, 50L)),
                "f2", List.of(colStats("col", 60L, 100L)));
        var boundary = new TopKBoundary("col", 55L, false, 10);

        var result = pruner.applyBoundary(splits, stats, boundary);

        assertThat(result).extracting(s -> s.dataFilePaths().get(0))
                .containsExactly("f1");
    }

    @Test
    void applyBoundaryKeepsSplitsWithNoStats() {
        var splits = List.of(split("f1"));
        var boundary = new TopKBoundary("col", 50L, true, 10);

        var result = pruner.applyBoundary(splits, Map.of(), boundary);

        assertThat(result).hasSize(1);
    }

    @Test
    void emptyInputReturnsEmpty() {
        var boundary = new TopKBoundary("col", 50L, true, 10);
        assertThat(pruner.applyBoundary(List.of(), Map.of(), boundary)).isEmpty();
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
