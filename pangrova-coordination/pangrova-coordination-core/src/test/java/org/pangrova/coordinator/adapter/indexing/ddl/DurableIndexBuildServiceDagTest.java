package org.pangrova.coordinator.adapter.indexing.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class DurableIndexBuildServiceDagTest {

    @Test
    void diskAnnUsesOnlyIndependentSourceMapPages() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(3), DataBuildOutputTypeIds.VECTOR_INDEX, 5);

        assertThat(pages).hasSize(3);
        assertThat(pages).allMatch(
                page -> page.kind() == DataBuildPageKind.SOURCE_MAP);
    }

    @Test
    void bm25AddsOneGenerationFinalizerWithinOneFanInLevel() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(64), DataBuildOutputTypeIds.TEXT_INDEX, 5);

        assertThat(pages).hasSize(65);
        DataBuildPage finalizer = pages.get(64);
        assertThat(finalizer.kind())
                .isEqualTo(DataBuildPageKind.GENERATION_FINALIZE);
        assertThat(finalizer.dependencyPageIds()).hasSize(64);
    }

    @Test
    void bm25BuildsLogarithmicBoundedFanInReductionTree() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(4_097), DataBuildOutputTypeIds.TEXT_INDEX, 5);

        assertThat(pages).hasSize(4_165);
        assertThat(pages.stream()
                .filter(page -> page.kind() == DataBuildPageKind.DEPENDENCY_REDUCE))
                .hasSize(67);
        assertThat(pages).allMatch(
                page -> page.dependencyPageIds().size() <= 64);
        DataBuildPage finalizer = pages.get(pages.size() - 1);
        assertThat(finalizer.kind())
                .isEqualTo(DataBuildPageKind.GENERATION_FINALIZE);
        assertThat(finalizer.stageId()).isEqualTo(3);
        assertThat(finalizer.dependencyPageIds()).hasSize(2);
    }

    private static List<DataBuildSplit> splits(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> DataBuildSplit.of(
                        "source-" + index,
                        ("split-" + index).getBytes(StandardCharsets.UTF_8)))
                .toList();
    }
}
