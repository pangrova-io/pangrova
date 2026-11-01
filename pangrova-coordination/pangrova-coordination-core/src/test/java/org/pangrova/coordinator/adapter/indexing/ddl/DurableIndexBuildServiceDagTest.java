package org.pangrova.coordinator.adapter.indexing.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.generation.job.GenerationPage;
import org.pangrova.spi.generation.GenerationPageKind;
import org.pangrova.spi.generation.job.GenerationSplit;
import org.pangrova.spi.generation.GenerationOutputTypeIds;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class DurableIndexBuildServiceDagTest {

    @Test
    void diskAnnUsesOnlyIndependentSourceMapPages() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(3), GenerationOutputTypeIds.VECTOR_INDEX, 5);

        assertThat(pages).hasSize(3);
        assertThat(pages).allMatch(
                page -> page.kind() == GenerationPageKind.SOURCE_MAP);
    }

    @Test
    void bm25AddsOneGenerationFinalizerWithinOneFanInLevel() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(64), GenerationOutputTypeIds.TEXT_INDEX, 5);

        assertThat(pages).hasSize(65);
        GenerationPage finalizer = pages.get(64);
        assertThat(finalizer.kind())
                .isEqualTo(GenerationPageKind.GENERATION_FINALIZE);
        assertThat(finalizer.dependencyPageIds()).hasSize(64);
    }

    @Test
    void bm25BuildsLogarithmicBoundedFanInReductionTree() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(4_097), GenerationOutputTypeIds.TEXT_INDEX, 5);

        assertThat(pages).hasSize(4_165);
        assertThat(pages.stream()
                .filter(page -> page.kind() == GenerationPageKind.DEPENDENCY_REDUCE))
                .hasSize(67);
        assertThat(pages).allMatch(
                page -> page.dependencyPageIds().size() <= 64);
        GenerationPage finalizer = pages.get(pages.size() - 1);
        assertThat(finalizer.kind())
                .isEqualTo(GenerationPageKind.GENERATION_FINALIZE);
        assertThat(finalizer.stageId()).isEqualTo(3);
        assertThat(finalizer.dependencyPageIds()).hasSize(2);
    }

    private static List<GenerationSplit> splits(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> GenerationSplit.of(
                        "source-" + index,
                        ("split-" + index).getBytes(StandardCharsets.UTF_8)))
                .toList();
    }
}
