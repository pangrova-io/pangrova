package org.pangrova.coordinator.index.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class DurableIndexBuildServiceDagTest {

    @Test
    void diskAnnUsesOnlyIndependentSourceMapPages() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(3), ArtifactTypeIds.VECTOR_INDEX, 5);

        assertThat(pages).hasSize(3);
        assertThat(pages).allMatch(
                page -> page.kind() == ArtifactBuildPageKind.SOURCE_MAP);
    }

    @Test
    void bm25AddsOneGenerationFinalizerWithinOneFanInLevel() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(64), ArtifactTypeIds.TEXT_INDEX, 5);

        assertThat(pages).hasSize(65);
        ArtifactBuildPage finalizer = pages.get(64);
        assertThat(finalizer.kind())
                .isEqualTo(ArtifactBuildPageKind.GENERATION_FINALIZE);
        assertThat(finalizer.dependencyPageIds()).hasSize(64);
    }

    @Test
    void bm25BuildsLogarithmicBoundedFanInReductionTree() {
        var pages = DurableIndexBuildService.buildPages(
                "job", splits(4_097), ArtifactTypeIds.TEXT_INDEX, 5);

        assertThat(pages).hasSize(4_165);
        assertThat(pages.stream()
                .filter(page -> page.kind() == ArtifactBuildPageKind.DEPENDENCY_REDUCE))
                .hasSize(67);
        assertThat(pages).allMatch(
                page -> page.dependencyPageIds().size() <= 64);
        ArtifactBuildPage finalizer = pages.get(pages.size() - 1);
        assertThat(finalizer.kind())
                .isEqualTo(ArtifactBuildPageKind.GENERATION_FINALIZE);
        assertThat(finalizer.stageId()).isEqualTo(3);
        assertThat(finalizer.dependencyPageIds()).hasSize(2);
    }

    private static List<ArtifactBuildSplit> splits(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> ArtifactBuildSplit.of(
                        "source-" + index,
                        ("split-" + index).getBytes(StandardCharsets.UTF_8)))
                .toList();
    }
}
