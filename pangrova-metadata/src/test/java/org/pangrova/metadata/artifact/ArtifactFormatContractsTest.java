package org.pangrova.metadata.artifact;

import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.metadata.artifact.contract.ArtifactFormatContracts;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactFormatContractsTest {

    @Test
    void resolvesCurrentDiskAnnAndBm25RoleLayouts() {
        assertThat(ArtifactFormatContracts.require(
                "pangrova.diskann.segment", ArtifactTypeIds.VECTOR_INDEX)
                .sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "DISKANN_GRAPH", source(0),
                        "RAW_VECTORS", source(1),
                        "ROW_LOCATORS", source(2)));
        var bm25 = ArtifactFormatContracts.require(
                "pangrova.bm25.segment", ArtifactTypeIds.TEXT_INDEX);
        assertThat(bm25.sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "BM25_POSTINGS", source(0),
                        "ROW_LOCATORS", source(1),
                        "BM25_STATS_SHARD", workUnit(2)));
        assertThat(bm25.dependencyReduceRoles())
                .containsExactlyEntriesOf(java.util.Map.of(
                        "BM25_STATS_SHARD", workUnit(0)));
        assertThat(bm25.generationFinalizeRoles())
                .containsExactlyEntriesOf(java.util.Map.of(
                        "BM25_CORPUS_STATS", generation(0)));
        assertThat(bm25.requiresGenerationFinalize()).isTrue();

        var generated = ArtifactFormatContracts.require(
                "pangrova.generated-values.segment",
                ArtifactTypeIds.GENERATED_COLUMN_VALUES);
        assertThat(generated.sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "GENERATED_VALUES", source(0),
                        "ROW_LOCATORS", source(1)));
        assertThat(generated.requiresGenerationFinalize()).isFalse();

        var tabular = ArtifactFormatContracts.require(
                "pangrova.tabular-export.arrow-stream",
                ArtifactTypeIds.TABULAR_EXPORT);
        assertThat(tabular.sourceMapRoles()).isEmpty();
        assertThat(tabular.relationalPartitionRoles())
                .containsExactlyEntriesOf(java.util.Map.of(
                        "TABULAR_DATA", workUnit(0)));
        assertThat(tabular.supportsEmptyPartitions()).isTrue();
    }

    @Test
    void rejectsUnknownOrMismatchedFormats() {
        assertThatThrownBy(() -> ArtifactFormatContracts.require(
                "pangrova.bm25.segment", ArtifactTypeIds.VECTOR_INDEX))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ArtifactFormatContracts.require(
                "unknown", ArtifactTypeIds.TEXT_INDEX))
                .isInstanceOf(IllegalStateException.class);
    }

    private static ArtifactFormatContracts.RoleContract source(int ordinal) {
        return new ArtifactFormatContracts.RoleContract(
                ordinal, ArtifactBuildOutputScope.SOURCE_OBJECT);
    }

    private static ArtifactFormatContracts.RoleContract workUnit(int ordinal) {
        return new ArtifactFormatContracts.RoleContract(
                ordinal, ArtifactBuildOutputScope.WORK_UNIT);
    }

    private static ArtifactFormatContracts.RoleContract generation(int ordinal) {
        return new ArtifactFormatContracts.RoleContract(
                ordinal, ArtifactBuildOutputScope.GENERATION);
    }
}
