package org.pangrova.generation.api.format;

import org.pangrova.generation.api.GenerationOutputScope;
import org.pangrova.generation.api.GenerationOutputTypeIds;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationOutputFormatContractsTest {

    @Test
    void resolvesCurrentDiskAnnAndBm25RoleLayouts() {
        assertThat(GenerationOutputFormatContracts.require(
                "pangrova.diskann.segment", GenerationOutputTypeIds.VECTOR_INDEX)
                .sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "DISKANN_GRAPH", source(0),
                        "RAW_VECTORS", source(1),
                        "ROW_LOCATORS", source(2)));
        var bm25 = GenerationOutputFormatContracts.require(
                "pangrova.bm25.segment", GenerationOutputTypeIds.TEXT_INDEX);
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

        var generated = GenerationOutputFormatContracts.require(
                "pangrova.generated-values.segment",
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES);
        assertThat(generated.sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "GENERATED_VALUES", source(0),
                        "ROW_LOCATORS", source(1)));
        assertThat(generated.requiresGenerationFinalize()).isFalse();

        var tabular = GenerationOutputFormatContracts.require(
                "pangrova.tabular-export.arrow-stream",
                GenerationOutputTypeIds.TABULAR_EXPORT);
        assertThat(tabular.sourceMapRoles()).isEmpty();
        assertThat(tabular.relationalPartitionRoles())
                .containsExactlyEntriesOf(java.util.Map.of(
                        "TABULAR_DATA", workUnit(0)));
        assertThat(tabular.supportsEmptyPartitions()).isTrue();
    }

    @Test
    void rejectsUnknownOrMismatchedFormats() {
        assertThatThrownBy(() -> GenerationOutputFormatContracts.require(
                "pangrova.bm25.segment", GenerationOutputTypeIds.VECTOR_INDEX))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> GenerationOutputFormatContracts.require(
                "unknown", GenerationOutputTypeIds.TEXT_INDEX))
                .isInstanceOf(IllegalStateException.class);
    }

    private static GenerationOutputFormatContracts.RoleContract source(int ordinal) {
        return new GenerationOutputFormatContracts.RoleContract(
                ordinal, GenerationOutputScope.SOURCE_OBJECT);
    }

    private static GenerationOutputFormatContracts.RoleContract workUnit(int ordinal) {
        return new GenerationOutputFormatContracts.RoleContract(
                ordinal, GenerationOutputScope.WORK_UNIT);
    }

    private static GenerationOutputFormatContracts.RoleContract generation(int ordinal) {
        return new GenerationOutputFormatContracts.RoleContract(
                ordinal, GenerationOutputScope.GENERATION);
    }
}
