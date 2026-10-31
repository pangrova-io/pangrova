package org.pangrova.databuild.api.format;

import org.pangrova.databuild.api.DataBuildOutputScope;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBuildOutputFormatContractsTest {

    @Test
    void resolvesCurrentDiskAnnAndBm25RoleLayouts() {
        assertThat(DataBuildOutputFormatContracts.require(
                "pangrova.diskann.segment", DataBuildOutputTypeIds.VECTOR_INDEX)
                .sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "DISKANN_GRAPH", source(0),
                        "RAW_VECTORS", source(1),
                        "ROW_LOCATORS", source(2)));
        var bm25 = DataBuildOutputFormatContracts.require(
                "pangrova.bm25.segment", DataBuildOutputTypeIds.TEXT_INDEX);
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

        var generated = DataBuildOutputFormatContracts.require(
                "pangrova.generated-values.segment",
                DataBuildOutputTypeIds.GENERATED_COLUMN_VALUES);
        assertThat(generated.sourceMapRoles())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "GENERATED_VALUES", source(0),
                        "ROW_LOCATORS", source(1)));
        assertThat(generated.requiresGenerationFinalize()).isFalse();

        var tabular = DataBuildOutputFormatContracts.require(
                "pangrova.tabular-export.arrow-stream",
                DataBuildOutputTypeIds.TABULAR_EXPORT);
        assertThat(tabular.sourceMapRoles()).isEmpty();
        assertThat(tabular.relationalPartitionRoles())
                .containsExactlyEntriesOf(java.util.Map.of(
                        "TABULAR_DATA", workUnit(0)));
        assertThat(tabular.supportsEmptyPartitions()).isTrue();
    }

    @Test
    void rejectsUnknownOrMismatchedFormats() {
        assertThatThrownBy(() -> DataBuildOutputFormatContracts.require(
                "pangrova.bm25.segment", DataBuildOutputTypeIds.VECTOR_INDEX))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> DataBuildOutputFormatContracts.require(
                "unknown", DataBuildOutputTypeIds.TEXT_INDEX))
                .isInstanceOf(IllegalStateException.class);
    }

    private static DataBuildOutputFormatContracts.RoleContract source(int ordinal) {
        return new DataBuildOutputFormatContracts.RoleContract(
                ordinal, DataBuildOutputScope.SOURCE_OBJECT);
    }

    private static DataBuildOutputFormatContracts.RoleContract workUnit(int ordinal) {
        return new DataBuildOutputFormatContracts.RoleContract(
                ordinal, DataBuildOutputScope.WORK_UNIT);
    }

    private static DataBuildOutputFormatContracts.RoleContract generation(int ordinal) {
        return new DataBuildOutputFormatContracts.RoleContract(
                ordinal, DataBuildOutputScope.GENERATION);
    }
}
