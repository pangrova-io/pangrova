package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.source.CanonicalConnectorSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests the split locations carried into cache affinity placement.
 */
class SplitFilePathExtractionTest {

    @Test
    void splitWithPathsPreservesFilePaths() {
        var data = split("data");
        var swp = new QuerySplit(data, List.of("s3://bucket/file1.parquet"));
        assertThat(swp.data()).isSameAs(data);
        assertThat(swp.filePaths()).containsExactly("s3://bucket/file1.parquet");
    }

    @Test
    void splitWithPathsDefensiveCopy() {
        var paths = new java.util.ArrayList<>(List.of("s3://a", "s3://b"));
        var swp = new QuerySplit(split("data"), paths);
        paths.add("s3://c");
        assertThat(swp.filePaths()).hasSize(2);
    }

    @Test
    void splitWithPathsNullFilePathsDefaultsToEmpty() {
        var swp = new QuerySplit(split("data"), null);
        assertThat(swp.filePaths()).isEmpty();
    }

    @Test
    void keepsTheExactConnectorMessageWhenBuildingTheStageSplit() {
        var data = split("data");
        CanonicalConnectorSplit connector = () -> data;
        assertThat(SplitFetcher.toSplitWithPaths(List.of(connector))).singleElement()
                .satisfies(stage -> assertThat(stage.data()).isSameAs(data));
    }

    @Test
    void missingOrEmptyExecutionMetadataFailsBeforeTheSplitEntersTheStage() {
        CanonicalConnectorSplit empty = DataSplit::getDefaultInstance;
        assertRejected(new ConnectorSplit() {});
        assertRejected(empty);
    }

    private static void assertRejected(ConnectorSplit invalid) {
        assertThatThrownBy(() -> SplitFetcher.toSplitWithPaths(List.of(invalid)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED))
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    private static DataSplit split(String id) {
        return DataSplit.newBuilder().setSplitId(id).build();
    }

}
