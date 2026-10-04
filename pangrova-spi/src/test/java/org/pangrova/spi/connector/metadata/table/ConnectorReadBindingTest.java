package org.pangrova.spi.connector.metadata.table;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Value-contract boundaries only; no connector, snapshot, authorization or execution substitute. */
class ConnectorReadBindingTest {
    private final ConnectorTableHandle handle = new SelectedHandle("source");
    private final ConnectorTableReadMetadata declaration = new ConnectorTableReadMetadata(
            new ConnectorTableMetadata(new SchemaTableName("db", "table"), List.of(), List.of(),
                    Map.of("credential", "must-not-print")), ConnectorTableReadMetadata.DeclarationEvidence.EXACT);

    @Test
    void selectedSnapshotsRequirePositiveExactIdentity() {
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.empty(),
                TableStatistics.UNKNOWN, "identity")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(0),
                TableStatistics.UNKNOWN, "identity")).isInstanceOf(IllegalArgumentException.class);
        assertThat(binding(ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1),
                TableStatistics.UNKNOWN, "identity").snapshotId()).hasValue(1);
    }

    @Test
    void emptyMeansProvenZeroNotUnknownOrUnspecifiedLive() {
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.EMPTY, OptionalLong.empty(),
                TableStatistics.UNKNOWN, "identity")).isInstanceOf(IllegalArgumentException.class);
        var empty = binding(ConnectorReadBinding.SnapshotState.EMPTY, OptionalLong.empty(),
                new TableStatistics(0, 0), "identity");
        assertThat(empty.statistics().rowCount()).isZero();
        assertThat(empty.snapshotId()).isEmpty();
    }

    @Test
    void unversionedAndMetadataSourcesCannotInventADataSnapshot() {
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.METADATA, OptionalLong.of(1),
                TableStatistics.UNKNOWN, "metadata")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.UNVERSIONED, OptionalLong.of(1),
                TableStatistics.UNKNOWN, "")).isInstanceOf(IllegalArgumentException.class);
        assertThat(binding(ConnectorReadBinding.SnapshotState.UNVERSIONED, OptionalLong.empty(),
                TableStatistics.UNKNOWN, "").state()).isEqualTo(ConnectorReadBinding.SnapshotState.UNVERSIONED);
    }

    @Test
    void missingEvidenceAndOtherNegativeEstimatesAreRejected() {
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1),
                TableStatistics.UNKNOWN, " ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.UNVERSIONED, OptionalLong.empty(),
                new TableStatistics(-2, -1), "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.UNVERSIONED, OptionalLong.empty(),
                new TableStatistics(-1, -2), "")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void descriptionsDoNotExposeConnectorCredentials() {
        var selected = binding(ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1),
                TableStatistics.UNKNOWN, "identity");
        assertThat(selected.toString()).contains("db.table", "SNAPSHOT").doesNotContain("must-not-print", "credential");
    }

    private ConnectorReadBinding binding(ConnectorReadBinding.SnapshotState state, OptionalLong snapshot,
            TableStatistics statistics, String evidence) {
        return new ConnectorReadBinding(handle, declaration, statistics, state, snapshot, evidence);
    }

    private record SelectedHandle(String identity) implements ConnectorTableHandle {}
}
