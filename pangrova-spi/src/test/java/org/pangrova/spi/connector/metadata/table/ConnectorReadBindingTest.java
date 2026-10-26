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
                new TableStatistics(0, 0, 0, Map.of()), "identity");
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
                new TableStatistics(-2, -1, -1, Map.of()), "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> binding(ConnectorReadBinding.SnapshotState.UNVERSIONED, OptionalLong.empty(),
                new TableStatistics(-1, -2, -1, Map.of()), "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TableStatistics(-1, -1, -2, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void existingStatisticsConstructorsPreserveUnknownSourceUnitSemantics() {
        assertThat(new TableStatistics(100, 800).sourceWorkUnits()).isEqualTo(-1);
        assertThat(new TableStatistics(100, 800, Map.of()).sourceWorkUnits()).isEqualTo(-1);
    }

    @Test
    void schemaEvidenceMustMatchTheExactSelectedDeclaration() {
        var field = new ConnectorTableMetadata.Column("email",
                org.pangrova.spi.connector.data.type.PangrovaType.VARCHAR, java.util.OptionalInt.of(0));
        var evidence = new ConnectorReadSchemaIdentity(new TableIdentity("FORMAT", "owner", "location", "schema"),
                "allocator", List.of(field), ConnectorReadSchemaIdentity.FieldIdStability.SELECTED_SOURCE);
        assertThatThrownBy(() -> new ConnectorReadBinding(handle, declaration, TableStatistics.UNKNOWN,
                ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1), "source",
                java.util.Optional.of(evidence))).isInstanceOf(IllegalArgumentException.class);
        var metadata = new ConnectorTableMetadata(new SchemaTableName("db", "table"),
                List.of("email"), List.of(field), Map.of());
        var unavailable = new ConnectorTableReadMetadata(metadata,
                ConnectorTableReadMetadata.DeclarationEvidence.UNAVAILABLE);
        assertThatThrownBy(() -> new ConnectorReadBinding(handle, unavailable, TableStatistics.UNKNOWN,
                ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1), "source",
                java.util.Optional.of(evidence))).isInstanceOf(IllegalArgumentException.class);
        var exact = new ConnectorTableReadMetadata(metadata, ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
        assertThatThrownBy(() -> new ConnectorReadBinding(handle, exact, TableStatistics.UNKNOWN,
                ConnectorReadBinding.SnapshotState.UNVERSIONED, OptionalLong.empty(), "",
                java.util.Optional.of(evidence))).isInstanceOf(IllegalArgumentException.class);
        assertThat(new ConnectorReadBinding(handle, exact, TableStatistics.UNKNOWN,
                ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1), "source",
                java.util.Optional.of(evidence)).schemaIdentity()).containsSame(evidence);
    }

    @Test
    void descriptionsDoNotExposeConnectorCredentials() {
        var selected = binding(ConnectorReadBinding.SnapshotState.SNAPSHOT, OptionalLong.of(1),
                TableStatistics.UNKNOWN, "identity");
        assertThat(selected.toString()).contains("db.table", "SNAPSHOT").doesNotContain("must-not-print", "credential");
    }

    private ConnectorReadBinding binding(ConnectorReadBinding.SnapshotState state, OptionalLong snapshot,
            TableStatistics statistics, String evidence) {
        return new ConnectorReadBinding(handle, declaration, statistics, state, snapshot, evidence, java.util.Optional.empty());
    }

    private record SelectedHandle(String identity) implements ConnectorTableHandle {}
}
