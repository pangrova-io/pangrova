package org.pangrova.connector.api.metadata.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.types.enums.PangrovaType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorDerivedQueryTest {
    @Test
    void retainsExactSelectedInputWithoutExposingItsHandleInDiagnostics() {
        var binding = binding(ReadSnapshotState.METADATA);
        var inputs = new ArrayList<>(List.of(new ConnectorDerivedQuery.Input("source", binding)));
        var query = new ConnectorDerivedQuery("SELECT id FROM source", inputs);
        inputs.clear();
        assertThat(query.inputs()).singleElement().satisfies(input -> assertThat(input.binding()).isSameAs(binding));
        assertThat(query.toString()).doesNotContain("SELECT", "private-source-location");
        assertThatThrownBy(() -> query.inputs().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsAmbiguousAliasesAndSourcesWithoutFrozenEvidence() {
        var source = binding(ReadSnapshotState.METADATA);
        assertThatThrownBy(() -> new ConnectorDerivedQuery("SELECT * FROM source", List.of(
                new ConnectorDerivedQuery.Input("source", source),
                new ConnectorDerivedQuery.Input("SOURCE", source)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectorDerivedQuery.Input("source",
                binding(ReadSnapshotState.UNVERSIONED))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectorDerivedQuery(" ", List.of(
                new ConnectorDerivedQuery.Input("source", source)))).isInstanceOf(IllegalArgumentException.class);
    }

    private static ConnectorReadBinding binding(ReadSnapshotState state) {
        var columns = List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT));
        var metadata = new ConnectorTableMetadata(new SchemaTableName("schema", "source"),
                List.of("id"), columns, Map.of());
        return new ConnectorReadBinding(new ConnectorTableHandle() {},
                new ConnectorTableReadMetadata(metadata, TableDeclarationEvidence.EXACT),
                TableStatistics.UNKNOWN, state, OptionalLong.empty(), "private-source-location", java.util.Optional.empty(), java.util.Optional.empty());
    }
}
