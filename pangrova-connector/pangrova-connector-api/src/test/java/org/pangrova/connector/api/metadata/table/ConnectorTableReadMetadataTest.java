package org.pangrova.connector.api.metadata.table;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.api.error.ConnectorException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure evidence contract, not connector snapshot or authorization proof. */
class ConnectorTableReadMetadataTest {
    @Test
    void metadataAndEvidenceAreRequiredIndependently() {
        var metadata = new ConnectorTableMetadata(new SchemaTableName("db", "table"), List.of(), List.of(), Map.of());
        assertThatThrownBy(() -> new ConnectorTableReadMetadata(null,
                ConnectorTableReadMetadata.DeclarationEvidence.EXACT))
                .isInstanceOf(NullPointerException.class).hasMessage("metadata");
        assertThatThrownBy(() -> new ConnectorTableReadMetadata(metadata, null))
                .isInstanceOf(NullPointerException.class).hasMessage("declarationEvidence");
    }

    @Test
    void exactEmptyDeclarationIsNotTheSameAsUnavailableEvidence() {
        var metadata = new ConnectorTableMetadata(new SchemaTableName("db", "table"), List.of(), List.of(), Map.of());
        assertThat(new ConnectorTableReadMetadata(metadata,
                ConnectorTableReadMetadata.DeclarationEvidence.EXACT).requireExactDeclaration()).isSameAs(metadata);
        assertThatThrownBy(() -> new ConnectorTableReadMetadata(metadata,
                ConnectorTableReadMetadata.DeclarationEvidence.UNAVAILABLE).requireExactDeclaration())
                .isInstanceOfSatisfying(ConnectorException.class, error -> assertThat(error.getErrorCode())
                        .isEqualTo(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION))
                .hasMessageContaining("Historical generated declaration evidence is unavailable");
    }
}
