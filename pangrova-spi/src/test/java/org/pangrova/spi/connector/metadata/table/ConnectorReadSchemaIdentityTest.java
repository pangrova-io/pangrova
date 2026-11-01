package org.pangrova.spi.connector.metadata.table;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.type.PangrovaType;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Schema evidence boundaries; no format allocation or governance qualification is simulated here. */
class ConnectorReadSchemaIdentityTest {
    private static final TableIdentity OWNER = new TableIdentity("FORMAT", "physical-owner",
            "s3://credential-must-not-print/table", "selected-schema");

    @Test
    void missingPhysicalOrSchemaEvidenceCannotBecomeFieldIdentity() {
        assertThatThrownBy(() -> evidence(new TableIdentity("FORMAT", "", "location", "schema"),
                List.of(field(0)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> evidence(new TableIdentity("FORMAT", "owner", "location", ""),
                List.of(field(0)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConnectorReadSchemaIdentity(OWNER, " ", List.of(field(0)),
                ConnectorReadSchemaIdentity.FieldIdStability.SELECTED_SOURCE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingOrDuplicateOrNegativeFormatIdsCannotBeCertified() {
        var missing = new ConnectorTableMetadata.Column("email", PangrovaType.VARCHAR);
        assertThatThrownBy(() -> evidence(OWNER, List.of(missing)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> evidence(OWNER, List.of(field(-1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> evidence(OWNER, List.of(field(0), field(0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void selectedFieldsAreImmutableAndDescriptionsDoNotExposeLocationsOrSchemaContents() {
        var fields = new ArrayList<>(List.of(field(0)));
        var evidence = evidence(OWNER, fields);
        fields.clear();
        assertThat(evidence.fields()).hasSize(1);
        assertThatThrownBy(() -> evidence.fields().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(evidence.stability()).isEqualTo(ConnectorReadSchemaIdentity.FieldIdStability.SELECTED_SOURCE);
        assertThat(evidence.toString()).contains("FORMAT", "fields=1")
                .doesNotContain("credential-must-not-print", "private_email", "selected-schema");
    }

    private static ConnectorReadSchemaIdentity evidence(TableIdentity owner, List<ConnectorTableMetadata.Column> fields) {
        return new ConnectorReadSchemaIdentity(owner, "qualified-allocator", fields,
                ConnectorReadSchemaIdentity.FieldIdStability.SELECTED_SOURCE);
    }

    private static ConnectorTableMetadata.Column field(int id) {
        return new ConnectorTableMetadata.Column("private_email", PangrovaType.VARCHAR, OptionalInt.of(id));
    }
}
