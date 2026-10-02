package org.pangrova.coordinator.metadata;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataResolvedTableTest {
    @Test
    void unwrapRetainsTheExactImmutablePlanningDeclaration() {
        var properties = new java.util.HashMap<String, String>();
        properties.put("declaration", "original");
        var metadata = new ConnectorTableMetadata(new SchemaTableName("schema", "table"), List.of("id"),
                List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT)), properties);
        var table = new MetadataResolvedTable(metadata);
        properties.put("declaration", "changed");

        assertThat(table.unwrap(ConnectorTableMetadata.class)).isSameAs(metadata);
        assertThat(table.unwrap(ConnectorTableMetadata.class).properties()).containsEntry("declaration", "original");
        assertThat(table.unwrap(MetadataResolvedTable.class)).isSameAs(table);
        assertThat(table.unwrap(String.class)).isNull();
    }

    @Test
    void rejectsMissingDeclaration() {
        assertThatThrownBy(() -> new MetadataResolvedTable(null)).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("metadata");
    }
}
