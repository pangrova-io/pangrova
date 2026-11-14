package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergNanoTableCreationTest {

    @TempDir
    Path warehouse;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("nano_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @Test
    void metadataCreatesNanoTableAsV3WithoutExplicitProperty() throws Exception {
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            var metadata = new IcebergMetadata(catalog, "nano_catalog");
            metadata.createTable(SESSION, tableMetadata(Map.of()));
            var table = catalog.loadTable(TableIdentifier.of("db", "events"));
            assertThat(IcebergFormatVersions.extract(table)).isEqualTo(3);
            assertThat(table.schema().findType("event_time")).isEqualTo(Types.TimestampNanoType.withoutZone());
        }
    }

    @Test
    void metadataRejectsExplicitV2BeforeTableIsCreated() throws Exception {
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            var metadata = new IcebergMetadata(catalog, "nano_catalog");
            assertThatThrownBy(() -> metadata.createTable(SESSION, tableMetadata(Map.of("format-version", "2"))))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires Iceberg format-version");
            assertThat(catalog.tableExists(TableIdentifier.of("db", "events"))).isFalse();
        }
    }

    @Test
    void nestedNanoTypeRequiresV3ButMicrosecondSchemaKeepsProperties() {
        var nested = new Schema(Types.NestedField.optional(1, "events", Types.ListType.ofOptional(2,
                Types.TimestampNanoType.withZone())));
        assertThat(IcebergFormatVersions.creationProperties(nested, Map.of("partition-by", "events")))
                .containsExactly(Map.entry("format-version", "3"));
        assertThat(IcebergFormatVersions.creationProperties(nested, Map.of("format-version", "3")))
                .containsExactly(Map.entry("format-version", "3"));
        var micro = new Schema(Types.NestedField.optional(1, "event_time", Types.TimestampType.withoutZone()));
        assertThat(IcebergFormatVersions.creationProperties(micro, Map.of())).isEmpty();
        assertThat(IcebergFormatVersions.creationProperties(micro, Map.of("format-version", "2")))
                .containsExactly(Map.entry("format-version", "2"));
    }

    private static ConnectorTableMetadata tableMetadata(Map<String, String> properties) {
        return new ConnectorTableMetadata(new SchemaTableName("db", "events"), List.of("event_time"),
                List.of(new ConnectorTableMetadata.Column("event_time", PangrovaType.TIMESTAMP_NS)), properties);
    }
}
