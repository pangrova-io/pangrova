package org.pangrova.connector.paimon;

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;
import org.pangrova.connector.spi.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.spi.metadata.table.ConnectorReadSchemaIdentity;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThat;

/** Exact SDK schema evidence, including the pinned version's rollback/reuse boundary; not Native SQL acceptance. */
class PaimonReadSchemaIdentityIntegrationTest extends PaimonDeleteTestSupport {
    @Test
    void currentAndHistoricalEvidenceUseTheirActualSelectedSchema() throws Exception {
        createTable(false, true);
        writeRows(new StoredRow(1, "east", "day", "private"));
        long snapshot = snapshotId();
        var original = (FileStoreTable) PaimonTableIdentity.loadFresh(catalog, IDENTIFIER);
        String originalSchema = PaimonTableIdentity.schemaFingerprint(original.schema());
        catalog.alterTable(IDENTIFIER, SchemaChange.addColumn("added", DataTypes.STRING()), false);
        try (var registry = new PaimonSplitRegistry()) {
            var metadata = new PaimonMetadata(catalog, "delete_catalog", registry);
            var current = bind(metadata);
            var historical = metadata.bindRead(SESSION, metadata.getTableHandle(SESSION,
                    new SchemaTableName(DATABASE, TABLE_NAME)).withTimeTravelSnapshot(snapshot));
            var selected = historical.schemaIdentity().orElseThrow();
            assertThat(selected.fields()).extracting(column -> column.fieldId().orElseThrow())
                    .containsExactly(0, 1, 2, 3);
            assertThat(selected.tableIdentity().schemaFingerprint()).isEqualTo(originalSchema);
            assertThat(current.schemaIdentity().orElseThrow().fields()).hasSize(5);
            assertThat(current.schemaIdentity().orElseThrow().tableIdentity().schemaFingerprint())
                    .isNotEqualTo(originalSchema);
            assertThat(selected.stability()).isEqualTo(ConnectorReadSchemaIdentity.FieldIdStability.SELECTED_SOURCE);
            assertThat(selected.fieldNamespace()).isEqualTo("paimon.branch-fields.v1:main");
            assertThat(current.snapshotId()).isEqualTo(historical.snapshotId());
        }
    }

    @Test
    void schemaRollbackCanReuseIdsWithoutPublishingANonReusableGuarantee() throws Exception {
        createTable(false, true);
        catalog.alterTable(IDENTIFIER, SchemaChange.addColumn("first_private", DataTypes.STRING()), false);
        try (var registry = new PaimonSplitRegistry()) {
            var metadata = new PaimonMetadata(catalog, "delete_catalog", registry);
            var first = bind(metadata).schemaIdentity().orElseThrow();
            int firstId = first.fields().get(4).fieldId().orElseThrow();
            var table = (FileStoreTable) PaimonTableIdentity.loadFresh(catalog, IDENTIFIER);
            table.schemaManager().rollbackTo(0, table.snapshotManager(), table.tagManager(), table.changelogManager());
            catalog.invalidateTable(IDENTIFIER);
            catalog.alterTable(IDENTIFIER, SchemaChange.addColumn("another_private", DataTypes.STRING()), false);
            var replacement = bind(metadata).schemaIdentity().orElseThrow();
            assertThat(replacement.fields().get(4).name()).isEqualTo("another_private");
            assertThat(replacement.fields().get(4).fieldId().orElseThrow()).isEqualTo(firstId);
            assertThat(replacement.tableIdentity().formatTableUuid()).isEqualTo(first.tableIdentity().formatTableUuid());
            assertThat(replacement.tableIdentity().schemaFingerprint())
                    .isNotEqualTo(first.tableIdentity().schemaFingerprint());
            assertThat(replacement.stability()).isEqualTo(ConnectorReadSchemaIdentity.FieldIdStability.SELECTED_SOURCE);
        }
    }

    private static ConnectorReadBinding bind(PaimonMetadata metadata) {
        return metadata.bindRead(SESSION,
                metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME)));
    }
}
