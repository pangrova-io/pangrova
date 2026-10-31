package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.scan.PaimonPartitionExtractor;
import org.pangrova.connector.paimon.scan.PaimonDataFileExtractor;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Physical schema mapping is derived from actual SDK revisions and immutable field IDs. */
class PaimonPhysicalSchemaIntegrationTest {
    @TempDir
    Path directory;

    @Test
    void oldSingleFileMapsRenameAndNewSameNameByFieldIdWithExactTypePromotion() throws Exception {
        Options options = new Options();
        options.setString("warehouse", directory.toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "evolved");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT())
                    .column("label", DataTypes.STRING()).option("bucket", "1").option("bucket-key", "id")
                    .option("file.format", "lance").build(), false);
            var initial = (FileStoreTable) catalog.getTable(id);
            var builder = initial.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                writer.write(GenericRow.of(1, BinaryString.fromString("original")));
                commit.commit(writer.prepareCommit());
            }
            catalog.alterTable(id, List.of(SchemaChange.renameColumn("label", "renamed"),
                    SchemaChange.addColumn("label", DataTypes.STRING()),
                    SchemaChange.updateColumnType("id", DataTypes.BIGINT())), false);
            var current = (FileStoreTable) catalog.getTable(id);
            var split = (DataSplit) current.newReadBuilder().newScan().plan().splits().get(0);
            var physical = split.dataFiles().get(0);
            assertThat(physical.schemaId()).isZero();
            assertThat(current.schema().id()).isPositive();
            var evolution = PaimonPartitionExtractor.extractSchemaEvolution(physical, current);
            assertThat(evolution.columnMappings()).filteredOn(mapping -> mapping.targetName().equals("renamed"))
                    .singleElement().satisfies(mapping -> {
                        assertThat(mapping.sourceName()).isEqualTo("label");
                        assertThat(mapping.sourceFieldId()).isEqualTo(mapping.targetFieldId());
                    });
            assertThat(evolution.columnMappings()).filteredOn(mapping -> mapping.targetName().equals("label"))
                    .singleElement().satisfies(mapping -> assertThat(mapping.sourceFieldId()).isEqualTo(-1));
            assertThat(evolution.typePromotions()).singleElement().satisfies(promotion -> {
                assertThat(promotion.columnName()).isEqualTo("id");
                assertThat(promotion.sourceType()).isEqualTo("INTEGER");
                assertThat(promotion.targetType()).isEqualTo("BIGINT");
            });
            var lsm = PaimonDataFileExtractor.extractLsmLevels(current, split).get(0).files().get(0);
            assertThat(lsm.columnMappings()).isEqualTo(evolution.columnMappings());
            assertThat(lsm.typePromotions()).isEqualTo(evolution.typePromotions());
        }
    }
}
