package org.pangrova.connector.paimon;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.handle.PaimonSystemTableHandle;
import org.pangrova.connector.paimon.handle.PaimonSystemTableSplit;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK metadata objects are pinned as Native splits without Coordinator row materialization. */
class PaimonNativeSystemTableBindingIntegrationTest {
    @TempDir
    Path warehouse;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public Optional<org.pangrova.spi.connector.ConnectorQueryAuthority>
                getQueryAuthority() { return Optional.empty(); }
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("paimon"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @Test
    void snapshotSqlBindingPinsSdkObjectAndTypedV2Split() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            writeRow((FileStoreTable) catalog.getTable(Identifier.create("db", "orders")));
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$snapshots"));
            assertThat(selector).isInstanceOf(PaimonSystemTableHandle.class);
            assertThat(metadata.getTableMetadata(SESSION, selector).columns())
                    .containsExactly("snapshot_id", "schema_id", "commit_user", "commit_identifier",
                            "commit_kind", "commit_time", "base_manifest_list", "delta_manifest_list",
                            "changelog_manifest_list", "total_record_count", "delta_record_count",
                            "changelog_record_count", "watermark", "next_row_id");

            var binding = metadata.bindRead(SESSION, selector);
            metadata.validateReadBinding(SESSION, binding);
            var bound = (PaimonSystemTableHandle) binding.handle();
            assertThat(bound.objects()).hasSize(1);
            assertThat(bound.objects().get(0).snapshotId()).isEqualTo(1);
            var split = new PaimonSystemTableSplit(
                    bound.tableName(), bound.sourceIdentity(), bound.kind(), bound.objects().get(0));
            var encoded = DataSplit.parseFrom(split.toSplitInfoProto());
            assertThat(encoded.getSourceSnapshot()).isEqualTo(bound.sourceIdentity());
            assertThat(encoded.getOpaqueLocation()).isEqualTo(bound.objects().get(0).uri());
            var detail = encoded.getConnectorDetail().unpack(FileSplitDetail.class);
            assertThat(detail.getSystemTable().getKind())
                    .isEqualTo(SystemTableSplitDetail.Kind.PAIMON_SNAPSHOTS);
            assertThat(detail.getSystemTable().getSnapshotId()).isEqualTo(1);
            assertThat(detail.getSystemTable().getObjectSha256().size()).isEqualTo(32);
        }
    }

    @Test
    void emptyTableBindsAnExactEmptyMetadataView() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$snapshots"));
            var binding = metadata.bindRead(SESSION, selector);
            assertThat(((PaimonSystemTableHandle) binding.handle()).objects()).isEmpty();
            assertThat(binding.statistics().rowCount()).isZero();
            metadata.validateReadBinding(SESSION, binding);
        }
    }

    @Test
    void optionsSqlBindingPinsLatestSchemaJsonAndTypedSplit() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            var schema = table.schemaManager().latest().orElseThrow();
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$options"));
            assertThat(selector).isInstanceOf(PaimonSystemTableHandle.class);
            assertThat(metadata.getTableMetadata(SESSION, selector).columns())
                    .containsExactly("key", "value");
            var binding = metadata.bindRead(SESSION, selector);
            var bound = (PaimonSystemTableHandle) binding.handle();
            assertThat(bound.objects()).hasSize(1);
            assertThat(bound.objects().get(0).schemaId()).isEqualTo(schema.id());
            assertThat(bound.objects().get(0).uri())
                    .isEqualTo(table.schemaManager().toSchemaPath(schema.id()).toString());
            metadata.validateReadBinding(SESSION, binding);
            var split = new PaimonSystemTableSplit(bound.tableName(), bound.sourceIdentity(),
                    bound.kind(), bound.objects().get(0));
            var detail = DataSplit.parseFrom(split.toSplitInfoProto())
                    .getConnectorDetail().unpack(FileSplitDetail.class).getSystemTable();
            assertThat(detail.getKind()).isEqualTo(SystemTableSplitDetail.Kind.PAIMON_OPTIONS);
            assertThat(detail.getSchemaId()).isEqualTo(schema.id());
            assertThat(detail.getObjectSha256().size()).isEqualTo(32);
        }
    }

    @Test
    void tagSqlBindingPinsSdkObjectAndTypedV2Split() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            table.tagManager().createTag(table.snapshotManager().snapshot(1),
                    "v1", null, List.of(), false);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$tags"));
            assertThat(selector).isInstanceOf(PaimonSystemTableHandle.class);
            assertThat(metadata.getTableMetadata(SESSION, selector).columns())
                    .containsExactly("tag_name", "snapshot_id", "schema_id", "commit_time",
                            "record_count", "create_time", "time_retained");
            var binding = metadata.bindRead(SESSION, selector);
            var bound = (PaimonSystemTableHandle) binding.handle();
            assertThat(bound.objects()).hasSize(1);
            assertThat(bound.objects().get(0).tagName()).isEqualTo("v1");
            assertThat(bound.objects().get(0).uri())
                    .isEqualTo(table.tagManager().tagPath("v1").toString());
            assertThat(bound.objects().get(0).length())
                    .isEqualTo(table.fileIO().getFileStatus(table.tagManager().tagPath("v1")).getLen());
            metadata.validateReadBinding(SESSION, binding);
            var split = new PaimonSystemTableSplit(bound.tableName(), bound.sourceIdentity(),
                    bound.kind(), bound.objects().get(0));
            var encoded = DataSplit.parseFrom(split.toSplitInfoProto());
            var detail = encoded.getConnectorDetail().unpack(FileSplitDetail.class).getSystemTable();
            assertThat(detail.getKind()).isEqualTo(SystemTableSplitDetail.Kind.PAIMON_TAGS);
            assertThat(detail.getTagName()).isEqualTo("v1");
            assertThat(detail.getSnapshotId()).isZero();
            assertThat(detail.getObjectSha256().size()).isEqualTo(32);
        }
    }

    @Test
    void ttlTagUsesTheNativeJsonFieldContract() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            table.tagManager().createTag(table.snapshotManager().snapshot(1),
                    "retained", Duration.ofHours(48), List.of(), false);
            String json;
            try (var input = table.fileIO().newInputStream(table.tagManager().tagPath("retained"))) {
                json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
            assertThat(json).containsPattern("\\\"tagCreateTime\\\"\\s*:\\s*\\[\\s*\\d{4},")
                    .containsPattern("\\\"tagTimeRetained\\\"\\s*:\\s*172800\\.0+");
            table.tagManager().createTag(table.snapshotManager().snapshot(1),
                    "negative", Duration.ofMillis(-3_600_500), List.of(), false);
            try (var input = table.fileIO().newInputStream(table.tagManager().tagPath("negative"))) {
                assertThat(new String(input.readAllBytes(), StandardCharsets.UTF_8))
                        .containsPattern("\\\"tagTimeRetained\\\"\\s*:\\s*-3600\\.5[0-9]*");
            }
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var binding = metadata.bindRead(SESSION, metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$tags")));
            metadata.validateReadBinding(SESSION, binding);
            assertThat(((PaimonSystemTableHandle) binding.handle()).objects()).hasSize(2);
        }
    }

    @Test
    void deletedPinnedSnapshotIsRejectedBeforeSplitPlanning() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$snapshots"));
            var binding = metadata.bindRead(SESSION, selector);
            table.snapshotManager().fileIO().delete(
                    table.snapshotManager().snapshotPath(1), false);
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                    .hasMessageContaining("Pinned Paimon metadata object is unavailable");
        }
    }

    @Test
    void sameLengthSnapshotReplacementIsRejectedBeforeSplitPlanning() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "orders$snapshots"));
            var binding = metadata.bindRead(SESSION, selector);
            var path = Path.of(table.snapshotManager().snapshotPath(1).toString());
            byte[] contents = Files.readAllBytes(path);
            contents[contents.length - 1] ^= 1;
            Files.write(path, contents);
            assertThat(Files.size(path)).isEqualTo(contents.length);
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                    .hasMessageContaining("Pinned Paimon metadata object changed");
        }
    }

    private Catalog catalog() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        var catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        return catalog;
    }

    private static void createTable(Catalog catalog) throws Exception {
        var schema = Schema.newBuilder().column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING()).primaryKey("id")
                .option("bucket", "1").build();
        catalog.createTable(Identifier.create("db", "orders"), schema, false);
    }

    private static void writeRow(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        var writer = builder.newWrite();
        writer.write(GenericRow.of(1L, BinaryString.fromString("alice")));
        List<org.apache.paimon.table.sink.CommitMessage> messages = writer.prepareCommit();
        writer.close();
        var commit = builder.newCommit();
        commit.commit(messages);
        commit.close();
    }
}
