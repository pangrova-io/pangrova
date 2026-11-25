package org.pangrova.connector.paimon;

import org.pangrova.protocol.source.CanonicalConnectorSplit;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.connector.paimon.handle.PaimonSystemTableHandle;
import org.pangrova.connector.paimon.handle.PaimonFilesTableHandle;
import org.pangrova.connector.paimon.handle.PaimonFilesSystemTableSplit;
import org.pangrova.connector.paimon.handle.PaimonSystemTableSplit;
import org.pangrova.connector.paimon.handle.PaimonBranchTableHandle;
import org.pangrova.connector.paimon.handle.PaimonBranchSystemTableSplit;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK metadata objects are pinned as Native splits without Coordinator row materialization. */
@Execution(ExecutionMode.SAME_THREAD)
class PaimonNativeSystemTableBindingIntegrationTest {
    @Test
    void listingsUseTheNativePaimonMetadataSchemas() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            for (var kind : List.of(org.pangrova.connector.api.enums.MetadataTableKind.TAGS,
                    org.pangrova.connector.api.enums.MetadataTableKind.BRANCHES,
                    org.pangrova.connector.api.enums.MetadataTableKind.FILES)) {
                var listing = metadata.getMetadataListing(SESSION, kind).orElseThrow();
                assertThat(listing.source()).isEqualTo(kind);
                assertThat(listing.equalityFilters()).isEmpty();
            }
            assertThat(metadata.getMetadataListing(SESSION,
                    org.pangrova.connector.api.enums.MetadataTableKind.ENTRIES)).isEmpty();
        }
    }
    @TempDir
    Path warehouse;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority>
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
            var encoded = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());
            assertThat(encoded.getSourceSnapshot()).isEqualTo(bound.sourceIdentity());
            assertThat(encoded.getOpaqueLocation()).isEqualTo(bound.objects().get(0).uri());
            var detail = encoded.getFileDetail();
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
            var detail = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray())
                    .getFileDetail().getSystemTable();
            assertThat(detail.getKind()).isEqualTo(SystemTableSplitDetail.Kind.PAIMON_OPTIONS);
            assertThat(detail.getSchemaId()).isEqualTo(schema.id());
            assertThat(detail.getObjectSha256().size()).isEqualTo(32);
        }
    }

    @Test
    void schemasSqlBindingPinsEveryVersionAndRejectsSameLengthReplacement() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var id = Identifier.create("db", "orders");
            catalog.alterTable(id, List.of(SchemaChange.addColumn("extra", DataTypes.STRING())), false);
            catalog.alterTable(id, List.of(SchemaChange.addColumn("more", DataTypes.BIGINT())), false);
            var table = (FileStoreTable) catalog.getTable(id);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$schemas"));
            assertThat(metadata.getTableMetadata(SESSION, selector).columns()).containsExactly(
                    "schema_id", "fields", "partition_keys", "primary_keys", "options",
                    "comment", "update_time");
            var binding = metadata.bindRead(SESSION, selector);
            var bound = (PaimonSystemTableHandle) binding.handle();
            assertThat(bound.objects()).extracting(PaimonSystemTableHandle.MetadataObject::schemaId)
                    .containsExactly(0L, 1L, 2L);
            metadata.validateReadBinding(SESSION, binding);
            for (var object : bound.objects()) {
                assertThat(Files.readString(Path.of(object.uri())))
                        .contains("\"id\"", "\"fields\"", "\"partitionKeys\"",
                                "\"primaryKeys\"", "\"options\"", "\"timeMillis\"");
                var split = new PaimonSystemTableSplit(
                        bound.tableName(), bound.sourceIdentity(), bound.kind(), object);
                var detail = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray())
                        .getFileDetail().getSystemTable();
                assertThat(detail.getKind()).isEqualTo(SystemTableSplitDetail.Kind.PAIMON_SCHEMAS);
                assertThat(detail.getSchemaId()).isEqualTo(object.schemaId());
                assertThat(detail.getObjectSha256().size()).isEqualTo(32);
            }
            var path = Path.of(table.schemaManager().toSchemaPath(1).toString());
            byte[] contents = Files.readAllBytes(path);
            contents[contents.length - 1] ^= 1;
            Files.write(path, contents);
            assertThat(Files.size(path)).isEqualTo(contents.length);
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                    .hasMessageContaining("Pinned Paimon metadata object changed");
        }
    }

    @Test
    @ResourceLock(value = "java.util.TimeZone", mode = ResourceAccessMode.READ_WRITE)
    void branchesSqlBindingPinsDirectoryStatusAndTypedSplit() throws Exception {
        warehouse = warehouse.resolve("with space");
        var previousZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
                createTable(catalog);
                var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
                var metadata = new PaimonMetadata(catalog, "paimon", registry);
                var selector = metadata.getTableHandle(SESSION,
                        new SchemaTableName("db", "orders$branches"));
                assertThat(selector).isInstanceOf(PaimonBranchTableHandle.class);
                assertThat(metadata.getTableMetadata(SESSION, selector).columns())
                        .containsExactly("branch_name", "create_time");
                var empty = metadata.bindRead(SESSION, selector);
                assertThat(((PaimonBranchTableHandle) empty.handle()).directories()).isEmpty();
                metadata.validateReadBinding(SESSION, empty);

                table.createBranch("a..b");
                table.createBranch("dev");
                table.createBranch("feature beta");
                table.createBranch("release");
                metadata.validateReadBinding(SESSION, empty);
                assertThat(((PaimonBranchTableHandle) empty.handle()).directories()).isEmpty();
                var binding = metadata.bindRead(SESSION, selector);
                var bound = (PaimonBranchTableHandle) binding.handle();
                assertThat(bound.directories()).extracting(PaimonBranchTableHandle.BranchDirectory::name)
                        .containsExactly("a..b", "dev", "feature beta", "release");
                assertThat(bound.directories()).allSatisfy(directory ->
                        assertThat(directory.uri()).contains("with%20space"));
                assertThat(bound.directories().get(2).uri()).contains("branch-feature%20beta");
                for (var directory : bound.directories()) {
                    assertThat(directory.modificationTimeMillis()).isEqualTo(
                            Files.getLastModifiedTime(Path.of(java.net.URI.create(directory.uri())))
                                    .toMillis());
                    assertThat(directory.zoneOffsetSeconds()).isEqualTo(8 * 3600);
                    assertThat(LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(directory.modificationTimeMillis()),
                            ZoneId.systemDefault()))
                            .isEqualTo(LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(directory.modificationTimeMillis()
                                            + directory.zoneOffsetSeconds() * 1000L),
                                    java.time.ZoneOffset.UTC));
                }
                metadata.validateReadBinding(SESSION, binding);
                TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                try {
                    assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                            .hasMessageContaining("Pinned Paimon branch directory changed");
                } finally {
                    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
                }
                for (var directory : bound.directories()) {
                    var split = new PaimonBranchSystemTableSplit(
                            bound.tableName(), bound.sourceIdentity(), directory);
                    var encoded = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());
                    assertThat(encoded.getLength()).isZero();
                    var detail = encoded.getFileDetail();
                    assertThat(detail.getFileFormat())
                            .isEqualTo(org.pangrova.protocol.v2.FileFormat.FILE_FORMAT_METADATA_DIRECTORY);
                    assertThat(detail.getSystemTable().getKind())
                            .isEqualTo(SystemTableSplitDetail.Kind.PAIMON_BRANCHES);
                    assertThat(detail.getSystemTable().getBranchName()).isEqualTo(directory.name());
                    assertThat(detail.getSystemTable().getDirectoryMtimeMs())
                            .isEqualTo(directory.modificationTimeMillis());
                    assertThat(detail.getSystemTable().getDirectoryDevice()).isEqualTo(directory.device());
                    assertThat(detail.getSystemTable().getDirectoryInode()).isEqualTo(directory.inode());
                    assertThat(detail.getSystemTable().getDirectoryZoneOffsetSeconds())
                            .isEqualTo(directory.zoneOffsetSeconds());
                }
                var path = Path.of(java.net.URI.create(bound.directories().get(0).uri()));
                Files.setLastModifiedTime(path, FileTime.fromMillis(
                        bound.directories().get(0).modificationTimeMillis() + 2_000));
                assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                        .hasMessageContaining("Pinned Paimon branch directory changed");
            }
        } finally {
            TimeZone.setDefault(previousZone);
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
            var encoded = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());
            var detail = encoded.getFileDetail().getSystemTable();
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

    @Test
    void branchCreationUsesTheRequestedTagWithoutBorrowingMovingMain() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            table.createTag("release");
            writeRow(table);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
            metadata.createBranch(SESSION, handle, "from_release",
                    new org.pangrova.connector.api.metadata.BranchCreationOptions(
                            java.util.OptionalLong.empty(), Optional.of("release"), java.util.OptionalLong.empty()));
            var branch = table.copy(Map.of("branch", "from_release"));
            assertThat(branch.snapshotManager().latestSnapshotId()).isEqualTo(1L);
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        }
    }

    @Test
    void unsupportedBranchOptionsFailBeforeCreatingAnyBranch() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
            for (var options : List.of(
                    new org.pangrova.connector.api.metadata.BranchCreationOptions(
                            java.util.OptionalLong.of(1), Optional.empty(), java.util.OptionalLong.empty()),
                    new org.pangrova.connector.api.metadata.BranchCreationOptions(
                            java.util.OptionalLong.empty(), Optional.empty(), java.util.OptionalLong.of(1)))) {
                assertThatThrownBy(() -> metadata.createBranch(SESSION, handle, "unsupported", options))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
            assertThat(table.branchManager().branches()).doesNotContain("unsupported");
        }
    }

    @Test
    void filesBindingPreservesTheSdkSchemaAndOnlyOriginalMetadataDescriptors() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files"));
            var binding = metadata.bindRead(SESSION, selector);
            var handle = (PaimonFilesTableHandle) binding.handle();
            assertThat(handle.typedColumns()).hasSize(20);
            var base = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
            assertThat(metadata.getSystemTable(SESSION, base, "files")).isEmpty();
            assertThat(metadata.getTableMetadata(SESSION, selector).columns()).containsExactlyElementsOf(
                    org.apache.paimon.table.system.FilesTable.TABLE_TYPE.getFieldNames());
            assertThat(handle.manifests()).hasSize(1);
            assertThat(binding.statistics().rowCount()).isEqualTo(1);
            var source = handle.manifests().get(0);
            var encoded = DataSplit.parseFrom(new PaimonFilesSystemTableSplit(
                    handle.tableName(), source).toDataSplit().toByteArray());
            var detail = encoded.getFileDetail();
            assertThat(detail.getSystemTable().getKind()).isEqualTo(SystemTableSplitDetail.Kind.PAIMON_FILES);
            assertThat(detail.getSystemTable().getPaimonFiles()).isEqualTo(source.context());
            assertThat(encoded.getSourceSnapshot()).isEqualTo(handle.snapshot().getUri());
            assertThat(source.context().getSchemasList()).singleElement().satisfies(schema ->
                    assertThat(schema.getSchemaJson())
                            .contains("\"id\"", "\"fields\"", "\"partitionKeys\"", "\"primaryKeys\"")
                            .doesNotContain("\"options\"", "\"timeMillis\"", "\"comment\""));
            assertThat(source.context().getFilesList()).singleElement().satisfies(file -> {
                assertThat(file.getIdentifierSha256().size()).isEqualTo(32);
                assertThat(file.getFileUri()).endsWith(".parquet");
            });
            metadata.validateReadBinding(SESSION, binding);
        }
    }

    @Test
    void filesBindingFreezesTheHeadWhileLaterCommitsRemainVisibleToNewBindings() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var table = (FileStoreTable) catalog.getTable(Identifier.create("db", "orders"));
            writeRow(table);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files"));
            var first = metadata.bindRead(SESSION, selector);
            writeRow(table);
            metadata.validateReadBinding(SESSION, first);
            var second = metadata.bindRead(SESSION, selector);
            assertThat(((PaimonFilesTableHandle) first.handle()).snapshotId()).isEqualTo(1);
            assertThat(((PaimonFilesTableHandle) second.handle()).snapshotId()).isEqualTo(2);
            assertThat(second.sourceIdentity()).isNotEqualTo(first.sourceIdentity());
            var historical = metadata.bindRead(SESSION, selector.withTimeTravelSnapshot("1"));
            assertThat(historical.snapshotId()).hasValue(1);
            assertThat(historical.state()).isEqualTo(
                    org.pangrova.connector.api.enums.ReadSnapshotState.SNAPSHOT);
            assertThat(historical.sourceIdentity()).isEqualTo(first.sourceIdentity());
        }
    }

    @Test
    void emptyFilesBindingStaysEmptyAfterTheFirstCommit() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files"));
            var empty = metadata.bindRead(SESSION, selector);
            assertThat(((PaimonFilesTableHandle) empty.handle()).manifests()).isEmpty();
            assertThat(empty.statistics().rowCount()).isZero();
            writeRow((FileStoreTable) catalog.getTable(Identifier.create("db", "orders")));
            metadata.validateReadBinding(SESSION, empty);
            assertThat(metadata.bindRead(SESSION, selector).sourceIdentity()).isNotEqualTo(empty.sourceIdentity());
        }
    }

    @Test
    void filesBindingRejectsSameLengthManifestReplacement() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            writeRow((FileStoreTable) catalog.getTable(Identifier.create("db", "orders")));
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var binding = metadata.bindRead(SESSION,
                    metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files")));
            var object = ((PaimonFilesTableHandle) binding.handle()).manifests().get(0);
            var path = Path.of(object.uri());
            byte[] bytes = Files.readAllBytes(path);
            bytes[bytes.length - 1] ^= 1;
            Files.write(path, bytes);
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                    .hasMessageContaining("Pinned Paimon manifest changed");
        }
    }

    @Test
    void filesBindingRejectsTableRecreationAtTheSameLocation() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var binding = metadata.bindRead(SESSION,
                    metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files")));
            catalog.dropTable(Identifier.create("db", "orders"), false);
            createTable(catalog);
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, binding))
                    .hasMessageContaining("incarnation changed");
        }
    }

    @Test
    void filesBindingSeesExternalSchemaEvolutionAcrossIndependentCatalogCaches() throws Exception {
        try (var writer = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(writer);
            var id = Identifier.create("db", "orders");
            writeRow((FileStoreTable) writer.getTable(id));
            var options = new Options();
            options.setString("warehouse", warehouse.toString());
            try (var reader = CatalogFactory.createCatalog(CatalogContext.create(options))) {
                var metadata = new PaimonMetadata(reader, "paimon", registry);
                var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files"));
                var original = metadata.bindRead(SESSION, selector);
                writer.alterTable(id, List.of(SchemaChange.renameColumn("name", "renamed"),
                        SchemaChange.addColumn("added", DataTypes.BIGINT())), false);
                var evolved = metadata.bindRead(SESSION, selector);
                var handle = (PaimonFilesTableHandle) evolved.handle();
                assertThat(handle.manifests()).singleElement().satisfies(manifest -> {
                    assertThat(manifest.context().getReadSchemaId()).isEqualTo(1);
                    assertThat(manifest.context().getSchemasList()).hasSize(2);
                });
                assertThat(evolved.sourceIdentity()).isNotEqualTo(original.sourceIdentity());
                metadata.validateReadBinding(SESSION, original);
                metadata.validateReadBinding(SESSION, evolved);
            }
        }
    }

    @Test
    void filesBindingDoesNotTransportTableOptionsInItsSchemaDeclaration() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            createTable(catalog);
            var id = Identifier.create("db", "orders");
            writeRow((FileStoreTable) catalog.getTable(id));
            catalog.alterTable(id, List.of(SchemaChange.setOption("s3.secret-key", "canary-test-secret")), false);
            var source = (FileStoreTable) catalog.getTable(id);
            assertThat(source.schema().options()).containsEntry("s3.secret-key", "canary-test-secret");
            var metadata = new PaimonMetadata(catalog, "paimon", registry);
            var binding = metadata.bindRead(SESSION,
                    metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders$files")));
            var handle = (PaimonFilesTableHandle) binding.handle();
            for (var manifest : handle.manifests()) {
                assertThat(manifest.context().getSchemasList()).allSatisfy(schema ->
                        assertThat(schema.getSchemaJson()).doesNotContain(
                                "s3.secret-key", "canary-test-secret", "\"options\""));
                assertThat(com.google.protobuf.ByteString.copyFrom(new PaimonFilesSystemTableSplit(
                        handle.tableName(), manifest).toDataSplit().toByteArray()).toStringUtf8())
                        .doesNotContain("s3.secret-key", "canary-test-secret");
            }
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
                .option("bucket", "1").option("file.format", "parquet").build();
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
