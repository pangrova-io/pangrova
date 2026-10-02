package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for IcebergMetadata DELETE and MERGE commit paths via finishDistributedWrite.
 * Uses real HadoopCatalog on local filesystem (no mocks per pangrova-rules §12).
 * Validates: Requirements 5.6, 2.6, 10.2
 */
class IcebergMetadataDeleteMergeTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    private static final String CATALOG_NAME = "dm_catalog";
    private static final String NAMESPACE = "dm_db";

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "test-tenant"); }
    };

    private static final Schema V2_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        createNamespace();
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
    }

    @Test
    void deleteFragment_commitsDeleteFilesViaRowDelta() {
        Table table = createV2TableWithData("delete_table");
        IcebergOutputTableHandle handle = beginDelete("delete_table");
        long snapshotBefore = table.currentSnapshot().snapshotId();

        String deleteFragment = buildDeleteFragmentJson(table);
        byte[] fragment = deleteFragment.getBytes(StandardCharsets.UTF_8);

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment));

        table.refresh();
        Snapshot afterSnapshot = table.currentSnapshot();
        assertThat(afterSnapshot).isNotNull();
        assertThat(afterSnapshot.snapshotId()).isNotEqualTo(snapshotBefore);
    }

    @Test
    void mergeFragment_commitsBothDataFileAndDeleteFile() {
        Table table = createV2TableWithData("merge_table");
        IcebergOutputTableHandle handle = beginDelete("merge_table");
        long snapshotBefore = table.currentSnapshot().snapshotId();

        String mergeFragment = buildMergeFragmentJson(table);
        byte[] fragment = mergeFragment.getBytes(StandardCharsets.UTF_8);

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment));

        table.refresh();
        Snapshot afterSnapshot = table.currentSnapshot();
        assertThat(afterSnapshot).isNotNull();
        assertThat(afterSnapshot.snapshotId()).isNotEqualTo(snapshotBefore);
    }

    @Test
    void deleteFragment_malformedDeleteFiles_throwsIllegalStateException() {
        createV2TableWithData("malformed_table");
        IcebergOutputTableHandle handle = beginDelete("malformed_table");

        String malformedFragment =
                "{\"operation\":\"DELETE\",\"deleteFiles\":[{\"path\":\"/tmp/bad.parquet\","
                + "\"format\":\"INVALID_FORMAT\",\"recordCount\":1,\"fileSizeInBytes\":100,"
                + "\"content\":\"POSITION_DELETES\"}]}";
        byte[] fragment = malformedFragment.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() ->
                metadata.finishDistributedWrite(SESSION, handle, List.of(fragment)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("malformed_table");
    }

    // --- helpers ---

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    /**
     * Creates a format-version=2 table and appends initial data so that
     * subsequent DELETE/MERGE operations have a base snapshot to work with.
     */
    private Table createV2TableWithData(String tableName) {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        Table table = catalog.createTable(tableId, V2_SCHEMA,
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));

        org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/init-data.parquet")
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withRecordCount(5)
                .withFileSizeInBytes(1024)
                .build();
        table.newAppend().appendFile(dataFile).commit();
        return table;
    }

    private IcebergOutputTableHandle beginDelete(String tableName) {
        var tableHandle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, tableName));
        return (IcebergOutputTableHandle) metadata.beginDelete(SESSION, tableHandle);
    }

    private String buildDeleteFragmentJson(Table table) {
        String deletePath = table.location() + "/data/pos-delete-001.parquet";
        return "{\"operation\":\"DELETE\",\"deleteFiles\":[{\"path\":\""
                + deletePath
                + "\",\"format\":\"PARQUET\",\"recordCount\":2,\"fileSizeInBytes\":512,"
                + "\"content\":\"POSITION_DELETES\"}]}";
    }

    private String buildMergeFragmentJson(Table table) {
        org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/merge-data-001.parquet")
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withRecordCount(10)
                .withFileSizeInBytes(2048)
                .build();
        String dataFileJson = org.apache.iceberg.ContentFileParser.toJson(
                dataFile, table.spec());
        String deletePath = table.location() + "/data/merge-delete-001.parquet";
        return "{\"operation\":\"MERGE\",\"dataFiles\":[" + dataFileJson + "],"
                + "\"deleteFiles\":[{\"path\":\""
                + deletePath
                + "\",\"format\":\"PARQUET\",\"recordCount\":3,\"fileSizeInBytes\":512,"
                + "\"content\":\"POSITION_DELETES\"}]}";
    }
}
