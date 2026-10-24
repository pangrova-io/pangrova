package org.pangrova.connector.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;

/** Commit-shape selection for partitioned overwrites and copy-on-write deletes. */
class IcebergOverwriteCommitTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private Table table;

    private static final String CATALOG_NAME = "commit_catalog";
    private static final String NAMESPACE = "commit_db";
    private static final String TABLE_NAME = "partitioned";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "region", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME,
                Map.of("type", "hadoop", "warehouse", warehouse.toString()),
                new org.apache.hadoop.conf.Configuration());
        if (catalog instanceof SupportsNamespaces namespaces) {
            namespaces.createNamespace(Namespace.of(NAMESPACE));
        }
        table = catalog.createTable(TableIdentifier.of(NAMESPACE, TABLE_NAME), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build(),
                Map.of("format-version", "2"));
    }

    @Test
    void partitionedDeleteOverwritesTheWholeTable() {
        assertThat(IcebergOverwriteCommit.build(table, handle(IcebergOutputTableHandle.MODE_DELETE),
                List.of(dataFile("eu"))))
                .isInstanceOf(OverwriteFiles.class);
    }

    @Test
    void partitionedDeleteWithoutSurvivorsStillPublishesAnOverwrite() {
        assertThat(IcebergOverwriteCommit.build(table, handle(IcebergOutputTableHandle.MODE_DELETE),
                List.of()))
                .isInstanceOf(OverwriteFiles.class);
    }

    @Test
    void partitionedOverwriteReplacesTheWholeTable() {
        assertThat(IcebergOverwriteCommit.build(table, handle(IcebergOutputTableHandle.MODE_OVERWRITE),
                List.of(dataFile("eu"))))
                .isInstanceOf(OverwriteFiles.class);
    }

    private IcebergOutputTableHandle handle(String writeMode) {
        var metadata = ((HasTableOperations) table).operations().refresh();
        long snapshotId = metadata.currentSnapshot() == null
                ? IcebergOutputTableHandle.NO_SNAPSHOT : metadata.currentSnapshot().snapshotId();
        return new IcebergOutputTableHandle(NAMESPACE, TABLE_NAME, metadata.uuid(),
                metadata.location(), SchemaParser.toJson(metadata.schema()),
                PartitionSpecParser.toJson(metadata.spec()), metadata.formatVersion(), snapshotId,
                writeMode, Map.of(), IcebergDeclarationProperties.capture(metadata.properties()));
    }

    private DataFile dataFile(String region) {
        return DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + region + ".parquet")
                .withFileSizeInBytes(128)
                .withRecordCount(1)
                .withPartitionPath("region=" + region)
                .build();
    }
}
