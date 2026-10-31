package org.pangrova.connector.iceberg;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergPartitionTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergPartitionReadBindingsTest {
    @TempDir Path warehouse;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority>
                getQueryAuthority() { return java.util.Optional.empty(); }
        @Override public String getUser() { return "partitions-test"; }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of("bound"); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void frozenInputsAndSchemaStayPairedAcrossCommitAndSpecChange(boolean partitioned) throws Exception {
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
            var spec = partitioned ? PartitionSpec.builderFor(schema).identity("id").build()
                    : PartitionSpec.unpartitioned();
            var table = catalog.createTable(TableIdentifier.of("db", "t"), schema, spec,
                    Map.of("format-version", "2"));
            var metadata = new IcebergMetadata(catalog, "bound");
            var name = new SchemaTableName("db", "t$partitions");
            var selected = (IcebergPartitionTableHandle) metadata.getTableHandle(SESSION, name);
            var bound = metadata.bindRead(SESSION, selected);
            assertThat(selected.typedColumns()).containsExactlyElementsOf(IcebergTypeMapper.toPangrovaColumns(
                    MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS).schema()));
            assertThat(metadata.getColumnHandles(SESSION, selected).keySet()).containsExactlyInAnyOrderElementsOf(
                    metadata.getTableMetadata(SESSION, selected).columns());
            assertThat(selected.derivedQuery().inputs()).hasSize(2);
            var entries = (IcebergFileTableHandle) selected.derivedQuery().inputs().get(0).binding().handle();
            var snapshots = (IcebergSystemTableHandle) selected.derivedQuery().inputs().get(1).binding().handle();
            assertThat(entries.manifestSelection().metadataLocation()).isEqualTo(snapshots.metadataLocation());
            assertThat(entries.manifests()).isEmpty();
            String sql = selected.derivedQuery().sql();
            var file = DataFiles.builder(spec).withPath(table.location() + "/data/a.parquet")
                    .withRecordCount(2).withFileSizeInBytes(100);
            if (partitioned) file.withPartitionPath("id=1");
            table.newFastAppend().appendFile(file.build()).commit();
            if (partitioned) table.updateSpec().renameField("id", "renamed").commit();
            metadata.validateReadBinding(SESSION, bound);
            assertThat(bound.handle()).isSameAs(selected);
            assertThat(selected.derivedQuery().sql()).isEqualTo(sql);
            assertThat(entries.manifests()).isEmpty();
            var next = (IcebergPartitionTableHandle) metadata.getTableHandle(SESSION, name);
            assertThat(next.sourceIdentity()).isNotEqualTo(selected.sourceIdentity());
            assertThat(((IcebergFileTableHandle) next.derivedQuery().inputs().get(0).binding().handle()).manifests())
                    .hasSize(1);
        }
    }
}
