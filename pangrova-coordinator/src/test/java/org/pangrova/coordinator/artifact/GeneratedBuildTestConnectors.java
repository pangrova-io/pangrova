package org.pangrova.coordinator.artifact;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Leaf split/SDK contract data; not authorization or Native execution proof. */
final class GeneratedBuildTestConnectors {
    private static final String SNAPSHOT = "41";

    private GeneratedBuildTestConnectors() {}

    static List<ConnectorTableMetadata.Column> sdkColumns() {
        return List.of(new ConnectorTableMetadata.Column("title", org.pangrova.spi.connector.data.type.PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("body", org.pangrova.spi.connector.data.type.PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("embedding", org.pangrova.spi.connector.data.type.PangrovaType.VARBINARY));
    }

    static final class SnapshotConnector implements Connector {
        private final ConnectorMetadata metadata;

        SnapshotConnector(Map<String, String> properties) {
            this.metadata = new SnapshotConnectorMetadata(properties);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return (transaction, session, table, dynamicFilter, constraint) ->
                    new FixedSplitSource(List.of(split(0), split(1)));
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }
    }

    static final class SnapshotConnectorMetadata implements ConnectorMetadata {
        private final Map<String, String> properties;

        private SnapshotConnectorMetadata(Map<String, String> properties) {
            this.properties = Map.copyOf(properties);
        }
        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session, SchemaTableName tableName) {
            return new SnapshotTableHandle(-1);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(
                    new SchemaTableName("sales", "documents"),
                    List.of("title", "body", "embedding"),
                    sdkColumns(),
                    properties);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("sales");
        }

        @Override
        public List<SchemaTableName> listTables(
                ConnectorSession session, Optional<String> schemaName) {
            return List.of(new SchemaTableName("sales", "documents"));
        }
    }

    static final class FixedCatalogRegistry extends CatalogRegistry {
        private final CatalogHandle handle;

        FixedCatalogRegistry(CatalogHandle handle) {
            super(null);
            this.handle = handle;
        }

        @Override
        public CatalogHandle getCatalog(String name) {
            if (!"lake".equals(name)) {
                throw new IllegalArgumentException("unknown catalog " + name);
            }
            return handle;
        }

        @Override
        public List<String> listCatalogNames() {
            return List.of("lake");
        }
    }

    record SnapshotTableHandle(long snapshot) implements ConnectorTableHandle {
        @Override
        public ConnectorTableHandle withTimeTravelSnapshot(long snapshotId) {
            return new SnapshotTableHandle(snapshotId);
        }
    }

    record SnapshotSplit(byte[] bytes) implements ConnectorSplit {
        @Override
        public byte[] toSplitInfoProto() {
            return bytes.clone();
        }
    }

    private static SnapshotSplit split(int ordinal) {
        var dataSplit = DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "test", "lake.sales.documents", SNAPSHOT,
                        "object-" + ordinal,
                        "s3://bucket/object-" + ordinal + ".parquet",
                        0, 1024, Map.of(),
                        FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                                .build()));
        return new SnapshotSplit(dataSplit.toByteArray());
    }

    static final class FixedSplitSource implements ConnectorSplitSource {
        private List<ConnectorSplit> remaining;

        private FixedSplitSource(List<? extends ConnectorSplit> splits) {
            this.remaining = List.copyOf(splits);
        }

        @Override
        public List<ConnectorSplit> getNextBatch(int maxSize) {
            List<ConnectorSplit> result = remaining.stream().limit(maxSize).toList();
            remaining = remaining.subList(result.size(), remaining.size());
            return result;
        }

        @Override
        public boolean isFinished() {
            return remaining.isEmpty();
        }

        @Override
        public void close() {
            remaining = List.of();
        }
    }
}
