package org.pangrova.coordinator.generation;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Leaf split/SDK contract data; not authorization or Native execution proof. */
public final class GeneratedBuildTestConnectors {
    private static final String SNAPSHOT = "41";

    private GeneratedBuildTestConnectors() {}

    static List<ConnectorTableMetadata.Column> sdkColumns() {
        return List.of(new ConnectorTableMetadata.Column("title", org.pangrova.types.enums.PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("body", org.pangrova.types.enums.PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("embedding", org.pangrova.types.enums.PangrovaType.VARBINARY));
    }

    public static final class SnapshotConnector implements Connector {
        private final ConnectorMetadata metadata;
        private final String snapshot;
        private final Runnable beforeEnumeration;
        private final AtomicInteger enumerations = new AtomicInteger();

        SnapshotConnector(Map<String, String> properties) {
            this(properties, SNAPSHOT, () -> {});
        }

        public SnapshotConnector(Map<String, String> properties, String snapshot, Runnable beforeEnumeration) {
            this.metadata = new SnapshotConnectorMetadata(properties);
            this.snapshot = snapshot;
            this.beforeEnumeration = beforeEnumeration;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return (transaction, session, table, dynamicFilter, constraint) -> {
                beforeEnumeration.run();
                enumerations.incrementAndGet();
                return new FixedSplitSource(List.of(split(0, snapshot), split(1, snapshot)));
            };
        }

        public int enumerations() {
            return enumerations.get();
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

    public static final class FixedCatalogRegistry extends CatalogRegistry {
        private final CatalogHandle handle;

        public FixedCatalogRegistry(CatalogHandle handle) {
            super(memoryTableStores(), null);
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

    record SnapshotSplit(org.pangrova.protocol.v2.DataSplit data)
            implements org.pangrova.protocol.source.CanonicalConnectorSplit {
        @Override
        public org.pangrova.protocol.v2.DataSplit toDataSplit() {
            return data;
        }
    }

    private static SnapshotSplit split(int ordinal, String snapshot) {
        var dataSplit = DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "test", "lake.sales.documents", snapshot,
                        "object-" + ordinal,
                        "s3://bucket/object-" + ordinal + ".parquet",
                        0, 1024, Map.of(),
                        FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                                .build()));
        return new SnapshotSplit(dataSplit);
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
