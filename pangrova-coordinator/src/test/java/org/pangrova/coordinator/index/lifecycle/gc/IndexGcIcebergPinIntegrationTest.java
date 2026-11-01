package org.pangrova.coordinator.index.lifecycle.gc;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.iceberg.IcebergConnector;
import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.coordinator.index.ddl.DeferredSnapshotPinProvider;
import org.pangrova.coordinator.index.ddl.IndexSourceAuthority;
import org.pangrova.coordinator.testinfra.CoordinatorArtifactAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.config.GcConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.core.catalog.ConnectorCatalogHandle;
import org.pangrova.indexing.store.jdbc.JdbcIndexMetadataStore;
import org.pangrova.indexing.api.IndexFileMapping;
import org.pangrova.metadata.store.jdbc.JdbcMetadataStore;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IndexGcIcebergPinIntegrationTest {
    @TempDir Path warehouse;

    @Test
    void droppedIndexReleasesAllGenerationTagsAndFilesBeforePurgingPostgresIdentity() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("index_gc_pin");
        var indexes = new JdbcIndexMetadataStore(dataSource);
        var registry = new CatalogRegistry(new JdbcMetadataStore(dataSource));
        registry.initialize();
        var resolver = new ConnectorAccessorResolver(registry);
        var lifecycle = new LifecycleConfig(new GcConfig(1, 0), null, null);
        var config = new IndexingConfig(warehouse.toString(), false, null, lifecycle);

        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var table = catalog.createTable(TableIdentifier.of("db", "items"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            var file = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/items.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(file).commit();
            long firstSnapshot = table.currentSnapshot().snapshotId();
            var nextFile = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/next.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(nextFile).commit();
            long rebuiltSnapshot = table.currentSnapshot().snapshotId();
            assertThat(rebuiltSnapshot).isNotEqualTo(firstSnapshot);
            var pin = new IcebergMetadata(catalog, "cat").getIndexSupport().orElseThrow()
                    .getSnapshotPinProvider().orElseThrow();
            var old = Instant.now().minusSeconds(3600);
            var descriptor = new IndexDescriptor("index-gc", "idx", "cat", "db", "items",
                    List.of("id"), IndexType.BLOOM_FILTER, Map.of(), IndexState.DROPPED,
                    null, 1, Long.toString(rebuiltSnapshot), Long.toString(rebuiltSnapshot), old, old, "tenant-a");
            indexes.registerIndex(descriptor);
            pin.pinSnapshot("cat", "db", "items", firstSnapshot, descriptor.indexId());
            pin.pinSnapshot("cat", "db", "items", rebuiltSnapshot, descriptor.indexId());
            assertThat(pin.getPinnedSnapshotsForIndex("cat", "db", "items", descriptor.indexId()))
                    .containsExactlyInAnyOrder(firstSnapshot, rebuiltSnapshot);
            Path firstIndexFile = warehouse.resolve("index-files/g0/one.idx");
            Path rebuiltIndexFile = warehouse.resolve("index-files/g1/two.idx");
            Files.createDirectories(firstIndexFile.getParent());
            Files.createDirectories(rebuiltIndexFile.getParent());
            byte[] firstBytes = {1, 2, 3};
            byte[] rebuiltBytes = {4, 5, 6, 7};
            Files.write(firstIndexFile, firstBytes);
            Files.write(rebuiltIndexFile, rebuiltBytes);
            indexes.registerIndexFile(new IndexFileMapping(descriptor.indexId(),
                    file.path().toString(), firstIndexFile.toString(), 1, firstBytes.length,
                    org.pangrova.common.util.DigestUtils.sha256Hex(firstBytes),
                    0, Long.toString(firstSnapshot), 0, old, 0));
            indexes.registerIndexFile(new IndexFileMapping(descriptor.indexId(),
                    nextFile.path().toString(), rebuiltIndexFile.toString(), 1, rebuiltBytes.length,
                    org.pangrova.common.util.DigestUtils.sha256Hex(rebuiltBytes),
                    1, Long.toString(rebuiltSnapshot), 0, old, 0));
            assertThat(indexes.getIndexFileMappings(descriptor.indexId()))
                    .extracting(IndexFileMapping::generation).containsExactlyInAnyOrder(0, 1);

            var context = new IndexGcContext(indexes, new IndexStorageManager(),
                    descriptorForFile -> new org.pangrova.indexing.fs.LocalFileSystemAccessor(),
                    config, pin);
            try (var gc = new IndexGarbageCollector(context, null)) {
                assertThat(gc.runGc().purgedCount()).isEqualTo(1);
            }
            assertThat(indexes.findIndex(descriptor.indexId())).isEmpty();
            assertThat(indexes.getIndexFileMappings(descriptor.indexId())).isEmpty();
            assertThat(Files.exists(firstIndexFile)).isFalse();
            assertThat(Files.exists(rebuiltIndexFile)).isFalse();
            assertThat(pin.getPinnedSnapshotsForIndex("cat", "db", "items", descriptor.indexId()))
                    .isEmpty();
        }
    }

    @Test
    void revokedSourceReadGrantDoesNotBlockDroppedIndexPinCleanup() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("index_gc_revoked_cleanup");
        var indexes = new JdbcIndexMetadataStore(dataSource);
        var source = new QualifiedTableName("cat", "db", "items");
        CoordinatorArtifactAuthorityFixtures.provision(dataSource, "tenant-a", "alice",
                DeferredAuthorizationScope.raw(source, Set.of("id"), source));
        var lifecycle = new LifecycleConfig(new GcConfig(1, 0), null, null);
        var config = new IndexingConfig(warehouse.toString(), false, null, lifecycle);

        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var table = catalog.createTable(TableIdentifier.of("db", "items"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            var file = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/items.parquet")
                    .withFormat(FileFormat.PARQUET).withRecordCount(1).withFileSizeInBytes(32).build();
            table.newFastAppend().appendFile(file).commit();
            long snapshot = table.currentSnapshot().snapshotId();
            var connector = new IcebergConnector("cat", catalog, table.io(), Set.of());
            try {
                var registry = new CatalogRegistry(new JdbcMetadataStore(dataSource),
                        descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
                registry.register(new CatalogDescriptor("cat", CatalogType.CONNECTOR,
                        "iceberg", CatalogState.ACTIVE, Map.of(), "", Set.of("tenant-a")));
                var resolver = new ConnectorAccessorResolver(registry);
                var old = Instant.now().minusSeconds(3600);
                var descriptor = new IndexDescriptor("index-revoked-gc", "idx", "cat", "db", "items",
                        List.of("id"), IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING,
                        null, 0, Long.toString(snapshot), null, old, old, "tenant-a");
                indexes.registerIndexWithBuildAuthority(descriptor, "alice");
                Path indexFile = warehouse.resolve("index-files/g0/one.idx");
                Files.createDirectories(indexFile.getParent());
                Files.write(indexFile, new byte[]{1, 2, 3});
                indexes.registerIndexFile(new IndexFileMapping(descriptor.indexId(),
                        file.path().toString(), indexFile.toUri().toString(), 1, 3,
                        "a".repeat(64), 0, Long.toString(snapshot), 0, old, 0));
                var directPin = connector.getMetadata(null).getIndexSupport().orElseThrow()
                        .getSnapshotPinProvider().orElseThrow();
                directPin.pinSnapshot("cat", "db", "items", snapshot, descriptor.indexId());
                indexes.dropIndex(descriptor.indexId(), old);
                try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                    statement.execute("DELETE FROM security_grants WHERE privilege='SELECT'");
                }

                var gc = new IndexGarbageCollector(new IndexGcContext(indexes,
                        new IndexStorageManager(),
                        dropped -> IndexSourceAuthority.forDescriptor(indexes, null, resolver, dropped)
                                .resolve(dropped.catalogName()),
                        config, new DeferredSnapshotPinProvider(indexes, null, resolver)), null);
                try (gc) {
                    assertThat(gc.runGc().purgedCount()).isEqualTo(1);
                }
                assertThat(indexes.findIndex(descriptor.indexId())).isEmpty();
                assertThat(Files.exists(indexFile)).isFalse();
                assertThat(directPin.getPinnedSnapshotsForIndex("cat", "db", "items", descriptor.indexId()))
                        .isEmpty();
            } finally {
                connector.shutdown();
            }
        }
    }
}
