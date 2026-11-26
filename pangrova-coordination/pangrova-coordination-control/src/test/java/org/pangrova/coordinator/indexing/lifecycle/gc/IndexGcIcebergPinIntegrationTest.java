package org.pangrova.coordinator.indexing.lifecycle.gc;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
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
import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.coordinator.indexing.ddl.DeferredSnapshotPinProvider;
import org.pangrova.coordinator.indexing.ddl.IndexSourceAuthority;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.config.GcConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IndexGcIcebergPinIntegrationTest {
    private static final String OWNER = "mapping-test-owner";
    @TempDir Path warehouse;

    @Test
    void droppedIndexReleasesAllGenerationTagsAndFilesBeforePurgingPostgresIdentity() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("index_gc_pin");
        var indexes = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        var source = new QualifiedTableName("cat", "db", "items");
        GenerationAuthorityFixtures.provision(dataSource, "tenant-a", "alice",
                DeferredAuthorizationScope.raw(source, Set.of("id"), source));
        var registry = new CatalogRegistry(memoryTableStores(), new PostgresCatalogDeploymentRepository(dataSource));
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
            var buildTime = Instant.now();
            var descriptor = new IndexDescriptor("index-gc", "idx", "cat", "db", "items",
                    List.of("id"), IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING,
                    null, 0, Long.toString(firstSnapshot), Long.toString(firstSnapshot),
                    buildTime, buildTime, "tenant-a");
            indexes.registerIndexWithBuildAuthority(descriptor, "alice");
            var firstOwner = new org.pangrova.connector.api.index.SnapshotPinOwner(
                    descriptor.indexId(), 0, "attempt-1");
            var secondOwner = new org.pangrova.connector.api.index.SnapshotPinOwner(
                    descriptor.indexId(), 1, "attempt-2");
            pin.pinSnapshot("cat", "db", "items", Long.toString(firstSnapshot), firstOwner);
            pin.pinSnapshot("cat", "db", "items", Long.toString(rebuiltSnapshot), secondOwner);
            assertThat(pin.getSnapshotPinsForIndex("cat", "db", "items", descriptor.indexId()))
                    .extracting(org.pangrova.connector.api.index.SnapshotPinReference::snapshotToken)
                    .containsExactlyInAnyOrder(Long.toString(firstSnapshot), Long.toString(rebuiltSnapshot));
            Path firstIndexFile = warehouse.resolve("index-files/g0/one.idx");
            Path rebuiltIndexFile = warehouse.resolve("index-files/g1/two.idx");
            Files.createDirectories(firstIndexFile.getParent());
            Files.createDirectories(rebuiltIndexFile.getParent());
            byte[] firstBytes = {1, 2, 3};
            byte[] rebuiltBytes = {4, 5, 6, 7};
            Files.write(firstIndexFile, firstBytes);
            Files.write(rebuiltIndexFile, rebuiltBytes);
            assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim(
                    descriptor.indexId(), 0, OWNER, Instant.now())).isTrue();
            indexes.registerIndexFile(OWNER, new IndexFileMapping(descriptor.indexId(),
                    file.path().toString(), firstIndexFile.toString(), 1, firstBytes.length,
                    org.pangrova.common.util.DigestUtils.sha256Hex(firstBytes),
                    0, Long.toString(firstSnapshot), 0, old, 0));
            publishGeneration(indexes, descriptor.indexId(), 0, firstSnapshot, buildTime);
            var ready = indexes.findIndex(descriptor.indexId()).orElseThrow();
            var nextBuildTime = ready.updatedAt().plusMillis(1);
            assertThat(indexes.beginIndexGeneration(ready.indexId(), 1, Long.toString(rebuiltSnapshot),
                    nextBuildTime, ready.updatedAt(), ready.state(), ready.generation())).isTrue();
            assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim(
                    descriptor.indexId(), 1, OWNER, Instant.now())).isTrue();
            indexes.registerIndexFile(OWNER, new IndexFileMapping(descriptor.indexId(),
                    nextFile.path().toString(), rebuiltIndexFile.toString(), 1, rebuiltBytes.length,
                    org.pangrova.common.util.DigestUtils.sha256Hex(rebuiltBytes),
                    1, Long.toString(rebuiltSnapshot), 0, old, 0));
            publishGeneration(indexes, descriptor.indexId(), 1, rebuiltSnapshot, nextBuildTime);
            indexes.updateIndexState(descriptor.indexId(), "DROPPED", nextBuildTime.plusMillis(2));
            assertThat(indexes.getIndexFileMappings(descriptor.indexId()))
                    .extracting(IndexFileMapping::generation).containsExactlyInAnyOrder(0, 1);

            var context = new IndexGcContext(indexes, new IndexStorageManager(),
                    descriptorForFile -> new org.pangrova.indexing.storage.LocalFileSystemAccessor(),
                    config, pin);
            try (var gc = new IndexGarbageCollector(context, null)) {
                assertThat(gc.runGc().purgedCount()).isEqualTo(1);
            }
            assertThat(indexes.findIndex(descriptor.indexId())).isEmpty();
            assertThat(indexes.getIndexFileMappings(descriptor.indexId())).isEmpty();
            assertThat(Files.exists(firstIndexFile)).isFalse();
            assertThat(Files.exists(rebuiltIndexFile)).isFalse();
            assertThat(pin.getSnapshotPinsForIndex("cat", "db", "items", descriptor.indexId()))
                    .isEmpty();
        }
    }

    @Test
    void revokedSourceReadGrantDoesNotBlockDroppedIndexPinCleanup() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("index_gc_revoked_cleanup");
        var indexes = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        var source = new QualifiedTableName("cat", "db", "items");
        GenerationAuthorityFixtures.provision(dataSource, "tenant-a", "alice",
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
            var connector = new IcebergConnector("cat", catalog,
                    new org.apache.iceberg.hadoop.HadoopFileIO(new Configuration()), Set.of());
            try {
                var registry = new CatalogRegistry(memoryTableStores(), new PostgresCatalogDeploymentRepository(dataSource),
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
                assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim(
                        descriptor.indexId(), 0, OWNER, Instant.now())).isTrue();
                indexes.registerIndexFile(OWNER, new IndexFileMapping(descriptor.indexId(),
                        file.path().toString(), indexFile.toUri().toString(), 1, 3,
                        "a".repeat(64), 0, Long.toString(snapshot), 0, old, 0));
                var directPin = connector.getMetadata(null).getIndexSupport().orElseThrow()
                        .getSnapshotPinProvider().orElseThrow();
                directPin.pinSnapshot("cat", "db", "items", Long.toString(snapshot),
                        new org.pangrova.connector.api.index.SnapshotPinOwner(
                                descriptor.indexId(), 0, "attempt-1"));
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
                assertThat(directPin.getSnapshotPinsForIndex("cat", "db", "items", descriptor.indexId()))
                        .isEmpty();
            } finally {
                connector.shutdown();
            }
        }
    }

    @Test
    void drainedConstructionPinReleasesOnlyAfterPublicationAndNeverReleasesAnUnprovedAttempt() throws Exception {
        verifyScalarConstructionPinRelease(SharedPostgresContainer.migratedDataSource("scalar_pin_publication"));
    }

    private void verifyScalarConstructionPinRelease(javax.sql.DataSource dataSource) throws Exception {
        var indexes = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        var source = new QualifiedTableName("cat", "db", "items");
        GenerationAuthorityFixtures.provision(dataSource, "tenant-a", "alice",
                DeferredAuthorizationScope.raw(source, Set.of("id"), source));
        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var table = catalog.createTable(TableIdentifier.of("db", "items"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get())));
            table.newAppend().commit();
            long snapshot = table.currentSnapshot().snapshotId();
            table.newAppend().commit();
            long userSnapshot = table.currentSnapshot().snapshotId();
            table.manageSnapshots().createTag("user_keep", userSnapshot).commit();
            table.newAppend().commit();

            var connector = new IcebergConnector("cat", catalog,
                    new org.apache.iceberg.hadoop.HadoopFileIO(new Configuration()), Set.of());
            try {
                var registry = new CatalogRegistry(memoryTableStores(),
                        new PostgresCatalogDeploymentRepository(dataSource),
                        descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
                registry.register(new CatalogDescriptor("cat", CatalogType.CONNECTOR,
                        "iceberg", CatalogState.ACTIVE, Map.of(), "", Set.of("tenant-a")));
                var provider = new DeferredSnapshotPinProvider(indexes, null, new ConnectorAccessorResolver(registry));
                var direct = new IcebergMetadata(catalog, "cat").getIndexSupport().orElseThrow()
                        .getSnapshotPinProvider().orElseThrow();
                Instant now = Instant.now().minusSeconds(2);
                String token = Long.toString(snapshot);
                var descriptor = new IndexDescriptor("scalar-pin-index", "idx", "cat", "db", "items",
                        List.of("id"), IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING,
                        null, 0, token, null, now, now, "tenant-a");
                indexes.registerIndexWithBuildAuthority(descriptor, "alice");
                var claims = new JdbcIndexBuildClaimRepository(dataSource);
                var unknown = new org.pangrova.connector.api.index.SnapshotPinOwner(
                        descriptor.indexId(), 0, java.util.UUID.randomUUID().toString());
                var drained = new org.pangrova.connector.api.index.SnapshotPinOwner(
                        descriptor.indexId(), 0, java.util.UUID.randomUUID().toString());
                assertThat(claims.tryClaim(descriptor.indexId(), 0, unknown.attemptId(), now)).isTrue();
                provider.pinSnapshot("cat", "db", "items", token, unknown);
                // Simulate a lost claim while its source-reading stack has not acknowledged drain.
                assertThat(claims.release(descriptor.indexId(), 0, unknown.attemptId())).isTrue();
                assertThat(claims.tryClaim(descriptor.indexId(), 0, drained.attemptId(), now)).isTrue();
                provider.pinSnapshot("cat", "db", "items", token, drained);
                provider.markBuildDrained(drained, token);
                assertThat(provider.cleanupDrainedScalarPins(() -> true)).isZero();
                assertThat(claims.release(descriptor.indexId(), 0, drained.attemptId())).isTrue();
                assertThat(provider.cleanupDrainedScalarPins(() -> true)).isZero();
                table.refresh();
                table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1)
                        .retainLast(1).cleanExpiredFiles(false).commit();
                table.refresh();
                assertThat(table.snapshot(snapshot)).isNotNull();

                indexes.publishIndexGeneration(new IndexPublicationRequest(
                        descriptor.indexId(), descriptor.indexId(), 0, token, IndexType.BLOOM_FILTER.name(),
                        new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                        0, indexes.findIndex(descriptor.indexId()).orElseThrow().updatedAt(), Instant.now()));
                assertThat(provider.cleanupDrainedScalarPins(() -> false)).isZero();
                assertThat(provider.cleanupDrainedScalarPins(() -> true)).isEqualTo(1);
                assertThat(indexes.findScalarSnapshotPin(drained).orElseThrow().state()).isEqualTo("RELEASED");
                assertThat(indexes.findScalarSnapshotPin(unknown).orElseThrow().state()).isEqualTo("INTENDED");
                assertThat(direct.getSnapshotPinsForIndex("cat", "db", "items", descriptor.indexId()))
                        .extracting(org.pangrova.connector.api.index.SnapshotPinReference::owner)
                        .containsExactly(unknown);
                assertThat(indexes.findActiveIndexGeneration(descriptor.indexId())).isPresent();
                table.refresh();
                assertThat(table.refs()).containsKey("user_keep");
                assertThat(table.snapshot(userSnapshot)).isNotNull();
                indexes.dropIndex(descriptor.indexId(), Instant.now());
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> indexes.purgeIndex(descriptor.indexId()))
                        .hasMessageContaining("cleanup is incomplete");
                // A durable release claim whose connector outcome was uncertain must not
                // be retried until its due timestamp, then the real connector retry is safe.
                Instant previousAttempt = Instant.now().minus(java.time.Duration.ofMinutes(16));
                indexes.markScalarSnapshotPinDrained(unknown, token, previousAttempt.minusSeconds(1));
                assertThat(indexes.claimScalarSnapshotPinCleanup(unknown, previousAttempt)).isPresent();
                assertThat(indexes.claimScalarSnapshotPinCleanup(unknown, previousAttempt.plusSeconds(1)))
                        .isEmpty();
                assertThat(indexes.listScalarSnapshotPinCleanupCandidates(Instant.now(), 10))
                        .extracting(org.pangrova.persistence.indexing.api.metadata.ScalarSnapshotPins.ScalarPin::owner)
                        .containsExactly(unknown);
                assertThat(provider.cleanupDrainedScalarPins(() -> true)).isEqualTo(1);
                assertThat(direct.getSnapshotPinsForIndex("cat", "db", "items", descriptor.indexId())).isEmpty();
                indexes.purgeIndex(descriptor.indexId());
                table.refresh();
                assertThat(table.refs()).containsKey("user_keep");
            } finally {
                connector.shutdown();
            }
        }
    }

    private static void publishGeneration(JdbcIndexMetadataStore indexes, String indexId,
            int generation, long snapshotId, Instant updatedAt) {
        indexes.publishIndexGeneration(new IndexPublicationRequest(indexId, indexId, generation,
                Long.toString(snapshotId), IndexType.BLOOM_FILTER.name(),
                new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                1, updatedAt, updatedAt.plusMillis(1)));
    }
}
