package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.coordinator.indexing.ddl.DurableIndexBuildService;
import org.pangrova.coordinator.indexing.ddl.IndexBuildServices;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.submission.GenerationStageGraphFactory;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.config.ConsistencyConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.testinfra.GenerationExecutionFixtures;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.postgresql.publication.JdbcIndexGenerationPublicationStore;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.enums.SnapshotRelation;
import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.connector.api.index.SnapshotFileDelta;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

/** Real publication and connector evidence for Native index maintenance coverage. */
class NativeIndexCoverageLifecycleIntegrationTest extends ContainerTestBase {

    private static final Instant START = Instant.parse("2026-10-01T00:00:00Z");
    private static final String TENANT = "native-coverage-tenant";
    private static final String INDEX = "native-coverage-index";
    private static final String JOB = "index-build:tenant-"
            + DigestUtils.sha256Hex(TENANT).substring(0, 32) + ":" + INDEX + ":generation:0";
    private static final String SNAPSHOT = "snapshot-0";
    private static final QualifiedTableName TABLE = new QualifiedTableName("catalog", "schema", "items");

    @TempDir
    Path temporaryDirectory;

    @Test
    void sameSnapshotNativeGenerationWithEmptyPartitionRemainsReadyAcrossCycles() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("native_coverage_lifecycle");
        PostgresGenerationJobStore builds = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        JdbcIndexGenerationPublicationStore publications =
                GenerationAuthorityFixtures.artifacts().indexPublications(dataSource);
        JdbcIndexMetadataStore indexes = new JdbcIndexMetadataStore(
                dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        IndexDescriptor descriptor = descriptor(IndexState.BUILDING);
        indexes.registerIndex(descriptor);
        GenerationAuthorityFixtures.provision(dataSource, TENANT, "native-builder", authority());
        publishTwoPartitions(dataSource, builds, publications, indexes);

        CatalogRegistry registry = registry(snapshotMetadata());
        IndexBuildCoordinator coordinator = buildCoordinator();
        IndexConsistencyChecker checker = null;
        try {
            var nativeBuilds = new DurableIndexBuildService(builds, indexes, registry, temporaryDirectory.toString(),
                    session -> new org.pangrova.connector.api.index.SnapshotPinProvider() {
                        @Override public void pinSnapshot(String catalog, String schema, String table,
                                String snapshot, org.pangrova.connector.api.index.SnapshotPinOwner owner) {}
                        @Override public void unpinSnapshot(String catalog, String schema, String table,
                                String snapshot, org.pangrova.connector.api.index.SnapshotPinOwner owner) {}
                        @Override public void validateSnapshotPin(String catalog, String schema, String table,
                                String snapshot, org.pangrova.connector.api.index.SnapshotPinOwner owner) {}
                        @Override public List<String> getPinnedSnapshots(String catalog, String schema, String table) {
                            return List.of();
                        }
                        @Override public List<org.pangrova.connector.api.index.SnapshotPinReference>
                                getSnapshotPinsForIndex(String catalog, String schema, String table, String index) {
                            return List.of();
                        }
                    });
            var config = new IndexingConfig(temporaryDirectory.toString(), false, null,
                    new LifecycleConfig(null, new ConsistencyConfig(30, 0.8, true, 2), null));
            var context = new IndexConsistencyContext(
                    indexes, new IndexBuildServices(coordinator, nativeBuilds),
                    new ConnectorAccessorResolver(registry), config,
                    new MaintenanceLeaderElection(new InMemoryDistributedLease(), "native-coverage-test"));
            checker = new IndexConsistencyChecker(context, null);

            for (int cycle = 0; cycle < 3; cycle++) {
                checker.checkAll();
                assertThat(indexes.findIndex(INDEX).orElseThrow())
                        .as("same-snapshot READY Native generation must not be replaced in cycle %s", cycle)
                        .satisfies(current -> {
                            assertThat(current.state()).isEqualTo(IndexState.READY);
                            assertThat(current.generation()).isZero();
                        });
            }
        } finally {
            if (checker != null) {
                checker.close();
            }
            coordinator.close();
            registry.shutdown();
        }
    }

    @Test
    void forwardCommitUsesNativeSourceCoverageAndCorruptionFailsClosed() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("native_coverage_commit");
        PostgresGenerationJobStore builds = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        JdbcIndexGenerationPublicationStore publications =
                GenerationAuthorityFixtures.artifacts().indexPublications(dataSource);
        JdbcIndexMetadataStore indexes = new JdbcIndexMetadataStore(
                dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        IndexDescriptor descriptor = descriptor(IndexState.BUILDING);
        indexes.registerIndex(descriptor);
        GenerationAuthorityFixtures.provision(dataSource, TENANT, "native-builder", authority());
        publishTwoPartitions(dataSource, builds, publications, indexes);

        CatalogRegistry registry = registry(forwardSnapshotMetadata());
        IndexBuildCoordinator coordinator = buildCoordinator();
        var nativeBuilds = new DurableIndexBuildService(builds, indexes, registry, temporaryDirectory.toString(),
                session -> new org.pangrova.connector.api.index.SnapshotPinProvider() {
                    @Override public void pinSnapshot(String catalog, String schema, String table,
                            String snapshot, org.pangrova.connector.api.index.SnapshotPinOwner owner) {}
                    @Override public void unpinSnapshot(String catalog, String schema, String table,
                            String snapshot, org.pangrova.connector.api.index.SnapshotPinOwner owner) {}
                    @Override public void validateSnapshotPin(String catalog, String schema, String table,
                            String snapshot, org.pangrova.connector.api.index.SnapshotPinOwner owner) {}
                    @Override public List<String> getPinnedSnapshots(String catalog, String schema, String table) {
                        return List.of();
                    }
                    @Override public List<org.pangrova.connector.api.index.SnapshotPinReference>
                            getSnapshotPinsForIndex(String catalog, String schema, String table, String index) {
                        return List.of();
                    }
                });
        var context = new IndexCommitContext(
                indexes, new IndexBuildServices(coordinator, nativeBuilds),
                new ConnectorAccessorResolver(registry));
        IndexCommitHandler handler = new IndexCommitHandler(context, null, 8);
        try {
            var event = new IndexCommitHandler.MergedCommitFiles(
                    TENANT, TABLE.catalog(), TABLE.schema(), TABLE.table(),
                    List.of("data-object-3"), List.of(),
                    org.pangrova.connector.api.enums.CommitEventType.APPEND, "snapshot-1", null);

            IndexRefreshInput delta = handler.resolveRefreshInput(
                    indexes, forwardSnapshotMetadata(), indexes.findIndex(INDEX).orElseThrow(), event);
            assertThat(delta.sourceSnapshot()).isEqualTo("snapshot-1");
            assertThat(delta.addedFiles()).extracting(file -> file.filePath())
                    .containsExactly("data-object-3");
            assertThat(delta.removedFiles()).isEmpty();
            assertThat(delta.forceReplacement()).isFalse();

            IndexRefreshInput replacement = handler.resolveRefreshInput(
                    indexes, forwardSnapshotMetadata(true), indexes.findIndex(INDEX).orElseThrow(), event);
            assertThat(replacement.addedFiles()).extracting(file -> file.filePath())
                    .containsExactly("data-object-1", "data-object-2", "data-object-3", "unexpected-object");
            assertThat(replacement.removedFiles()).containsExactlyInAnyOrder("data-object-1", "data-object-2");

            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE index_generation_manifests SET manifest_sha256='"
                        + "c".repeat(64) + "' WHERE index_id='" + INDEX + "' AND generation=0");
            }
            assertThatThrownBy(() -> handler.resolveRefreshInput(
                    indexes, forwardSnapshotMetadata(), indexes.findIndex(INDEX).orElseThrow(), event))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("differs from its durable publication");
        } finally {
            handler.close();
            coordinator.close();
            registry.shutdown();
        }
    }

    private void publishTwoPartitions(
            DataSource dataSource,
            PostgresGenerationJobStore builds,
            JdbcIndexGenerationPublicationStore publications,
            JdbcIndexMetadataStore indexes) {
        var executions = new GenerationExecutionFixtures(dataSource);
        List<GenerationPage> pages = List.of(page("page-1", "data-object-1", 0), page("page-2", "data-object-2", 1));
        GenerationJob job = new GenerationJob(
                TENANT, JOB, INDEX, GenerationOutputTypeIds.VECTOR_INDEX,
                "0", TABLE.toString(), SNAPSHOT, GenerationExecutionKind.BUILD,
                GenerationDependencySet.sourceSnapshot(TABLE.toString(), SNAPSHOT),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "file://" + temporaryDirectory.resolve("staging"), "pangrova.diskann.segment", 2, 0,
                "a".repeat(64), "native-build-1", "", "native-coverage-request",
                pages.size(), 0, GenerationJobState.QUEUED, 0, START, START, START);
        builds.createJob(job, dispatch(pages), pages);
        completePage(builds, executions, pages.get(0), 11);
        completePage(builds, executions, pages.get(1), 0);
        var claim = builds.claimReadyForPublication(
                TENANT, JOB, executions.takeOwnership(TENANT, JOB, "publisher", 9),
                9, Duration.ofSeconds(30), START.plusSeconds(10)).orElseThrow();
        publications.publishClaimed(claim, "publisher", 9, START.plusSeconds(11));
        assertThat(indexes.findIndex(INDEX).orElseThrow().state()).isEqualTo(IndexState.READY);
    }

    private void completePage(
            PostgresGenerationJobStore builds, GenerationExecutionFixtures executions,
            GenerationPage page, long rows) {
        var lease = builds.claimNextPage(
                TENANT, JOB, executions.takeOwnership(TENANT, JOB, "builder", 7),
                7, Duration.ofSeconds(30), START.plusSeconds(1)).orElseThrow();
        assertThat(lease.pageId()).isEqualTo(page.pageId());
        byte[] report = ("report-" + page.pageId()).getBytes(StandardCharsets.UTF_8);
        assertThat(builds.completePage(new GenerationPageCompletion(
                TENANT, JOB, page.pageId(), lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), List.of(
                        output("graph-" + page.pageId(), "DISKANN_GRAPH", page.pageId(), 0,
                                GenerationOutputScope.SOURCE_OBJECT, rows),
                        output("vectors-" + page.pageId(), "RAW_VECTORS", page.pageId(), 1,
                                GenerationOutputScope.SOURCE_OBJECT, rows),
                        output("locators-" + page.pageId(), "ROW_LOCATORS", page.pageId(), 2,
                                GenerationOutputScope.SOURCE_OBJECT, rows)),
                START.plusSeconds(2), executions.reportedOwner(TENANT, JOB, 7)))).isTrue();
    }

    private static GenerationOutput output(
            String objectId, String role, String pageId, int ordinal,
            GenerationOutputScope scope, long rows) {
        String sourceId = pageId.equals("page-1") ? "data-object-1" : "data-object-2";
        return new GenerationOutput(
                objectId, "file:///staging/" + objectId, role,
                scope == GenerationOutputScope.SOURCE_OBJECT ? sourceId : pageId,
                ordinal, scope, 100, rows, "b".repeat(64), "");
    }

    private static GenerationPage page(String pageId, String sourceId, int partition) {
        return new GenerationPage(
                JOB, pageId, 0, partition, partition, GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(sourceId, ("split-" + sourceId).getBytes(StandardCharsets.UTF_8))),
                List.of(), 5);
    }

    private static GenerationDispatchSpec dispatch(List<GenerationPage> pages) {
        return GenerationDispatchSpecFactory.create(
                JOB, GenerationStageGraphFactory.forArtifactPages(
                        "plan".getBytes(StandardCharsets.UTF_8),
                        "bindings".getBytes(StandardCharsets.UTF_8), pages, List.of()),
                TENANT, "native-builder", authority(),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static DeferredAuthorizationScope authority() {
        return DeferredAuthorizationScope.raw(TABLE, Set.of("embedding"), TABLE);
    }

    private static IndexDescriptor descriptor(IndexState state) {
        return new IndexDescriptor(
                INDEX, "native_idx", TABLE.catalog(), TABLE.schema(), TABLE.table(),
                List.of("embedding"), IndexType.VECTOR, Map.of("algorithm", "DISK_ANN"),
                state, null, 0, SNAPSHOT, null, START, START, TENANT);
    }

    private static SnapshotAwareMetadata snapshotMetadata() {
        return new SnapshotAwareMetadata() {
            @Override
            public Optional<String> currentSnapshotId() {
                return Optional.of(SNAPSHOT);
            }

            @Override
            public Optional<CurrentSnapshotFiles> currentSnapshotFiles() {
                return Optional.of(new CurrentSnapshotFiles(SNAPSHOT, files()));
            }

            @Override
            public List<SnapshotDataFile> listFilesAtSnapshot(String snapshotId) {
                assertThat(snapshotId).isEqualTo(SNAPSHOT);
                return files();
            }

            @Override
            public SnapshotRelation relation(String leftSnapshotId, String rightSnapshotId) {
                throw new AssertionError("Same-snapshot consistency does not compare lineage");
            }

            @Override
            public SnapshotAdvance observeAdvance(String fromSnapshotId, String toSnapshotId) {
                throw new AssertionError("Same-snapshot consistency does not advance lineage");
            }

            @Override
            public SnapshotFileDelta diffFiles(String fromSnapshotId, String toSnapshotId) {
                throw new AssertionError("Same-snapshot consistency does not diff files");
            }
        };
    }

    private static SnapshotAwareMetadata forwardSnapshotMetadata() {
        return forwardSnapshotMetadata(false);
    }

    private static SnapshotAwareMetadata forwardSnapshotMetadata(boolean drift) {
        List<SnapshotDataFile> target = new java.util.ArrayList<>(files());
        target.add(new SnapshotDataFile("data-object-3", 7, 100, "parquet"));
        if (drift) {
            target.add(new SnapshotDataFile("unexpected-object", 9, 100, "parquet"));
        }
        List<SnapshotDataFile> targetFiles = List.copyOf(target);
        return new SnapshotAwareMetadata() {
            @Override
            public Optional<String> currentSnapshotId() {
                return Optional.of("snapshot-1");
            }

            @Override
            public Optional<CurrentSnapshotFiles> currentSnapshotFiles() {
                return Optional.of(new CurrentSnapshotFiles("snapshot-1", targetFiles));
            }

            @Override
            public List<SnapshotDataFile> listFilesAtSnapshot(String snapshotId) {
                return targetFiles;
            }

            @Override
            public SnapshotRelation relation(String leftSnapshotId, String rightSnapshotId) {
                return SnapshotRelation.LEFT_ANCESTOR;
            }

            @Override
            public SnapshotAdvance observeAdvance(String fromSnapshotId, String toSnapshotId) {
                assertThat(fromSnapshotId).isEqualTo(SNAPSHOT);
                assertThat(toSnapshotId).isEqualTo("snapshot-1");
                return new SnapshotAdvance(SnapshotRelation.LEFT_ANCESTOR, targetFiles,
                        drift ? new SnapshotFileDelta(List.of(), List.of())
                                : new SnapshotFileDelta(List.of(targetFiles.get(2)), List.of()));
            }

            @Override
            public SnapshotFileDelta diffFiles(String fromSnapshotId, String toSnapshotId) {
                throw new AssertionError("Commit handling uses one lineage observation");
            }
        };
    }

    private static List<SnapshotDataFile> files() {
        return List.of(
                new SnapshotDataFile("data-object-1", 11, 100, "parquet"),
                new SnapshotDataFile("data-object-2", 0, 100, "parquet"));
    }

    private static CatalogRegistry registry(SnapshotAwareMetadata snapshots) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                TABLE.catalog(), CatalogType.CONNECTOR, "fixture", CatalogState.ACTIVE,
                Map.of(), "", Set.of());
        var connector = new Connector() {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session) {
                return metadata(snapshots);
            }

            @Override
            public ConnectorSplitManager getSplitManager() {
                return null;
            }

            @Override
            public ConnectorPageSourceProvider getPageSourceProvider() {
                return null;
            }
        };
        CatalogDeploymentRepository deployments = new CatalogDeploymentRepository() {
            @Override
            public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
                    String name, String incarnationId, long revision) {
                return Optional.empty();
            }

            @Override
            public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            }

            @Override
            public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
                return name.equals(TABLE.catalog()) ? Optional.of(descriptor) : Optional.empty();
            }

            @Override
            public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) {
                return false;
            }

            @Override
            public List<CatalogDescriptor> listAllCatalogDescriptors() {
                return List.of(descriptor);
            }
        };
        var registry = new CatalogRegistry(
                memoryTableStores(), deployments,
                ignored -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        registry.initialize();
        return registry;
    }

    private static ConnectorMetadata metadata(SnapshotAwareMetadata snapshots) {
        return new ConnectorMetadata() {
            @Override
            public Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(String schema, String table) {
                return Optional.of(snapshots);
            }

            @Override
            public List<String> listSchemaNames(ConnectorSession session) {
                return List.of();
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
                return List.of();
            }

            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
                return null;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
                return null;
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
                return Map.of();
            }

            @Override
            public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
                return TableStatistics.UNKNOWN;
            }
        };
    }

    private static IndexBuildCoordinator buildCoordinator() {
        var config = new IndexingConfig("/tmp/native-coverage-build", false, null, null);
        return new IndexBuildCoordinator(new IndexBuildContext(
                config, new IndexEngineRegistry(List.of()), new IndexStorageManager(),
                new IndexSerializer(TypeSerializerRegistry.fromStrategies(List.of()))), null);
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
