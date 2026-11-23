package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.persistence.indexing.api.metadata.CommitEventRequest;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcIndexGenerationPublicationTest {
    private static final String OWNER = "mapping-test-owner";

    private JdbcIndexMetadataStore store;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("index_publication");
        store = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
    }

    @Test
    void publicationIsAtomicAndGenerationFenced() {
        IndexDescriptor descriptor = descriptor();
        store.registerIndex(descriptor);

        assertThatThrownBy(() -> store.publishIndexGeneration(request(descriptor, 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("object count mismatch");
        assertThat(store.findIndex(descriptor.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.BUILDING);
        assertThat(store.findActiveIndexGeneration(descriptor.indexId())).isEmpty();

        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim("idx", 0, OWNER, Instant.now())).isTrue();
        store.registerIndexFile(OWNER, mapping(0));
        var first = store.publishIndexGeneration(request(descriptor, 1));
        assertThat(first.generation()).isZero();
        assertThat(store.findIndex(descriptor.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.READY);

        IndexDescriptor ready = store.findIndex(descriptor.indexId()).orElseThrow();
        Instant nextTimestamp = ready.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration(
                ready.indexId(), 1, "snapshot-2", nextTimestamp,
                ready.updatedAt(), ready.state(), ready.generation())).isTrue();
        assertThat(store.isIndexGenerationBuildable(ready.indexId(), 1)).isTrue();
        assertThat(store.findIndex(ready.indexId()).orElseThrow())
                .satisfies(active -> {
                    assertThat(active.state()).isEqualTo(IndexState.READY);
                    assertThat(active.generation()).isZero();
                });
        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim("idx", 1, OWNER, Instant.now())).isTrue();
        store.registerIndexFile(OWNER, mapping(1));
        IndexDescriptor reservedGeneration = ready.withGeneration(1, nextTimestamp)
                .withBuildSnapshotId("snapshot-2", nextTimestamp);
        var second = store.publishIndexGeneration(request(reservedGeneration, 1));
        assertThat(store.isIndexGenerationBuildable(ready.indexId(), 1)).isFalse();

        assertThat(second.generation()).isEqualTo(1);
        assertThat(store.findActiveIndexGeneration(ready.indexId())
                .orElseThrow().generation()).isEqualTo(1);
        assertThat(store.publishIndexGeneration(request(reservedGeneration, 1)))
                .isEqualTo(second);
    }

    @Test
    void failedOnlineGenerationLeavesActiveGenerationQueryable() {
        IndexDescriptor descriptor = descriptor();
        store.registerIndex(descriptor);
        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim("idx", 0, OWNER, Instant.now())).isTrue();
        store.registerIndexFile(OWNER, mapping(0));
        store.publishIndexGeneration(request(descriptor, 1));

        IndexDescriptor ready = store.findIndex(descriptor.indexId()).orElseThrow();
        Instant buildVersion = ready.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration(
                ready.indexId(), 1, "snapshot-2", buildVersion,
                ready.updatedAt(), ready.state(), ready.generation())).isTrue();
        assertThat(store.failIndexGeneration(
                ready.indexId(), 1, buildVersion, buildVersion.plusMillis(1), "boom"))
                .isTrue();
        assertThat(store.isIndexGenerationBuildable(ready.indexId(), 1)).isFalse();

        assertThat(store.findIndex(ready.indexId()).orElseThrow())
                .satisfies(active -> {
                    assertThat(active.state()).isEqualTo(IndexState.READY);
                    assertThat(active.generation()).isZero();
                });
        assertThat(store.findActiveIndexGeneration(ready.indexId()).orElseThrow().generation())
                .isZero();
    }

    @Test
    void orphanRemovalTargetsExactFileGenerationAndObservedDescriptor() {
        IndexDescriptor descriptor = descriptor();
        store.registerIndex(descriptor);
        IndexFileMapping oldFile = new IndexFileMapping(
                descriptor.indexId(), "same-data", "v1/shared.idx",
                10, 256, "a".repeat(64), 0,
                "old", 0, Instant.EPOCH, 0);
        IndexFileMapping currentFile = new IndexFileMapping(
                descriptor.indexId(), "same-data", "v2/g2.idx",
                10, 256, "b".repeat(64), 2,
                "current", 0, Instant.EPOCH, 0);
        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim("idx", 0, OWNER, Instant.now())).isTrue();
        store.registerIndexFile(OWNER, oldFile);
        store.publishIndexGeneration(request(descriptor, 1));
        var ready = store.findIndex(descriptor.indexId()).orElseThrow();
        var buildVersion = ready.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration(ready.indexId(), 1, "snapshot-2", buildVersion,
                ready.updatedAt(), ready.state(), ready.generation())).isTrue();
        var reservedGeneration = ready.withGeneration(1, buildVersion)
                .withBuildSnapshotId("snapshot-2", buildVersion);
        store.publishIndexGeneration(request(reservedGeneration, 0));
        var readyGen1 = store.findIndex(descriptor.indexId()).orElseThrow();
        var gen2Version = readyGen1.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration(readyGen1.indexId(), 2, "snapshot-3", gen2Version,
                readyGen1.updatedAt(), readyGen1.state(), readyGen1.generation())).isTrue();
        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim("idx", 2, OWNER, Instant.now())).isTrue();
        store.registerIndexFile(OWNER, currentFile);

        assertThat(store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 2, List.of(oldFile))
                .mappingsRemoved()).isZero();
        assertThat(store.getIndexFileMappings(descriptor.indexId())).hasSize(2);

        var gen2 = readyGen1.withGeneration(2, gen2Version)
                .withBuildSnapshotId("snapshot-3", gen2Version);
        store.publishIndexGeneration(request(gen2, 1));
        var removed = store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 2, List.of(oldFile));
        assertThat(removed.mappingsRemoved()).isOne();
        assertThat(removed.pathsToDelete()).containsExactly("v1/shared.idx");
        assertThat(store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 2, List.of(oldFile))
                .mappingsRemoved()).isZero();
        assertThat(store.getIndexFileMappings(descriptor.indexId()))
                .extracting(IndexFileMapping::indexFilePath)
                .containsExactly("v2/g2.idx");
    }

    @Test
    void simultaneousObsoleteOwnersOnlyReleaseThePathOnce() throws Exception {
        var descriptor = readyDescriptor("cohort", "tenant").withGeneration(4, Instant.now());
        store.registerIndex(descriptor);
        var first = seedObsoleteMapping(descriptor.indexId(), "first", "shared.idx", 0);
        var second = seedObsoleteMapping(descriptor.indexId(), "second", "shared.idx", 1);
        var current = seedObsoleteMapping(descriptor.indexId(), "serving", "serving.idx", 4);
        var executor = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var firstDecision = executor.submit(() -> {
                assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                return store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 4, List.of(first));
            });
            var secondDecision = executor.submit(() -> {
                assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                return store.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 4, List.of(second));
            });
            start.countDown();
            var results = List.of(firstDecision.get(10, TimeUnit.SECONDS), secondDecision.get(10, TimeUnit.SECONDS));
            assertThat(results).allMatch(result -> result.mappingsRemoved() == 1);
            assertThat(results.stream().flatMap(result -> result.pathsToDelete().stream()).toList())
                    .containsExactly("shared.idx");
            assertThat(store.getIndexFileMappings(descriptor.indexId()))
                    .extracting(IndexFileMapping::indexFilePath).containsExactly(current.indexFilePath());
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void obsoleteCohortUsesOneDescriptorLockAndOneSqlMutation() throws Exception {
        var descriptor = readyDescriptor("bulk", "tenant").withGeneration(4, Instant.now());
        store.registerIndex(descriptor);
        List<IndexFileMapping> candidates = new ArrayList<>();
        List<String> expectedPaths = new ArrayList<>();
        for (int file = 0; file < 100; file++) {
            String id = "data-" + file;
            String path = "old-" + file + ".idx";
            candidates.add(seedObsoleteMapping(descriptor.indexId(), id, path, 2));
            expectedPaths.add(path);
        }
        var current = seedObsoleteMapping(descriptor.indexId(), "serving", expectedPaths.remove(0), 4);
        var preparedSql = new ArrayList<String>();
        var counted = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                Connection actual = dataSource.getConnection();
                return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                        new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                            if (method.getName().equals("prepareStatement")) preparedSql.add((String) args[0]);
                            try {
                                return method.invoke(actual, args);
                            } catch (InvocationTargetException failure) {
                                throw failure.getCause();
                            }
                        });
            }
        };
        var gc = new JdbcIndexGcStore(counted);

        var removed = gc.removeOrphanMappings(descriptor.indexId(), IndexState.READY, 4, candidates);

        assertThat(removed.mappingsRemoved()).isEqualTo(100);
        assertThat(removed.pathsToDelete()).containsExactlyInAnyOrderElementsOf(expectedPaths);
        assertThat(preparedSql).hasSize(2);
        assertThat(preparedSql.get(0)).startsWith("SELECT index_id FROM index_descriptors");
        assertThat(preparedSql.get(1)).startsWith("WITH candidates");
        assertThat(store.getIndexFileMappings(descriptor.indexId()))
                .extracting(IndexFileMapping::indexFilePath).containsExactly(current.indexFilePath());
    }

    private IndexFileMapping seedObsoleteMapping(String indexId, String data, String path, int generation)
            throws java.sql.SQLException {
        try (var connection = dataSource.getConnection();
                var insert = connection.prepareStatement("""
                        INSERT INTO index_file_mappings(
                            index_id, data_file_path, index_file_path, generation, created_at, checksum)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """)) {
            insert.setString(1, indexId);
            insert.setString(2, data);
            insert.setString(3, path);
            insert.setInt(4, generation);
            insert.setTimestamp(5, java.sql.Timestamp.from(Instant.EPOCH));
            insert.setString(6, "f".repeat(64));
            insert.executeUpdate();
        }
        return new IndexFileMapping(indexId, data, path, 0, 0, "f".repeat(64), generation,
                null, 0, Instant.EPOCH, 0);
    }

    @Test
    void lifecycleLockSerializesIndependentMetadataConnections() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var competing = new CountDownLatch(1);
        var acquired = new CountDownLatch(1);
        var workers = Executors.newFixedThreadPool(2);
        try {
            var first = workers.submit(() -> store.withIndexLifecycleLock("same-index", () -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Lifecycle lock test timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var second = workers.submit(() -> {
                competing.countDown();
                new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent)
                        .withIndexLifecycleLock("same-index", acquired::countDown);
            });
            assertThat(competing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(acquired.await(200, TimeUnit.MILLISECONDS)).isFalse();
            release.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertThat(acquired.getCount()).isZero();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void sameNamedIndexesRemainIsolatedAcrossTenants() {
        IndexDescriptor tenantA = readyDescriptor("index-a", "tenant-a");
        IndexDescriptor tenantB = readyDescriptor("index-b", "tenant-b");
        store.registerIndex(tenantA);
        store.registerIndex(tenantB);

        assertThat(store.findIndexesByTable(
                "tenant-a", "catalog", "schema", "table"))
                .extracting(IndexDescriptor::indexId)
                .containsExactly("index-a");
        assertThat(store.findReadyIndexesForColumns(
                "tenant-b", "catalog", "schema", "table",
                List.of("embedding"), false))
                .extracting(IndexDescriptor::indexId)
                .containsExactly("index-b");

        store.renameColumnInIndexes(
                "tenant-a", "catalog", "schema", "table",
                "embedding", "vector_value");
        assertThat(store.findIndex("index-a").orElseThrow().columnNames())
                .containsExactly("vector_value");
        assertThat(store.findIndex("index-b").orElseThrow().columnNames())
                .containsExactly("embedding");

        store.dropAllIndexesForTable(
                "tenant-a", "catalog", "schema", "table", Instant.now());
        assertThat(store.findIndex("index-a").orElseThrow().state())
                .isEqualTo(IndexState.DROPPED);
        assertThat(store.findIndex("index-b").orElseThrow().state())
                .isEqualTo(IndexState.READY);
    }

    @Test
    void commitReplayPreservesTenantIdentityAndUtcInstant() {
        Instant tenantATime = Instant.parse("2026-09-05T01:02:03.123456Z");
        Instant tenantBTime = tenantATime.plusMillis(1);
        store.recordCommitEvent(new CommitEventRequest(
                "tenant-a", "catalog", "schema", "table", "snapshot-a",
                "[]", "[]", "APPEND", tenantATime));
        store.recordCommitEvent(new CommitEventRequest(
                "tenant-b", "catalog", "schema", "table", "snapshot-b",
                "[]", "[]", "APPEND", tenantBTime));

        assertThat(store.pollUnprocessedCommitEvents(10, 1L))
                .extracting(entry -> List.of(
                        entry.tenantId(), entry.catalogName(), entry.schemaName(),
                        entry.tableName(), entry.snapshotId(), entry.createdAt()))
                .containsExactly(
                        List.of("tenant-a", "catalog", "schema", "table",
                                "snapshot-a", tenantATime),
                        List.of("tenant-b", "catalog", "schema", "table",
                                "snapshot-b", tenantBTime));
    }

    @Test
    void journaledCommitReplayIsIdempotentAndRejectsChangedContent() {
        String fingerprint = "a".repeat(64);
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        var first = new CommitEventRequest(
                "tenant-a", "catalog", "schema", "table", "snapshot-a",
                "[\"file-a\"]", "[]", "APPEND", now, fingerprint);
        store.recordCommitEvent(first);
        store.recordCommitEvent(first);
        assertThat(store.pollUnprocessedCommitEvents(10, 1L))
                .extracting(entry -> entry.snapshotId())
                .containsExactly("snapshot-a");

        var changedSnapshot = new CommitEventRequest(
                "tenant-a", "catalog", "schema", "table", "snapshot-b",
                "[\"file-a\"]", "[]", "APPEND", now, fingerprint);
        assertThatThrownBy(() -> store.recordCommitEvent(changedSnapshot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different event content");

        var changedFiles = new CommitEventRequest(
                "tenant-a", "catalog", "schema", "table", "snapshot-a",
                "[\"file-b\"]", "[]", "APPEND", now, fingerprint);
        assertThatThrownBy(() -> store.recordCommitEvent(changedFiles))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different event content");
        assertThat(store.pollUnprocessedCommitEvents(10, 1L)).hasSize(1);
    }

    @Test
    void nonJournaledCommitsAreNotMergedByNullFingerprint() {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        var ordinary = new CommitEventRequest(
                "tenant-a", "catalog", "schema", "table", "snapshot-a",
                "[]", "[]", "APPEND", now);
        store.recordCommitEvent(ordinary);
        store.recordCommitEvent(ordinary);
        assertThat(store.pollUnprocessedCommitEvents(10, 1L)).hasSize(2);
    }

    @Test
    void pollingDoesNotStarveAnotherTableBehindOneTablesBacklog() {
        Instant first = Instant.parse("2026-09-05T01:02:03Z");
        for (int i = 0; i < 5; i++) {
            store.recordCommitEvent(new CommitEventRequest(
                    "tenant-a", "catalog", "schema", "blocked", "snapshot-" + i,
                    "[]", "[]", "APPEND", first.plusSeconds(i)));
        }
        store.recordCommitEvent(new CommitEventRequest(
                "tenant-a", "catalog", "schema", "healthy", "snapshot-healthy",
                "[]", "[]", "APPEND", first.plusSeconds(10)));

        assertThat(store.pollUnprocessedCommitEvents(3, 1L))
                .extracting(entry -> entry.tableName() + ":" + entry.snapshotId())
                .containsExactly("blocked:snapshot-0", "healthy:snapshot-healthy",
                        "blocked:snapshot-1");
    }

    private IndexPublicationRequest request(IndexDescriptor descriptor, long objects) {
        return new IndexPublicationRequest(
                descriptor.indexId(), descriptor.indexId(), descriptor.generation(),
                descriptor.buildSnapshotId(), descriptor.indexType().name(),
                new IndexGenerationFormat(
                        "pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                objects, descriptor.updatedAt(), descriptor.updatedAt().plusMillis(1));
    }

    private static IndexDescriptor descriptor() {
        Instant now = Instant.parse("2026-09-05T00:00:00Z");
        return new IndexDescriptor(
                "idx", "idx_name", "catalog", "schema", "table",
                List.of("embedding"), IndexType.VECTOR, Map.of(),
                IndexState.BUILDING, null, 0, "snapshot-1", null,
                now, now, "tenant");
    }

    private static IndexDescriptor readyDescriptor(String indexId, String tenantId) {
        Instant now = Instant.parse("2026-09-05T00:00:00Z");
        return new IndexDescriptor(
                indexId, "shared_name", "catalog", "schema", "table",
                List.of("embedding"), IndexType.VECTOR, Map.of(),
                IndexState.READY, null, 0, "snapshot-1", "snapshot-1",
                now, now, tenantId);
    }

    private static IndexFileMapping mapping(int generation) {
        return new IndexFileMapping(
                "idx", "data-" + generation, "index-" + generation,
                10, 256, "a".repeat(64), generation,
                "object-snapshot", 0, Instant.EPOCH, 0);
    }
}
