package org.pangrova.persistence.indexing.postgresql.metadata;

import java.time.Instant;
import java.time.Duration;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.metadata.service.TableControlWriteAuthorities;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.testing.testkit.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcIndexMappingPublicationFenceTest extends ContainerTestBase {
    private static final Instant START = Instant.parse("2026-11-01T00:00:00Z");
    private DataSource dataSource;
    private JdbcIndexMetadataStore store;
    private IndexDescriptor initial;

    @BeforeEach
    void initialize() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("mapping_publication_fence");
        var table = new QualifiedTableName("lake", "raw", "events");
        GenerationAuthorityFixtures.provision(dataSource, "tenant", "builder",
                DeferredAuthorizationScope.raw(table, Set.of("value"), table));
        store = new JdbcIndexMetadataStore(dataSource, TableControlWriteAuthorities.postgres()::requireCurrent);
        initial = new IndexDescriptor("index", "idx", "lake", "raw", "events", List.of("value"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 0, "snapshot-0", null,
                START, START, "tenant");
        store.registerIndexWithBuildAuthority(initial, "builder");
    }

    @Test
    void publishedGenerationRejectsLateSingleMapping() {
        var manifest = store.publishIndexGeneration(publication(initial, 0));
        assertThatThrownBy(() -> store.registerIndexFile(mapping(0, "late")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).isEmpty();
        assertThat(store.findActiveIndexGeneration("index")).contains(manifest);
    }

    @Test
    void publishedGenerationRejectsLateMappingBatch() {
        var manifest = store.publishIndexGeneration(publication(initial, 0));
        assertThatThrownBy(() -> store.batchRegisterIndexFiles(List.of(mapping(0, "late"))))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).isEmpty();
        assertThat(store.findActiveIndexGeneration("index")).contains(manifest);
    }

    @Test
    void mixedGenerationBatchIsRejectedBeforeAnyRowIsInserted() {
        assertThatThrownBy(() -> store.batchRegisterIndexFiles(List.of(mapping(0, "first"), mapping(1, "second"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.getIndexFileMappings("index")).isEmpty();
    }

    @Test
    void unreservedGenerationCannotAcceptMappings() {
        assertThatThrownBy(() -> store.registerIndexFile(mapping(9, "wrong-generation")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).isEmpty();
    }

    @Test
    void copyRequiresAnActiveTargetReservation() {
        store.registerIndexFile(mapping(0, "source"));
        store.publishIndexGeneration(publication(initial, 1));
        assertThatThrownBy(() -> store.copyIndexGenerationMappings("index", 0, 1, List.of()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).extracting(IndexFileMapping::generation).containsExactly(0);
    }

    @Test
    void publishedTargetCannotBeRepopulatedByCopy() {
        store.registerIndexFile(mapping(0, "source"));
        store.publishIndexGeneration(publication(initial, 1));
        var ready = store.findIndex("index").orElseThrow();
        Instant next = ready.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration("index", 1, "snapshot-1", next,
                ready.updatedAt(), ready.state(), 0)).isTrue();
        var replacement = new IndexDescriptor("index", "idx", "lake", "raw", "events", List.of("value"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 1, "snapshot-1", null,
                START, next, "tenant");
        var manifest = store.publishIndexGeneration(publication(replacement, 0));
        assertThatThrownBy(() -> store.copyIndexGenerationMappings("index", 0, 1, List.of()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).extracting(IndexFileMapping::generation).containsExactly(0);
        assertThat(store.findActiveIndexGeneration("index")).contains(manifest);
    }

    @Test
    void initialAndReservedReplacementWritesPublishNormally() {
        store.registerIndexFile(mapping(0, "first"));
        store.batchRegisterIndexFiles(List.of(mapping(0, "second"), mapping(0, "third")));
        assertThat(store.publishIndexGeneration(publication(initial, 3)).objectCount()).isEqualTo(3);
        var replacement = reserveReplacement();
        assertThat(store.copyIndexGenerationMappings("index", 0, 1, List.of("second"))).isEqualTo(2);
        store.registerIndexFile(mapping(1, "new"));
        assertThat(store.publishIndexGeneration(publication(replacement, 3)).objectCount()).isEqualTo(3);
        assertThat(store.getIndexFileMappings("index").stream().filter(row -> row.generation() == 1))
                .extracting(IndexFileMapping::dataFilePath).containsExactlyInAnyOrder("first", "third", "new");
    }

    @Test
    void duplicateBatchRollsBackItsEarlierNewRows() {
        store.registerIndexFile(mapping(0, "existing"));
        assertThatThrownBy(() -> store.batchRegisterIndexFiles(List.of(mapping(0, "new"), mapping(0, "existing"))))
                .isInstanceOf(org.pangrova.persistence.catalog.api.error.MetastoreException.class);
        assertThat(store.getIndexFileMappings("index")).extracting(IndexFileMapping::dataFilePath)
                .containsExactly("existing");
        assertThat(store.publishIndexGeneration(publication(initial, 1)).objectCount()).isEqualTo(1);
    }

    @Test
    void failedReservationRejectsLateWrites() {
        store.publishIndexGeneration(publication(initial, 0));
        var replacement = reserveReplacement();
        assertThat(store.failIndexGeneration("index", 1, replacement.updatedAt(),
                replacement.updatedAt().plusMillis(1), "failed")).isTrue();
        assertThatThrownBy(() -> store.registerIndexFile(mapping(1, "late")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).isEmpty();
    }

    @Test
    void droppedInitialBuildRejectsLateWrites() {
        store.dropIndex("index", START.plusSeconds(1));
        assertThatThrownBy(() -> store.registerIndexFile(mapping(0, "late")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.getIndexFileMappings("index")).isEmpty();
    }

    @Test
    void publisherWinningTheDescriptorLockRejectsTheWaitingWriter() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var publisherPid = new AtomicInteger();
        var publisher = new JdbcIndexMetadataStore(dataSource, (connection, principal, scope) -> {
            TableControlWriteAuthorities.postgres().requireCurrent(connection, principal, scope);
            publisherPid.set(backendPid(connection));
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Publication barrier timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Publication barrier interrupted", interrupted);
            }
        });
        var executor = Executors.newFixedThreadPool(2);
        try {
            var publication = executor.submit(() -> publisher.publishIndexGeneration(publication(initial, 0)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var write = executor.submit(() -> store.registerIndexFile(mapping(0, "late")));
            awaitBlockedBy(publisherPid.get());
            release.countDown();
            assertThat(publication.get(5, TimeUnit.SECONDS).objectCount()).isZero();
            assertThatThrownBy(() -> write.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(store.getIndexFileMappings("index")).isEmpty();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void writerKeepsDescriptorLockUntilItsMappingsAreCommitted() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION wait_for_mapping_test() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      PERFORM pg_advisory_xact_lock(hashtext(current_schema()), 731);
                      RETURN NEW;
                    END $$
                    """);
            statement.execute("CREATE TRIGGER mapping_test_barrier BEFORE INSERT ON index_file_mappings "
                    + "FOR EACH ROW EXECUTE FUNCTION wait_for_mapping_test()");
        }
        var executor = Executors.newFixedThreadPool(2);
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()), 731)");
            }
            var write = executor.submit(() -> store.registerIndexFile(mapping(0, "first")));
            int writerPid = awaitBlockedBy(backendPid(blocker));
            var publication = executor.submit(() -> store.publishIndexGeneration(publication(initial, 1)));
            awaitBlockedBy(writerPid);
            blocker.commit();
            write.get(5, TimeUnit.SECONDS);
            assertThat(publication.get(5, TimeUnit.SECONDS).objectCount()).isEqualTo(1);
            assertThat(store.getIndexFileMappings("index")).extracting(IndexFileMapping::dataFilePath)
                    .containsExactly("first");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private int awaitBlockedBy(int blockerPid) {
        var waiter = new AtomicInteger();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "SELECT pid FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid))")) {
                statement.setInt(1, blockerPid);
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    waiter.set(rows.getInt(1));
                }
            }
        });
        return waiter.get();
    }

    private static int backendPid(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(rows.next()).isTrue();
            return rows.getInt(1);
        }
    }

    private IndexDescriptor reserveReplacement() {
        var ready = store.findIndex("index").orElseThrow();
        Instant next = ready.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration("index", 1, "snapshot-1", next,
                ready.updatedAt(), ready.state(), 0)).isTrue();
        return new IndexDescriptor("index", "idx", "lake", "raw", "events", List.of("value"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 1, "snapshot-1", null,
                START, next, "tenant");
    }

    private static IndexFileMapping mapping(int generation, String file) {
        return new IndexFileMapping("index", file, "index/" + generation + "/" + file,
                1, 10, "a".repeat(64), generation, "snapshot-" + generation, 0, START, 0);
    }

    private static IndexPublicationRequest publication(IndexDescriptor descriptor, long objects) {
        return new IndexPublicationRequest(descriptor.indexId(), descriptor.indexId(), descriptor.generation(),
                descriptor.buildSnapshotId(), descriptor.indexType().name(),
                new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                objects, descriptor.updatedAt(), descriptor.updatedAt().plusMillis(1));
    }
}
