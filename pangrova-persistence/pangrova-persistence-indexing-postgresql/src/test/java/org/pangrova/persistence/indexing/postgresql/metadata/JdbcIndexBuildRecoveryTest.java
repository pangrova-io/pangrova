package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcIndexBuildRecoveryTest extends ContainerTestBase {
    private static final String TENANT = "tenant-recovery";
    private static final String PRINCIPAL = "builder";
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "raw", "events");
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private DataSource dataSource;
    private JdbcIndexMetadataStore store;
    private JdbcIndexBuildClaimRepository claims;

    @BeforeEach
    void initialize() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource(
                "index_build_recovery_" + UUID.randomUUID().toString().replace("-", ""));
        GenerationAuthorityFixtures.provision(dataSource, TENANT, PRINCIPAL,
                DeferredAuthorizationScope.raw(TABLE, Set.of("value"), TABLE));
        store = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        claims = new JdbcIndexBuildClaimRepository(dataSource);
    }

    @Test
    void initialFailureUsesTheExactDescriptorFenceAndPreservesItsReason() throws Exception {
        IndexDescriptor initial = descriptor("initial-failure", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "old-attempt", START)).isTrue();
        store.registerIndexFile("old-attempt", mapping(initial.indexId(), 0, "partial.idx"));

        Instant failedAt = START.plusSeconds(1);
        assertThat(store.failIndexGeneration(initial.indexId(), 0, START, failedAt, "source read failed"))
                .isTrue();
        assertThat(store.findIndex(initial.indexId()).orElseThrow())
                .satisfies(failed -> {
                    assertThat(failed.state()).isEqualTo(IndexState.STALE);
                    assertThat(failed.generation()).isZero();
                    assertThat(failed.staleReason()).isEqualTo("source read failed");
                    assertThat(failed.updatedAt()).isEqualTo(failedAt);
                });
        assertThat(buildState(initial.indexId(), 0)).isEqualTo("FAILED");
        assertThat(buildReason(initial.indexId(), 0)).isEqualTo("source read failed");
        assertThat(store.failIndexGeneration(initial.indexId(), 0, START,
                failedAt.plusSeconds(1), "late failure")).isFalse();
        assertThat(store.findIndex(initial.indexId()).orElseThrow().staleReason())
                .isEqualTo("source read failed");
    }

    @Test
    void failedInitialGenerationRemainsVisibleToCleanupRecovery() throws Exception {
        IndexDescriptor initial = descriptor("initial-cleanup", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "failed-attempt", START)).isTrue();
        store.registerIndexFile("failed-attempt", mapping(initial.indexId(), 0, "failed.idx"));
        Instant failedAt = START.plusSeconds(1);
        assertThat(store.failIndexGeneration(initial.indexId(), 0, START, failedAt, "source read failed"))
                .isTrue();

        assertThat(store.findIndexBuildRecoveryCandidates())
                .singleElement().satisfies(candidate -> {
                    assertThat(candidate.descriptor().indexId()).isEqualTo(initial.indexId());
                    assertThat(candidate.generation()).isZero();
                    assertThat(candidate.state()).isEqualTo("FAILED");
                });

        IndexDescriptor failed = store.findIndex(initial.indexId()).orElseThrow();
        assertThat(store.completeIndexBuildCleanup(initial.indexId(), 0, failed.updatedAt(),
                failedAt, IndexState.STALE, 0)).isTrue();
        assertThat(store.getIndexFileMappings(initial.indexId())).isEmpty();
        assertThat(buildState(initial.indexId(), 0)).isNull();
    }

    @Test
    void onlineAbandonmentFencesLateWritesAndPublicationBeforeCleanup() throws Exception {
        IndexDescriptor initial = descriptor("online-abandon", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        store.publishIndexGeneration(publication(initial, 0));
        IndexDescriptor ready = store.findIndex(initial.indexId()).orElseThrow();
        IndexDescriptor rebuilding = reserveReplacement(ready, "snapshot-1");
        assertThat(claims.tryClaim(initial.indexId(), 1, "old-attempt", START)).isTrue();
        assertThat(claims.heartbeat(initial.indexId(), 1, "old-attempt", START.plusSeconds(1))).isTrue();
        store.registerIndexFile("old-attempt", mapping(initial.indexId(), 1, "staging.idx"));

        Instant abandonedAt = START.plusSeconds(3);
        var abandonment = store.abandonIndexGeneration(initial.indexId(), 1, ready.updatedAt(),
                START.plusSeconds(2), abandonedAt, "coordinator crashed");
        assertThat(abandonment).isPresent();
        assertThat(abandonment.orElseThrow().pathsToDelete()).containsExactly("staging.idx");
        assertThat(abandonment.orElseThrow().descriptor().state()).isEqualTo(IndexState.READY);
        assertThat(abandonment.orElseThrow().descriptor().generation()).isZero();
        assertThat(buildState(initial.indexId(), 1)).isEqualTo("FAILED");
        assertThat(buildReason(initial.indexId(), 1)).isEqualTo("coordinator crashed");
        assertThat(claims.release(initial.indexId(), 1, "old-attempt")).isFalse();
        assertThat(claims.heartbeat(initial.indexId(), 1, "old-attempt", abandonedAt)).isFalse();
        assertThatThrownBy(() -> store.registerIndexFile("old-attempt",
                mapping(initial.indexId(), 1, "late.idx")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.publishIndexGeneration(publication(rebuilding, 1)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("fence");
        assertThat(store.findActiveIndexGeneration(initial.indexId()).orElseThrow().generation()).isZero();
        assertThat(store.getIndexFileMappings(initial.indexId())).hasSize(1);

        assertThat(store.completeIndexBuildCleanup(initial.indexId(), 1, ready.updatedAt(),
                abandonedAt, IndexState.READY, 0)).isTrue();
        assertThat(store.getIndexFileMappings(initial.indexId())).isEmpty();
        assertThat(buildState(initial.indexId(), 1)).isNull();
    }

    @Test
    void aFreshClaimHeartbeatPreventsAbandonment() {
        IndexDescriptor initial = descriptor("live-claim", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "live-attempt", START)).isTrue();
        assertThat(claims.heartbeat(initial.indexId(), 0, "live-attempt", START.plusSeconds(10))).isTrue();

        assertThat(store.abandonIndexGeneration(initial.indexId(), 0, START,
                START.plusSeconds(5), START.plusSeconds(11), "too early")).isEmpty();
        assertThat(store.findIndex(initial.indexId()).orElseThrow().state()).isEqualTo(IndexState.BUILDING);
        assertThat(claims.heartbeat(initial.indexId(), 0, "live-attempt", START.plusSeconds(12))).isTrue();
    }

    @Test
    void replacementAttemptCannotBeCleanedByThePreviousAbandonment() {
        IndexDescriptor initial = descriptor("replacement-race", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        store.publishIndexGeneration(publication(initial, 0));
        IndexDescriptor ready = store.findIndex(initial.indexId()).orElseThrow();
        IndexDescriptor rebuilding = reserveReplacement(ready, "snapshot-1");
        assertThat(claims.tryClaim(initial.indexId(), 1, "old-attempt", START)).isTrue();
        store.registerIndexFile("old-attempt", mapping(initial.indexId(), 1, "old-staging.idx"));
        Instant abandonedAt = START.plusSeconds(2);
        assertThat(store.abandonIndexGeneration(initial.indexId(), 1, ready.updatedAt(),
                START.plusSeconds(1), abandonedAt, "crashed")).isPresent();

        Instant replacementAt = abandonedAt.plusSeconds(1);
        assertThat(store.beginIndexGeneration(initial.indexId(), 1, "snapshot-2", replacementAt,
                ready.updatedAt(), IndexState.READY, 0)).isTrue();
        assertThat(claims.tryClaim(initial.indexId(), 1, "new-attempt", replacementAt)).isTrue();
        store.registerIndexFile("new-attempt", mapping(initial.indexId(), 1, "new-staging.idx"));

        assertThat(store.completeIndexBuildCleanup(initial.indexId(), 1, ready.updatedAt(),
                abandonedAt, IndexState.READY, 0)).isFalse();
        assertThat(store.getIndexFileMappings(initial.indexId()))
                .extracting(IndexFileMapping::indexFilePath).containsExactly("new-staging.idx");
        assertThat(claims.release(initial.indexId(), 1, "old-attempt")).isFalse();
        assertThat(claims.heartbeat(initial.indexId(), 1, "new-attempt", replacementAt.plusSeconds(1))).isTrue();
        assertThat(rebuilding.generation()).isOne();
    }

    @Test
    void dropWinningBeforeAbandonmentIsNeverResurrected() {
        IndexDescriptor initial = descriptor("drop-before-recovery", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "old-attempt", START)).isTrue();
        store.dropIndex(initial.indexId(), START.plusSeconds(1));

        assertThat(store.abandonIndexGeneration(initial.indexId(), 0, START,
                START.plusSeconds(2), START.plusSeconds(3), "crashed")).isEmpty();
        assertThat(store.findIndex(initial.indexId()).orElseThrow().state()).isEqualTo(IndexState.DROPPED);
        assertThat(store.findIndexBuildRecoveryCandidates()).isEmpty();
    }

    @Test
    void dropAfterAbandonmentPreventsCleanupFromMutatingTheDroppedIndex() {
        IndexDescriptor initial = descriptor("drop-after-recovery", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "old-attempt", START)).isTrue();
        store.registerIndexFile("old-attempt", mapping(initial.indexId(), 0, "staging.idx"));
        Instant abandonedAt = START.plusSeconds(2);
        var abandonment = store.abandonIndexGeneration(initial.indexId(), 0, START,
                START.plusSeconds(1), abandonedAt, "crashed");
        assertThat(abandonment).isPresent();
        store.dropIndex(initial.indexId(), abandonedAt.plusSeconds(1));

        assertThat(store.completeIndexBuildCleanup(initial.indexId(), 0,
                abandonment.orElseThrow().descriptor().updatedAt(), abandonedAt,
                IndexState.STALE, 0)).isFalse();
        assertThat(store.findIndex(initial.indexId()).orElseThrow().state()).isEqualTo(IndexState.DROPPED);
        assertThat(store.getIndexFileMappings(initial.indexId())).hasSize(1);
    }

    @Test
    void abandonmentRetainsObjectsSharedWithTheActiveGeneration() {
        IndexDescriptor initial = descriptor("shared-artifact", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "initial-attempt", START)).isTrue();
        store.registerIndexFile("initial-attempt", mapping(initial.indexId(), 0, "shared.idx"));
        store.publishIndexGeneration(publication(initial, 1));
        IndexDescriptor ready = store.findIndex(initial.indexId()).orElseThrow();
        reserveReplacement(ready, "snapshot-1");
        assertThat(claims.tryClaim(initial.indexId(), 1, "old-attempt", START)).isTrue();
        store.registerIndexFile("old-attempt", mapping(initial.indexId(), 1, "shared.idx"));

        var abandonment = store.abandonIndexGeneration(initial.indexId(), 1, ready.updatedAt(),
                START.plusSeconds(1), START.plusSeconds(2), "crashed");
        assertThat(abandonment).isPresent();
        assertThat(abandonment.orElseThrow().pathsToDelete()).isEmpty();
        assertThat(store.completeIndexBuildCleanup(initial.indexId(), 1, ready.updatedAt(),
                START.plusSeconds(2), IndexState.READY, 0)).isTrue();
        assertThat(store.getIndexFileMappings(initial.indexId()))
                .extracting(IndexFileMapping::indexFilePath).containsExactly("shared.idx");
    }

    @Test
    void publicationWinningTheDescriptorLockRejectsWaitingAbandonment() throws Exception {
        IndexDescriptor initial = descriptor("publication-race", IndexState.BUILDING, 0, "snapshot-0");
        store.registerIndexWithBuildAuthority(initial, PRINCIPAL);
        assertThat(claims.tryClaim(initial.indexId(), 0, "old-attempt", START)).isTrue();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var publisherPid = new AtomicInteger();
        var publisher = new JdbcIndexMetadataStore(dataSource, (connection, principal, scope) -> {
            TableWriteAuthorityFixture.create().requireCurrent(connection, principal, scope);
            publisherPid.set(backendPid(connection));
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Publication barrier timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Publication barrier interrupted", interrupted);
            }
        });
        var executor = Executors.newFixedThreadPool(2);
        try {
            var publication = executor.submit(() -> publisher.publishIndexGeneration(publication(initial, 0)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var abandonment = executor.submit(() -> store.abandonIndexGeneration(
                    initial.indexId(), 0, START, START.plusSeconds(1), START.plusSeconds(2), "crashed"));
            awaitBlockedBy(publisherPid.get());
            release.countDown();
            assertThat(publication.get(5, TimeUnit.SECONDS).generation()).isZero();
            assertThat(abandonment.get(5, TimeUnit.SECONDS)).isEmpty();
            assertThat(store.findIndex(initial.indexId()).orElseThrow().state()).isEqualTo(IndexState.READY);
            assertThat(store.findActiveIndexGeneration(initial.indexId())).isPresent();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void awaitBlockedBy(int blockerPid) {
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(4)).untilAsserted(() -> {
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT pid FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid))")) {
                statement.setInt(1, blockerPid);
                try (ResultSet rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                }
            }
        });
    }

    private static int backendPid(Connection connection) throws SQLException {
        try (var statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(rows.next()).isTrue();
            return rows.getInt(1);
        }
    }

    private IndexDescriptor reserveReplacement(IndexDescriptor ready, String snapshot) {
        Instant startedAt = ready.updatedAt().plusMillis(1);
        assertThat(store.beginIndexGeneration(ready.indexId(), 1, snapshot, startedAt,
                ready.updatedAt(), ready.state(), ready.generation())).isTrue();
        return ready.withGeneration(1, startedAt).withBuildSnapshotId(snapshot, startedAt);
    }

    private static IndexPublicationRequest publication(IndexDescriptor descriptor, long objects) {
        return new IndexPublicationRequest(descriptor.indexId(), descriptor.indexId(), descriptor.generation(),
                descriptor.buildSnapshotId(), descriptor.indexType().name(),
                new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                objects, descriptor.updatedAt(), descriptor.updatedAt().plusMillis(1));
    }

    private IndexDescriptor descriptor(String indexId, IndexState state, int generation, String snapshot) {
        return new IndexDescriptor(indexId, indexId, TABLE.catalog(), TABLE.schema(), TABLE.table(),
                List.of("value"), IndexType.BLOOM_FILTER, Map.of(), state, null, generation,
                snapshot, state == IndexState.READY ? snapshot : null, START, START, TENANT);
    }

    private static IndexFileMapping mapping(String indexId, int generation, String path) {
        return new IndexFileMapping(indexId, "data-" + generation, path, 1, 16,
                "a".repeat(64), generation, "snapshot-" + generation, 0, START, 0);
    }

    private String buildState(String indexId, int generation) throws Exception {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT state FROM index_generation_builds WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    private String buildReason(String indexId, int generation) throws Exception {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT failure_reason FROM index_generation_builds WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getString(1);
            }
        }
    }
}
