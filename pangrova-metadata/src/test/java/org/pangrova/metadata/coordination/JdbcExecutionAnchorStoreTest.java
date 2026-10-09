package org.pangrova.metadata.coordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.ExecutionAnchor;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class JdbcExecutionAnchorStoreTest {

    private JdbcExecutionAnchorStore store;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("execution_anchor");
        store = new JdbcExecutionAnchorStore(dataSource);
    }

    @Test
    void acquireReleaseAndReacquireUsesNeverReusedToken() {
        ExecutionAnchor first = store.tryAcquire(
                "q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        assertThat(store.tryAcquire(
                "q-1", "b", "b-1", Duration.ofMinutes(1))).isEmpty();
        assertThat(store.release(first)).isTrue();

        ExecutionAnchor second = store.tryAcquire(
                "q-1", "b", "b-1", Duration.ofMinutes(1)).orElseThrow();
        assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
        assertThat(store.release(first)).isFalse();
        assertThat(store.current("q-1")).contains(second);
    }

    @Test
    void batchRenewIsBoundToExactProcessIncarnation() {
        store.tryAcquire("q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        store.tryAcquire("q-2", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        store.tryAcquire("q-3", "a", "a-2", Duration.ofMinutes(1)).orElseThrow();

        var renewed = store.renewOwned("a", "a-1", Duration.ofMinutes(2));
        assertThat(renewed).extracting(ExecutionAnchor::executionId).containsExactlyInAnyOrder("q-1", "q-2");
        for (ExecutionAnchor receipt : renewed) {
            assertThat(receipt.ownerId()).isEqualTo("a");
            assertThat(receipt.ownerIncarnation()).isEqualTo("a-1");
            assertThat(store.current(receipt.executionId())).contains(receipt);
        }
        assertThat(store.renewOwned("a", "missing", Duration.ofMinutes(2))).isEmpty();
    }

    @Test
    void yieldAndBoundedClaimAdvanceEveryExecutionToken() {
        ExecutionAnchor first = store.tryAcquire(
                "q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        ExecutionAnchor second = store.tryAcquire(
                "q-2", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        assertThat(store.yieldOwned("a", "a-1")).isEqualTo(2);

        var batchOne = store.claimExpired(
                "b", "b-1", Duration.ofMinutes(1), 1);
        var batchTwo = store.claimExpired(
                "b", "b-1", Duration.ofMinutes(1), 1);

        assertThat(batchOne).hasSize(1);
        assertThat(batchTwo).hasSize(1);
        assertThat(batchOne.get(0).executionId()).isNotEqualTo(batchTwo.get(0).executionId());
        assertThat(batchOne.get(0).fencingToken())
                .isGreaterThan(Math.min(first.fencingToken(), second.fencingToken()));
        assertThat(batchTwo.get(0).fencingToken())
                .isGreaterThan(Math.min(first.fencingToken(), second.fencingToken()));
    }

    @Test
    void renewalBlockedByARowLockCannotReviveAnExpiredLease() throws Exception {
        var dataSource = (org.postgresql.ds.PGSimpleDataSource)
                SharedPostgresContainer.migratedDataSource("renewal_lock_expiry");
        String application = "renewal_lock_" + java.util.UUID.randomUUID();
        dataSource.setApplicationName(application);
        var backend = new JdbcExecutionAnchorStore(dataSource);
        var initial = backend.tryAcquire("locked", "owner", "process", Duration.ofSeconds(1)).orElseThrow();
        var executor = Executors.newSingleThreadExecutor();
        try (var lock = dataSource.getConnection()) {
            lock.setAutoCommit(false);
            try (var statement = lock.prepareStatement(
                    "SELECT execution_id FROM execution_anchor WHERE execution_id=? FOR UPDATE")) {
                statement.setString(1, initial.executionId());
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                }
            }
            try {
                var renewal = executor.submit(() -> backend.renewOwned("owner", "process", Duration.ofMinutes(1)));
                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                            "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=? "
                                    + "AND wait_event_type='Lock'")) {
                        statement.setString(1, application);
                        try (var result = statement.executeQuery()) {
                            result.next();
                            return result.getInt(1) == 1;
                        }
                    }
                });
                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                            "SELECT clock_timestamp() > ?")) {
                        statement.setTimestamp(1, java.sql.Timestamp.from(initial.expiresAt()));
                        try (var result = statement.executeQuery()) {
                            result.next();
                            return result.getBoolean(1);
                        }
                    }
                });
                lock.rollback();
                assertThat(renewal.get(5, TimeUnit.SECONDS)).isEmpty();
                assertThat(backend.current(initial.executionId())).isEmpty();
            } finally {
                lock.rollback();
            }
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void acquisitionUsesTheClockAfterWaitingForAnExistingOwner() throws Exception {
        assertAcquisitionSurvivesLockWait(true);
    }

    @Test
    void insertionUsesTheClockAfterAnUncommittedConflictRollsBack() throws Exception {
        assertAcquisitionSurvivesLockWait(false);
    }

    private void assertAcquisitionSurvivesLockWait(boolean existingOwner) throws Exception {
        var dataSource = (org.postgresql.ds.PGSimpleDataSource)
                SharedPostgresContainer.migratedDataSource("acquisition_lock_expiry");
        String application = "acquisition_lock_" + java.util.UUID.randomUUID();
        dataSource.setApplicationName(application);
        var backend = new JdbcExecutionAnchorStore(dataSource);
        var first = existingOwner
                ? backend.tryAcquire("locked", "old-owner", "old-process", Duration.ofSeconds(1)).orElseThrow()
                : null;
        var executor = Executors.newSingleThreadExecutor();
        try (var lock = dataSource.getConnection()) {
            lock.setAutoCommit(false);
            if (existingOwner) {
                try (var statement = lock.prepareStatement(
                        "SELECT execution_id FROM execution_anchor WHERE execution_id='locked' FOR UPDATE");
                     var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                }
            } else {
                try (var statement = lock.prepareStatement(
                        "INSERT INTO execution_anchor (execution_id, owner_id, owner_incarnation, "
                                + "fencing_token, acquired_at, expires_at, version) VALUES "
                                + "('locked', 'old-owner', 'old-process', nextval('execution_anchor_token_seq'), "
                                + "clock_timestamp(), clock_timestamp() + INTERVAL '1 minute', 1)")) {
                    assertThat(statement.executeUpdate()).isEqualTo(1);
                }
            }
            try {
                var acquisition = executor.submit(() -> backend.tryAcquire(
                        "locked", "new-owner", "new-process", Duration.ofSeconds(1)));
                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                            "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=? "
                                    + "AND wait_event_type='Lock' "
                                    + "AND clock_timestamp() > query_start + INTERVAL '1 second'")) {
                        statement.setString(1, application);
                        try (var result = statement.executeQuery()) {
                            result.next();
                            return result.getInt(1) == 1;
                        }
                    }
                });
                java.time.Instant unlockedAfter;
                try (var statement = lock.prepareStatement("SELECT clock_timestamp()");
                     var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    unlockedAfter = result.getTimestamp(1).toInstant();
                }
                lock.rollback();
                var acquired = acquisition.get(5, TimeUnit.SECONDS).orElseThrow();
                assertThat(acquired.ownerId()).isEqualTo("new-owner");
                assertThat(acquired.ownerIncarnation()).isEqualTo("new-process");
                assertThat(acquired.acquiredAt()).isAfterOrEqualTo(unlockedAfter);
                assertThat(Duration.between(acquired.acquiredAt(), acquired.expiresAt()))
                        .isEqualTo(Duration.ofSeconds(1));
                assertThat(backend.current("locked")).contains(acquired);
                if (first != null) assertThat(acquired.fencingToken()).isGreaterThan(first.fencingToken());
            } finally {
                lock.rollback();
            }
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
