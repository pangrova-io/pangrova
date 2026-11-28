package org.pangrova.persistence.coordination.postgresql.lease;

import org.pangrova.persistence.coordination.api.exception.CoordinationStorageException;

import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.pangrova.persistence.jdbc.operation.JdbcOperations.getInstant;

/** Uses actual PostgreSQL locks and delayed mutations to check the live grant and commit fence. */
class JdbcDistributedLeaseLockClockIntegrationTest extends ContainerTestBase {
    private DataSource dataSource;
    private JdbcDistributedLease lease;

    @BeforeEach
    void migrateActualSchema() throws Exception {
        dataSource = MigratedPostgres.create();
        lease = new JdbcDistributedLease(dataSource);
    }

    @Test
    void renewalCannotResurrectLeaseThatExpiredDuringActualLockWait() throws Exception {
        LeaseEntry first = acquire(Duration.ofMillis(500));
        assertThat(holdPastExpiry(() -> lease.tryRenew("k", "owner", first.epoch(), Duration.ofSeconds(5))))
                .isFalse();
        assertThat(lease.currentHolder("k")).isEmpty();
        assertThat(acquire(Duration.ofSeconds(5)).epoch()).isEqualTo(first.epoch() + 1);
    }

    @Test
    void releaseCannotReportActiveOwnershipAfterActualLockWait() throws Exception {
        LeaseEntry first = acquire(Duration.ofMillis(500));
        assertThat(holdPastExpiry(() -> lease.release("k", "owner", first.epoch()))).isFalse();
        assertThat(acquire(Duration.ofSeconds(5)).epoch()).isEqualTo(first.epoch() + 1);
    }

    @Test
    void reentrantAcquireAfterActualLockWaitAllocatesNewEpochFromLiveClock() throws Exception {
        LeaseEntry first = acquire(Duration.ofMillis(500));
        LeaseEntry next = holdPastExpiry(() -> lease.tryAcquire("k", "owner", Duration.ofSeconds(5)))
                .orElseThrow();
        assertThat(next.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(next.acquiredAt()).isAfter(first.expiresAt());
        assertThat(lease.currentHolder("k")).contains(next);
        assertThat(lease.tryAcquire("k", "owner", Duration.ofSeconds(30))).contains(next);
    }

    @Test
    void contendingAcquireUsesClockAfterActualLockWait() throws Exception {
        LeaseEntry first = acquire(Duration.ofMillis(500));
        LeaseEntry next = holdPastExpiry(() -> lease.tryAcquire("k", "successor", Duration.ofSeconds(5)))
                .orElseThrow();
        assertThat(next.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(next.holderId()).isEqualTo("successor");
        assertThat(next.acquiredAt()).isAfter(first.expiresAt());
        assertThat(Duration.between(next.acquiredAt(), next.expiresAt())).isEqualTo(Duration.ofSeconds(5));
        assertThat(next.expiresAt()).isAfter(databaseNow());
    }

    @Test
    void invisibleInsertRollbackDoesNotConsumeTheNewGrantDuringConflictWait() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) {
                statement.execute("INSERT INTO distributed_lease "
                        + "(lease_key,holder_id,epoch,acquired_at,expires_at,version) "
                        + "VALUES ('k','uncommitted',1,clock_timestamp(),clock_timestamp()+interval '5 seconds',1)");
            }
            int blockingPid = backendPid(blocker);
            Duration grantDuration = Duration.ofSeconds(5);
            var pending = executor.submit(() -> lease.tryAcquire("k", "owner", grantDuration));
            awaitLockWait(blockingPid);
            sleepOnDatabase(blocker);
            Instant beforeRollback = databaseNow(blocker);
            blocker.rollback();
            LeaseEntry acquired = pending.get(5, TimeUnit.SECONDS).orElseThrow();
            assertThat(acquired.epoch()).isEqualTo(1);
            // Compare database timestamps across the lock wait, not time left after
            // client scheduling and additional connections consume part of the lease.
            assertThat(acquired.acquiredAt()).isAfterOrEqualTo(beforeRollback);
            assertThat(Duration.between(acquired.acquiredAt(), acquired.expiresAt())).isEqualTo(grantDuration);
            assertThat(lease.currentHolder("k")).contains(acquired);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void delayedRenewalRollsBackInsteadOfPublishingAlreadyExpiredGrant() throws Exception {
        LeaseEntry first = acquire(Duration.ofSeconds(30));
        installDelayedUpdate();
        try {
            assertThat(lease.tryRenew("k", "owner", first.epoch(), Duration.ofMillis(100))).isFalse();
            assertThat(lease.currentHolder("k")).contains(first);
        } finally {
            removeDelayedUpdate();
        }
    }

    @Test
    void delayedReplacementRollsBackEpochAndDeadlineTogether() throws Exception {
        LeaseEntry first = acquire(Duration.ofMillis(100));
        sql("SELECT pg_sleep(0.2)");
        installDelayedUpdate();
        try {
            assertThat(lease.tryAcquire("k", "successor", Duration.ofMillis(100))).isEmpty();
            assertThat(lease.currentHolder("k")).isEmpty();
        } finally {
            removeDelayedUpdate();
        }
        LeaseEntry next = lease.tryAcquire("k", "successor", Duration.ofSeconds(5)).orElseThrow();
        assertThat(next.epoch()).isEqualTo(first.epoch() + 1);
    }

    @Test
    void delayedFirstGrantRefreshRollsBackTheEntireInsertedRow() throws Exception {
        installDelayedUpdate();
        try {
            assertThat(lease.tryAcquire("k", "owner", Duration.ofMillis(100))).isEmpty();
            assertThat(lease.currentHolder("k")).isEmpty();
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                    var rows = statement.executeQuery("SELECT count(*) FROM distributed_lease")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isZero();
            }
        } finally {
            removeDelayedUpdate();
        }
        assertThat(acquire(Duration.ofSeconds(5)).epoch()).isEqualTo(1);
    }

    @Test
    void actualDatabaseFailureIsNeverReportedAsContentionOrAbsence() throws Exception {
        LeaseEntry first = acquire(Duration.ofSeconds(30));
        sql("DROP TABLE distributed_lease");
        assertThatThrownBy(() -> lease.tryAcquire("k", "owner", Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(CoordinationStorageException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR))
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> lease.tryRenew("k", "owner", first.epoch(), Duration.ofSeconds(5)))
                .isInstanceOfSatisfying(CoordinationStorageException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR))
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> lease.release("k", "owner", first.epoch()))
                .isInstanceOfSatisfying(CoordinationStorageException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR))
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> lease.currentHolder("k"))
                .isInstanceOfSatisfying(CoordinationStorageException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR))
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
    }

    private LeaseEntry acquire(Duration duration) {
        return lease.tryAcquire("k", "owner", duration).orElseThrow();
    }

    private <T> T holdPastExpiry(Callable<T> operation) throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement();
                    var rows = statement.executeQuery(
                            "SELECT lease_key FROM distributed_lease WHERE lease_key='k' FOR UPDATE")) {
                assertThat(rows.next()).isTrue();
            }
            int blockingPid = backendPid(blocker);
            var pending = executor.submit(operation);
            awaitLockWait(blockingPid);
            sleepOnDatabase(blocker);
            blocker.commit();
            return pending.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void awaitLockWait(int blockerPid) {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' "
                            + "AND ?=ANY(pg_blocking_pids(pid))")) {
                statement.setInt(1, blockerPid);
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getLong(1)).isPositive();
                }
            }
        });
    }

    private static int backendPid(Connection connection) throws Exception {
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(rows.next()).isTrue();
            return rows.getInt(1);
        }
    }

    private static void sleepOnDatabase(Connection connection) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.execute("SELECT pg_sleep(0.7)");
        }
    }

    private Instant databaseNow() throws Exception {
        try (var connection = dataSource.getConnection()) {
            return databaseNow(connection);
        }
    }

    private static Instant databaseNow(Connection connection) throws Exception {
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT clock_timestamp()")) {
            assertThat(rows.next()).isTrue();
            return getInstant(rows, 1);
        }
    }

    private void installDelayedUpdate() throws Exception {
        sql("CREATE FUNCTION delay_lease_update() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN PERFORM pg_sleep(0.25); RETURN NEW; END $$");
        sql("CREATE TRIGGER lease_update_delay BEFORE UPDATE ON distributed_lease "
                + "FOR EACH ROW EXECUTE FUNCTION delay_lease_update()");
    }

    private void removeDelayedUpdate() throws Exception {
        sql("DROP TRIGGER lease_update_delay ON distributed_lease");
        sql("DROP FUNCTION delay_lease_update()");
    }

    private void sql(String sql) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
