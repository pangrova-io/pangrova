package org.pangrova.metadata.coordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class JdbcExecutionAnchorStoreTest {

    private JdbcExecutionAnchorStore store;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("execution_anchor");
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

        var receipts = store.renewOwned("a", "a-1", Duration.ofMinutes(2));
        assertThat(receipts).extracting(ExecutionAnchor::executionId)
                .containsExactlyInAnyOrder("q-1", "q-2");
        for (var receipt : receipts) {
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
    void renewalCannotReviveOwnerWhoseLeaseExpiredWhileWaitingForRowLock() throws Exception {
        var original = lease();
        var result = afterDeadline(original,
                () -> store.renewOwned("a", "a-1", Duration.ofSeconds(5)));
        assertThat(result.value()).isEmpty();
        assertExpiredRowUnchanged(original);
    }

    @Test
    void renewalOnlyUpdatesRowsLockedBeforeItsClockSample() throws Exception {
        var original = lease();
        var fresh = new AtomicReference<ExecutionAnchor>();
        var result = afterDeadline(original,
                () -> store.renewOwned("a", "a-1", Duration.ofSeconds(5)),
                Optional.of(() -> fresh.set(store.tryAcquire("q-new", "a", "a-1", Duration.ofSeconds(5)).orElseThrow())));
        assertThat(result.value()).isEmpty();
        assertExpiredRowUnchanged(original);
        assertThat(store.current("q-new")).contains(fresh.get());
    }

    @Test
    void releaseCannotAcceptOwnerWhoseLeaseExpiredWhileWaitingForRowLock() throws Exception {
        var original = lease();
        assertThat(afterDeadline(original, () -> store.release(original)).value()).isFalse();
        assertExpiredRowUnchanged(original);
    }

    @Test
    void yieldCannotAcceptOwnerWhoseLeaseExpiredWhileWaitingForRowLock() throws Exception {
        var original = lease();
        assertThat(afterDeadline(original, () -> store.yield(original)).value()).isFalse();
        assertExpiredRowUnchanged(original);
    }

    @Test
    void batchYieldCannotAcceptOwnerWhoseLeaseExpiredWhileWaitingForRowLock() throws Exception {
        var original = lease();
        assertThat(afterDeadline(original, () -> store.yieldOwned("a", "a-1")).value()).isZero();
        assertExpiredRowUnchanged(original);
    }

    @Test
    void acquisitionUsesClockAfterLockWaitAndAllocatesNewFencingToken() throws Exception {
        var original = lease();
        var result = afterDeadline(original,
                () -> store.tryAcquire("q-lock", "b", "b-1", Duration.ofSeconds(5)).orElseThrow());
        var acquired = result.value();
        assertThat(acquired.fencingToken()).isGreaterThan(original.fencingToken());
        assertThat(acquired.acquiredAt()).isAfterOrEqualTo(result.releasedAt());
        assertThat(acquired.expiresAt()).isEqualTo(acquired.acquiredAt().plusSeconds(5));
        assertThat(store.current("q-lock")).contains(acquired);
        assertThat(store.renewOwned("a", "a-1", Duration.ofSeconds(5))).isEmpty();
    }

    @Test
    void insertionGrantsLeaseAfterInvisibleCompetingInsertRollsBack() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            Instant deadline;
            try (var insert = blocker.createStatement(); var result = insert.executeQuery(
                    "INSERT INTO execution_anchor VALUES ('q-insert','a','a-1',"
                            + "nextval('execution_anchor_token_seq'),clock_timestamp(),"
                            + "clock_timestamp()+INTERVAL '5 seconds',1) RETURNING expires_at")) {
                assertThat(result.next()).isTrue();
                deadline = result.getObject(1, OffsetDateTime.class).toInstant();
            }
            int blockerPid = backendPid(blocker);
            var acquiring = executor.submit(
                    () -> store.tryAcquire("q-insert", "b", "b-1", Duration.ofSeconds(5)).orElseThrow());
            awaitBlockedBeforeDeadline(blockerPid, deadline);
            Instant releasedAt = awaitDatabaseDeadline(blocker, deadline);
            assertThat(acquiring.isDone()).isFalse();
            blocker.rollback();
            var acquired = acquiring.get(10, TimeUnit.SECONDS);
            assertThat(acquired.acquiredAt()).isAfterOrEqualTo(releasedAt);
            assertThat(acquired.expiresAt()).isEqualTo(acquired.acquiredAt().plusSeconds(5));
            assertThat(store.current("q-insert")).contains(acquired);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private ExecutionAnchor lease() {
        return store.tryAcquire("q-lock", "a", "a-1", Duration.ofSeconds(5)).orElseThrow();
    }

    private <T> DelayedResult<T> afterDeadline(ExecutionAnchor original, Callable<T> operation) throws Exception {
        return afterDeadline(original, operation, Optional.empty());
    }

    private <T> DelayedResult<T> afterDeadline(ExecutionAnchor original, Callable<T> operation,
                                            Optional<Runnable> beforeUnlock) throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var lock = blocker.prepareStatement(
                    "SELECT execution_id FROM execution_anchor WHERE execution_id=? FOR UPDATE")) {
                lock.setString(1, original.executionId());
                try (var row = lock.executeQuery()) {
                    assertThat(row.next()).isTrue();
                }
            }
            var pending = executor.submit(operation);
            awaitBlockedBeforeDeadline(backendPid(blocker), original.expiresAt());
            Instant releasedAt = awaitDatabaseDeadline(blocker, original.expiresAt());
            assertThat(pending.isDone()).isFalse();
            beforeUnlock.ifPresent(Runnable::run);
            blocker.commit();
            return new DelayedResult<>(pending.get(10, TimeUnit.SECONDS), releasedAt);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void awaitBlockedBeforeDeadline(int blockerPid, Instant deadline) throws Exception {
        await().atMost(Duration.ofSeconds(10)).until(() -> blockedTransactionStart(blockerPid) != null);
        assertThat(blockedTransactionStart(blockerPid)).isBefore(deadline);
        try (var observer = dataSource.getConnection(); var statement = observer.createStatement();
                var result = statement.executeQuery("SELECT clock_timestamp()")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getObject(1, OffsetDateTime.class).toInstant()).isBefore(deadline);
        }
    }

    private Instant blockedTransactionStart(int blockerPid) throws Exception {
        try (var observer = dataSource.getConnection(); var statement = observer.prepareStatement(
                "SELECT xact_start FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid)) "
                        + "AND wait_event_type='Lock'")) {
            statement.setInt(1, blockerPid);
            try (var result = statement.executeQuery()) {
                return result.next() ? result.getObject(1, OffsetDateTime.class).toInstant() : null;
            }
        }
    }

    private static int backendPid(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(result.next()).isTrue();
            return result.getInt(1);
        }
    }

    private static Instant awaitDatabaseDeadline(Connection blocker, Instant deadline) throws Exception {
        // Real database time, while keeping the row/unique-key lock held. No test clock or JVM sleep.
        try (var wait = blocker.prepareStatement(
                "SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (?::timestamptz-clock_timestamp())))+0.05)")) {
            wait.setObject(1, deadline.atOffset(ZoneOffset.UTC));
            wait.executeQuery().close();
        }
        try (var statement = blocker.createStatement(); var result = statement.executeQuery("SELECT clock_timestamp()")) {
            assertThat(result.next()).isTrue();
            Instant now = result.getObject(1, OffsetDateTime.class).toInstant();
            assertThat(now).isAfter(deadline);
            return now;
        }
    }

    private void assertExpiredRowUnchanged(ExecutionAnchor original) throws Exception {
        assertThat(store.current(original.executionId())).isEmpty();
        try (var observer = dataSource.getConnection(); var statement = observer.prepareStatement(
                "SELECT owner_id,owner_incarnation,fencing_token,expires_at FROM execution_anchor WHERE execution_id=?")) {
            statement.setString(1, original.executionId());
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.getString(1)).isEqualTo(original.ownerId());
                assertThat(row.getString(2)).isEqualTo(original.ownerIncarnation());
                assertThat(row.getLong(3)).isEqualTo(original.fencingToken());
                assertThat(row.getObject(4, OffsetDateTime.class).toInstant()).isEqualTo(original.expiresAt());
            }
        }
    }

    private record DelayedResult<T>(T value, Instant releasedAt) {}

}
