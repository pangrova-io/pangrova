package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.postgresql.transaction.JdbcObservedWriteStaging;
import org.pangrova.persistence.streaming.postgresql.PostgresChangeSubscription;
import org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrapConfig.MetadataJdbcConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/** Drives the durable staging queue on notifications and actual lease deadlines. */
final class ObservedWriteStagingRecovery implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ObservedWriteStagingRecovery.class);
    private static final int SIGNAL_WAIT_MILLIS = 1_000;
    private static final int LOCK_RECHECK_MILLIS = 25;
    private static final int CLOSE_WAIT_MILLIS = 30_000;

    private final JdbcObservedWriteStaging staging;
    private final MetadataJdbcConfig jdbc;
    private final String coordinatorId;
    private final Predicate<WriteCommitEntry> reclaim;
    private final AtomicReference<PostgresChangeSubscription> active = new AtomicReference<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, "coordinator-observed-write-staging"));

    ObservedWriteStagingRecovery(JdbcObservedWriteStaging staging, MetadataJdbcConfig jdbc, String coordinatorId,
            Predicate<WriteCommitEntry> reclaim) {
        this.staging = staging;
        this.jdbc = jdbc;
        this.coordinatorId = coordinatorId;
        this.reclaim = reclaim;
    }

    void start() {
        executor.execute(() -> ExecutorDrain.runOwned(executor, this::recover));
    }

    private void recover() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try (var subscription = PostgresChangeSubscription.open(jdbc.jdbcUrl(), jdbc.username(),
                        jdbc.password(), "write_commit_journal", "pangrova_write_staging_")) {
                    active.set(subscription);
                    try {
                        recoverSubscribed(subscription);
                    } finally {
                        active.compareAndSet(subscription, null);
                    }
                } catch (SQLException | RuntimeException failure) {
                    if (Thread.currentThread().isInterrupted()) return;
                    LOG.warn("event=observed_write_staging_recovery_disconnected", failure);
                    TimeUnit.MILLISECONDS.sleep(SIGNAL_WAIT_MILLIS);
                }
            }
        } catch (InterruptedException shutdown) {
            Thread.currentThread().interrupt();
        }
    }

    private void recoverSubscribed(PostgresChangeSubscription subscription) throws SQLException {
        while (!Thread.currentThread().isInterrupted()) {
            var next = staging.claim(coordinatorId);
            if (next.claim() != null) {
                reclaim(next.claim());
            } else {
                awaitWork(subscription, next.delayMillis());
            }
        }
    }

    private static void awaitWork(PostgresChangeSubscription subscription, OptionalLong delay)
            throws SQLException {
        long deadline = delay.isEmpty() ? 0L : System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(Math.max(LOCK_RECHECK_MILLIS, delay.getAsLong()));
        while (!Thread.currentThread().isInterrupted()) {
            int wait = SIGNAL_WAIT_MILLIS;
            if (delay.isPresent()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return;
                wait = (int) Math.min(wait, Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
            if (!subscription.awaitChanges(wait).isEmpty()) return;
        }
    }

    private void reclaim(JdbcObservedWriteStaging.Claim claim) throws SQLException {
        boolean reclaimed;
        try {
            reclaimed = reclaim.test(claim.intent());
        } catch (RuntimeException failure) {
            LOG.warn("event=observed_write_staging_reclaim_failed queryId={}", claim.intent().queryId(), failure);
            reclaimed = false;
        }
        if (Thread.currentThread().isInterrupted()) return;
        boolean recorded = reclaimed ? staging.complete(claim) : staging.decline(claim, coordinatorId);
        if (!recorded) {
            LOG.debug("event=observed_write_staging_claim_lost queryId={}", claim.intent().queryId());
        } else if (!reclaimed) {
            LOG.debug("event=observed_write_staging_deferred queryId={} catalog={}", claim.intent().queryId(),
                    claim.intent().catalogName());
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            var subscription = active.get();
            if (subscription != null) subscription.abort();
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot stop observed staging notification connection", failure);
        } finally {
            ExecutorDrain.shutdownAndAwait(executor, CLOSE_WAIT_MILLIS);
        }
    }
}
