package org.pangrova.persistence.jdbc.query.state;

import java.sql.Connection;
import javax.sql.DataSource;
import org.pangrova.spi.query.state.StateJournalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One actual flush thread owns admitted writes; close joins it rather than cancelling its future. */
final class JournalFlushOwner implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(JournalFlushOwner.class);
    private static final long JOIN_MS = 5000;
    private final DataSource dataSource;
    private final StateJournalConfig config;
    private final JournalPendingWrites pending;
    private final Thread thread;
    private volatile Connection connectionOwner;
    private volatile Throwable connectionFailure;
    private volatile Throwable ownerFailure;

    JournalFlushOwner(DataSource dataSource, StateJournalConfig config, JournalPendingWrites pending) {
        this.dataSource = dataSource;
        this.config = config;
        this.pending = pending;
        thread = new Thread(this::runOwned, "query-journal-flush");
        thread.setDaemon(true);
        thread.start();
    }

    private void runOwned() {
        try {
            run();
        } catch (RuntimeException | Error failure) {
            ownerFailure = failure;
            pending.seal();
            LOG.error("event=journal_flush_owner_failed admitted_events_retained=true", failure);
        }
    }

    void awaitCommit(java.util.concurrent.CompletableFuture<Void> attempt)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        while (true) {
            try {
                attempt.get(1, java.util.concurrent.TimeUnit.SECONDS);
                return;
            } catch (java.util.concurrent.TimeoutException notExited) {
                if (ownerFailure != null) throw JournalPendingWrites.failure(
                        "Journal flush owner failed; admitted event remains owned", ownerFailure);
            }
        }
    }

    private void run() {
        while (true) {
            JournalPendingWrites.Batch batch;
            try {
                retryExitedSqlOwners();
                if (!closeConnection()) {
                    pending.pauseRetry(config.flushIntervalMs());
                    continue;
                }
                batch = pending.awaitBatch(config.batchSize(), config.flushIntervalMs());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw JournalPendingWrites.failure("Journal flush thread was interrupted before drain", interrupted);
            }
            if (batch == null) return;
            if (!batch.writes().isEmpty() && !flush(batch)) {
                try {
                    pending.pauseRetry(config.flushIntervalMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw JournalPendingWrites.failure("Journal flush retry interrupted before drain", interrupted);
                }
            }
        }
    }

    private void retryExitedSqlOwners() {
        for (JournalSqlOperation operation : pending.sqlOwners()) {
            try {
                operation.retryClose();
            } catch (RuntimeException | Error failure) {
                LOG.error("event=journal_entered_sql_owner_close_pending", failure);
            }
        }
    }

    private boolean flush(JournalPendingWrites.Batch batch) {
        boolean committed = false;
        try {
            connectionOwner = dataSource.getConnection();
            JournalEventSql.commit(connectionOwner, batch.writes());
            committed = true;
            pending.committed(batch);
            return true;
        } catch (RuntimeException | Error | java.sql.SQLException failure) {
            if (committed) {
                if (failure instanceof Error error) throw error;
                throw JournalPendingWrites.failure("Confirmed journal commit publication failed", failure);
            }
            var rejected = failure instanceof JournalEventSql.PayloadConflict conflict && conflict.rollbackConfirmed
                    ? conflict.eventId : null;
            pending.failed(batch, failure, rejected);
            LOG.warn("event=journal_commit_pending events={}", batch.writes().size(), failure);
            return false;
        }
    }

    private boolean closeConnection() {
        if (connectionOwner == null) return true;
        try {
            connectionOwner.close();
            connectionOwner = null;
            connectionFailure = null;
            return true;
        } catch (RuntimeException | Error | java.sql.SQLException failure) {
            connectionFailure = failure;
            LOG.error("event=journal_sql_owner_close_pending", failure);
            return false;
        }
    }

    @Override public void close() {
        pending.seal();
        if (Thread.currentThread() == thread) {
            throw JournalPendingWrites.failure("Journal flush owner cannot join itself", null);
        }
        try {
            thread.join(JOIN_MS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw JournalPendingWrites.failure("Interrupted joining journal flush owner", interrupted);
        }
        if (thread.isAlive() || !pending.drained() || connectionOwner != null) {
            Throwable failure = ownerFailure != null ? ownerFailure
                    : connectionFailure != null ? connectionFailure : pending.pendingFailure();
            throw JournalPendingWrites.failure("Journal still owns pending writes or SQL operations", failure);
        }
        if (pending.semanticFailure() != null) {
            throw JournalPendingWrites.failure("Journal drained after rejecting a conflicting immutable event",
                    pending.semanticFailure());
        }
        if (ownerFailure != null) throw JournalPendingWrites.failure(
                "Journal flush owner failed even though its resources have exited", ownerFailure);
    }

    boolean isDrained() { return !thread.isAlive() && pending.drained() && connectionOwner == null; }
}
