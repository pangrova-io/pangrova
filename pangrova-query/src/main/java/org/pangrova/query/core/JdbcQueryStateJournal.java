package org.pangrova.query.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.StateJournalConfig;
import org.pangrova.spi.coordination.TaskStateEvent;

/** PostgreSQL journal with one query-fair, per-query ordered commit owner and retryable actual SQL drain. */
@ThreadSafe
public final class JdbcQueryStateJournal implements QueryStateJournal {
    private static final long DRAIN_MS = 5000;
    private static final String ACTIVE = "query_id NOT IN (SELECT query_id FROM query_state_journal "
            + "WHERE state IN ('FINISHED','FAILED','CANCELLED'))";
    private final DataSource dataSource;
    private final JournalPendingWrites pending;
    private final JournalFlushOwner flushOwner;

    /** Starts the sole writer; the external DataSource remains owned by the caller. */
    public JdbcQueryStateJournal(DataSource dataSource, StateJournalConfig config) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        Objects.requireNonNull(config, "config");
        pending = new JournalPendingWrites(config.maxResidentEvents());
        flushOwner = new JournalFlushOwner(dataSource, config, pending);
    }

    @Override public void appendQueryEvent(QueryStateEvent event) {
        pending.admit(new JournalPendingWrites.Write(pending, event), false);
    }
    @Override public void appendTaskEvent(TaskStateEvent event) {
        pending.admit(new JournalPendingWrites.Write(pending, event), false);
    }
    @Override public void appendQueryEventSync(QueryStateEvent event) {
        await(pending.admit(new JournalPendingWrites.Write(pending, event), true));
    }
    @Override public void appendTaskEventSync(TaskStateEvent event) {
        await(pending.admit(new JournalPendingWrites.Write(pending, event), true));
    }

    private void await(CompletableFuture<Void> attempt) {
        try {
            flushOwner.awaitCommit(attempt);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw JournalPendingWrites.failure("Interrupted awaiting journal commit; event remains owned", interrupted);
        } catch (ExecutionException failure) {
            throw JournalPendingWrites.failure("Journal commit was not established", failure.getCause());
        }
    }

    @Override public List<QueryStateEvent> replayActiveQueries() {
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                List<QueryStateEvent> events = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.QUERY_COLUMNS
                        + " FROM query_state_journal WHERE " + ACTIVE + " ORDER BY id ASC");
                        ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) events.add(JournalEventSql.readQuery(rows));
                }
                return List.copyOf(events);
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to replay active queries", failure);
        }
    }

    @Override public List<TaskStateEvent> replayActiveTasks() {
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                List<TaskStateEvent> events = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.TASK_COLUMNS
                        + " FROM task_state_journal WHERE query_id IN (SELECT query_id FROM query_state_journal WHERE "
                        + ACTIVE + ") ORDER BY id ASC"); ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) events.add(JournalEventSql.readTask(rows));
                }
                return List.copyOf(events);
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to replay active tasks", failure);
        }
    }

    @Override public Optional<QueryStateEvent> latestQueryEvent(String queryId) {
        Objects.requireNonNull(queryId, "queryId");
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.QUERY_COLUMNS
                        + " FROM query_state_journal WHERE query_id = ? ORDER BY id DESC LIMIT 1")) {
                    statement.setString(1, queryId);
                    try (ResultSet rows = statement.executeQuery()) {
                        return rows.next() ? Optional.of(JournalEventSql.readQuery(rows)) : Optional.empty();
                    }
                }
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to read latest query event", failure);
        }
    }

    @Override public void purgeQuery(String queryId) {
        Objects.requireNonNull(queryId, "queryId");
        JournalSqlOperation operation = pending.enterPurge(queryId);
        try {
            pending.awaitQueryWrites(queryId, DRAIN_MS);
        } catch (InterruptedException interrupted) {
            operation.closeAfterFailure(interrupted);
            Thread.currentThread().interrupt();
            throw JournalPendingWrites.failure("Interrupted draining query journal before purge", interrupted);
        } catch (RuntimeException | Error failure) {
            operation.closeAfterFailure(failure);
            throw failure;
        }
        try {
            operation.perform(dataSource, connection -> {
                purgeCommitted(connection, queryId);
                return null;
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to atomically purge query journal: " + queryId, failure);
        }
    }

    private void purgeCommitted(Connection connection, String queryId) throws SQLException {
        connection.setAutoCommit(false);
        try {
            delete(connection, "task_state_journal", queryId);
            delete(connection, "query_state_journal", queryId);
            connection.commit();
        } catch (SQLException failure) {
            try {
                connection.rollback();
            } catch (SQLException rollback) {
                if (rollback != failure) failure.addSuppressed(rollback);
            }
            throw failure;
        }
    }

    private static void delete(Connection connection, String table, String queryId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM " + table + " WHERE query_id = ?")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        }
    }

    @Override public void close() { flushOwner.close(); }
    @Override public boolean isDrained() { return flushOwner.isDrained(); }
}
