package org.pangrova.persistence.query.postgresql.state;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/** Retains an entered SQL connection until the body exits and actual close succeeds. */
final class JournalSqlOperation implements AutoCloseable {
    private final JournalPendingWrites owner;
    private final String purgeQuery;
    private final AtomicBoolean cleaning = new AtomicBoolean();
    private volatile Connection connection;
    private volatile boolean exited;
    private volatile boolean returned;
    private volatile Throwable failure;

    JournalSqlOperation(JournalPendingWrites owner, String purgeQuery) {
        this.owner = owner;
        this.purgeQuery = purgeQuery;
    }

    <T> T perform(DataSource source, SqlWork<T> work) throws SQLException {
        Throwable original = null;
        try {
            connection = source.getConnection();
            return work.run(connection);
        } catch (SQLException | RuntimeException | Error error) {
            original = error;
            throw error;
        } finally {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                if (original == null) throw cleanup;
                if (original != cleanup) original.addSuppressed(cleanup);
            }
        }
    }

    @Override public void close() {
        if (returned) return;
        if (!cleaning.compareAndSet(false, true)) {
            if (returned) return;
            throw JournalPendingWrites.failure("Entered journal SQL close remains in flight", failure);
        }
        // Publish body exit only after the caller owns the first close. The
        // journal sweeper can retry a failed close but cannot take that first close.
        exited = true;
        closeOwnedConnection();
    }

    void closeAfterFailure(Throwable original) {
        try {
            close();
        } catch (RuntimeException | Error cleanup) {
            if (original != cleanup) original.addSuppressed(cleanup);
        }
    }

    boolean retryClose() {
        if (returned) return true;
        if (!exited || !cleaning.compareAndSet(false, true)) return false;
        return closeOwnedConnection();
    }

    private boolean closeOwnedConnection() {
        try {
            // Another closer can return the owner between our first read and
            // acquiring cleaning. Recheck under exclusive cleanup ownership.
            if (returned) return true;
            if (connection != null) connection.close();
            connection = null;
            owner.leaveSql(this, purgeQuery);
            returned = true;
            failure = null;
            return true;
        } catch (SQLException | RuntimeException | Error error) {
            failure = error;
            throw JournalPendingWrites.failure("Entered journal SQL owner could not close", error);
        } finally {
            cleaning.set(false);
        }
    }

    Throwable failure() { return failure; }

    @FunctionalInterface interface SqlWork<T> { T run(Connection connection) throws SQLException; }
}
