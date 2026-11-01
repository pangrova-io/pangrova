package org.pangrova.worker.file;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.file.directory.FileDirectoryReference;
import org.pangrova.filesystem.api.object.FileStat;

/**
 * Worker-owned, bounded reads of the published PostgreSQL directory index.
 *
 * <p>One read-only repeatable-read transaction pins the directory across refresh,
 * rename and drop. The admitted reference must match before that snapshot opens;
 * name reuse can never rebind a scan. This class is a storage reader, not an
 * authorization boundary or a SQL execution implementation. Callers must bind
 * an authorized reference and retain this owner until {@link #close()} succeeds.</p>
 */
public final class JdbcFileDirectoryPageSource implements AutoCloseable {
    private static final int MAX_PAGE_SIZE = 1000;
    private static final int MAX_QUERY_SECONDS = 30;

    private final FileDirectoryReference reference;
    private final int pageSize;
    private final Instant deadline;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile PreparedStatement activeQuery;
    private Connection connection;
    private String after = "";
    private boolean exhausted;

    private JdbcFileDirectoryPageSource(FileDirectoryReference reference, int pageSize, Instant deadline) {
        this.reference = Objects.requireNonNull(reference, "reference");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("Directory page size must be between 1 and 1000");
        }
        this.pageSize = pageSize;
    }

    /** Opens a pinned snapshot; the supplied data source owns connection acquisition timeouts. */
    public static JdbcFileDirectoryPageSource open(DataSource source, FileDirectoryReference reference,
            int pageSize, Instant deadline) {
        Objects.requireNonNull(source, "source");
        var reader = new JdbcFileDirectoryPageSource(reference, pageSize, deadline);
        reader.initialize(source);
        return reader;
    }

    private synchronized void initialize(DataSource source) {
        try {
            requireActive();
            connection = source.getConnection();
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            validateReference();
        } catch (SQLException | RuntimeException failure) {
            closeAfterFailure(failure);
            throw unavailable("Cannot open directory snapshot", failure);
        }
    }

    /** Returns at most the configured page size; an empty page means EOF, never a transient gap. */
    public synchronized List<FileStat> nextPage() {
        if (exhausted) {
            return List.of();
        }
        if (connection == null) {
            throw new IllegalStateException("Directory snapshot is closed");
        }
        try {
            requireActive();
            List<FileStat> files = new ArrayList<>(pageSize);
            try (var query = connection.prepareStatement("/* pangrova directory page */ SELECT relative_path, "
                    + "size_bytes,last_modified,etag,content_type,provider_version,md5 "
                    + "FROM file_directory_index WHERE file_space_id=? AND relative_path>? "
                    + "ORDER BY relative_path LIMIT ?")) {
                activeQuery = query;
                query.setQueryTimeout(queryTimeout());
                query.setObject(1, reference.spaceId());
                query.setString(2, after);
                query.setInt(3, pageSize);
                requireActive();
                try (var rows = query.executeQuery()) {
                    while (rows.next()) {
                        requireActive();
                        files.add(new FileStat(rows.getString(1), rows.getLong(2),
                                rows.getTimestamp(3).toInstant(), rows.getString(4),
                                rows.getString(5), rows.getString(6), rows.getString(7)));
                    }
                }
            } finally {
                activeQuery = null;
            }
            requireActive();
            if (!files.isEmpty()) {
                after = files.get(files.size() - 1).path();
            }
            if (files.size() < pageSize) {
                release();
                exhausted = true;
            }
            return List.copyOf(files);
        } catch (SQLException | RuntimeException failure) {
            closeAfterFailure(failure);
            throw unavailable("Directory snapshot read failed", failure);
        }
    }

    /** Requests cancellation of an entered JDBC query; callers still join and close its owner. */
    public void cancel() {
        cancelled.set(true);
        PreparedStatement query = activeQuery;
        if (query != null) {
            try {
                query.cancel();
            } catch (SQLException failure) {
                throw unavailable("Directory query cancellation failed", failure);
            }
        }
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            cancel();
        } catch (RuntimeException cancellation) {
            failure = cancellation;
        }
        synchronized (this) {
            try {
                release();
            } catch (SQLException cleanup) {
                if (failure == null) {
                    failure = unavailable("Directory snapshot close failed", cleanup);
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private synchronized void validateReference() throws SQLException {
        try (var query = connection.prepareStatement("SELECT 1 FROM file_space_registry "
                + "WHERE file_space_id=? AND space_type='STAGE' AND directory_enabled AND updated_at=?")) {
            query.setQueryTimeout(queryTimeout());
            query.setObject(1, reference.spaceId());
            query.setTimestamp(2, Timestamp.from(reference.definitionRevision()));
            try (var row = query.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("Directory reference was dropped, disabled or changed");
                }
            }
        }
    }

    private int queryTimeout() {
        requireActive();
        long seconds = Duration.between(Instant.now(), deadline).getSeconds();
        return (int) Math.max(1, Math.min(MAX_QUERY_SECONDS, seconds));
    }

    private void requireActive() {
        if (cancelled.get() || Thread.currentThread().isInterrupted() || !Instant.now().isBefore(deadline)) {
            throw new IllegalStateException("Directory snapshot cancelled or deadline exceeded");
        }
    }

    private synchronized void release() throws SQLException {
        if (connection == null) {
            return;
        }
        SQLException failure = null;
        try {
            connection.rollback();
        } catch (SQLException rollback) {
            failure = rollback;
        }
        try {
            // Retain the connection on close failure so physical release can be retried.
            connection.close();
            connection = null;
        } catch (SQLException cleanup) {
            if (failure == null) failure = cleanup;
            else failure.addSuppressed(cleanup);
        }
        if (failure != null) throw failure;
    }

    private void closeAfterFailure(Throwable failure) {
        try {
            close();
        } catch (RuntimeException cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    private static PangrovaException unavailable(String message, Throwable cause) {
        return new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE, message, cause);
    }
}
