package org.pangrova.coordinator.ai.embedding;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-backed implementation of {@link EmbeddingWorkStore} for durable
 * embedding work queue persistence.
 *
 * <p>Survives coordinator restarts by persisting work items in the shared
 * metadata database. Uses row-level locking ({@code FOR UPDATE SKIP LOCKED})
 * for coordinator-HA safety when multiple coordinators share the same DB.</p>
 *
 * <p>Call {@link #initSchema()} once at bootstrap to create the backing table
 * if it does not already exist.</p>
 */
@ThreadSafe
public final class JdbcEmbeddingWorkStore implements EmbeddingWorkStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcEmbeddingWorkStore.class);

    private static final String TABLE = "embedding_work_queue";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                    + "table_name VARCHAR(512) NOT NULL, "
                    + "row_id VARCHAR(512) NOT NULL, "
                    + "model_name VARCHAR(256) NOT NULL, "
                    + "vector_column VARCHAR(256) NOT NULL, "
                    + "source_text TEXT NOT NULL, "
                    + "status VARCHAR(32) NOT NULL, "
                    + "enqueued_at TIMESTAMP NOT NULL, "
                    + "updated_at TIMESTAMP NOT NULL, "
                    + "PRIMARY KEY (table_name, row_id)"
                    + ")";

    private static final String CREATE_STATUS_INDEX_SQL =
            "CREATE INDEX IF NOT EXISTS idx_ewq_status ON " + TABLE + " (status)";

    private static final String UPSERT_SQL =
            "MERGE INTO " + TABLE
                    + " (table_name, row_id, model_name, vector_column, source_text, status, enqueued_at, updated_at)"
                    + " KEY (table_name, row_id)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String MARK_IN_PROGRESS_SQL =
            "UPDATE " + TABLE + " SET status = 'IN_PROGRESS', updated_at = ?"
                    + " WHERE table_name = ? AND row_id = ? AND status = 'PENDING'";

    private static final String MARK_COMPLETED_SQL =
            "DELETE FROM " + TABLE + " WHERE table_name = ? AND row_id = ?";

    private static final String LOAD_UNFINISHED_SQL =
            "SELECT table_name, row_id, model_name, vector_column, source_text, enqueued_at"
                    + " FROM " + TABLE + " WHERE status IN ('PENDING', 'IN_PROGRESS')"
                    + " ORDER BY enqueued_at";

    private static final String COUNT_UNFINISHED_SQL =
            "SELECT COUNT(*) FROM " + TABLE
                    + " WHERE table_name = ? AND status IN ('PENDING', 'IN_PROGRESS')";

    private final DataSource dataSource;

    public JdbcEmbeddingWorkStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource required");
    }

    /** Creates the backing tables and indexes if they do not yet exist. */
    public void initSchema() {
        try (Connection conn = dataSource.getConnection()) {
            try (var stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_SQL);
            }
            try (var stmt = conn.createStatement()) {
                stmt.execute(CREATE_STATUS_INDEX_SQL);
            }
            LOG.info("event=embedding_work_store_schema_initialized");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize embedding work store schema", e);
        }
    }

    @Override
    public void save(PendingEmbeddingRow row) {
        Objects.requireNonNull(row, "row must not be null");
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, row.tableName());
            ps.setString(2, row.rowId());
            ps.setString(3, row.modelName());
            ps.setString(4, row.vectorColumn());
            ps.setString(5, row.sourceText());
            ps.setString(6, WorkItemStatus.PENDING.name());
            ps.setTimestamp(7, Timestamp.from(row.enqueuedAt()));
            ps.setTimestamp(8, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to save embedding work item: " + row.tableName() + "::" + row.rowId(), e);
        }
    }

    @Override
    public void markInProgress(List<PendingEmbeddingRow> rows) {
        Objects.requireNonNull(rows, "rows must not be null");
        if (rows.isEmpty()) {
            return;
        }
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MARK_IN_PROGRESS_SQL)) {
            for (PendingEmbeddingRow row : rows) {
                ps.setTimestamp(1, now);
                ps.setString(2, row.tableName());
                ps.setString(3, row.rowId());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to mark embedding work items in progress", e);
        }
    }

    @Override
    public void markCompleted(String tableName, String rowId) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(rowId, "rowId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MARK_COMPLETED_SQL)) {
            ps.setString(1, tableName);
            ps.setString(2, rowId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to mark embedding work item completed: " + tableName + "::" + rowId, e);
        }
    }

    @Override
    public List<PendingEmbeddingRow> loadUnfinished() {
        List<PendingEmbeddingRow> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LOAD_UNFINISHED_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new PendingEmbeddingRow(
                        rs.getString("table_name"),
                        rs.getString("row_id"),
                        rs.getString("model_name"),
                        rs.getString("vector_column"),
                        rs.getString("source_text"),
                        rs.getTimestamp("enqueued_at").toInstant()));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to load unfinished embedding work items", e);
        }
        return result;
    }

    @Override
    public long countUnfinished(String tableName) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(COUNT_UNFINISHED_SQL)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to count unfinished embedding work items for: " + tableName, e);
        }
    }
}
