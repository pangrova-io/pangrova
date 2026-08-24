package org.pangrova.metadata.mv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC-backed persistence hook for materialized view metadata.
 * Stores MV definitions and state in a {@code materialized_views} table.
 *
 * <p>Table DDL (auto-created if missing):
 * <pre>
 * CREATE TABLE IF NOT EXISTS materialized_views (
 *   mv_name        VARCHAR(512) PRIMARY KEY,
 *   base_query     TEXT NOT NULL,
 *   state          VARCHAR(32) NOT NULL,
 *   refresh_strategy VARCHAR(64) NOT NULL,
 *   created_at     TIMESTAMP NOT NULL,
 *   last_refreshed_at TIMESTAMP
 * )
 * </pre>
 */
public final class JdbcMaterializedViewPersistenceHook
        implements InMemoryMaterializedViewStore.PersistenceHook {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMaterializedViewPersistenceHook.class);

    private static final String TABLE_NAME = "materialized_views";

    private static final String SQL_UPSERT =
            "MERGE INTO " + TABLE_NAME
                    + " (mv_name, base_query, state, refresh_strategy, created_at, last_refreshed_at) "
                    + "KEY (mv_name) VALUES (?, ?, ?, ?, ?, ?)";

    private static final String SQL_DELETE =
            "DELETE FROM " + TABLE_NAME + " WHERE mv_name = ?";

    private static final String SQL_LOAD_ALL =
            "SELECT mv_name, base_query, state, refresh_strategy, created_at, last_refreshed_at "
                    + "FROM " + TABLE_NAME;

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
                    + "mv_name VARCHAR(512) PRIMARY KEY, "
                    + "base_query TEXT NOT NULL, "
                    + "state VARCHAR(32) NOT NULL, "
                    + "refresh_strategy VARCHAR(64) NOT NULL, "
                    + "created_at TIMESTAMP NOT NULL, "
                    + "last_refreshed_at TIMESTAMP"
                    + ")";

    private final DataSource dataSource;

    public JdbcMaterializedViewPersistenceHook(DataSource dataSource) {
        this.dataSource = dataSource;
        ensureTable();
    }

    private void ensureTable() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_CREATE_TABLE)) {
            stmt.execute();
        } catch (SQLException e) {
            LOG.warn("event=mv_table_creation_failed error={}", e.getMessage(), e);
        }
    }

    @Override
    public void onSave(MaterializedViewMetadata metadata) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_UPSERT)) {
            stmt.setString(1, metadata.mvName());
            stmt.setString(2, metadata.baseQuery());
            stmt.setString(3, metadata.state().name());
            stmt.setString(4, metadata.refreshStrategy());
            stmt.setTimestamp(5, Timestamp.from(metadata.createdAt()));
            stmt.setTimestamp(6, metadata.lastRefreshedAt() != null
                    ? Timestamp.from(metadata.lastRefreshedAt()) : null);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=mv_save_failed mvName={} error={}",
                    metadata.mvName(), e.getMessage(), e);
        }
    }

    @Override
    public void onDelete(String mvName) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_DELETE)) {
            stmt.setString(1, mvName);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=mv_delete_failed mvName={} error={}", mvName, e.getMessage(), e);
        }
    }

    @Override
    public List<MaterializedViewMetadata> loadAll() {
        List<MaterializedViewMetadata> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_LOAD_ALL);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                results.add(fromResultSet(rs));
            }
        } catch (SQLException e) {
            LOG.error("event=mv_load_all_failed error={}", e.getMessage(), e);
        }
        return results;
    }

    private static MaterializedViewMetadata fromResultSet(ResultSet rs) throws SQLException {
        Timestamp lastRefreshed = rs.getTimestamp("last_refreshed_at");
        return new MaterializedViewMetadata(
                rs.getString("mv_name"),
                rs.getString("base_query"),
                MaterializedViewState.valueOf(rs.getString("state")),
                rs.getString("refresh_strategy"),
                rs.getTimestamp("created_at").toInstant(),
                lastRefreshed != null ? lastRefreshed.toInstant() : null);
    }
}
