package org.pangrova.query.store.jdbc.transaction;

import org.pangrova.query.api.transaction.QuarantinedWriteCommit;
import org.pangrova.query.api.transaction.WriteCommitQuarantineStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed quarantine store ({@code write_commit_quarantine}). Decisions made by one
 * coordinator are visible to every peer and survive crash/failover.
 */
public final class JdbcWriteCommitQuarantineStore implements WriteCommitQuarantineStore {

    private static final String INSERT =
            "INSERT INTO write_commit_quarantine (query_id, commit_fingerprint, catalog_name, "
                    + "table_name, reason, quarantined_at) VALUES (?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (query_id, commit_fingerprint) DO NOTHING";

    private static final String SELECT_EXISTS =
            "SELECT 1 FROM write_commit_quarantine "
                    + "WHERE query_id = ? AND commit_fingerprint = ?";

    private static final String SELECT_BY_QUERY_ID =
            "SELECT query_id, commit_fingerprint, catalog_name, table_name, reason, "
                    + "quarantined_at FROM write_commit_quarantine WHERE query_id = ? "
                    + "ORDER BY quarantined_at DESC LIMIT 1";

    private final DataSource dataSource;

    public JdbcWriteCommitQuarantineStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public boolean exists(String queryId, String commitFingerprint) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_EXISTS)) {
            statement.setString(1, queryId);
            statement.setString(2, commitFingerprint);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to read write-commit quarantine for " + queryId, e);
        }
    }

    @Override
    public void record(QuarantinedWriteCommit entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT)) {
            statement.setString(1, entry.queryId());
            statement.setString(2, entry.commitFingerprint());
            statement.setString(3, entry.catalogName());
            statement.setString(4, entry.tableName());
            statement.setString(5, entry.reason());
            statement.setTimestamp(6, Timestamp.from(entry.quarantinedAt()));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to quarantine write intent " + entry.queryId(), e);
        }
    }

    @Override
    public Optional<QuarantinedWriteCommit> find(String queryId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_QUERY_ID)) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(new QuarantinedWriteCommit(
                        rows.getString("query_id"),
                        rows.getString("commit_fingerprint"),
                        rows.getString("catalog_name"),
                        rows.getString("table_name"),
                        rows.getString("reason"),
                        rows.getTimestamp("quarantined_at").toInstant()));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to read write-commit quarantine for " + queryId, e);
        }
    }
}
