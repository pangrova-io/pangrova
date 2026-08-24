package org.pangrova.metadata.coordination;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed write commit journal for crash-recovery idempotency.
 *
 * <p>All operations are synchronous and block until persisted, ensuring that
 * a crash between intent-record and commit-execute leaves a recoverable
 * PENDING entry in the database.</p>
 *
 * <p>Schema (auto-created on first use):
 * <pre>{@code
 * CREATE TABLE IF NOT EXISTS write_commit_journal (
 *     query_id     VARCHAR(255) PRIMARY KEY,
 *     catalog_name VARCHAR(255) NOT NULL,
 *     table_name   VARCHAR(1024) NOT NULL,
 *     state        VARCHAR(32) NOT NULL,
 *     timestamp_ms BIGINT NOT NULL,
 *     fragments    TEXT,               -- Base64 worker commit fragments (replay payload)
 *     write_mode   VARCHAR(64),
 *     idempotency_key VARCHAR(512),
 *     coordinator_id  VARCHAR(255),
 *     term         BIGINT NOT NULL DEFAULT 0
 * )
 * }</pre>
 */
@ThreadSafe
public final class JdbcWriteCommitJournal implements WriteCommitJournal {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriteCommitJournal.class);

    private static final String CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS write_commit_journal ("
                    + "query_id VARCHAR(255) PRIMARY KEY, "
                    + "catalog_name VARCHAR(255) NOT NULL, "
                    + "table_name VARCHAR(1024) NOT NULL, "
                    + "state VARCHAR(32) NOT NULL, "
                    + "timestamp_ms BIGINT NOT NULL, "
                    + "fragments TEXT, "
                    + "write_mode VARCHAR(64), "
                    + "idempotency_key VARCHAR(512), "
                    + "coordinator_id VARCHAR(255), "
                    + "term BIGINT NOT NULL DEFAULT 0)";

    private static final String INSERT_ENTRY =
            "INSERT INTO write_commit_journal (query_id, catalog_name, table_name, state, "
                    + "timestamp_ms, fragments, write_mode, idempotency_key, coordinator_id, term) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_STATE =
            "UPDATE write_commit_journal SET state = ?, timestamp_ms = ? WHERE query_id = ?";

    private static final String SELECT_BY_QUERY_ID =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, coordinator_id, term "
                    + "FROM write_commit_journal WHERE query_id = ?";

    private static final String SELECT_PENDING =
            "SELECT query_id, catalog_name, table_name, state, timestamp_ms, "
                    + "fragments, write_mode, idempotency_key, coordinator_id, term "
                    + "FROM write_commit_journal WHERE state = 'PENDING' ORDER BY timestamp_ms ASC";

    private static final String DELETE_ENTRY =
            "DELETE FROM write_commit_journal WHERE query_id = ?";

    private final DataSource dataSource;

    public JdbcWriteCommitJournal(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        ensureTableExists();
        LOG.info("event=write_commit_journal_started type=JDBC");
    }

    private void ensureTableExists() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CREATE_TABLE)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=write_commit_journal_table_creation_failed error={}", e.getMessage(), e);
            throw new JournalWriteException("Failed to create write_commit_journal table", e);
        }
    }

    @Override
    public void recordIntent(WriteCommitEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_ENTRY)) {
            ps.setString(1, entry.queryId());
            ps.setString(2, entry.catalogName());
            ps.setString(3, entry.tableName());
            ps.setString(4, entry.state().name());
            ps.setLong(5, entry.timestampMs());
            ps.setString(6, encodeFragments(entry.fragments()));
            ps.setString(7, entry.writeMode());
            ps.setString(8, entry.idempotencyKey());
            ps.setString(9, entry.coordinatorId());
            ps.setLong(10, entry.term());
            ps.executeUpdate();
            LOG.info("event=write_commit_intent_recorded queryId={} catalog={} table={}",
                    entry.queryId(), entry.catalogName(), entry.tableName());
        } catch (SQLException e) {
            LOG.error("event=write_commit_intent_failed queryId={} error={}",
                    entry.queryId(), e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to record write commit intent for queryId=" + entry.queryId(), e);
        }
    }

    @Override
    public void markCommitted(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_STATE)) {
            ps.setString(1, WriteCommitEntry.State.COMMITTED.name());
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, queryId);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                LOG.warn("event=write_commit_mark_noop queryId={} reason=no_pending_entry", queryId);
            } else {
                LOG.info("event=write_commit_marked_committed queryId={}", queryId);
            }
        } catch (SQLException e) {
            LOG.error("event=write_commit_mark_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to mark write commit as COMMITTED for queryId=" + queryId, e);
        }
    }

    @Override
    public boolean isCommitted(String queryId) {
        return getEntry(queryId)
                .map(e -> e.state() == WriteCommitEntry.State.COMMITTED)
                .orElse(false);
    }

    @Override
    public Optional<WriteCommitEntry> getEntry(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_QUERY_ID)) {
            ps.setString(1, queryId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(readEntry(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("event=write_commit_lookup_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException(
                    "Failed to look up write commit entry for queryId=" + queryId, e);
        }
        return Optional.empty();
    }

    @Override
    public List<WriteCommitEntry> replayPendingEntries() {
        List<WriteCommitEntry> entries = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_PENDING);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                entries.add(readEntry(rs));
            }
        } catch (SQLException e) {
            LOG.error("event=write_commit_replay_failed error={}", e.getMessage(), e);
            throw new JournalWriteException("Failed to replay pending write commit entries", e);
        }
        LOG.info("event=write_commit_replay count={}", entries.size());
        return List.copyOf(entries);
    }

    @Override
    public void purge(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_ENTRY)) {
            ps.setString(1, queryId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=write_commit_purge_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
        }
    }

    private static WriteCommitEntry readEntry(ResultSet rs) throws SQLException {
        return new WriteCommitEntry(
                rs.getString("query_id"),
                rs.getString("catalog_name"),
                rs.getString("table_name"),
                WriteCommitEntry.State.valueOf(rs.getString("state")),
                rs.getLong("timestamp_ms"),
                decodeFragments(rs.getString("fragments")),
                rs.getString("write_mode"),
                rs.getString("idempotency_key"),
                rs.getString("coordinator_id"),
                rs.getLong("term"));
    }

    /** Encodes worker commit fragments as a comma-separated Base64 list for durable storage. */
    private static String encodeFragments(List<byte[]> fragments) {
        if (fragments == null || fragments.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fragments.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Base64.getEncoder().encodeToString(fragments.get(i)));
        }
        return sb.toString();
    }

    /** Reconstructs worker commit fragments from the comma-separated Base64 storage form. */
    private static List<byte[]> decodeFragments(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return List.of();
        }
        List<byte[]> out = new ArrayList<>();
        for (String token : encoded.split(",")) {
            if (!token.isEmpty()) {
                out.add(Base64.getDecoder().decode(token));
            }
        }
        return out;
    }
}
