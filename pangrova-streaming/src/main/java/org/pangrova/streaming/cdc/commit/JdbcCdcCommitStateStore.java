package org.pangrova.streaming.cdc.commit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * JDBC-backed persistence for CDC two-phase commit state.
 *
 * <p>Stores transaction state in the {@code cdc_commit_state} table, enabling
 * crash recovery and idempotent deduplication based on the
 * (source_id, partition, start_offset, end_offset) tuple.</p>
 *
 * <p>Thread-safe: each operation acquires its own connection from the pool.</p>
 */
@ThreadSafe
public final class JdbcCdcCommitStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCdcCommitStateStore.class);

    private static final String SQL_INSERT = """
            INSERT INTO cdc_commit_state (id, source_id, partition_num, start_offset, end_offset,
                state, deadline, created_at, batch_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""";

    private static final String SQL_UPDATE_STATE = """
            UPDATE cdc_commit_state SET state = ? WHERE id = ?""";

    private static final String SQL_FIND_BY_ID = """
            SELECT * FROM cdc_commit_state WHERE id = ?""";

    private static final String SQL_FIND_DUPLICATE = """
            SELECT * FROM cdc_commit_state
            WHERE source_id = ? AND batch_key = ?
            AND state IN ('PREPARED', 'COMMITTING', 'COMMITTED')""";

    private static final String SQL_FIND_IN_FLIGHT = """
            SELECT * FROM cdc_commit_state
            WHERE source_id = ? AND state IN ('PREPARED', 'COMMITTING', 'ROLLING_BACK')""";

    private static final String SQL_FIND_LAST_COMMITTED = """
            SELECT partition_num, MAX(end_offset) AS max_end_offset
            FROM cdc_commit_state
            WHERE source_id = ? AND state = 'COMMITTED'
            GROUP BY partition_num""";

    private final DataSource dataSource;

    public JdbcCdcCommitStateStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Persists a new commit state row. Returns false if a duplicate
     * (source_id, partition, start_offset, end_offset) already exists.
     */
    public boolean insert(PrepareToken token, CdcCommitState state) {
        return insert(token, state, dedupKeyForRange(token.range()));
    }

    /**
     * Persists a new commit state row keyed for deduplication by {@code batchKey} (A-EOS-01: a
     * collision-free full-batch fingerprint). Returns false if a row with the same
     * (source_id, batch_key) already exists (unique-index violation).
     */
    public boolean insert(PrepareToken token, CdcCommitState state, String batchKey) {
        Objects.requireNonNull(token, "token must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setString(1, token.id().toString());
            ps.setString(2, token.source().value());
            ps.setInt(3, token.range().partition());
            ps.setLong(4, token.range().startOffset());
            ps.setLong(5, token.range().endOffset());
            ps.setString(6, state.name());
            ps.setTimestamp(7, Timestamp.from(token.deadline()));
            ps.setTimestamp(8, Timestamp.from(Instant.now()));
            ps.setString(9, batchKey);
            int rows = ps.executeUpdate();
            if (rows > 0) {
                LOG.info("event=cdc_commit_state_inserted id={} source={} state={}",
                        token.id(), token.source(), state);
            }
            return rows > 0;
        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState()) || "23000".equals(e.getSQLState())) {
                LOG.debug("event=cdc_commit_state_duplicate source={} batchKey={}",
                        token.source(), batchKey);
                return false;
            }
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to insert CDC commit state id=" + token.id(), e);
        }
    }

    /**
     * A-EOS-01: the default dedup key for a single OffsetRange, preserving the pre-fingerprint
     * (partition, start, end) identity for callers that do not supply a full-batch fingerprint.
     */
    static String dedupKeyForRange(OffsetRange range) {
        return range.partition() + ":" + range.startOffset() + ":" + range.endOffset();
    }

    /**
     * Transitions the state of an existing commit record.
     */
    public void updateState(UUID id, CdcCommitState newState) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(newState, "newState must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_STATE)) {
            ps.setString(1, newState.name());
            ps.setString(2, id.toString());
            int rows = ps.executeUpdate();
            if (rows == 0) {
                throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                        "CDC commit state not found for id=" + id);
            }
            LOG.info("event=cdc_commit_state_updated id={} newState={}", id, newState);
        } catch (SQLException e) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to update CDC commit state id=" + id, e);
        }
    }

    /**
     * Finds an existing commit record by the deduplication key (source_id, batch_key) in a
     * non-terminal or committed state. This overload derives the default (single-range) key.
     */
    public Optional<PrepareToken> findDuplicate(SourceId source, OffsetRange range) {
        Objects.requireNonNull(range, "range must not be null");
        return findDuplicate(source, dedupKeyForRange(range));
    }

    /**
     * Finds an existing commit record by the deduplication key (source_id, batch_key) in a
     * non-terminal or committed state (A-EOS-01: batch_key is a collision-free full-batch
     * fingerprint, replacing the former single-partition range key).
     */
    public Optional<PrepareToken> findDuplicate(SourceId source, String batchKey) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_DUPLICATE)) {
            ps.setString(1, source.value());
            ps.setString(2, batchKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapToken(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to find duplicate CDC commit state source=" + source, e);
        }
    }

    /**
     * Finds all in-flight (non-terminal) transactions for a given source.
     * Used during recovery.
     */
    public List<PrepareToken> findInFlight(SourceId source) {
        Objects.requireNonNull(source, "source must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_IN_FLIGHT)) {
            ps.setString(1, source.value());
            try (ResultSet rs = ps.executeQuery()) {
                List<PrepareToken> tokens = new ArrayList<>();
                while (rs.next()) {
                    tokens.add(mapToken(rs));
                }
                return Collections.unmodifiableList(tokens);
            }
        } catch (SQLException e) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to find in-flight CDC commit states source=" + source, e);
        }
    }

    /**
     * Looks up the current state of a transaction by id.
     */
    public Optional<CdcCommitState> findStateById(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_BY_ID)) {
            ps.setString(1, id.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(CdcCommitState.valueOf(rs.getString("state")));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to find CDC commit state id=" + id, e);
        }
    }

    /**
     * Finds the maximum committed end_offset for each partition of the given source.
     *
     * <p>Used during pipeline startup to determine where to seek the source reader.
     * Returns partition → (endOffset + 1), i.e., the next offset to consume.</p>
     */
    public Map<Integer, Long> findLastCommittedOffsets(SourceId source) {
        Objects.requireNonNull(source, "source must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_LAST_COMMITTED)) {
            ps.setString(1, source.value());
            try (ResultSet rs = ps.executeQuery()) {
                Map<Integer, Long> offsets = new HashMap<>();
                while (rs.next()) {
                    int partition = rs.getInt("partition_num");
                    long maxEndOffset = rs.getLong("max_end_offset");
                    // Return endOffset + 1 = next offset to consume
                    offsets.put(partition, maxEndOffset + 1);
                }
                return Collections.unmodifiableMap(offsets);
            }
        } catch (SQLException e) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to find last committed offsets for source=" + source, e);
        }
    }

    private static PrepareToken mapToken(ResultSet rs) throws SQLException {
        return new PrepareToken(
                UUID.fromString(rs.getString("id")),
                new SourceId(rs.getString("source_id")),
                new OffsetRange(
                        rs.getInt("partition_num"),
                        rs.getLong("start_offset"),
                        rs.getLong("end_offset")),
                rs.getTimestamp("deadline").toInstant());
    }
}
