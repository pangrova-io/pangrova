package org.pangrova.streaming.core.commit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * crash recovery and idempotent deduplication based on the source-scoped
 * full-batch key. Every topic/partition range is stored in the same row and
 * becomes visible to recovery only when its publication is COMMITTED.</p>
 *
 * <p>Thread-safe: each operation acquires its own connection from the pool.</p>
 */
@ThreadSafe
public final class JdbcCdcCommitStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCdcCommitStateStore.class);

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String SQL_INSERT = """
            INSERT INTO cdc_commit_state (id, source_id, offset_ranges,
                state, deadline, created_at, batch_key)
            VALUES (?, ?, CAST(? AS jsonb), ?, ?, ?, ?)""";

    private static final String SQL_UPDATE_STATE = """
            UPDATE cdc_commit_state SET state = ? WHERE id = ?""";

    private static final String SQL_COMPARE_AND_SET_STATE = """
            UPDATE cdc_commit_state SET state = ? WHERE id = ? AND state = ?""";

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
            SELECT r.value->>'topic' AS topic, (r.value->>'partition')::int AS partition_num,
                   MAX((r.value->>'endOffset')::bigint) AS max_end_offset
            FROM cdc_commit_state c
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.offset_ranges, '[null]'::jsonb)) r(value)
            WHERE c.source_id = ? AND c.state = 'COMMITTED'
            GROUP BY r.value->>'topic', r.value->>'partition'""";

    private final DataSource dataSource;

    public JdbcCdcCommitStateStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Persists a complete source checkpoint. Returns false if an active or
     * committed record with the same source-scoped checkpoint key already exists.
     */
    public boolean insert(PrepareToken token, CdcCommitState state) {
        return insert(token, state, token.checkpoint().fingerprint());
    }

    /**
     * Persists a new commit state row keyed by a stable source-scoped {@code batchKey}.
     * Returns false if a row with the same (source_id, batch_key) already exists (unique-index violation).
     */
    public boolean insert(PrepareToken token, CdcCommitState state, String batchKey) {
        Objects.requireNonNull(token, "token must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setString(1, token.id().toString());
            ps.setString(2, token.source().value());
            ps.setString(3, encodeCheckpoint(token.checkpoint()));
            ps.setString(4, state.name());
            ps.setTimestamp(5, Timestamp.from(token.deadline()));
            ps.setTimestamp(6, Timestamp.from(Instant.now()));
            ps.setString(7, batchKey);
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

    /** Atomically claims one state transition across coordinator processes. */
    public boolean compareAndSetState(
            UUID id, CdcCommitState expected, CdcCommitState next) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(expected, "expected must not be null");
        Objects.requireNonNull(next, "next must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_COMPARE_AND_SET_STATE)) {
            ps.setString(1, next.name());
            ps.setString(2, id.toString());
            ps.setString(3, expected.name());
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Failed to claim CDC commit state id=" + id, e);
        }
    }

    /** Finds an active batch and verifies that its immutable source checkpoint agrees. */
    public Optional<PrepareToken> findDuplicate(SourceId source, CdcBatchCheckpoint checkpoint) {
        return findDuplicate(source, checkpoint, checkpoint.fingerprint());
    }

    /** Resolve an explicit batch identity without permitting its source ranges to change. */
    public Optional<PrepareToken> findDuplicate(
            SourceId source, CdcBatchCheckpoint checkpoint, String batchKey) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_DUPLICATE)) {
            ps.setString(1, source.value());
            ps.setString(2, batchKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    PrepareToken token = mapToken(rs);
                    if (!token.checkpoint().equals(checkpoint)) {
                        throw new IllegalStateException("CDC batch identity reused with a different checkpoint: "
                                + token.id());
                    }
                    return Optional.of(token);
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
    public Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source) {
        Objects.requireNonNull(source, "source must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_LAST_COMMITTED)) {
            ps.setString(1, source.value());
            try (ResultSet rs = ps.executeQuery()) {
                Map<TopicPartitionOffset, Long> offsets = new HashMap<>();
                while (rs.next()) {
                    String topic = rs.getString("topic");
                    if (topic == null) {
                        throw new IllegalStateException("CDC history lacks exact topic/partition checkpoints; "
                                + "reconcile source=" + source + " before resuming");
                    }
                    Integer partition = rs.getObject("partition_num", Integer.class);
                    Long maxEndOffset = rs.getObject("max_end_offset", Long.class);
                    if (partition == null || maxEndOffset == null || maxEndOffset < 0) {
                        throw new IllegalStateException("Incomplete durable CDC checkpoint for source=" + source);
                    }
                    // Return endOffset + 1 = next offset to consume
                    offsets.put(new TopicPartitionOffset(topic, partition), Math.addExact(maxEndOffset, 1));
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
                decodeCheckpoint(rs.getString("offset_ranges")),
                rs.getTimestamp("deadline").toInstant());
    }

    private static String encodeCheckpoint(CdcBatchCheckpoint checkpoint) {
        try {
            return JSON.writeValueAsString(checkpoint.ranges());
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Cannot encode CDC checkpoint", failure);
        }
    }

    private static CdcBatchCheckpoint decodeCheckpoint(String encoded) {
        if (encoded == null) {
            throw new IllegalStateException("CDC intent lacks exact source ranges; reconcile before resuming");
        }
        try {
            return new CdcBatchCheckpoint(List.of(JSON.readValue(encoded, OffsetRange[].class)));
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Invalid durable CDC checkpoint", failure);
        }
    }

}
