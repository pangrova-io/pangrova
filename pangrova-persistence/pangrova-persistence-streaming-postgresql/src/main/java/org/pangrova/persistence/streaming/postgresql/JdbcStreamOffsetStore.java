package org.pangrova.persistence.streaming.postgresql;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.checkpoint.StreamOffsetStore;
import org.pangrova.streaming.api.exception.StreamingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * JDBC-backed implementation of {@link StreamOffsetStore}.
 *
 * <p>Persists committed Kafka offsets to the {@code ingestion_stream_offsets}
 * table created by migration V6. Uses PostgreSQL ON CONFLICT upsert for
 * atomic batch persistence and single-connection transactions for atomicity.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking for
 * atomicity. No in-process mutable state.</p>
 */
@ThreadSafe
public final class JdbcStreamOffsetStore implements StreamOffsetStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcStreamOffsetStore.class);

    private static final String UPSERT_OFFSETS = """
            INSERT INTO ingestion_stream_offsets (stream_name, topic, partition_id, committed_offset, updated_at)
            VALUES (?,?,?,?,?) ON CONFLICT (stream_name, topic, partition_id) DO UPDATE SET
            committed_offset=EXCLUDED.committed_offset, updated_at=EXCLUDED.updated_at
            """;
    private static final String ACKNOWLEDGE_OFFSETS = """
            WITH current_revision AS MATERIALIZED (
                SELECT stream_name FROM ingestion_stream_state
                WHERE stream_name = ? AND tenant_id = ? AND checkpoint_id = ? AND epoch = ?
                  AND lifecycle_state = 'RUNNING'
                FOR UPDATE
            ), advanced AS (
                INSERT INTO ingestion_stream_offsets (stream_name, topic, partition_id, committed_offset, updated_at)
                SELECT revision.stream_name, requested.topic, requested.partition_id, requested.committed_offset,
                       statement_timestamp()
                FROM current_revision revision
                CROSS JOIN unnest(?::varchar[], ?::integer[], ?::bigint[])
                    AS requested(topic, partition_id, committed_offset)
                ON CONFLICT (stream_name, topic, partition_id) DO UPDATE
                   SET committed_offset = EXCLUDED.committed_offset, updated_at = EXCLUDED.updated_at
                 WHERE ingestion_stream_offsets.committed_offset < EXCLUDED.committed_offset
            )
            SELECT EXISTS (SELECT 1 FROM current_revision)
            """;

    private final DataSource dataSource;

    public JdbcStreamOffsetStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void saveOffsets(String streamName, Map<TopicPartitionOffset, Long> offsets) {
        Objects.requireNonNull(streamName, "streamName must not be null");
        Objects.requireNonNull(offsets, "offsets must not be null");
        if (offsets.isEmpty()) {
            return;
        }
        executeBatchSave(streamName, offsets);
        LOG.info("action=stream_offsets_save streamName={} partitions={}",
                streamName, offsets.size());
    }

    /** Acknowledges a journaled batch only for the current, running checkpoint revision. */
    public void acknowledge(StreamState expected, Map<TopicPartitionOffset, Long> offsets) {
        Objects.requireNonNull(expected, "expected must not be null");
        Objects.requireNonNull(offsets, "offsets must not be null");
        if (offsets.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(ACKNOWLEDGE_OFFSETS)) {
            statement.setString(1, expected.streamName());
            statement.setString(2, expected.tenantId());
            statement.setString(3, expected.checkpointId());
            statement.setLong(4, expected.epoch());
            bindAcknowledgedOffsets(conn, statement, offsets);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || !result.getBoolean(1)) {
                    throw new StaleEpochException(expected.streamName(), expected.epoch());
                }
            }
        } catch (SQLException e) {
            throw new StreamingStoreException("Failed to acknowledge offsets for stream=" + expected.streamName(), e);
        }
    }

    private static void bindAcknowledgedOffsets(Connection conn, PreparedStatement statement,
            Map<TopicPartitionOffset, Long> offsets) throws SQLException {
        String[] topics = new String[offsets.size()];
        Integer[] partitions = new Integer[offsets.size()];
        Long[] committed = new Long[offsets.size()];
        int index = 0;
        for (var entry : offsets.entrySet()) {
            topics[index] = entry.getKey().topic();
            partitions[index] = entry.getKey().partition();
            committed[index++] = Objects.requireNonNull(entry.getValue(), "committedOffset");
        }
        statement.setArray(5, conn.createArrayOf("varchar", topics));
        statement.setArray(6, conn.createArrayOf("integer", partitions));
        statement.setArray(7, conn.createArrayOf("bigint", committed));
    }

    private void executeBatchSave(String streamName, Map<TopicPartitionOffset, Long> offsets) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_OFFSETS)) {
                bindOffsetBatch(ps, streamName, offsets);
                ps.executeBatch();
                conn.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to save offsets for stream=" + streamName, e);
        }
    }

    private static void bindOffsetBatch(PreparedStatement ps, String streamName,
                                        Map<TopicPartitionOffset, Long> offsets) throws SQLException {
        Timestamp now = Timestamp.from(Instant.now());
        for (Map.Entry<TopicPartitionOffset, Long> entry : offsets.entrySet()) {
            TopicPartitionOffset tp = entry.getKey();
            ps.setString(1, streamName);
            ps.setString(2, tp.topic());
            ps.setInt(3, tp.partition());
            ps.setLong(4, entry.getValue());
            ps.setTimestamp(5, now);
            ps.addBatch();
        }
    }

    @Override
    public Map<TopicPartitionOffset, Long> loadOffsets(String streamName) {
        Objects.requireNonNull(streamName, "streamName must not be null");
        String sql = "SELECT topic, partition_id, committed_offset "
                + "FROM ingestion_stream_offsets WHERE stream_name = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, streamName);
            try (ResultSet rs = ps.executeQuery()) {
                Map<TopicPartitionOffset, Long> result = new LinkedHashMap<>();
                while (rs.next()) {
                    TopicPartitionOffset tp = new TopicPartitionOffset(
                            rs.getString("topic"),
                            rs.getInt("partition_id"));
                    result.put(tp, rs.getLong("committed_offset"));
                }
                return Map.copyOf(result);
            }
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to load offsets for stream=" + streamName, e);
        }
    }

    @Override
    public void deleteOffsets(String streamName) {
        Objects.requireNonNull(streamName, "streamName must not be null");
        String sql = "DELETE FROM ingestion_stream_offsets WHERE stream_name = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, streamName);
            int affected = ps.executeUpdate();
            if (affected > 0) {
                LOG.info("action=stream_offsets_delete streamName={} deleted={}",
                        streamName, affected);
            }
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to delete offsets for stream=" + streamName, e);
        }
    }
}
