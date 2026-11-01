package org.pangrova.streaming.jdbc;

import org.pangrova.spi.streaming.store.StreamOffsetStore;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.streaming.exception.StreamingStoreException;
import org.pangrova.spi.streaming.source.TopicPartitionOffset;
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

    private void executeBatchSave(String streamName, Map<TopicPartitionOffset, Long> offsets) {
        // Plain replace: a lower offset than the stored one is persisted verbatim.
        // Explicit seeks/rewinds must survive a restart; monotonicity is enforced
        // by the pipeline's commit protocol and HA epoch fencing, not by SQL.
        String sql = "INSERT INTO ingestion_stream_offsets "
                + "(stream_name, topic, partition_id, committed_offset, updated_at) "
                + "VALUES (?,?,?,?,?) ON CONFLICT (stream_name, topic, partition_id) DO UPDATE SET "
                + "committed_offset=EXCLUDED.committed_offset, updated_at=EXCLUDED.updated_at";

        try (Connection conn = dataSource.getConnection()) {
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindOffsetBatch(ps, streamName, offsets);
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(originalAutoCommit);
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
