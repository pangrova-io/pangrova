package org.pangrova.persistence.indexing.postgresql.metadata;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.rollbackAfterFailure;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.metadata.CommitEventRequest;
import org.pangrova.persistence.indexing.api.metadata.CommitLogEntry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.setJson;
import static org.pangrova.persistence.jdbc.operation.JdbcOperations.getInstant;
import static org.pangrova.persistence.jdbc.operation.JdbcOperations.setInstant;

/** Maintains the durable per-table commit-event queue and its fenced head. */
final class JdbcIndexCommitQueue extends JdbcStoreSupport {
    private static final int TABLE_SEQUENCE_PARAMETER = 11;

    private static final String TABLE_KEY =
            "tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=?";

    static final String POLL_SQL = "WITH picked AS MATERIALIZED ("
            + "SELECT tenant_id,catalog_name,schema_name,table_name,head_sequence,last_polled_at "
            + "FROM index_commit_table_heads "
            + "WHERE head_event_id IS NOT NULL AND claim_epoch <= ? "
            + "ORDER BY last_polled_at,tenant_id,catalog_name,schema_name,table_name "
            + "LIMIT ? FOR UPDATE SKIP LOCKED), "
            + "claimed AS (UPDATE index_commit_table_heads h "
            + "SET claim_epoch=?,last_polled_at=clock_timestamp() FROM picked p "
            + "WHERE h.tenant_id=p.tenant_id AND h.catalog_name=p.catalog_name "
            + "AND h.schema_name=p.schema_name AND h.table_name=p.table_name "
            + "RETURNING h.tenant_id,h.catalog_name,h.schema_name,h.table_name), "
            + "quota AS (SELECT CEIL(?::numeric/GREATEST(COUNT(*),1))::integer AS per_table "
            + "FROM claimed), bounded AS ("
            + "SELECT p.last_polled_at AS fairness_time, "
            + "e.event_id,e.tenant_id,e.catalog_name,e.schema_name,e.table_name,"
            + "e.snapshot_id,e.added_files,e.removed_files,e.event_type,e.created_at,e.commit_fingerprint,"
            + "e.table_sequence-p.head_sequence AS table_position "
            + "FROM picked p JOIN claimed c USING "
            + "(tenant_id,catalog_name,schema_name,table_name) "
            + "CROSS JOIN quota q CROSS JOIN LATERAL ("
            + "SELECT * FROM index_commit_log e WHERE e.tenant_id=p.tenant_id "
            + "AND e.catalog_name=p.catalog_name AND e.schema_name=p.schema_name "
            + "AND e.table_name=p.table_name AND e.processed=FALSE "
            + "AND e.table_sequence>=p.head_sequence "
            + "ORDER BY e.table_sequence LIMIT q.per_table) e) "
            + "SELECT * FROM bounded ORDER BY table_position,fairness_time,event_id LIMIT ?";

    JdbcIndexCommitQueue(DataSource dataSource) {
        super(dataSource);
    }

    void recordCommitEvent(CommitEventRequest request) {
        inTransaction("record commit event", connection -> {
            createHeadIfMissing(connection, request);
            Head head = lockHead(connection, request);
            Long eventId = insertEvent(connection, request, head.nextSequence());
            if (eventId == null) {
                if (!sameJournaledCommit(connection, request)) {
                    throw new IllegalStateException(
                            "Index commit fingerprint was replayed with different event content");
                }
                return null;
            }
            advanceAfterInsert(connection, request, head, eventId);
            return null;
        });
    }

    List<CommitLogEntry> pollUnprocessedCommitEvents(int limit, long leaseEpoch) {
        requirePollArguments(limit, leaseEpoch);
        return inTransaction("poll commit events", connection -> {
            try (PreparedStatement ps = connection.prepareStatement(POLL_SQL)) {
                ps.setLong(1, leaseEpoch);
                ps.setInt(2, limit);
                ps.setLong(3, leaseEpoch);
                ps.setInt(4, limit);
                ps.setInt(5, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    List<CommitLogEntry> entries = new ArrayList<>();
                    while (rs.next()) {
                        entries.add(mapEvent(rs));
                    }
                    return entries;
                }
            }
        });
    }

    void markCommitEventProcessed(long eventId, long leaseEpoch) {
        if (eventId <= 0 || leaseEpoch <= 0) {
            throw new IllegalArgumentException("eventId and leaseEpoch must be positive");
        }
        inTransaction("acknowledge commit event", connection -> {
            EventPosition event = eventPosition(connection, eventId);
            Head head = lockHead(connection, event.table());
            if (head.claimEpoch() != leaseEpoch) {
                throw new IllegalStateException("Stale index commit-event lease epoch");
            }
            if (event.processed()) {
                return null;
            }
            if (!eventIdEquals(head.headEventId(), eventId)
                    || head.headSequence() != event.sequence()) {
                throw new IllegalStateException("Index commit event is not the current table head");
            }
            updateProcessed(connection, eventId);
            EventPosition next = nextEvent(connection, event);
            advanceAfterAck(connection, event.table(), next);
            return null;
        });
    }

    private static void requirePollArguments(int limit, long leaseEpoch) {
        if (limit <= 0 || leaseEpoch <= 0) {
            throw new IllegalArgumentException("limit and leaseEpoch must be positive");
        }
    }

    private static boolean eventIdEquals(Long expected, long actual) {
        return expected != null && expected == actual;
    }

    private static CommitLogEntry mapEvent(ResultSet rs) throws SQLException {
        return new CommitLogEntry(
                rs.getLong("event_id"), rs.getString("tenant_id"),
                rs.getString("catalog_name"), rs.getString("schema_name"),
                rs.getString("table_name"), rs.getString("snapshot_id"),
                rs.getString("added_files"), rs.getString("removed_files"),
                rs.getString("event_type"), getInstant(rs, "created_at"), rs.getString("commit_fingerprint"));
    }

    private static void createHeadIfMissing(Connection connection, CommitEventRequest request)
            throws SQLException {
        String sql = "INSERT INTO index_commit_table_heads "
                + "(tenant_id,catalog_name,schema_name,table_name,next_sequence) "
                + "VALUES (?,?,?,?,1) ON CONFLICT DO NOTHING";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bindTable(ps, 1, request);
            ps.executeUpdate();
        }
    }

    private static Head lockHead(Connection connection, TableKey table) throws SQLException {
        String sql = "SELECT head_event_id,head_sequence,next_sequence,claim_epoch "
                + "FROM index_commit_table_heads WHERE " + TABLE_KEY + " FOR UPDATE";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bindTable(ps, 1, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Index commit table head is missing");
                }
                Long headEventId = (Long) rs.getObject("head_event_id");
                Long headSequence = (Long) rs.getObject("head_sequence");
                return new Head(headEventId, headSequence,
                        rs.getLong("next_sequence"), rs.getLong("claim_epoch"));
            }
        }
    }

    private static Head lockHead(Connection connection, CommitEventRequest request)
            throws SQLException {
        return lockHead(connection, new TableKey(request.tenantId(), request.catalogName(),
                request.schemaName(), request.tableName()));
    }

    private static Long insertEvent(
            Connection connection, CommitEventRequest request, long sequence) throws SQLException {
        String sql = "INSERT INTO index_commit_log "
                + "(tenant_id,catalog_name,schema_name,table_name,snapshot_id,"
                + "added_files,removed_files,event_type,processed,created_at,"
                + "commit_fingerprint,table_sequence) "
                + "VALUES (?,?,?,?,?,?,?,?,FALSE,?,?,?) "
                + "ON CONFLICT (tenant_id,catalog_name,schema_name,table_name,commit_fingerprint) "
                + "WHERE commit_fingerprint IS NOT NULL DO NOTHING RETURNING event_id";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bindTable(ps, 1, request);
            ps.setString(5, request.snapshotId());
            setJson(ps, 6, request.addedFilesJson());
            setJson(ps, 7, request.removedFilesJson());
            ps.setString(8, request.eventType());
            setInstant(ps, 9, request.createdAt());
            ps.setString(10, request.commitFingerprint());
            ps.setLong(TABLE_SEQUENCE_PARAMETER, sequence);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    private static void advanceAfterInsert(
            Connection connection, CommitEventRequest request, Head head, long eventId)
            throws SQLException {
        String sql = "UPDATE index_commit_table_heads SET next_sequence=?,"
                + "head_event_id=COALESCE(head_event_id,?),"
                + "head_sequence=COALESCE(head_sequence,?),"
                + "last_polled_at=CASE WHEN head_event_id IS NULL "
                + "THEN '-infinity'::timestamptz ELSE last_polled_at END "
                + "WHERE " + TABLE_KEY;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, Math.addExact(head.nextSequence(), 1));
            ps.setLong(2, eventId);
            ps.setLong(3, head.nextSequence());
            bindTable(ps, 4, request);
            if (ps.executeUpdate() != 1) {
                throw new IllegalStateException("Index commit table head disappeared on insert");
            }
        }
    }

    private static boolean sameJournaledCommit(
            Connection connection, CommitEventRequest request) throws SQLException {
        String sql = "SELECT snapshot_id IS NOT DISTINCT FROM ? AS same_snapshot, "
                + "event_type = ? AS same_type, "
                + "added_files IS NOT DISTINCT FROM ?::jsonb AS same_added, "
                + "removed_files IS NOT DISTINCT FROM ?::jsonb AS same_removed "
                + "FROM index_commit_log WHERE " + TABLE_KEY + " AND commit_fingerprint=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, request.snapshotId());
            ps.setString(2, request.eventType());
            setJson(ps, 3, request.addedFilesJson());
            setJson(ps, 4, request.removedFilesJson());
            bindTable(ps, 5, request);
            ps.setString(9, request.commitFingerprint());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean("same_snapshot")
                        && rs.getBoolean("same_type") && rs.getBoolean("same_added")
                        && rs.getBoolean("same_removed");
            }
        }
    }

    private static EventPosition eventPosition(Connection connection, long eventId)
            throws SQLException {
        String sql = "SELECT tenant_id,catalog_name,schema_name,table_name,"
                + "table_sequence,processed FROM index_commit_log WHERE event_id=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Index commit event is missing");
                }
                return new EventPosition(eventId, new TableKey(
                        rs.getString("tenant_id"), rs.getString("catalog_name"),
                        rs.getString("schema_name"), rs.getString("table_name")),
                        rs.getLong("table_sequence"), rs.getBoolean("processed"));
            }
        }
    }

    private static void updateProcessed(Connection connection, long eventId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE index_commit_log SET processed=TRUE "
                        + "WHERE event_id=? AND processed=FALSE")) {
            ps.setLong(1, eventId);
            if (ps.executeUpdate() != 1) {
                throw new IllegalStateException("Index commit event changed during acknowledgement");
            }
        }
    }

    private static EventPosition nextEvent(Connection connection, EventPosition current)
            throws SQLException {
        String sql = "SELECT event_id,table_sequence FROM index_commit_log "
                + "WHERE " + TABLE_KEY + " AND processed=FALSE AND table_sequence>? "
                + "ORDER BY table_sequence LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bindTable(ps, 1, current.table());
            ps.setLong(5, current.sequence());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new EventPosition(rs.getLong(1), current.table(),
                        rs.getLong(2), false) : null;
            }
        }
    }

    private static void advanceAfterAck(
            Connection connection, TableKey table, EventPosition next) throws SQLException {
        String sql = "UPDATE index_commit_table_heads "
                + "SET head_event_id=?,head_sequence=? WHERE " + TABLE_KEY;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if (next == null) {
                ps.setNull(1, java.sql.Types.BIGINT);
                ps.setNull(2, java.sql.Types.BIGINT);
            } else {
                ps.setLong(1, next.eventId());
                ps.setLong(2, next.sequence());
            }
            bindTable(ps, 3, table);
            if (ps.executeUpdate() != 1) {
                throw new IllegalStateException("Index commit table head disappeared on acknowledgement");
            }
        }
    }

    private static void bindTable(PreparedStatement ps, int first, CommitEventRequest request)
            throws SQLException {
        bindTable(ps, first, new TableKey(request.tenantId(), request.catalogName(),
                request.schemaName(), request.tableName()));
    }

    private static void bindTable(PreparedStatement ps, int first, TableKey table)
            throws SQLException {
        ps.setString(first, table.tenant());
        ps.setString(first + 1, table.catalog());
        ps.setString(first + 2, table.schema());
        ps.setString(first + 3, table.table());
    }

    private <T> T inTransaction(String action, SqlFunction<Connection, T> work) {
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                T value = work.apply(connection);
                connection.commit();
                return value;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to " + action, failure);
        }
    }

    private record TableKey(String tenant, String catalog, String schema, String table) {}

    private record Head(Long headEventId, Long headSequence, long nextSequence,
                        long claimEpoch) {}

    private record EventPosition(long eventId, TableKey table, long sequence,
                                 boolean processed) {}
}
