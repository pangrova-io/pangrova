package org.pangrova.persistence.indexing.postgresql.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.build.ScalarArtifactIntent;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.rollbackAfterFailure;

/** Durable handoff between one scalar writer, its mapping transaction and the orphan collector. */
final class JdbcScalarArtifactIntentStore extends JdbcStoreSupport {
    private static final String IDENTITY = "artifact_digest=? AND index_id=? AND generation=? "
            + "AND attempt_owner=? AND data_file_path=? AND artifact_path=?";
    private static final String SELECT_IDENTITY =
            "SELECT index_id,generation,attempt_owner,data_file_path,artifact_path";
    private static final String TABLE = "scalar_index_artifact_intents";
    private static final String RECLAIMABLE = "i.drained_at<=? AND i.state IN ('DRAINED','DELETE_PENDING') "
            + "AND NOT EXISTS (SELECT 1 FROM index_build_claims c WHERE c.index_id=i.index_id "
            + "AND c.generation=i.generation AND c.owner_id=i.attempt_owner) "
            + "AND NOT EXISTS (SELECT 1 FROM index_file_mappings m WHERE "
            + "md5(m.index_file_path)=md5(i.artifact_path) AND m.index_file_path=i.artifact_path)";
    private static final String RECLAIM_ORDER =
            " ORDER BY i.reclaim_poll_order NULLS FIRST,i.drained_at,i.artifact_digest LIMIT ?";
    private static final String LIST_RECLAIMABLE =
            SELECT_IDENTITY + " FROM " + TABLE + " i WHERE " + RECLAIMABLE + RECLAIM_ORDER;
    private static final String POLL_RECLAIMABLE = "WITH candidates AS (SELECT i.artifact_digest FROM "
            + TABLE + " i WHERE " + RECLAIMABLE + RECLAIM_ORDER + " FOR UPDATE OF i SKIP LOCKED) "
            + "UPDATE " + TABLE + " i SET reclaim_poll_order=nextval('scalar_artifact_reclaim_poll_seq') "
            + "FROM candidates c WHERE i.artifact_digest=c.artifact_digest "
            + "RETURNING i.index_id,i.generation,i.attempt_owner,i.data_file_path,i.artifact_path";

    JdbcScalarArtifactIntentStore(DataSource source) {
        super(source);
    }

    void beginWrite(ScalarArtifactIntent intent) {
        String sql = "INSERT INTO " + TABLE + " "
                + "(artifact_digest,index_id,generation,attempt_owner,data_file_path,artifact_path,state) "
                + "VALUES (?,?,?,?,?,?,'WRITING')";
        try (Connection connection = dataSource().getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                JdbcIndexMappingWriteFence.requireBuildable(connection, intent.indexId(), intent.generation());
                JdbcIndexMappingWriteFence.requireCurrentAttempt(connection, intent.indexId(),
                        intent.generation(), intent.attemptOwner());
                if (hasMapping(connection, intent.artifactPath())) {
                    throw new IllegalStateException("A mapped scalar artifact cannot be rewritten");
                }
                try (var statement = connection.prepareStatement(sql)) {
                    bindIdentity(statement, intent);
                    statement.executeUpdate();
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to register scalar artifact write intent", failure);
        }
    }

    void markDrained(ScalarArtifactIntent intent) {
        // The former owner may mark successful physical close after losing its claim.
        String sql = "UPDATE " + TABLE + " SET state='DRAINED', "
                + "drained_at=COALESCE(drained_at,CURRENT_TIMESTAMP) WHERE " + IDENTITY
                + " AND state IN ('WRITING','DRAINED')";
        try (Connection connection = dataSource().getConnection();
                var statement = connection.prepareStatement(sql)) {
            bindIdentity(statement, intent);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Scalar artifact write intent disappeared before drain");
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to record scalar artifact drain", failure);
        }
    }

    List<ScalarArtifactIntent> listReclaimable(Instant cutoff, int limit) {
        if (limit <= 0) throw new IllegalArgumentException("A positive scalar orphan batch limit is required");
        return executeQuery(LIST_RECLAIMABLE, bindCandidateSelection(cutoff, limit),
                JdbcScalarArtifactIntentStore::readCandidates);
    }

    List<ScalarArtifactIntent> pollReclaimable(Instant cutoff, int limit) {
        if (limit <= 0) throw new IllegalArgumentException("A positive scalar orphan batch limit is required");
        // Advancing priority is not a deletion claim, and never substitutes for the exact ownership recheck.
        // A crash after polling still leaves every selected row eligible; an inaccessible oldest batch cannot
        // repeatedly starve later work, even across collector handoff/restart.
        return executeQuery(POLL_RECLAIMABLE, bindCandidateSelection(cutoff, limit),
                JdbcScalarArtifactIntentStore::readCandidates);
    }

    private static SqlConsumer bindCandidateSelection(Instant cutoff, int limit) {
        return statement -> {
            statement.setTimestamp(1, Timestamp.from(cutoff));
            statement.setInt(2, limit);
        };
    }

    private static List<ScalarArtifactIntent> readCandidates(ResultSet rows) throws SQLException {
        List<ScalarArtifactIntent> intents = new ArrayList<>();
        while (rows.next()) intents.add(readIntent(rows));
        return List.copyOf(intents);
    }

    boolean beginReclaim(ScalarArtifactIntent intent) {
        // Descriptor lock serializes with all mapping insertion and generation publication.
        return changeReclaim(intent, false);
    }

    void completeReclaim(ScalarArtifactIntent intent) {
        if (!changeReclaim(intent, true)) {
            throw new IllegalStateException("Scalar artifact reclaim acknowledgement lost its exact fence");
        }
    }

    private boolean changeReclaim(ScalarArtifactIntent intent, boolean complete) {
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                boolean changed = false;
                if (lockDescriptor(connection, intent.indexId())) {
                    var current = lockIntent(connection, intent.artifactPath());
                    if (current != null && current.intent().equals(intent)
                            && canReclaim(current.state(), complete)
                            && !hasClaim(connection, intent) && !hasMapping(connection, intent.artifactPath())) {
                        changed = writeReclaimState(connection, intent, complete);
                    }
                }
                connection.commit();
                return changed;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to reconcile scalar artifact reclamation", failure);
        }
    }

    private static boolean canReclaim(String state, boolean complete) {
        return "DELETE_PENDING".equals(state) || !complete && "DRAINED".equals(state);
    }

    private static boolean writeReclaimState(Connection connection, ScalarArtifactIntent intent, boolean complete)
            throws SQLException {
        if (complete) {
            try (var statement = connection.prepareStatement("DELETE FROM " + TABLE + " WHERE " + IDENTITY)) {
                bindIdentity(statement, intent);
                return statement.executeUpdate() == 1;
            }
        }
        try (var statement = connection.prepareStatement(
                "UPDATE " + TABLE + " SET state='DELETE_PENDING' WHERE " + IDENTITY)) {
            bindIdentity(statement, intent);
            return statement.executeUpdate() == 1;
        }
    }

    boolean hasIntents(String indexId) {
        return executeQuery("SELECT 1 FROM " + TABLE + " WHERE index_id=? LIMIT 1",
                statement -> statement.setString(1, indexId), ResultSet::next);
    }

    /** Mapping insertion owns the same transaction; a rollback restores the intent. */
    static void transferToMapping(Connection connection, String owner, IndexFileMapping mapping) throws SQLException {
        var current = lockIntent(connection, mapping.indexFilePath());
        if (current == null) {
            // Pre-V127 mappings may still be written by the previous binary during rollout.
            return;
        }
        var intent = current.intent();
        if (!"DRAINED".equals(current.state()) || !intent.indexId().equals(mapping.indexId())
                || intent.generation() != mapping.generation() || !intent.attemptOwner().equals(owner)
                || !intent.dataFilePath().equals(mapping.dataFilePath())
                || !intent.artifactPath().equals(mapping.indexFilePath())) {
            throw new IllegalStateException("Scalar artifact mapping does not own a drained write intent");
        }
        try (var statement = connection.prepareStatement("DELETE FROM " + TABLE + " WHERE " + IDENTITY)) {
            bindIdentity(statement, intent);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Scalar artifact intent changed before mapping transfer");
            }
        }
    }

    private static LockedIntent lockIntent(Connection connection, String path) throws SQLException {
        String sql = SELECT_IDENTITY + ",state FROM " + TABLE + " WHERE artifact_digest=? FOR UPDATE";
        try (var statement = connection.prepareStatement(sql)) {
            statement.setString(1, Sha256Digests.sha256Hex(path));
            try (var rows = statement.executeQuery()) {
                return rows.next() ? new LockedIntent(readIntent(rows), rows.getString(6)) : null;
            }
        }
    }

    private static ScalarArtifactIntent readIntent(ResultSet rows) throws SQLException {
        return new ScalarArtifactIntent(rows.getString(1), rows.getInt(2), rows.getString(3),
                rows.getString(4), rows.getString(5));
    }

    private static boolean lockDescriptor(Connection connection, String indexId) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_descriptors WHERE index_id=? FOR UPDATE")) {
            statement.setString(1, indexId);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static boolean hasClaim(Connection connection, ScalarArtifactIntent intent) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_build_claims WHERE index_id=? AND generation=? AND owner_id=? FOR UPDATE")) {
            statement.setString(1, intent.indexId());
            statement.setInt(2, intent.generation());
            statement.setString(3, intent.attemptOwner());
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static boolean hasMapping(Connection connection, String path) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_file_mappings WHERE md5(index_file_path)=md5(?) AND index_file_path=? LIMIT 1")) {
            statement.setString(1, path);
            statement.setString(2, path);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static void bindIdentity(PreparedStatement statement, ScalarArtifactIntent intent) throws SQLException {
        statement.setString(1, Sha256Digests.sha256Hex(intent.artifactPath()));
        statement.setString(2, intent.indexId());
        statement.setInt(3, intent.generation());
        statement.setString(4, intent.attemptOwner());
        statement.setString(5, intent.dataFilePath());
        statement.setString(6, intent.artifactPath());
    }

    private record LockedIntent(ScalarArtifactIntent intent, String state) { }
}
