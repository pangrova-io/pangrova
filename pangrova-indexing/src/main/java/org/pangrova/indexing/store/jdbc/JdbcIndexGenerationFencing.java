package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.IndexStoreException;
import org.pangrova.spi.index.IndexState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;

import static org.pangrova.indexing.store.jdbc.JdbcIndexSql.validateGenerationTransition;
import static org.pangrova.indexing.store.jdbc.JdbcIndexSupport.getInstant;
import static org.pangrova.indexing.store.jdbc.JdbcIndexSupport.setInstant;
import static org.pangrova.indexing.store.jdbc.JdbcIndexSupport.truncateReason;

/**
 * Fenced staging-generation transactions of the JDBC index authority.
 *
 * <p>A build generation is reserved without replacing the active descriptor
 * generation; the compare-and-set fences concurrent coordinators by descriptor
 * timestamp, state, and active generation. Queries therefore keep using the
 * active manifest while the new generation builds. Failed generations are
 * restarted in place or fenced to FAILED without retiring the active one.</p>
 */
final class JdbcIndexGenerationFencing {

    private final DataSource dataSource;

    JdbcIndexGenerationFencing(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration) {
        validateGenerationTransition(newGeneration, expectedState, expectedGeneration);
        try (Connection conn = dataSource.getConnection()) {
            return reserveGeneration(conn, new GenerationReservation(
                    indexId, newGeneration, buildSnapshotId, timestamp,
                    expectedUpdatedAt, expectedState, expectedGeneration));
        } catch (SQLException e) {
            throw new IndexStoreException(
                    "Failed to begin fenced index generation", e);
        }
    }

    boolean failIndexGeneration(
            String indexId,
            int generation,
            Instant expectedBuildUpdatedAt,
            Instant failedAt,
            String failureReason) {
        String reason = truncateReason(failureReason);
        String sql = "UPDATE index_generation_builds SET state='FAILED', "
                + "failure_reason=?, updated_at=? WHERE index_id=? AND generation=? "
                + "AND state='BUILDING' AND updated_at=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, reason);
            setInstant(ps, 2, failedAt);
            ps.setString(3, indexId);
            ps.setInt(4, generation);
            setInstant(ps, 5, expectedBuildUpdatedAt);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IndexStoreException("Failed to fence failed index generation", e);
        }
    }

    boolean isIndexGenerationBuildable(String indexId, int generation) {
        if (generation < 0) {
            return false;
        }
        String sql = "SELECT 1 FROM index_descriptors d WHERE d.index_id=? AND ("
                + "(d.state='BUILDING' AND d.generation=?) OR "
                + "(d.state IN ('READY','STALE') AND d.generation=? AND EXISTS ("
                + "SELECT 1 FROM index_generation_builds b WHERE b.index_id=d.index_id "
                + "AND b.generation=? AND b.base_generation=d.generation "
                + "AND b.state='BUILDING')))";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, generation - 1);
            statement.setInt(4, generation);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException failure) {
            throw new IndexStoreException(
                    "Failed to validate buildable index generation", failure);
        }
    }

    private static boolean reserveGeneration(Connection conn, GenerationReservation request)
            throws SQLException {
        conn.setAutoCommit(false);
        try {
            if (!lockExpectedDescriptor(
                    conn, request.indexId(), request.expectedUpdatedAt(),
                    request.expectedState(), request.expectedGeneration())
                    || hasBuildingGeneration(conn, request.indexId())) {
                conn.rollback();
                return false;
            }
            if (!restartFailedGeneration(
                    conn, request.indexId(), request.newGeneration(),
                    request.expectedGeneration(), request.buildSnapshotId(),
                    request.timestamp())) {
                insertBuildingGeneration(
                        conn, request.indexId(), request.newGeneration(),
                        request.expectedGeneration(), request.buildSnapshotId(),
                        request.timestamp());
            }
            conn.commit();
            return true;
        } catch (SQLException | RuntimeException failure) {
            rollback(conn, failure);
            throw failure;
        }
    }

    private static boolean lockExpectedDescriptor(
            Connection connection,
            String indexId,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration) throws SQLException {
        String sql = "SELECT state,generation,updated_at FROM index_descriptors "
                + "WHERE index_id=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        && expectedState.name().equals(result.getString("state"))
                        && expectedGeneration == result.getInt("generation")
                        && expectedUpdatedAt.equals(getInstant(result, "updated_at"));
            }
        }
    }

    private static boolean hasBuildingGeneration(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM index_generation_builds "
                        + "WHERE index_id=? AND state='BUILDING'")) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static void insertBuildingGeneration(
            Connection connection,
            String indexId,
            int generation,
            int baseGeneration,
            String sourceSnapshot,
            Instant timestamp) throws SQLException {
        String sql = "INSERT INTO index_generation_builds "
                + "(index_id,generation,base_generation,source_snapshot,state,"
                + "failure_reason,started_at,updated_at) VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, baseGeneration);
            statement.setString(4, sourceSnapshot == null ? "" : sourceSnapshot);
            statement.setString(5, "BUILDING");
            statement.setString(6, null);
            setInstant(statement, 7, timestamp);
            setInstant(statement, 8, timestamp);
            statement.executeUpdate();
        }
    }

    private static boolean restartFailedGeneration(
            Connection connection,
            String indexId,
            int generation,
            int baseGeneration,
            String sourceSnapshot,
            Instant timestamp) throws SQLException {
        String update = "UPDATE index_generation_builds SET base_generation=?, "
                + "source_snapshot=?, state='BUILDING', failure_reason=NULL, "
                + "started_at=?, updated_at=? WHERE index_id=? AND generation=? "
                + "AND state='FAILED'";
        try (PreparedStatement statement = connection.prepareStatement(update)) {
            statement.setInt(1, baseGeneration);
            statement.setString(2, sourceSnapshot == null ? "" : sourceSnapshot);
            setInstant(statement, 3, timestamp);
            setInstant(statement, 4, timestamp);
            statement.setString(5, indexId);
            statement.setInt(6, generation);
            if (statement.executeUpdate() != 1) {
                return false;
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM index_file_mappings WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.executeUpdate();
        }
        return true;
    }

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }
}
