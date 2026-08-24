package org.pangrova.metadata.index.jdbc;

import org.pangrova.metadata.exception.MetastoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * JDBC operations for index build claim coordination.
 */
final class JdbcIndexStateStore extends JdbcStoreSupport {

    JdbcIndexStateStore(DataSource dataSource) {
        super(dataSource);
    }

    boolean claimBuild(String indexId, int generation, String ownerId, Instant now) {
        String sql = "INSERT INTO index_build_claims "
                + "(index_id, generation, owner_id, claimed_at, heartbeat_at) "
                + "VALUES (?,?,?,?,?)";
        try {
            executeUpdate(sql, ps -> {
                ps.setString(1, indexId);
                ps.setInt(2, generation);
                ps.setString(3, ownerId);
                ps.setTimestamp(4, Timestamp.from(now));
                ps.setTimestamp(5, Timestamp.from(now));
            });
            return true;
        } catch (MetastoreException e) {
            if (isUniqueConstraintViolation(e)) {
                return false;
            }
            throw e;
        }
    }

    void releaseBuildClaim(String indexId, int generation) {
        executeUpdate("DELETE FROM index_build_claims WHERE index_id=? AND generation=?",
                ps -> {
                    ps.setString(1, indexId);
                    ps.setInt(2, generation);
                });
    }

    void heartbeatBuildClaim(String indexId, int generation, Instant now) {
        executeUpdate("UPDATE index_build_claims SET heartbeat_at=? WHERE index_id=? AND generation=?",
                ps -> {
                    ps.setTimestamp(1, Timestamp.from(now));
                    ps.setString(2, indexId);
                    ps.setInt(3, generation);
                });
    }

    int expireStaleBuilds(Instant olderThan) {
        String sql = "DELETE FROM index_build_claims WHERE heartbeat_at < ?";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(olderThan));
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException("Failed to expire stale builds", e);
        }
    }
}
