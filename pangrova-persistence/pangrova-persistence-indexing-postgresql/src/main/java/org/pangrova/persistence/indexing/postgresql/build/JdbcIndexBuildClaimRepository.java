package org.pangrova.persistence.indexing.postgresql.build;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import javax.sql.DataSource;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;

/** PostgreSQL claim ownership; each operation owns its connection and borrows the pool. */
public final class JdbcIndexBuildClaimRepository implements IndexBuildClaimRepository {
    private static final int ID_MAX_LENGTH = 255;
    private final DataSource dataSource;

    /** Uses the server-owned data source and the canonical initialized schema. */
    public JdbcIndexBuildClaimRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public boolean tryClaim(String indexId, int generation, String ownerId, Instant claimedAt) {
        validate(indexId, generation, ownerId);
        Objects.requireNonNull(claimedAt, "claimedAt");
        String sql = "INSERT INTO index_build_claims "
                + "(index_id, generation, owner_id, claimed_at, heartbeat_at) VALUES (?,?,?,?,?) "
                + "ON CONFLICT (index_id, generation) DO NOTHING";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setString(3, ownerId);
            statement.setTimestamp(4, Timestamp.from(claimedAt));
            statement.setTimestamp(5, Timestamp.from(claimedAt));
            return statement.executeUpdate() == 1;
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to acquire index build claim", failure);
        }
    }

    @Override
    public boolean release(String indexId, int generation, String ownerId) {
        validate(indexId, generation, ownerId);
        String sql = "DELETE FROM index_build_claims WHERE index_id=? AND generation=? AND owner_id=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setString(3, ownerId);
            return statement.executeUpdate() == 1;
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to release index build claim", failure);
        }
    }

    @Override
    public boolean heartbeat(String indexId, int generation, String ownerId, Instant heartbeatAt) {
        validate(indexId, generation, ownerId);
        Objects.requireNonNull(heartbeatAt, "heartbeatAt");
        String sql = "UPDATE index_build_claims SET heartbeat_at=GREATEST(heartbeat_at,?) "
                + "WHERE index_id=? AND generation=? AND owner_id=?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(heartbeatAt));
            statement.setString(2, indexId);
            statement.setInt(3, generation);
            statement.setString(4, ownerId);
            return statement.executeUpdate() == 1;
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to renew index build claim", failure);
        }
    }

    private static void validate(String indexId, int generation, String ownerId) {
        if (indexId == null || indexId.isBlank() || indexId.length() > ID_MAX_LENGTH
                || ownerId == null || ownerId.isBlank() || ownerId.length() > ID_MAX_LENGTH || generation < 0) {
            throw new IllegalArgumentException("Valid index, generation and unique attempt owner are required");
        }
    }
}
