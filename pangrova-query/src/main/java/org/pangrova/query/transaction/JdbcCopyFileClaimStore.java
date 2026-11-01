package org.pangrova.query.transaction;

import org.pangrova.common.annotation.DeadCode;
import org.pangrova.metadata.api.error.MetastoreException;
import org.pangrova.spi.coordination.CopyFileClaim;
import org.pangrova.spi.coordination.CopyFileClaimStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/** PostgreSQL authority for immutable COPY source-file admissions; awaiting URI execution wiring. */
@DeadCode(issueReference = "#19 COPY INTO URI execution and publication")
public final class JdbcCopyFileClaimStore implements CopyFileClaimStore {

    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");
    private static final String COLUMNS = "claim_key, load_generation, claim_digest, "
            + "tenant_id, table_uuid, source_url, "
            + "source_version_id, source_sha256, source_size, profile_name, "
            + "profile_incarnation_id, profile_revision, execution_id";
    private static final String INSERT = "INSERT INTO copy_file_claims (" + COLUMNS + ") "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            + "ON CONFLICT (claim_key, load_generation) DO NOTHING";

    private final DataSource dataSource;

    public JdbcCopyFileClaimStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public ClaimResult claim(CopyFileClaim claim) {
        Objects.requireNonNull(claim, "claim must not be null");
        if (claim.loadGeneration() != 0) {
            throw new IllegalArgumentException(
                    "Reload generation requires an atomic catalog-head fence and is not yet admitted");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
                bind(statement, claim);
                boolean inserted = statement.executeUpdate() == 1;
                connection.commit();
                if (inserted) {
                    return ClaimResult.ACQUIRED;
                }
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to durably claim COPY source file", failure);
        }
        // A fresh transaction sees the winner even if the INSERT's snapshot did not.
        CopyFileClaim existing = get(claim.claimKey(), claim.loadGeneration()).orElseThrow(() ->
                new IllegalStateException("Conflicting COPY file claim disappeared during admission"));
        if (!sameFileIdentity(existing, claim)) {
            throw new IllegalStateException("COPY file claim digest collision or identity corruption");
        }
        if (!existing.claimDigest().equals(claim.claimDigest())) {
            throw new IllegalStateException("COPY source version has conflicting byte or profile evidence");
        }
        return existing.executionId().equals(claim.executionId())
                ? ClaimResult.SAME_EXECUTION : ClaimResult.BLOCKED;
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    @Override
    public Optional<CopyFileClaim> get(String claimKey, long loadGeneration) {
        if (claimKey == null || !SHA_256.matcher(claimKey).matches()) {
            throw new IllegalArgumentException("claimKey must be lowercase SHA-256 hex");
        }
        if (loadGeneration < 0) {
            throw new IllegalArgumentException("loadGeneration must not be negative");
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT " + COLUMNS + " FROM copy_file_claims "
                                + "WHERE claim_key=? AND load_generation=?")) {
            statement.setString(1, claimKey);
            statement.setLong(2, loadGeneration);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                CopyFileClaim claim = read(rows);
                if (!claimKey.equals(claim.claimKey()) || loadGeneration != claim.loadGeneration()
                        || !rows.getString("claim_digest").equals(claim.claimDigest())) {
                    throw new IllegalStateException("Stored COPY file claim identity differs from its digest");
                }
                return Optional.of(claim);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to read durable COPY file claim", failure);
        }
    }

    private static void bind(PreparedStatement statement, CopyFileClaim claim) throws SQLException {
        int column = 1;
        statement.setString(column++, claim.claimKey());
        statement.setLong(column++, claim.loadGeneration());
        statement.setString(column++, claim.claimDigest());
        statement.setString(column++, claim.tenantId());
        statement.setObject(column++, UUID.fromString(claim.tableUuid()));
        statement.setString(column++, claim.sourceUrl());
        statement.setString(column++, claim.sourceVersionId());
        statement.setString(column++, claim.sourceSha256());
        statement.setLong(column++, claim.sourceSize());
        statement.setString(column++, claim.profileName());
        statement.setObject(column++, UUID.fromString(claim.profileIncarnationId()));
        statement.setLong(column++, claim.profileRevision());
        statement.setString(column, claim.executionId());
    }

    private static CopyFileClaim read(ResultSet rows) throws SQLException {
        return new CopyFileClaim(rows.getString("tenant_id"),
                rows.getObject("table_uuid", UUID.class).toString(),
                rows.getString("source_url"), rows.getString("source_version_id"),
                rows.getString("source_sha256"), rows.getLong("source_size"),
                rows.getString("profile_name"),
                rows.getObject("profile_incarnation_id", UUID.class).toString(),
                rows.getLong("profile_revision"), rows.getLong("load_generation"),
                rows.getString("execution_id"));
    }

    private static boolean sameFileIdentity(CopyFileClaim left, CopyFileClaim right) {
        return left.tenantId().equals(right.tenantId())
                && left.tableUuid().equals(right.tableUuid())
                && left.sourceUrl().equals(right.sourceUrl())
                && left.sourceVersionId().equals(right.sourceVersionId());
    }
}
