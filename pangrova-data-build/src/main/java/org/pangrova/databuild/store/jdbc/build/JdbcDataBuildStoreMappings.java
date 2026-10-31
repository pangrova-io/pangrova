package org.pangrova.databuild.store.jdbc.build;

import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildLease;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.build.DataBuildPageCompletion;
import org.pangrova.databuild.api.DataBuildOutputScope;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildStorageException;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.common.jdbc.JdbcSupport.getInstant;
import static org.pangrova.databuild.store.jdbc.query.JdbcDataBuildQueries.loadArtifactDependencies;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
abstract class JdbcDataBuildStoreMappings {

    protected static final long ATTEMPTS_PER_EPOCH = 1L << 32;
    private static final int MAX_FAILURE_LENGTH = 2048;

    protected final DataSource dataSource;

    JdbcDataBuildStoreMappings(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    protected static Optional<DataBuildJob> findJob(
            Connection connection, String tenantId, String jobId, boolean lock)
            throws SQLException {
        String sql = "SELECT * FROM artifact_build_jobs WHERE tenant_id=? AND job_id=?"
                + (lock ? " FOR UPDATE" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapJob(connection, result)) : Optional.empty();
            }
        }
    }

    protected static DataBuildJob mapJob(Connection connection, ResultSet result)
            throws SQLException {
        String jobId = result.getString("job_id");
        return new DataBuildJob(
                result.getString("tenant_id"), jobId, result.getString("artifact_id"),
                result.getString("artifact_type"), result.getString("staging_generation"),
                result.getString("source_id"), result.getString("source_snapshot"),
                DataBuildExecutionKind.valueOf(result.getString("execution_kind")),
                new DataBuildDependencySet(
                        loadArtifactDependencies(
                                connection, result.getString("tenant_id"), jobId),
                        result.getString("dependency_set_sha256")),
                DataBuildEmptyPartitionPolicy.valueOf(
                        result.getString("empty_partition_policy")),
                result.getString("staging_uri"),
                result.getString("format_id"), result.getInt("format_major"),
                result.getInt("format_minor"), result.getString("schema_sha256"),
                result.getString("writer_build_id"), result.getString("encryption_key_ref"),
                result.getString("idempotency_key"), result.getInt("expected_pages"),
                result.getInt("completed_pages"),
                DataBuildJobState.valueOf(result.getString("state")),
                result.getLong("publication_fencing_token"),
                getInstant(result, "publication_expected_updated_at"),
                getInstant(result, "created_at"),
                getInstant(result, "updated_at"));
    }

    protected static PublicationLease loadPublicationLease(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT publication_owner,publication_owner_epoch,"
                        + "publication_lease_expires_at FROM artifact_build_jobs "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("artifact job disappeared during publication claim");
                }
                Instant expiresAt = getInstant(result, 3);
                return new PublicationLease(
                        result.getString(1), result.getLong(2),
                        expiresAt);
            }
        }
    }

    protected static DataBuildOutput mapOutput(ResultSet result) throws SQLException {
        return new DataBuildOutput(
                result.getString("object_id"), result.getString("opaque_uri"),
                result.getString("object_role"), result.getString("source_object_id"),
                result.getInt("object_ordinal"),
                DataBuildOutputScope.valueOf(result.getString("object_scope")),
                result.getLong("size_bytes"),
                result.getLong("row_count"), result.getString("checksum_sha256"),
                result.getString("encryption_key_ref"));
    }

    protected static long fencingToken(long ownerEpoch, int attemptId) {
        return Math.addExact(
                Math.multiplyExact(ownerEpoch, ATTEMPTS_PER_EPOCH),
                Integer.toUnsignedLong(attemptId) + 1L);
    }

    protected static boolean terminal(DataBuildJobState state) {
        return state == DataBuildJobState.READY_TO_PUBLISH
                || state == DataBuildJobState.PUBLISHED
                || state == DataBuildJobState.FAILED
                || state == DataBuildJobState.CANCELLED;
    }

    protected static void requireCompletion(DataBuildPageCompletion completion) {
        requireText(completion.tenantId(), "tenantId");
        requireText(completion.jobId(), "jobId");
        requireText(completion.pageId(), "pageId");
        byte[] reportBytes = completion.reportBytes();
        List<DataBuildOutput> outputs = completion.outputs();
        String reportSha256 = completion.reportSha256();
        Objects.requireNonNull(reportBytes, "reportBytes must not be null");
        Objects.requireNonNull(outputs, "outputs must not be null");
        Objects.requireNonNull(completion.completedAt(), "completedAt must not be null");
        if (completion.fencingToken() <= 0 || completion.ownerEpoch() <= 0
                || reportBytes.length == 0
                || reportSha256 == null || !reportSha256.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("invalid artifact completion report");
        }
        String actual = HexFormat.of().formatHex(sha256(reportBytes));
        if (!actual.equalsIgnoreCase(reportSha256)) {
            throw new IllegalArgumentException("artifact report storage checksum mismatch");
        }
    }

    protected static void requireLeaseRequest(
            String tenantId, String jobId, String owner, long ownerEpoch,
            Duration duration, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        requireText(owner, "owner");
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        requirePositiveDuration(duration);
        Objects.requireNonNull(now, "now must not be null");
    }

    protected static void requirePositiveDuration(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
    }

    protected static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    protected static String normalizeFailure(String value) {
        if (value == null || value.isBlank()) {
            return "unspecified artifact build failure";
        }
        return value.length() <= MAX_FAILURE_LENGTH
                ? value : value.substring(0, MAX_FAILURE_LENGTH);
    }

    protected static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    protected static void bindLeaseIdentity(
            PreparedStatement statement, int offset,
            DataBuildLease lease) throws SQLException {
        statement.setString(offset, lease.tenantId());
        statement.setString(offset + 1, lease.jobId());
        statement.setString(offset + 2, lease.pageId());
        statement.setLong(offset + 3, lease.fencingToken());
        statement.setString(offset + 4, lease.leaseOwner());
        statement.setLong(offset + 5, lease.ownerEpoch());
    }

    protected <T> T inTransaction(String operation, SqlWork<T> work) {
        // This method owns and closes the connection. Re-enabling auto-commit can commit work after a failed rollback.
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.execute(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                throw e;
            }
        } catch (SQLException e) {
            throw failure(operation, e);
        }
    }

    protected static DataBuildStorageException failure(String operation, SQLException cause) {
        return new DataBuildStorageException("Failed to " + operation, cause);
    }

    @FunctionalInterface
    protected interface SqlWork<T> {
        T execute(Connection connection) throws SQLException;
    }

    protected record PageRow(
            String tenantId, String jobId, String pageId, int stageId, int partitionId,
            int pageOrdinal, DataBuildPageKind kind, String state,
            int attemptId, long fencingToken,
            String leaseOwner, long ownerEpoch, Instant leaseExpiresAt,
            int maxAttempts, byte[] reportBytes, String reportSha256,
            org.pangrova.databuild.api.execution.DataBuildExecutionOwner executionOwner) {
        /** Owns the nullable persisted report independently of the JDBC buffer. */
        protected PageRow {
            reportBytes = reportBytes == null ? null : reportBytes.clone();
        }

        /** Exports a report snapshot without permitting mutation of the page fact. */
        @Override
        public byte[] reportBytes() {
            return reportBytes == null ? null : reportBytes.clone();
        }
    }

    protected record PublicationLease(String owner, long ownerEpoch, Instant expiresAt) {
    }
}
