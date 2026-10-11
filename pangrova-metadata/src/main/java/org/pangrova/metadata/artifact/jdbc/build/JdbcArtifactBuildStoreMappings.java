package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.build.ArtifactPageCompletion;
import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.contract.ArtifactDependency;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.exception.MetastoreException;

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

import static org.pangrova.metadata.store.JdbcSupport.getInstant;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
@org.pangrova.spi.annotation.Internal
public abstract class JdbcArtifactBuildStoreMappings {

    protected static final long ATTEMPTS_PER_EPOCH = 1L << 32;
    private static final int MAX_FAILURE_LENGTH = 2048;

    protected final DataSource dataSource;

    JdbcArtifactBuildStoreMappings(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    protected static Optional<ArtifactBuildJob> findJob(
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

    /** Reads the immutable scoped dispatch contract without synthesizing missing state. */
    public static Optional<ArtifactBuildDispatchSpec> findDispatchSpec(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM artifact_build_dispatch_specs "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new ArtifactBuildDispatchSpec(
                        jobId, org.pangrova.metadata.artifact.build.ArtifactStageGraph.decode(
                                result.getBytes("stage_graph_bytes"), result.getString("stage_graph_sha256")),
                        result.getString("tenant_id"), result.getString("principal"),
                        org.pangrova.security.authz.DeferredAuthorizationScopeCodec.decode(
                                result.getBytes("authorization_scope_bytes")),
                        result.getLong("max_memory_bytes"), result.getInt("max_threads"),
                        result.getLong("max_spill_bytes"),
                        result.getLong("task_timeout_millis")));
            }
        }
    }

    protected static ArtifactBuildJob mapJob(Connection connection, ResultSet result)
            throws SQLException {
        String jobId = result.getString("job_id");
        return new ArtifactBuildJob(
                result.getString("tenant_id"), jobId, result.getString("artifact_id"),
                result.getString("artifact_type"), result.getString("staging_generation"),
                result.getString("source_id"), result.getString("source_snapshot"),
                ArtifactExecutionKind.valueOf(result.getString("execution_kind")),
                new ArtifactDependencySet(
                        loadArtifactDependencies(
                                connection, result.getString("tenant_id"), jobId),
                        result.getString("dependency_set_sha256")),
                ArtifactEmptyPartitionPolicy.valueOf(
                        result.getString("empty_partition_policy")),
                result.getString("staging_uri"),
                result.getString("format_id"), result.getInt("format_major"),
                result.getInt("format_minor"), result.getString("schema_sha256"),
                result.getString("writer_build_id"), result.getString("encryption_key_ref"),
                result.getString("idempotency_key"), result.getInt("expected_pages"),
                result.getInt("completed_pages"),
                ArtifactBuildJobState.valueOf(result.getString("state")),
                result.getLong("publication_fencing_token"),
                getInstant(result, "publication_expected_updated_at"),
                getInstant(result, "created_at"),
                getInstant(result, "updated_at"));
    }

    /** Loads the exact version-pinned input dependencies in their canonical ordinal order. */
    public static List<ArtifactDependency> loadArtifactDependencies(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT dependency_kind,dependency_id,dependency_version "
                        + "FROM artifact_build_dependencies "
                        + "WHERE tenant_id=? AND job_id=? "
                        + "ORDER BY dependency_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                java.util.ArrayList<ArtifactDependency> dependencies =
                        new java.util.ArrayList<>();
                while (result.next()) {
                    dependencies.add(new ArtifactDependency(
                            result.getString(1), result.getString(2), result.getString(3)));
                }
                return List.copyOf(dependencies);
            }
        }
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

    protected static ArtifactBuildOutput mapOutput(ResultSet result) throws SQLException {
        return new ArtifactBuildOutput(
                result.getString("object_id"), result.getString("opaque_uri"),
                result.getString("object_role"), result.getString("source_object_id"),
                result.getInt("object_ordinal"),
                ArtifactBuildOutputScope.valueOf(result.getString("object_scope")),
                result.getLong("size_bytes"),
                result.getLong("row_count"), result.getString("checksum_sha256"),
                result.getString("encryption_key_ref"));
    }

    protected static long fencingToken(long ownerEpoch, int attemptId) {
        return Math.addExact(
                Math.multiplyExact(ownerEpoch, ATTEMPTS_PER_EPOCH),
                Integer.toUnsignedLong(attemptId) + 1L);
    }

    protected static boolean terminal(ArtifactBuildJobState state) {
        return state == ArtifactBuildJobState.READY_TO_PUBLISH
                || state == ArtifactBuildJobState.PUBLISHED
                || state == ArtifactBuildJobState.FAILED
                || state == ArtifactBuildJobState.CANCELLED;
    }

    protected static void requireCompletion(ArtifactPageCompletion completion) {
        requireText(completion.tenantId(), "tenantId");
        requireText(completion.jobId(), "jobId");
        requireText(completion.pageId(), "pageId");
        byte[] reportBytes = completion.reportBytes();
        List<ArtifactBuildOutput> outputs = completion.outputs();
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

    protected boolean updateLease(
            String sql, SqlBinder binder, String operation) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw failure(operation, e);
        }
    }

    protected static void bindLeaseIdentity(
            PreparedStatement statement, int offset,
            ArtifactBuildLease lease) throws SQLException {
        statement.setString(offset, lease.tenantId());
        statement.setString(offset + 1, lease.jobId());
        statement.setString(offset + 2, lease.pageId());
        statement.setLong(offset + 3, lease.fencingToken());
        statement.setString(offset + 4, lease.leaseOwner());
        statement.setLong(offset + 5, lease.ownerEpoch());
    }

    protected <T> T inTransaction(String operation, SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = work.execute(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            throw failure(operation, e);
        }
    }

    protected static MetastoreException failure(String operation, SQLException cause) {
        return new MetastoreException("Failed to " + operation, cause);
    }

    @FunctionalInterface
    protected interface SqlWork<T> {
        T execute(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    protected interface SqlBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    protected record PageRow(
            String tenantId, String jobId, String pageId, int stageId, int partitionId,
            int pageOrdinal, ArtifactBuildPageKind kind, String state,
            int attemptId, long fencingToken,
            String leaseOwner, long ownerEpoch, Instant leaseExpiresAt,
            int maxAttempts, byte[] reportBytes, String reportSha256,
            org.pangrova.metadata.artifact.build.ArtifactExecutionOwner executionOwner) {
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
