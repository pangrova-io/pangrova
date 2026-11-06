package org.pangrova.metadata.generation.jdbc;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.output.GenerationOutputScope;
import org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.indexing.api.descriptor.IndexType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.canonicalInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** PostgreSQL/H2 implementation of atomic generic-artifact index publication. */
public final class JdbcIndexGenerationPublicationStore
        implements GenerationPublicationStore {

    private final DataSource dataSource;
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;
    private final GenerationTransactionInputs inputs;
    private final GenerationPublicationOutputValidator outputValidator;

    /** Composes index publication with current authority, execution fencing and same-connection input reads. */
    public JdbcIndexGenerationPublicationStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence, GenerationTransactionInputs inputs) {
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.inputs = Objects.requireNonNull(inputs, "inputs");
        this.outputValidator = new GenerationPublicationOutputValidator(inputs);
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public GenerationManifest publishClaimed(
            GenerationJob claimedJob, String owner, long ownerEpoch, Instant now) {
        Objects.requireNonNull(claimedJob, "claimedJob must not be null");
        requireText(owner, "owner");
        Objects.requireNonNull(now, "now must not be null");
        if (ownerEpoch <= 0 || claimedJob.publicationFencingToken() <= 0) {
            throw new IllegalArgumentException("publication owner epoch and fence must be positive");
        }
        BuiltInGenerationOutputFormats.require(
                claimedJob.formatId(), claimedJob.artifactType());
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                transactionAuthority.requireCurrent(connection, claimedJob.tenantId(), claimedJob.jobId());
                executionFence.requirePublication(connection, claimedJob.tenantId(), claimedJob.jobId());
                JobRow job = lockClaimedJob(connection, claimedJob, owner, ownerEpoch, now);
                List<GenerationOutput> outputs =
                        outputValidator.loadAndValidate(connection, job);
                Aggregate aggregate = aggregate(job, outputs);
                DescriptorFence descriptor = lockDescriptor(connection, job.artifactId());
                PublicationMode mode = validateDescriptorFence(connection, descriptor, job);
                long publicationEpoch = nextPublicationEpoch(connection, job.artifactId());
                GenerationManifest manifest = manifest(job, aggregate, now);

                retireActiveGeneration(connection, job.artifactId());
                insertIndexGeneration(connection, manifest, publicationEpoch);
                activateDescriptor(connection, descriptor, job, mode, now);
                if (mode == PublicationMode.ONLINE_REPLACEMENT) {
                    completeBuildingGeneration(connection, job, now);
                }
                publishArtifactJob(connection, job, aggregate, owner, ownerEpoch, now);
                // Recheck after every potentially blocking publication write, before commit.
                // Expiry here rolls back the generation, descriptor pointer and job together.
                executionFence.requirePublication(connection, claimedJob.tenantId(), claimedJob.jobId());
                connection.commit();
                return manifest;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Atomic index artifact publication failed", failure);
        }
    }

    private JobRow lockClaimedJob(
            Connection connection, GenerationJob claimed,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM artifact_build_jobs "
                        + "WHERE tenant_id=? AND job_id=? FOR UPDATE")) {
            statement.setString(1, claimed.tenantId());
            statement.setString(2, claimed.jobId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Artifact job disappeared before publication");
                }
                requireLiveLease(result, claimed, owner, ownerEpoch, now);
                JobRow row = mapJobRow(connection, result);
                requireStableIdentity(row, claimed);
                return row;
            }
        }
    }

    private static void requireLiveLease(
            ResultSet result, GenerationJob claimed,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        Instant expiresAt = getInstant(result, "publication_lease_expires_at");
        if (!GenerationJobState.READY_TO_PUBLISH.name().equals(result.getString("state"))
                || !owner.equals(result.getString("publication_owner"))
                || ownerEpoch != result.getLong("publication_owner_epoch")
                || claimed.publicationFencingToken()
                != result.getLong("publication_fencing_token")
                || expiresAt == null || expiresAt.isBefore(now)) {
            throw new IllegalStateException("Artifact publication lease is stale or mismatched");
        }
    }

    private JobRow mapJobRow(Connection connection, ResultSet result) throws SQLException {
        String jobId = result.getString("job_id");
        return new JobRow(
                result.getString("tenant_id"), jobId, result.getString("artifact_id"),
                result.getString("artifact_type"), result.getString("staging_generation"),
                result.getString("source_id"), result.getString("source_snapshot"),
                org.pangrova.generation.api.execution.GenerationExecutionKind.valueOf(
                        result.getString("execution_kind")),
                new org.pangrova.generation.api.dependency.GenerationDependencySet(
                        inputs.loadJobDependencies(
                                connection, result.getString("tenant_id"), jobId),
                        result.getString("dependency_set_sha256")),
                org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy.valueOf(
                        result.getString("empty_partition_policy")),
                result.getString("format_id"),
                result.getInt("format_major"), result.getInt("format_minor"),
                result.getString("schema_sha256"), result.getString("writer_build_id"),
                result.getString("encryption_key_ref"), result.getInt("expected_pages"),
                result.getInt("completed_pages"),
                result.getLong("publication_fencing_token"),
                getInstant(result, "publication_expected_updated_at"));
    }

    private static void requireStableIdentity(JobRow row, GenerationJob claimed) {
        if (!sameCoreIdentity(row, claimed) || !sameFormatIdentity(row, claimed)) {
            throw new IllegalStateException("Claimed artifact identity changed before publication");
        }
    }

    private static boolean sameCoreIdentity(JobRow row, GenerationJob claimed) {
        return row.tenantId().equals(claimed.tenantId())
                && row.artifactId().equals(claimed.artifactId())
                && row.artifactType().equals(claimed.artifactType())
                && row.generation().equals(claimed.stagingGeneration())
                && row.sourceId().equals(claimed.sourceId())
                && row.sourceSnapshot().equals(claimed.sourceSnapshot())
                && row.executionKind() == claimed.executionKind()
                && row.dependencySet().equals(claimed.dependencySet())
                && row.emptyPartitionPolicy() == claimed.emptyPartitionPolicy();
    }

    private static boolean sameFormatIdentity(JobRow row, GenerationJob claimed) {
        return row.formatId().equals(claimed.formatId())
                && row.formatMajor() == claimed.formatMajor()
                && row.formatMinor() == claimed.formatMinor()
                && row.schemaSha256().equalsIgnoreCase(claimed.schemaSha256())
                && row.writerBuildId().equals(claimed.writerBuildId())
                && row.publicationExpectedUpdatedAt()
                .equals(claimed.publicationExpectedUpdatedAt());
    }

    private static Aggregate aggregate(JobRow job, List<GenerationOutput> outputs) {
        long bytes = outputs.stream().mapToLong(GenerationOutput::sizeBytes)
                .reduce(0L, Math::addExact);
        Map<String, Long> sourceRows = new HashMap<>();
        for (GenerationOutput output : outputs) {
            if (output.scope() != GenerationOutputScope.SOURCE_OBJECT) {
                continue;
            }
            Long previous = sourceRows.putIfAbsent(output.sourceObjectId(), output.rowCount());
            if (previous != null && previous.longValue() != output.rowCount()) {
                throw new IllegalStateException("Artifact objects disagree on source row count");
            }
        }
        long rows = sourceRows.values().stream().mapToLong(Long::longValue)
                .reduce(0L, Math::addExact);
        String sha = GenerationPublicationManifestDigest.compute(job, outputs);
        return new Aggregate(outputs.size(), rows, bytes, sha);
    }

    private static GenerationManifest manifest(
            JobRow job, Aggregate aggregate, Instant now) {
        return new GenerationManifest(
                job.tenantId(), job.jobId(), job.artifactId(),
                job.artifactType(), job.generation(),
                job.sourceId(), job.sourceSnapshot(), job.executionKind(),
                job.dependencySet(), job.emptyPartitionPolicy(), job.formatId(),
                job.formatMajor(), job.formatMinor(),
                job.schemaSha256(), job.writerBuildId(),
                "metadata://artifact-generation/" + aggregate.manifestSha256(),
                aggregate.manifestSha256(), aggregate.objectCount(),
                aggregate.rowCount(), aggregate.totalBytes(), canonicalInstant(now));
    }

    private static DescriptorFence lockDescriptor(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT index_type,state,generation,updated_at FROM index_descriptors "
                        + "WHERE index_id=? FOR UPDATE")) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Index disappeared before artifact publication");
                }
                return new DescriptorFence(
                        IndexType.valueOf(result.getString(1)),
                        IndexState.valueOf(result.getString(2)), result.getInt(3),
                        getInstant(result, 4));
            }
        }
    }

    private static PublicationMode validateDescriptorFence(
            Connection connection, DescriptorFence descriptor,
            JobRow job) throws SQLException {
        if (descriptor.type() != requiredIndexType(job.artifactType())) {
            throw new IllegalStateException(
                    "Index type differs from the artifact generation type");
        }
        int generation = parseGeneration(job.generation());
        if (isInitialBuild(descriptor, job, generation)) {
            return PublicationMode.INITIAL_BUILD;
        }
        if (isOnlineReplacement(connection, descriptor, job, generation)) {
            return PublicationMode.ONLINE_REPLACEMENT;
        }
        throw new IllegalStateException("Index generation CAS fence differs from artifact job");
    }

    private static boolean isInitialBuild(
            DescriptorFence descriptor, JobRow job, int generation) {
        return descriptor.state() == IndexState.BUILDING
                && descriptor.generation() == generation
                && descriptor.updatedAt().equals(job.publicationExpectedUpdatedAt());
    }

    private static boolean isOnlineReplacement(
            Connection connection, DescriptorFence descriptor, JobRow job, int generation)
            throws SQLException {
        return (descriptor.state() == IndexState.READY || descriptor.state() == IndexState.STALE)
                && descriptor.generation() == generation - 1
                && hasBuildingGeneration(connection, job, descriptor.generation());
    }

    private static IndexType requiredIndexType(String artifactType) {
        return switch (artifactType) {
            case GenerationOutputTypeIds.VECTOR_INDEX -> IndexType.VECTOR;
            case GenerationOutputTypeIds.TEXT_INDEX -> IndexType.INVERTED;
            default -> throw new IllegalStateException(
                    "Artifact type cannot be published as an index");
        };
    }

    private static boolean hasBuildingGeneration(
            Connection connection, JobRow job, int baseGeneration) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM index_generation_builds WHERE index_id=? "
                        + "AND generation=? AND base_generation=? AND source_snapshot=? "
                        + "AND state='BUILDING' AND updated_at=? FOR UPDATE")) {
            statement.setString(1, job.artifactId());
            statement.setInt(2, parseGeneration(job.generation()));
            statement.setInt(3, baseGeneration);
            statement.setString(4, job.sourceSnapshot());
            setInstant(statement, 5, job.publicationExpectedUpdatedAt());
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static long nextPublicationEpoch(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COALESCE(MAX(publication_epoch),0) "
                        + "FROM index_generation_manifests WHERE index_id=?")) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return Math.addExact(result.getLong(1), 1L);
            }
        }
    }

    private static void retireActiveGeneration(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_generation_manifests SET state='RETIRED' "
                        + "WHERE index_id=? AND state='ACTIVE'")) {
            statement.setString(1, indexId);
            statement.executeUpdate();
        }
    }

    private static void insertIndexGeneration(
            Connection connection, GenerationManifest manifest,
            long publicationEpoch) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO index_generation_manifests "
                        + "(index_id,generation,source_id,source_snapshot,artifact_type,format_id,"
                        + "format_major,format_minor,schema_sha256,writer_build_id,object_count,"
                        + "row_count,total_bytes,manifest_sha256,publication_epoch,state,published_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            int parameter = 1;
            statement.setString(parameter++, manifest.artifactId());
            statement.setInt(parameter++, parseGeneration(manifest.generation()));
            statement.setString(parameter++, manifest.sourceId());
            statement.setString(parameter++, manifest.sourceSnapshot());
            statement.setString(parameter++, manifest.artifactType());
            statement.setString(parameter++, manifest.formatId());
            statement.setInt(parameter++, manifest.formatMajor());
            statement.setInt(parameter++, manifest.formatMinor());
            statement.setString(parameter++, manifest.schemaSha256().toLowerCase(java.util.Locale.ROOT));
            statement.setString(parameter++, manifest.writerBuildId());
            statement.setLong(parameter++, manifest.objectCount());
            statement.setLong(parameter++, manifest.rowCount());
            statement.setLong(parameter++, manifest.totalBytes());
            statement.setString(parameter++, manifest.manifestSha256());
            statement.setLong(parameter++, publicationEpoch);
            statement.setString(parameter++, "ACTIVE");
            setInstant(statement, parameter, manifest.publishedAt());
            statement.executeUpdate();
        }
    }

    private static void activateDescriptor(
            Connection connection, DescriptorFence descriptor, JobRow job,
            PublicationMode mode, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_descriptors SET state='READY', generation=?, "
                        + "build_snapshot_id=?, stale_reason=NULL, last_indexed_snapshot_id=?, "
                        + "updated_at=? WHERE index_id=? AND state=? AND generation=? AND updated_at=?")) {
            statement.setInt(1, parseGeneration(job.generation()));
            statement.setString(2, job.sourceSnapshot());
            statement.setString(3, job.sourceSnapshot());
            setInstant(statement, 4, now);
            statement.setString(5, job.artifactId());
            statement.setString(6, descriptor.state().name());
            statement.setInt(7, descriptor.generation());
            setInstant(statement, 8, descriptor.updatedAt());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException(
                        "Index activation fence was lost for " + mode.name().toLowerCase());
            }
        }
    }

    private static void completeBuildingGeneration(
            Connection connection, JobRow job, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_generation_builds SET state='PUBLISHED', updated_at=?, "
                        + "failure_reason=NULL WHERE index_id=? AND generation=? "
                        + "AND state='BUILDING' AND updated_at=?")) {
            setInstant(statement, 1, now);
            statement.setString(2, job.artifactId());
            statement.setInt(3, parseGeneration(job.generation()));
            setInstant(statement, 4, job.publicationExpectedUpdatedAt());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Index staging-generation fence was lost");
            }
        }
    }

    private static void publishArtifactJob(
            Connection connection, JobRow job, Aggregate aggregate,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='PUBLISHED', manifest_sha256=?, "
                        + "published_object_count=?, published_row_count=?, published_total_bytes=?, "
                        + "published_at=?, publication_lease_expires_at=NULL, failure_reason=NULL, "
                        + "updated_at=? WHERE tenant_id=? AND job_id=? "
                        + "AND state='READY_TO_PUBLISH' "
                        + "AND publication_owner=? AND publication_owner_epoch=? "
                        + "AND publication_fencing_token=? AND publication_lease_expires_at>=?")) {
            int parameter = 1;
            statement.setString(parameter++, aggregate.manifestSha256());
            statement.setLong(parameter++, aggregate.objectCount());
            statement.setLong(parameter++, aggregate.rowCount());
            statement.setLong(parameter++, aggregate.totalBytes());
            setInstant(statement, parameter++, now);
            setInstant(statement, parameter++, now);
            statement.setString(parameter++, job.tenantId());
            statement.setString(parameter++, job.jobId());
            statement.setString(parameter++, owner);
            statement.setLong(parameter++, ownerEpoch);
            statement.setLong(parameter++, job.publicationFence());
            setInstant(statement, parameter, now);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Artifact publication fence was lost");
            }
        }
    }

    private static int parseGeneration(String generation) {
        try {
            int result = Integer.parseInt(generation);
            if (result < 0) {
                throw new NumberFormatException("negative");
            }
            return result;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Index artifact generation must be a non-negative integer", failure);
        }
    }

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private enum PublicationMode { INITIAL_BUILD, ONLINE_REPLACEMENT }

    private record DescriptorFence(
            IndexType type, IndexState state, int generation, Instant updatedAt) {}

    private record Aggregate(
            long objectCount, long rowCount, long totalBytes, String manifestSha256) {}

    record JobRow(
            String tenantId,
            String jobId,
            String artifactId,
            String artifactType,
            String generation,
            String sourceId,
            String sourceSnapshot,
            org.pangrova.generation.api.execution.GenerationExecutionKind executionKind,
            org.pangrova.generation.api.dependency.GenerationDependencySet dependencySet,
            org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy emptyPartitionPolicy,
            String formatId,
            int formatMajor,
            int formatMinor,
            String schemaSha256,
            String writerBuildId,
            String encryptionKeyRef,
            int expectedPages,
            int completedPages,
            long publicationFence,
            Instant publicationExpectedUpdatedAt) implements GenerationPublicationJob {}
}
