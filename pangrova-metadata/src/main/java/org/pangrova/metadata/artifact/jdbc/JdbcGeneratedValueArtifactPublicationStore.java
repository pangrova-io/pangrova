package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreMappings;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.metadata.artifact.contract.ArtifactFormatContracts;
import org.pangrova.metadata.artifact.ArtifactGenerationManifest;
import org.pangrova.metadata.artifact.ArtifactPublicationStore;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.GeneratedValueArtifactDescriptor;
import org.pangrova.metadata.artifact.GeneratedValueArtifactMetadataStore;
import org.pangrova.metadata.artifact.GeneratedValueArtifactReference;
import org.pangrova.metadata.artifact.GeneratedValueBuildSubmission;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.metadata.exception.MetastoreException;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.metadata.store.JdbcSupport.canonicalInstant;
import static org.pangrova.metadata.store.JdbcSupport.getInstant;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/**
 * Atomic publication and snapshot lookup for generated-column value sidecars.
 *
 * <p>This adapter deliberately does not use index descriptors. It validates
 * the generic artifact work-page contract, inserts an immutable generation,
 * updates the generated-column active pointer, and publishes the generic job
 * under one database transaction and fencing token.</p>
 */
public final class JdbcGeneratedValueArtifactPublicationStore
        implements ArtifactPublicationStore, GeneratedValueArtifactMetadataStore {

    private final DataSource dataSource;
    private final JdbcGeneratedValueDescriptorStore descriptorStore;

    public JdbcGeneratedValueArtifactPublicationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.descriptorStore = new JdbcGeneratedValueDescriptorStore(dataSource);
    }

    @Override
    public void register(GeneratedValueArtifactDescriptor descriptor) {
        descriptorStore.register(descriptor);
    }

    @Override
    public void submitCommittedBuilds(WriteCommitEntry write, List<GeneratedValueBuildSubmission> builds) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                JdbcGeneratedWriteOutbox.submit(connection, write, List.copyOf(builds));
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Persist generated write outbox failed", failure);
        }
    }

    @Override
    public Optional<GeneratedValueArtifactReference> findGeneration(
            String tenantId, String sourceId, String targetColumn, String sourceSnapshot) {
        requireText(tenantId, "tenantId");
        requireText(sourceId, "sourceId");
        requireText(targetColumn, "targetColumn");
        requireText(sourceSnapshot, "sourceSnapshot");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT tenant_id,artifact_id,generation,source_id,source_snapshot,target_column "
                             + "FROM generated_value_generation_manifests "
                             + "WHERE tenant_id=? AND source_id=? "
                             + "AND LOWER(target_column)=LOWER(?) "
                             + "AND source_snapshot=? ORDER BY publication_epoch DESC")) {
            statement.setString(1, tenantId);
            statement.setString(2, sourceId);
            statement.setString(3, targetColumn);
            statement.setString(4, sourceSnapshot);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new GeneratedValueArtifactReference(
                        result.getString(1), result.getString(2), result.getString(3),
                        result.getString(4), result.getString(5), result.getString(6)));
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Find generated-value artifact failed", failure);
        }
    }

    @Override
    public ArtifactGenerationManifest publishClaimed(
            ArtifactBuildJob claimedJob, String owner, long ownerEpoch, Instant now) {
        Objects.requireNonNull(claimedJob, "claimedJob must not be null");
        requireText(owner, "owner");
        Objects.requireNonNull(now, "now must not be null");
        if (!ArtifactTypeIds.GENERATED_COLUMN_VALUES.equals(claimedJob.artifactType())) {
            throw new IllegalArgumentException(
                    "generated-value publisher cannot publish " + claimedJob.artifactType());
        }
        if (ownerEpoch <= 0 || claimedJob.publicationFencingToken() <= 0) {
            throw new IllegalArgumentException("publication owner epoch and fence must be positive");
        }
        ArtifactFormatContracts.require(claimedJob.formatId(), claimedJob.artifactType());
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                ArtifactCurrentAuthority.requireCurrent(connection, claimedJob.tenantId(), claimedJob.jobId());
                ArtifactExecutionFence.requirePublication(connection, claimedJob.tenantId(), claimedJob.jobId());
                ArtifactGenerationManifest manifest = publishLocked(
                        connection, claimedJob, owner, ownerEpoch, now);
                connection.commit();
                return manifest;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Atomic generated-value publication failed", failure);
        }
    }

    private static ArtifactGenerationManifest publishLocked(
            Connection connection, ArtifactBuildJob claimedJob,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        JobRow job = lockClaimedJob(connection, claimedJob, owner, ownerEpoch, now);
        List<ArtifactBuildOutput> outputs =
                ArtifactPublicationOutputValidator.loadAndValidate(connection, job);
        Aggregate aggregate = aggregate(job, outputs);
        JdbcGeneratedValueDescriptorStore.GeneratedDescriptor descriptor =
                JdbcGeneratedValueDescriptorStore.lock(
                        connection, job.tenantId(), job.artifactId());
        if (descriptor == null || !descriptor.sourceId().equals(job.sourceId())) {
            throw new IllegalStateException(
                    "Generated-value descriptor is absent or differs from the build");
        }
        long publicationEpoch = Math.addExact(descriptor.publicationEpoch(), 1L);
        boolean activate = JdbcGeneratedValueDescriptorStore.shouldActivate(descriptor, job);
        ArtifactGenerationManifest manifest = manifest(job, aggregate, now);
        if (activate) {
            retireActiveGeneration(connection, job.tenantId(), job.artifactId());
        }
        insertGeneration(connection, descriptor.targetColumn(), manifest,
                publicationEpoch, activate ? "ACTIVE" : "RETIRED");
        if (activate) {
            JdbcGeneratedValueDescriptorStore.activate(
                    connection, descriptor, job, publicationEpoch, now);
        } else {
            JdbcGeneratedValueDescriptorStore.advanceEpoch(
                    connection, descriptor, publicationEpoch, now);
        }
        publishArtifactJob(connection, job, aggregate, owner, ownerEpoch, now);
        return manifest;
    }

    private static JobRow lockClaimedJob(
            Connection connection, ArtifactBuildJob claimed,
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
                Instant expiresAt = getInstant(result, "publication_lease_expires_at");
                if (!ArtifactBuildJobState.READY_TO_PUBLISH.name().equals(result.getString("state"))
                        || !owner.equals(result.getString("publication_owner"))
                        || ownerEpoch != result.getLong("publication_owner_epoch")
                        || claimed.publicationFencingToken()
                        != result.getLong("publication_fencing_token")
                        || expiresAt == null || expiresAt.isBefore(now)) {
                    throw new IllegalStateException(
                            "Artifact publication lease is stale or mismatched");
                }
                JobRow row = mapJobRow(connection, result);
                requireStableIdentity(row, claimed);
                return row;
            }
        }
    }

    private static JobRow mapJobRow(Connection connection, ResultSet result) throws SQLException {
        String jobId = result.getString("job_id");
        return new JobRow(
                result.getString("tenant_id"), jobId, result.getString("artifact_id"),
                result.getString("artifact_type"), result.getString("staging_generation"),
                result.getString("source_id"), result.getString("source_snapshot"),
                org.pangrova.metadata.artifact.contract.ArtifactExecutionKind.valueOf(
                        result.getString("execution_kind")),
                new org.pangrova.metadata.artifact.contract.ArtifactDependencySet(
                        JdbcArtifactBuildStoreMappings.loadArtifactDependencies(
                                connection, result.getString("tenant_id"), jobId),
                        result.getString("dependency_set_sha256")),
                org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy.valueOf(
                        result.getString("empty_partition_policy")),
                result.getString("format_id"), result.getInt("format_major"),
                result.getInt("format_minor"), result.getString("schema_sha256"),
                result.getString("writer_build_id"), result.getString("encryption_key_ref"),
                result.getInt("expected_pages"), result.getInt("completed_pages"),
                result.getLong("publication_fencing_token"),
                getInstant(result, "publication_expected_updated_at"));
    }

    private static void requireStableIdentity(JobRow row, ArtifactBuildJob claimed) {
        if (!row.tenantId().equals(claimed.tenantId())
                || !row.artifactId().equals(claimed.artifactId())
                || !row.artifactType().equals(claimed.artifactType())
                || !row.generation().equals(claimed.stagingGeneration())
                || !row.sourceId().equals(claimed.sourceId())
                || !row.sourceSnapshot().equals(claimed.sourceSnapshot())
                || row.executionKind() != claimed.executionKind()
                || !row.dependencySet().equals(claimed.dependencySet())
                || row.emptyPartitionPolicy() != claimed.emptyPartitionPolicy()
                || !row.formatId().equals(claimed.formatId())
                || row.formatMajor() != claimed.formatMajor()
                || row.formatMinor() != claimed.formatMinor()
                || !row.schemaSha256().equalsIgnoreCase(claimed.schemaSha256())
                || !row.writerBuildId().equals(claimed.writerBuildId())
                || !row.publicationExpectedUpdatedAt()
                .equals(claimed.publicationExpectedUpdatedAt())) {
            throw new IllegalStateException("Claimed artifact identity changed before publication");
        }
    }

    private static Aggregate aggregate(
            ArtifactPublicationJob job, List<ArtifactBuildOutput> outputs) {
        long bytes = outputs.stream().mapToLong(ArtifactBuildOutput::sizeBytes)
                .reduce(0L, Math::addExact);
        Map<String, Long> sourceRows = new HashMap<>();
        for (ArtifactBuildOutput output : outputs) {
            if (output.scope() != ArtifactBuildOutputScope.SOURCE_OBJECT) {
                continue;
            }
            Long previous = sourceRows.putIfAbsent(output.sourceObjectId(), output.rowCount());
            if (previous != null && previous.longValue() != output.rowCount()) {
                throw new IllegalStateException("Artifact objects disagree on source row count");
            }
        }
        long rows = sourceRows.values().stream().mapToLong(Long::longValue)
                .reduce(0L, Math::addExact);
        return new Aggregate(outputs.size(), rows, bytes, manifestDigest(job, outputs));
    }

    private static ArtifactGenerationManifest manifest(
            JobRow job, Aggregate aggregate, Instant now) {
        return new ArtifactGenerationManifest(
                job.tenantId(), job.jobId(), job.artifactId(),
                job.artifactType(), job.generation(),
                job.sourceId(), job.sourceSnapshot(), job.executionKind(),
                job.dependencySet(), job.emptyPartitionPolicy(), job.formatId(),
                job.formatMajor(), job.formatMinor(), job.schemaSha256(),
                job.writerBuildId(),
                "metadata://generated-value-generation/" + aggregate.manifestSha256(),
                aggregate.manifestSha256(), aggregate.objectCount(), aggregate.rowCount(),
                aggregate.totalBytes(), canonicalInstant(now));
    }

    private static String manifestDigest(
            ArtifactPublicationJob job, List<ArtifactBuildOutput> outputs) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            update(digest, job.tenantId());
            update(digest, job.artifactId());
            update(digest, job.artifactType());
            update(digest, job.generation());
            update(digest, job.sourceId());
            update(digest, job.sourceSnapshot());
            update(digest, job.executionKind().name());
            update(digest, job.dependencySet().sha256());
            update(digest, job.emptyPartitionPolicy().name());
            update(digest, job.formatId());
            update(digest, Integer.toString(job.formatMajor()));
            update(digest, Integer.toString(job.formatMinor()));
            update(digest, job.schemaSha256().toLowerCase(java.util.Locale.ROOT));
            update(digest, job.writerBuildId());
            outputs.stream().sorted(Comparator
                            .comparing(ArtifactBuildOutput::sourceObjectId)
                            .thenComparingInt(ArtifactBuildOutput::ordinal)
                            .thenComparing(ArtifactBuildOutput::objectId))
                    .forEach(output -> {
                        update(digest, output.sourceObjectId());
                        update(digest, output.objectRole());
                        update(digest, output.objectId());
                        update(digest, output.opaqueUri());
                        update(digest, output.checksumSha256().toLowerCase(java.util.Locale.ROOT));
                        update(digest, Long.toString(output.sizeBytes()));
                        update(digest, Long.toString(output.rowCount()));
                        update(digest, Integer.toString(output.ordinal()));
                        update(digest, output.scope().name());
                        update(digest, output.encryptionKeyRef());
                    });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void retireActiveGeneration(
            Connection connection, String tenantId, String artifactId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE generated_value_generation_manifests SET state='RETIRED' "
                        + "WHERE tenant_id=? AND artifact_id=? AND state='ACTIVE'")) {
            statement.setString(1, tenantId);
            statement.setString(2, artifactId);
            statement.executeUpdate();
        }
    }

    private static void insertGeneration(
            Connection connection, String targetColumn,
            ArtifactGenerationManifest manifest, long publicationEpoch, String state)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO generated_value_generation_manifests "
                        + "(tenant_id,artifact_id,generation,source_id,source_snapshot,"
                        + "target_column,format_id,"
                        + "format_major,format_minor,schema_sha256,writer_build_id,object_count,"
                        + "row_count,total_bytes,manifest_sha256,publication_epoch,state,published_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            int parameter = 1;
            statement.setString(parameter++, manifest.tenantId());
            statement.setString(parameter++, manifest.artifactId());
            statement.setString(parameter++, manifest.generation());
            statement.setString(parameter++, manifest.sourceId());
            statement.setString(parameter++, manifest.sourceSnapshot());
            statement.setString(parameter++, targetColumn);
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
            statement.setString(parameter++, state);
            setInstant(statement, parameter, manifest.publishedAt());
            statement.executeUpdate();
        }
    }

    private static void publishArtifactJob(
            Connection connection, JobRow job, Aggregate aggregate,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='PUBLISHED',manifest_sha256=?,"
                        + "published_object_count=?,published_row_count=?,published_total_bytes=?,"
                        + "published_at=?,publication_lease_expires_at=NULL,failure_reason=NULL,"
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

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
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

    private record JobRow(
            String tenantId,
            String jobId,
            String artifactId,
            String artifactType,
            String generation,
            String sourceId,
            String sourceSnapshot,
            org.pangrova.metadata.artifact.contract.ArtifactExecutionKind executionKind,
            org.pangrova.metadata.artifact.contract.ArtifactDependencySet dependencySet,
            org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy emptyPartitionPolicy,
            String formatId,
            int formatMajor,
            int formatMinor,
            String schemaSha256,
            String writerBuildId,
            String encryptionKeyRef,
            int expectedPages,
            int completedPages,
            long publicationFence,
            Instant publicationExpectedUpdatedAt) implements ArtifactPublicationJob {}

    private record Aggregate(
            long objectCount, long rowCount, long totalBytes, String manifestSha256) {}
}
