/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.metadata.generation.jdbc;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.output.GenerationOutputScope;
import org.pangrova.generation.api.execution.GenerationExecutionKind;
import org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.persistence.catalog.api.error.MetastoreException;

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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.canonicalInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Atomic PostgreSQL publisher for generic immutable tabular exports. */
public final class JdbcTabularExportPublicationStore implements GenerationPublicationStore {

    private final DataSource dataSource;
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;
    private final GenerationTransactionInputs inputs;
    private final GenerationPublicationOutputValidator outputValidator;

    /** Composes tabular publication with current authority, execution fencing and same-connection input reads. */
    public JdbcTabularExportPublicationStore(
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
            GenerationJob claimed, String owner, long ownerEpoch, Instant now) {
        Objects.requireNonNull(claimed, "claimed must not be null");
        requireText(owner, "owner");
        Objects.requireNonNull(now, "now must not be null");
        if (!GenerationOutputTypeIds.TABULAR_EXPORT.equals(claimed.artifactType())
                || claimed.executionKind() != GenerationExecutionKind.EXPORT) {
            throw new IllegalArgumentException("tabular publisher accepts only generic EXPORT jobs");
        }
        if (ownerEpoch <= 0 || claimed.publicationFencingToken() <= 0) {
            throw new IllegalArgumentException("publication owner epoch and fence must be positive");
        }
        BuiltInGenerationOutputFormats.require(claimed.formatId(), claimed.artifactType());
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                transactionAuthority.requireCurrent(connection, claimed.tenantId(), claimed.jobId());
                executionFence.requirePublication(connection, claimed.tenantId(), claimed.jobId());
                JobRow job = lockClaim(connection, claimed, owner, ownerEpoch, now);
                List<GenerationOutput> outputs =
                        outputValidator.loadAndValidate(connection, job);
                Aggregate aggregate = aggregate(job, outputs);
                long epoch = nextEpoch(connection, job.tenantId(), job.artifactId());
                GenerationManifest manifest = manifest(job, aggregate, now);
                retireActive(connection, job.tenantId(), job.artifactId());
                insertGeneration(connection, manifest, epoch);
                publishJob(connection, job, aggregate, owner, ownerEpoch, now);
                // Recheck after every potentially blocking publication write, before commit.
                // Expiry here rolls back the generation, descriptor pointer and job together.
                executionFence.requirePublication(connection, claimed.tenantId(), claimed.jobId());
                connection.commit();
                return manifest;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Atomic tabular export publication failed", failure);
        }
    }

    private JobRow lockClaim(
            Connection connection, GenerationJob claimed,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM artifact_build_jobs WHERE tenant_id=? AND job_id=? FOR UPDATE")) {
            statement.setString(1, claimed.tenantId());
            statement.setString(2, claimed.jobId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Tabular export job disappeared");
                }
                Instant expires = getInstant(result, "publication_lease_expires_at");
                if (!GenerationJobState.READY_TO_PUBLISH.name().equals(
                        result.getString("state"))
                        || !owner.equals(result.getString("publication_owner"))
                        || ownerEpoch != result.getLong("publication_owner_epoch")
                        || claimed.publicationFencingToken()
                                != result.getLong("publication_fencing_token")
                        || expires == null || expires.isBefore(now)) {
                    throw new IllegalStateException("Tabular export publication lease is stale");
                }
                JobRow row = mapJob(connection, result);
                requireStableIdentity(row, claimed);
                return row;
            }
        }
    }

    private JobRow mapJob(Connection connection, ResultSet result) throws SQLException {
        String tenantId = result.getString("tenant_id");
        String jobId = result.getString("job_id");
        return new JobRow(
                tenantId, jobId, result.getString("artifact_id"),
                result.getString("artifact_type"), result.getString("staging_generation"),
                result.getString("source_id"), result.getString("source_snapshot"),
                GenerationExecutionKind.valueOf(result.getString("execution_kind")),
                new org.pangrova.generation.api.dependency.GenerationDependencySet(
                        inputs.loadJobDependencies(
                                connection, tenantId, jobId),
                        result.getString("dependency_set_sha256")),
                org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy.valueOf(
                        result.getString("empty_partition_policy")),
                result.getString("format_id"), result.getInt("format_major"),
                result.getInt("format_minor"), result.getString("schema_sha256"),
                result.getString("writer_build_id"), result.getString("encryption_key_ref"),
                result.getInt("expected_pages"), result.getInt("completed_pages"),
                result.getLong("publication_fencing_token"),
                getInstant(result, "publication_expected_updated_at"));
    }

    private static void requireStableIdentity(JobRow row, GenerationJob claimed) {
        if (!matchesSourceIdentity(row, claimed)
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
            throw new IllegalStateException("Claimed tabular export identity changed");
        }
    }

    private static boolean matchesSourceIdentity(JobRow row, GenerationJob claimed) {
        return row.tenantId().equals(claimed.tenantId())
                && row.jobId().equals(claimed.jobId())
                && row.artifactId().equals(claimed.artifactId())
                && row.artifactType().equals(claimed.artifactType())
                && row.generation().equals(claimed.stagingGeneration())
                && row.sourceId().equals(claimed.sourceId())
                && row.sourceSnapshot().equals(claimed.sourceSnapshot());
    }

    private static Aggregate aggregate(
            GenerationPublicationJob job, List<GenerationOutput> outputs) {
        long bytes = outputs.stream().mapToLong(GenerationOutput::sizeBytes)
                .reduce(0L, Math::addExact);
        Map<String, Long> sourceRows = new HashMap<>();
        for (GenerationOutput output : outputs) {
            if (output.scope() == GenerationOutputScope.WORK_UNIT) {
                Long previous = sourceRows.putIfAbsent(
                        output.sourceObjectId(), output.rowCount());
                if (previous != null && previous.longValue() != output.rowCount()) {
                    throw new IllegalStateException("Export objects disagree on source row count");
                }
            }
        }
        long rows = sourceRows.values().stream().mapToLong(Long::longValue)
                .reduce(0L, Math::addExact);
        return new Aggregate(outputs.size(), rows, bytes, manifestDigest(job, outputs));
    }

    private static GenerationManifest manifest(
            JobRow job, Aggregate aggregate, Instant now) {
        return new GenerationManifest(
                job.tenantId(), job.jobId(), job.artifactId(), job.artifactType(),
                job.generation(), job.sourceId(), job.sourceSnapshot(),
                job.executionKind(), job.dependencySet(), job.emptyPartitionPolicy(),
                job.formatId(), job.formatMajor(), job.formatMinor(), job.schemaSha256(),
                job.writerBuildId(), "metadata://artifact-generation/"
                        + aggregate.manifestSha256(), aggregate.manifestSha256(),
                aggregate.objectCount(), aggregate.rowCount(), aggregate.totalBytes(),
                canonicalInstant(now));
    }

    private static long nextEpoch(
            Connection connection, String tenantId, String artifactId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COALESCE(MAX(publication_epoch),0)+1 "
                        + "FROM tabular_export_generations WHERE tenant_id=? AND artifact_id=?")) {
            statement.setString(1, tenantId);
            statement.setString(2, artifactId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    private static void retireActive(
            Connection connection, String tenantId, String artifactId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE tabular_export_generations SET state='RETIRED' "
                        + "WHERE tenant_id=? AND artifact_id=? AND state='ACTIVE'")) {
            statement.setString(1, tenantId);
            statement.setString(2, artifactId);
            statement.executeUpdate();
        }
    }

    private static void insertGeneration(
            Connection connection, GenerationManifest manifest,
            long epoch) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO tabular_export_generations "
                        + "(tenant_id,artifact_id,generation,job_id,source_id,source_snapshot,"
                        + "format_id,format_major,format_minor,schema_sha256,writer_build_id,"
                        + "manifest_uri,manifest_sha256,object_count,row_count,total_bytes,"
                        + "publication_epoch,state,published_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?)")) {
            int parameter = 1;
            statement.setString(parameter++, manifest.tenantId());
            statement.setString(parameter++, manifest.artifactId());
            statement.setString(parameter++, manifest.generation());
            statement.setString(parameter++, manifest.jobId());
            statement.setString(parameter++, manifest.sourceId());
            statement.setString(parameter++, manifest.sourceSnapshot());
            statement.setString(parameter++, manifest.formatId());
            statement.setInt(parameter++, manifest.formatMajor());
            statement.setInt(parameter++, manifest.formatMinor());
            statement.setString(parameter++, manifest.schemaSha256().toLowerCase(Locale.ROOT));
            statement.setString(parameter++, manifest.writerBuildId());
            statement.setString(parameter++, manifest.manifestUri());
            statement.setString(parameter++, manifest.manifestSha256());
            statement.setLong(parameter++, manifest.objectCount());
            statement.setLong(parameter++, manifest.rowCount());
            statement.setLong(parameter++, manifest.totalBytes());
            statement.setLong(parameter++, epoch);
            setInstant(statement, parameter, manifest.publishedAt());
            statement.executeUpdate();
        }
    }

    private static void publishJob(
            Connection connection, JobRow job, Aggregate aggregate,
            String owner, long ownerEpoch, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='PUBLISHED',manifest_sha256=?,"
                        + "published_object_count=?,published_row_count=?,published_total_bytes=?,"
                        + "published_at=?,publication_lease_expires_at=NULL,failure_reason=NULL,"
                        + "updated_at=? WHERE tenant_id=? AND job_id=? "
                        + "AND state='READY_TO_PUBLISH' AND publication_owner=? "
                        + "AND publication_owner_epoch=? AND publication_fencing_token=? "
                        + "AND publication_lease_expires_at>=?")) {
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
                throw new IllegalStateException("Tabular export publication fence was lost");
            }
        }
    }

    private static String manifestDigest(
            GenerationPublicationJob job, List<GenerationOutput> outputs) {
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
            update(digest, job.schemaSha256().toLowerCase(Locale.ROOT));
            update(digest, job.writerBuildId());
            outputs.stream().sorted(Comparator
                            .comparing(GenerationOutput::sourceObjectId)
                            .thenComparingInt(GenerationOutput::ordinal)
                            .thenComparing(GenerationOutput::objectId))
                    .forEach(output -> {
                        update(digest, output.sourceObjectId());
                        update(digest, output.objectRole());
                        update(digest, output.objectId());
                        update(digest, output.opaqueUri());
                        update(digest, output.checksumSha256().toLowerCase(Locale.ROOT));
                        update(digest, Long.toString(output.sizeBytes()));
                        update(digest, Long.toString(output.rowCount()));
                        update(digest, Integer.toString(output.ordinal()));
                        update(digest, output.scope().name());
                    });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static void rollback(Connection connection, Exception failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private record JobRow(
            String tenantId, String jobId, String artifactId, String artifactType,
            String generation, String sourceId, String sourceSnapshot,
            GenerationExecutionKind executionKind,
            org.pangrova.generation.api.dependency.GenerationDependencySet dependencySet,
            org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy emptyPartitionPolicy,
            String formatId, int formatMajor, int formatMinor, String schemaSha256,
            String writerBuildId, String encryptionKeyRef, int expectedPages,
            int completedPages, long publicationFence,
            Instant publicationExpectedUpdatedAt) implements GenerationPublicationJob {}

    private record Aggregate(
            long objectCount, long rowCount, long totalBytes, String manifestSha256) {}
}
