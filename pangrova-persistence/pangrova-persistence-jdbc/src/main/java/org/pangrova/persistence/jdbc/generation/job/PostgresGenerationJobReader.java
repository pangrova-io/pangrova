package org.pangrova.persistence.jdbc.generation.job;

import org.pangrova.spi.generation.job.GenerationJob;
import org.pangrova.spi.generation.job.GenerationJobKey;
import org.pangrova.persistence.jdbc.generation.execution.PostgresGenerationExecutionRoutes;
import org.pangrova.spi.generation.job.GenerationJobState;
import org.pangrova.spi.generation.job.GenerationOutput;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
final class PostgresGenerationJobReader extends PostgresGenerationJobStoreSupport {

    private static final int OUTPUT_LOOKUP_BATCH_SIZE = 500;

    PostgresGenerationJobReader(DataSource dataSource) {
        super(dataSource);
    }

    Optional<GenerationJobKey> findExecutionJob(String executionId) {
        Objects.requireNonNull(executionId, "executionId");
        try (var connection = dataSource.getConnection()) {
            return PostgresGenerationExecutionRoutes.findRoute(connection, executionId);
        } catch (SQLException failure) {
            throw failure("resolve artifact execution association", failure);
        }
    }

    Optional<GenerationJob> findJob(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        try (Connection connection = dataSource.getConnection()) {
            return findJob(connection, tenantId, jobId, false);
        } catch (SQLException e) {
            throw failure("find artifact build job", e);
        }
    }

    Optional<GenerationDispatchSpec> findDispatchSpec(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        try (Connection connection = dataSource.getConnection()) {
            return INPUTS.findDispatchSpec(connection, tenantId, jobId);
        } catch (SQLException e) {
            throw failure("find artifact dispatch spec", e);
        }
    }

    Optional<org.pangrova.spi.generation.manifest.GenerationManifest> findPublishedGeneration(
            String tenantId, String artifactId, String generation, String sourceSnapshot) {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(generation, "generation");
        requireText(sourceSnapshot, "sourceSnapshot");
        try (Connection connection = dataSource.getConnection()) {
            return PostgresGenerationPublishedInputs.findManifest(
                    connection, tenantId, artifactId, generation, sourceSnapshot);
        } catch (SQLException failure) {
            throw failure("find published artifact generation", failure);
        }
    }

    List<GenerationOutput> loadCompleteOutputs(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        return inTransaction("load complete artifact outputs", connection -> {
            GenerationJob job = findJob(connection, tenantId, jobId, true).orElseThrow(
                    () -> new IllegalArgumentException("unknown artifact build job: " + jobId));
            if (job.state() != GenerationJobState.READY_TO_PUBLISH
                    && job.state() != GenerationJobState.PUBLISHED) {
                throw new IllegalStateException("artifact build has not crossed all-pages barrier");
            }
            if (countSucceededPages(connection, tenantId, jobId) != job.expectedPages()) {
                throw new IllegalStateException("artifact build page counter is inconsistent");
            }
            List<GenerationOutput> outputs = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT object_id,opaque_uri,object_role,source_object_id,object_ordinal,object_scope,"
                            + "size_bytes,row_count,checksum_sha256,encryption_key_ref "
                            + "FROM artifact_build_outputs WHERE tenant_id=? AND job_id=? "
                            + "ORDER BY page_id,object_ordinal")) {
                statement.setString(1, tenantId);
                statement.setString(2, jobId);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        outputs.add(mapOutput(result));
                    }
                }
            }
            if (outputs.isEmpty() && job.expectedPages() != 0) {
                throw new IllegalStateException("complete artifact build has no outputs");
            }
            return List.copyOf(outputs);
        });
    }

    List<GenerationOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        List<String> dependencies =
                PostgresGenerationOutputQueries.requireDependencies(dependencyPageIds);
        return inTransaction("load artifact dependency outputs", connection ->
                PostgresGenerationOutputQueries.loadDependencies(
                        connection, tenantId, jobId, dependencies));
    }

    List<GenerationOutput> loadPublishedOutputs(
            String tenantId, String jobId,
            java.util.Set<String> sourceObjectIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(sourceObjectIds, "sourceObjectIds must not be null");
        if (sourceObjectIds.isEmpty()) {
            return List.of();
        }
        List<String> requested =
                PostgresGenerationOutputQueries.requireSourceIds(sourceObjectIds);
        return inTransaction("load published artifact outputs", connection ->
                PostgresGenerationOutputQueries.loadPublished(
                        connection, tenantId, jobId, requested));
    }

    boolean matchesPublishedSplits(
            String tenantId, String jobId,
            java.util.Map<String, String> sourceSplitSha256) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(
                sourceSplitSha256, "sourceSplitSha256 must not be null");
        if (sourceSplitSha256.isEmpty()) {
            return false;
        }
        List<String> sourceIds =
                PostgresGenerationOutputQueries.requireSplitChecksums(sourceSplitSha256);
        return inTransaction("match published artifact splits", connection ->
                PostgresGenerationOutputQueries.matchesPublishedSplits(
                        connection, tenantId, jobId, sourceIds, sourceSplitSha256));
    }

    java.util.Map<String, org.pangrova.spi.generation.job.GenerationSplit> loadPublishedSourceSplits(
            String tenantId, String jobId, java.util.Set<String> sourceObjectIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(sourceObjectIds, "sourceObjectIds must not be null");
        if (sourceObjectIds.isEmpty()) return java.util.Map.of();
        List<String> sourceIds = PostgresGenerationOutputQueries.requireSourceIds(sourceObjectIds);
        return inTransaction("load published artifact source splits", connection ->
                PostgresGenerationOutputQueries.loadPublishedSourceSplits(
                        connection, tenantId, jobId, sourceIds));
    }

}
