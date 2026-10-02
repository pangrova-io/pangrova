package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;

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
final class JdbcArtifactBuildReader extends JdbcArtifactBuildStoreSupport {

    private static final int OUTPUT_LOOKUP_BATCH_SIZE = 500;

    JdbcArtifactBuildReader(DataSource dataSource) {
        super(dataSource);
    }

    Optional<ArtifactBuildJob> findJob(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        try (Connection connection = dataSource.getConnection()) {
            return findJob(connection, tenantId, jobId, false);
        } catch (SQLException e) {
            throw failure("find artifact build job", e);
        }
    }

    Optional<ArtifactBuildDispatchSpec> findDispatchSpec(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        try (Connection connection = dataSource.getConnection()) {
            return findDispatchSpec(connection, tenantId, jobId);
        } catch (SQLException e) {
            throw failure("find artifact dispatch spec", e);
        }
    }

    Optional<org.pangrova.metadata.artifact.ArtifactGenerationManifest> findPublishedGeneration(
            String tenantId, String artifactId, String generation, String sourceSnapshot) {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(generation, "generation");
        requireText(sourceSnapshot, "sourceSnapshot");
        try (Connection connection = dataSource.getConnection()) {
            return JdbcArtifactPublishedInputs.findManifest(connection, tenantId, artifactId, generation, sourceSnapshot);
        } catch (SQLException failure) {
            throw failure("find published artifact generation", failure);
        }
    }

    List<ArtifactBuildOutput> loadCompleteOutputs(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        return inTransaction("load complete artifact outputs", connection -> {
            ArtifactBuildJob job = findJob(connection, tenantId, jobId, true).orElseThrow(
                    () -> new IllegalArgumentException("unknown artifact build job: " + jobId));
            if (job.state() != ArtifactBuildJobState.READY_TO_PUBLISH
                    && job.state() != ArtifactBuildJobState.PUBLISHED) {
                throw new IllegalStateException("artifact build has not crossed all-pages barrier");
            }
            if (countSucceededPages(connection, tenantId, jobId) != job.expectedPages()) {
                throw new IllegalStateException("artifact build page counter is inconsistent");
            }
            List<ArtifactBuildOutput> outputs = new ArrayList<>();
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

    List<ArtifactBuildOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        List<String> dependencies =
                JdbcArtifactBuildOutputQueries.requireDependencies(dependencyPageIds);
        return inTransaction("load artifact dependency outputs", connection ->
                JdbcArtifactBuildOutputQueries.loadDependencies(
                        connection, tenantId, jobId, dependencies));
    }

    List<ArtifactBuildOutput> loadPublishedOutputs(
            String tenantId, String jobId,
            java.util.Set<String> sourceObjectIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(sourceObjectIds, "sourceObjectIds must not be null");
        if (sourceObjectIds.isEmpty()) {
            return List.of();
        }
        List<String> requested =
                JdbcArtifactBuildOutputQueries.requireSourceIds(sourceObjectIds);
        return inTransaction("load published artifact outputs", connection ->
                JdbcArtifactBuildOutputQueries.loadPublished(
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
                JdbcArtifactBuildOutputQueries.requireSplitChecksums(sourceSplitSha256);
        return inTransaction("match published artifact splits", connection ->
                JdbcArtifactBuildOutputQueries.matchesPublishedSplits(
                        connection, tenantId, jobId, sourceIds, sourceSplitSha256));
    }

}
