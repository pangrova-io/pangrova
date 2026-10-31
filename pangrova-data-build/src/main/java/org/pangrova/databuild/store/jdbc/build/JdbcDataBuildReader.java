package org.pangrova.databuild.store.jdbc.build;

import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.databuild.store.jdbc.execution.JdbcDataBuildExecutionRoutes;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;

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
final class JdbcDataBuildReader extends JdbcDataBuildStoreSupport {

    private static final int OUTPUT_LOOKUP_BATCH_SIZE = 500;

    JdbcDataBuildReader(DataSource dataSource) {
        super(dataSource);
    }

    Optional<DataBuildJobKey> findExecutionJob(String executionId) {
        Objects.requireNonNull(executionId, "executionId");
        try (var connection = dataSource.getConnection()) {
            return JdbcDataBuildExecutionRoutes.findRoute(connection, executionId);
        } catch (SQLException failure) {
            throw failure("resolve artifact execution association", failure);
        }
    }

    Optional<DataBuildJob> findJob(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        try (Connection connection = dataSource.getConnection()) {
            return findJob(connection, tenantId, jobId, false);
        } catch (SQLException e) {
            throw failure("find artifact build job", e);
        }
    }

    Optional<DataBuildDispatchSpec> findDispatchSpec(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        try (Connection connection = dataSource.getConnection()) {
            return org.pangrova.databuild.store.jdbc.query.JdbcDataBuildQueries
                    .findDispatchSpec(connection, tenantId, jobId);
        } catch (SQLException e) {
            throw failure("find artifact dispatch spec", e);
        }
    }

    Optional<org.pangrova.databuild.api.manifest.DataBuildGenerationManifest> findPublishedGeneration(
            String tenantId, String artifactId, String generation, String sourceSnapshot) {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(generation, "generation");
        requireText(sourceSnapshot, "sourceSnapshot");
        try (Connection connection = dataSource.getConnection()) {
            return JdbcDataBuildPublishedInputs.findManifest(
                    connection, tenantId, artifactId, generation, sourceSnapshot);
        } catch (SQLException failure) {
            throw failure("find published artifact generation", failure);
        }
    }

    List<DataBuildOutput> loadCompleteOutputs(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        return inTransaction("load complete artifact outputs", connection -> {
            DataBuildJob job = findJob(connection, tenantId, jobId, true).orElseThrow(
                    () -> new IllegalArgumentException("unknown artifact build job: " + jobId));
            if (job.state() != DataBuildJobState.READY_TO_PUBLISH
                    && job.state() != DataBuildJobState.PUBLISHED) {
                throw new IllegalStateException("artifact build has not crossed all-pages barrier");
            }
            if (countSucceededPages(connection, tenantId, jobId) != job.expectedPages()) {
                throw new IllegalStateException("artifact build page counter is inconsistent");
            }
            List<DataBuildOutput> outputs = new ArrayList<>();
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

    List<DataBuildOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        List<String> dependencies =
                JdbcDataBuildOutputQueries.requireDependencies(dependencyPageIds);
        return inTransaction("load artifact dependency outputs", connection ->
                JdbcDataBuildOutputQueries.loadDependencies(
                        connection, tenantId, jobId, dependencies));
    }

    List<DataBuildOutput> loadPublishedOutputs(
            String tenantId, String jobId,
            java.util.Set<String> sourceObjectIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(sourceObjectIds, "sourceObjectIds must not be null");
        if (sourceObjectIds.isEmpty()) {
            return List.of();
        }
        List<String> requested =
                JdbcDataBuildOutputQueries.requireSourceIds(sourceObjectIds);
        return inTransaction("load published artifact outputs", connection ->
                JdbcDataBuildOutputQueries.loadPublished(
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
                JdbcDataBuildOutputQueries.requireSplitChecksums(sourceSplitSha256);
        return inTransaction("match published artifact splits", connection ->
                JdbcDataBuildOutputQueries.matchesPublishedSplits(
                        connection, tenantId, jobId, sourceIds, sourceSplitSha256));
    }

    java.util.Map<String, org.pangrova.databuild.api.build.DataBuildSplit> loadPublishedSourceSplits(
            String tenantId, String jobId, java.util.Set<String> sourceObjectIds) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(sourceObjectIds, "sourceObjectIds must not be null");
        if (sourceObjectIds.isEmpty()) return java.util.Map.of();
        List<String> sourceIds = JdbcDataBuildOutputQueries.requireSourceIds(sourceObjectIds);
        return inTransaction("load published artifact source splits", connection ->
                JdbcDataBuildOutputQueries.loadPublishedSourceSplits(
                        connection, tenantId, jobId, sourceIds));
    }

}
