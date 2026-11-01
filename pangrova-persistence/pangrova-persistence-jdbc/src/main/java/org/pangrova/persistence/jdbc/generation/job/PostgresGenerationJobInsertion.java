package org.pangrova.persistence.jdbc.generation.job;

import org.pangrova.spi.generation.job.GenerationJob;
import org.pangrova.spi.generation.GenerationDependency;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/** Inserts a new durable artifact job and its ordered dependency fence. */
final class PostgresGenerationJobInsertion {

    private PostgresGenerationJobInsertion() {}

    static void insertJob(Connection connection, GenerationJob job) throws SQLException {
        String sql = "INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,"
                + "staging_generation,source_id,source_snapshot,staging_uri,format_id,format_major,"
                + "format_minor,schema_sha256,writer_build_id,encryption_key_ref,idempotency_key,"
                + "expected_pages,completed_pages,state,publication_fencing_token,"
                + "publication_expected_updated_at,created_at,updated_at,execution_kind,"
                + "empty_partition_policy,dependency_set_sha256) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, job.tenantId());
            statement.setString(parameter++, job.jobId());
            statement.setString(parameter++, job.artifactId());
            statement.setString(parameter++, job.artifactType());
            statement.setString(parameter++, job.stagingGeneration());
            statement.setString(parameter++, job.sourceId());
            statement.setString(parameter++, job.sourceSnapshot());
            statement.setString(parameter++, job.stagingUri());
            statement.setString(parameter++, job.formatId());
            statement.setInt(parameter++, job.formatMajor());
            statement.setInt(parameter++, job.formatMinor());
            statement.setString(parameter++, job.schemaSha256().toLowerCase(Locale.ROOT));
            statement.setString(parameter++, job.writerBuildId());
            statement.setString(parameter++, job.encryptionKeyRef());
            statement.setString(parameter++, job.idempotencyKey());
            statement.setInt(parameter++, job.expectedPages());
            statement.setInt(parameter++, job.completedPages());
            statement.setString(parameter++, job.state().name());
            statement.setLong(parameter++, job.publicationFencingToken());
            setInstant(statement, parameter++, job.publicationExpectedUpdatedAt());
            setInstant(statement, parameter++, job.createdAt());
            setInstant(statement, parameter++, job.updatedAt());
            statement.setString(parameter++, job.executionKind().name());
            statement.setString(parameter++, job.emptyPartitionPolicy().name());
            statement.setString(parameter, job.dependencySet().sha256());
            statement.executeUpdate();
        }
        insertDependencies(connection, job);
    }

    private static void insertDependencies(Connection connection, GenerationJob job)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO artifact_build_dependencies "
                        + "(tenant_id,job_id,dependency_ordinal,dependency_kind,dependency_id,"
                        + "dependency_version) VALUES (?,?,?,?,?,?)")) {
            int ordinal = 0;
            for (GenerationDependency dependency : job.dependencySet().dependencies()) {
                statement.setString(1, job.tenantId());
                statement.setString(2, job.jobId());
                statement.setInt(3, ordinal++);
                statement.setString(4, dependency.kind());
                statement.setString(5, dependency.id());
                statement.setString(6, dependency.version());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }
}
