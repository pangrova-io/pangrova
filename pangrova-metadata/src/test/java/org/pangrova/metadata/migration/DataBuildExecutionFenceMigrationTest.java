package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBuildExecutionFenceMigrationTest {
    @Test
    void refusesUnownedExistingWorkWithoutDeletingOrRewritingItsState() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("execution_fence_rebuild");
        migrate(dataSource, "45");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,"
                    + "staging_generation,source_id,source_snapshot,staging_uri,format_id,format_major,format_minor,"
                    + "schema_sha256,writer_build_id,idempotency_key,expected_pages,state,created_at,updated_at,"
                    + "publication_expected_updated_at,dependency_set_sha256) VALUES ('tenant','recoverable-job','artifact',"
                    + "'VECTOR_INDEX','1','lake.raw.items','1','s3://warehouse/build','pangrova.hnsw.segment',1,0,"
                    + "repeat('a',64),'native','request',1,'RUNNING',now(),now(),now(),repeat('b',64))");
        }
        assertThatThrownBy(() -> migrate(dataSource, "46")).hasStackTraceContaining("requires explicit rebuild");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT tenant_id,job_id,state,staging_uri FROM artifact_build_jobs")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("tenant");
            assertThat(result.getString(2)).isEqualTo("recoverable-job");
            assertThat(result.getString(3)).isEqualTo("RUNNING");
            assertThat(result.getString(4)).isEqualTo("s3://warehouse/build");
            assertThat(result.next()).isFalse();
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT to_regclass('artifact_execution_routes')")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isNull();
        }
    }

    @Test
    void createsOnlyExactRoutesAndMandatoryLeasedOwnerProofForANewDeployment() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("execution_fence_new");
        migrate(dataSource, "46");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT execution_owner_id,execution_owner_incarnation,"
                        + "execution_owner_token FROM artifact_build_pages")) {
            assertThat(result.next()).isFalse();
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT execution_id,tenant_id,job_id FROM artifact_execution_routes")) {
            assertThat(result.next()).isFalse();
        }
    }

    private static void migrate(DataSource dataSource, String version) {
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version)).load().migrate();
    }
}
