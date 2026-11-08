package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationExchangeBarrierMigrationTest {
    @Test
    void refusesExistingTasksAndPreservesTheirOriginalOwnerRouteAndJob() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("exchange_rebuild_guard");
        migrate(dataSource, "46");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,"
                    + "staging_generation,source_id,source_snapshot,staging_uri,format_id,format_major,format_minor,"
                    + "schema_sha256,writer_build_id,idempotency_key,expected_pages,state,created_at,updated_at,"
                    + "publication_expected_updated_at,dependency_set_sha256) VALUES ('tenant','recoverable-job','artifact',"
                    + "'VECTOR_INDEX','1','lake.raw.items','1','s3://warehouse/build','pangrova.hnsw.segment',1,0,"
                    + "repeat('a',64),'native','request',1,'RUNNING',now(),now(),now(),repeat('b',64))");
            statement.execute("INSERT INTO artifact_execution_routes VALUES ('actual-route','tenant','recoverable-job')");
        }
        assertThatThrownBy(() -> migrate(dataSource, "47")).hasStackTraceContaining("requires explicit rebuild");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery("SELECT j.state,j.staging_uri,r.execution_id FROM artifact_build_jobs j "
                        + "JOIN artifact_execution_routes r USING (tenant_id,job_id)")) {
            assertThat(row.next()).isTrue();
            assertThat(row.getString(1)).isEqualTo("RUNNING");
            assertThat(row.getString(2)).isEqualTo("s3://warehouse/build");
            assertThat(row.getString(3)).isEqualTo("actual-route");
            assertThat(row.next()).isFalse();
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery("SELECT to_regclass('artifact_exchange_finalizations')")) {
            assertThat(row.next()).isTrue();
            assertThat(row.getString(1)).isNull();
        }
    }

    @Test
    void createsSingleCapabilityFreeRegistrationAndFinalizationContractOnEmptyDeployment() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("exchange_new_deployment");
        migrate(dataSource, "47");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery("SELECT registration_bytes,registration_sha256 "
                        + "FROM artifact_exchange_registrations")) {
            assertThat(row.next()).isFalse();
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery("SELECT finalization_bytes,finalization_sha256 "
                        + "FROM artifact_exchange_finalizations")) {
            assertThat(row.next()).isFalse();
        }
    }

    private static void migrate(DataSource dataSource, String version) {
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version)).load().migrate();
    }
}
