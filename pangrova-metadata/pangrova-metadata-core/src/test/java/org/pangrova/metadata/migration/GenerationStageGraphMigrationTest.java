package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationStageGraphMigrationTest {
    @Test void rejectsLegacyPayloadAndPreservesRecoverableJobAndBytes() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("graph_rebuild_guard");
        migrate(dataSource, "42");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,"
                    + "staging_generation,source_id,source_snapshot,staging_uri,format_id,format_major,format_minor,"
                    + "schema_sha256,writer_build_id,idempotency_key,expected_pages,state,created_at,updated_at,"
                    + "publication_expected_updated_at,dependency_set_sha256) VALUES ('tenant','job','artifact',"
                    + "'VECTOR_INDEX','1','lake.raw.items','1','s3://warehouse/build','pangrova.hnsw.segment',1,0,"
                    + "repeat('a',64),'native','request',1,'QUEUED',now(),now(),now(),repeat('b',64))");
            statement.execute("INSERT INTO artifact_build_dispatch_specs (tenant_id,job_id,plan_bytes,plan_sha256,"
                    + "operator_bindings_bytes,operator_bindings_sha256,principal,max_memory_bytes,max_threads,"
                    + "max_spill_bytes,task_timeout_millis) VALUES ('tenant','job',decode('0102','hex'),repeat('a',64),"
                    + "decode('0304','hex'),repeat('b',64),'alice',1024,1,1024,1000)");
        }
        assertThatThrownBy(() -> migrate(dataSource, "43"))
                .hasStackTraceContaining("requires explicit rebuild");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT j.state,encode(d.plan_bytes,'hex'),"
                        + "encode(d.operator_bindings_bytes,'hex') FROM artifact_build_jobs j "
                        + "JOIN artifact_build_dispatch_specs d USING (tenant_id,job_id)")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("QUEUED");
            assertThat(result.getString(2)).isEqualTo("0102");
            assertThat(result.getString(3)).isEqualTo("0304");
            assertThat(result.next()).isFalse();
        }
    }

    @Test void createsOnlyCurrentGraphColumnsForAnEmptyDeployment() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("graph_new_deployment");
        migrate(dataSource, "43");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT stage_graph_bytes,stage_graph_sha256 "
                        + "FROM artifact_build_dispatch_specs")) {
            assertThat(result.next()).isFalse();
        }
        try (var connection = dataSource.getConnection()) {
            assertThatThrownBy(() -> connection.createStatement().executeQuery(
                    "SELECT plan_bytes FROM artifact_build_dispatch_specs"))
                    .isInstanceOf(java.sql.SQLException.class).hasMessageContaining("does not exist");
        }
    }

    private static void migrate(DataSource dataSource, String version) {
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion(version)).load().migrate();
    }
}
