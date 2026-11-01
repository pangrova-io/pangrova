package org.pangrova.coordinator.schema;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Audits the final per-owner baseline chain: every instant column in production DDL must be
 * {@code timestamptz(6)} so microsecond instants survive cross-connection time zones. Lives in
 * coordinator because only the integrator's test classpath carries every owner baseline.
 */
class MigrationsTimeZoneConformanceTest {

    @Test
    void migrationsUseMicrosecondTimeZoneAwareAbsoluteTimes() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("absolute_columns");
        List<Column> columns = List.of(
                new Column("distributed_lease", "acquired_at"),
                new Column("distributed_lease", "expires_at"),
                new Column("index_maintenance_lease", "acquired_at"),
                new Column("index_maintenance_lease", "expires_at"),
                new Column("index_descriptors", "created_at"),
                new Column("index_descriptors", "updated_at"),
                new Column("index_generation_manifests", "published_at"),
                new Column("index_generation_builds", "started_at"),
                new Column("index_generation_builds", "updated_at"),
                new Column("artifact_build_jobs", "publication_lease_expires_at"),
                new Column("artifact_build_jobs", "created_at"),
                new Column("artifact_build_jobs", "updated_at"),
                new Column("artifact_build_jobs", "publication_expected_updated_at"),
                new Column("artifact_build_jobs", "published_at"),
                new Column("artifact_build_pages", "lease_expires_at"),
                new Column("artifact_build_pages", "next_available_at"),
                new Column("artifact_build_pages", "created_at"),
                new Column("artifact_build_pages", "updated_at"),
                new Column("execution_anchor", "acquired_at"),
                new Column("execution_anchor", "expires_at"),
                new Column("generated_value_artifacts", "created_at"),
                new Column("generated_value_artifacts", "updated_at"),
                new Column("generated_value_generation_manifests", "published_at"),
                new Column("semantic_view_versions", "created_at"),
                new Column("semantic_view_active_versions", "published_at"),
                new Column("evaluation_dataset_versions", "created_at"),
                new Column("evaluation_runs", "started_at"),
                new Column("evaluation_runs", "completed_at"),
                new Column("evaluation_release_decisions", "decided_at"));

        try (Connection connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT data_type,datetime_precision FROM information_schema.columns "
                                + "WHERE table_schema=current_schema() "
                                + "AND table_name=? AND column_name=?")) {
            for (Column column : columns) {
                statement.setString(1, column.table());
                statement.setString(2, column.name());
                try (var result = statement.executeQuery()) {
                    assertThat(result.next())
                            .as("%s.%s exists", column.table(), column.name())
                            .isTrue();
                    assertThat(result.getString("data_type"))
                            .as("%s.%s type", column.table(), column.name())
                            .isEqualTo("timestamp with time zone");
                    assertThat(result.getInt("datetime_precision"))
                            .as("%s.%s precision", column.table(), column.name())
                            .isEqualTo(6);
                }
            }
        }
    }

    private record Column(String table, String name) {}
}
