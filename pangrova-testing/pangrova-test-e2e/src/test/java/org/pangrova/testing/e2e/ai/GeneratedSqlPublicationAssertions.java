package org.pangrova.testing.e2e.ai;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads only terminal metadata produced by the actual SQL/queue/Native path; never submits work. */
final class GeneratedSqlPublicationAssertions {
    private GeneratedSqlPublicationAssertions() {}

    static void requirePublished(DataSource dataSource, String table) throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT g.active_generation, g.active_source_snapshot, m.manifest_sha256,
                       m.object_count, m.row_count, m.total_bytes, j.state, j.expected_pages,
                       j.completed_pages, e.commit_fingerprint, e.processed
                FROM generated_value_artifacts g
                JOIN generated_value_generation_manifests m
                  ON m.tenant_id=g.tenant_id AND m.artifact_id=g.artifact_id
                 AND m.generation=g.active_generation AND m.source_snapshot=g.active_source_snapshot
                JOIN artifact_build_jobs j
                  ON j.tenant_id=m.tenant_id AND j.artifact_id=m.artifact_id
                 AND j.staging_generation=m.generation AND j.source_snapshot=m.source_snapshot
                JOIN index_commit_log e
                  ON e.tenant_id=g.tenant_id AND e.catalog_name=g.catalog_name
                 AND e.schema_name=g.schema_name AND e.table_name=g.table_name
                 AND e.snapshot_id=m.source_snapshot
                WHERE g.catalog_name='default' AND g.schema_name='default'
                  AND g.table_name=? AND g.target_column='embedding' AND m.state='ACTIVE'
                """)) {
            statement.setString(1, table);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).as("Native generation must be published from the authentic commit event").isTrue();
                assertThat(rows.getString(1)).isNotBlank();
                assertThat(rows.getString(2)).isNotBlank();
                assertThat(rows.getString(3)).matches("[0-9a-f]{64}");
                assertThat(rows.getLong(4)).isGreaterThanOrEqualTo(2);
                assertThat(rows.getLong(5)).isPositive();
                assertThat(rows.getLong(6)).isPositive();
                assertThat(rows.getString(7)).isEqualTo("PUBLISHED");
                assertThat(rows.getInt(8)).isPositive();
                assertThat(rows.getInt(9)).isEqualTo(rows.getInt(8));
                assertThat(rows.getString(10)).matches("[0-9a-f]{64}");
                assertThat(rows.getBoolean(11)).isTrue();
                assertThat(rows.next()).as("Exactly one generation/event identity for this write").isFalse();
            }
        }
        requireNativeOutputs(dataSource, table);
    }

    private static void requireNativeOutputs(DataSource dataSource, String table) throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT o.object_role,o.size_bytes,o.row_count,o.checksum_sha256,
                       p.report_bytes,p.report_sha256,p.execution_owner_token,
                       p.execution_owner_id=j.publication_execution_owner_id
                         AND p.execution_owner_incarnation=j.publication_execution_owner_incarnation
                         AND p.execution_owner_token=j.publication_execution_owner_token AS exact_owner
                FROM generated_value_artifacts g
                JOIN artifact_build_jobs j ON j.tenant_id=g.tenant_id AND j.artifact_id=g.artifact_id
                  AND j.staging_generation=g.active_generation AND j.state='PUBLISHED'
                JOIN artifact_build_pages p ON p.tenant_id=j.tenant_id AND p.job_id=j.job_id
                  AND p.state='SUCCEEDED' AND p.page_kind='SOURCE_MAP'
                JOIN artifact_build_outputs o ON o.tenant_id=p.tenant_id AND o.job_id=p.job_id AND o.page_id=p.page_id
                WHERE g.catalog_name='default' AND g.schema_name='default' AND g.table_name=?
                """)) {
            statement.setString(1, table);
            var roles = new java.util.ArrayList<String>();
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    roles.add(rows.getString(1));
                    assertThat(rows.getLong(2)).isPositive();
                    assertThat(rows.getLong(3)).isEqualTo(1);
                    assertThat(rows.getString(4)).matches("[0-9a-f]{64}");
                    assertThat(rows.getBytes(5)).isNotEmpty();
                    assertThat(rows.getString(6)).matches("[0-9a-f]{64}");
                    assertThat(rows.getLong(7)).isPositive();
                    assertThat(rows.getBoolean(8)).as("Page and publication preserve the actual fenced owner").isTrue();
                }
            }
            assertThat(roles).containsExactlyInAnyOrder("GENERATED_VALUES", "ROW_LOCATORS");
        }
    }
}
